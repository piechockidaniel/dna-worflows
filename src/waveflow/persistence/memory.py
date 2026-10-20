"""In-memory persistence provider (plan item P1.11). For tests, demos and single-process dev.

State is round-tripped through JSON on every write/read so it behaves like the durable providers:
callers never share mutable objects with the store, and non-serialisable state fails loudly here
exactly as it would in production.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature
from DnA Workflows.persistence.contracts import (ConcurrencyConflict, DefinitionStore, DuplicateDelivery,
                                            ExecutionStore, JournalStore, KeyValueStore, OutboxStore)
from DnA Workflows.runtime.models import (Bookmark, Checkpoint, ExecutionStatus, IntentStatus, JournalEvent,
                                     OutboxIntent, WorkflowDefinition, WorkflowExecution, WorkState)


_MIN = datetime.min.replace(tzinfo=UTC)


def _freeze(model: Any) -> str:
    return json.dumps(model.model_dump(mode="json"))


def bookmark_matches(bookmark: Bookmark, stimulus_type: str, keys: Sequence[str],
                     correlation_id: str | None) -> bool:
    if bookmark.stimulus_type != stimulus_type or bookmark.key not in keys:
        return False
    return bookmark.correlation_id is None or bookmark.correlation_id == correlation_id


def journal_matches(e: JournalEvent, execution_id: str | None, correlation_id: str | None,
                    trace_id: str | None, kinds: Sequence[str] | None) -> bool:
    return ((execution_id is None or e.execution_id == execution_id)
            and (correlation_id is None or e.correlation_id == correlation_id)
            and (trace_id is None or e.trace_id == trace_id)
            and (not kinds or e.kind in kinds))


class InMemoryState:
    """Shared state behind the in-memory stores (one 'database')."""

    def __init__(self) -> None:
        self.executions: dict[str, str] = {}
        self.intents: dict[str, str] = {}
        self.journal: list[str] = []
        self.definitions: dict[tuple[str, int], str] = {}
        self.inbox: set[str] = set()
        self.kv: dict[tuple[str, str], str] = {}
        self.seq = 0
        self.lock = asyncio.Lock()
        # secondary indexes over executions (P15.2): scans touch only matching documents
        self.meta: dict[str, tuple[str, str, str | None, datetime | None, datetime | None]] = {}
        self.bookmarks: dict[tuple[str, str], dict[str, tuple[str, str | None, str]]] = {}
        self.bookmarks_of: dict[str, list[tuple[str, str, str]]] = {}
        self.parked: dict[str, list[tuple[str, str | None, str | None]]] = {}
        self.by_correlation: dict[str, set[str]] = {}

    def index(self, ex: WorkflowExecution) -> None:
        """(Re)index one execution: status/definition/correlation/wake time, bookmarks, parked work."""
        old = self.meta.get(ex.id)
        if old is not None and old[2] is not None:
            self.by_correlation.get(old[2], set()).discard(ex.id)
        for stimulus_type, key, bm_id in self.bookmarks_of.pop(ex.id, []):
            self.bookmarks.get((stimulus_type, key), {}).pop(bm_id, None)
        live = not ex.status.is_terminal
        self.meta[ex.id] = (ex.status.value, ex.definition_id, ex.correlation_id, ex.wake_at(), ex.created_at)
        if ex.correlation_id is not None:
            self.by_correlation.setdefault(ex.correlation_id, set()).add(ex.id)
        if live:
            entries = []
            for b in ex.bookmarks:
                self.bookmarks.setdefault((b.stimulus_type, b.key), {})[b.id] = (
                    ex.id, b.correlation_id, b.model_dump_json())
                entries.append((b.stimulus_type, b.key, b.id))
            self.bookmarks_of[ex.id] = entries
            parked = [(w.id, w.dependency, w.hold_id) for w in ex.work if w.state is WorkState.PARKED]
            if parked:
                self.parked[ex.id] = parked
            else:
                self.parked.pop(ex.id, None)
        else:
            self.parked.pop(ex.id, None)

    def append_journal(self, events: Sequence[JournalEvent]) -> None:
        for e in events:
            self.seq += 1
            self.journal.append(_freeze(e.model_copy(update={"seq": self.seq})))


class InMemoryExecutionStore(ExecutionStore):
    def __init__(self, state: InMemoryState) -> None:
        self._s = state

    def _all(self) -> list[WorkflowExecution]:
        return [WorkflowExecution.model_validate_json(r) for r in self._s.executions.values()]

    async def commit(self, checkpoint: Checkpoint) -> int:
        s = self._s
        async with s.lock:
            ex = checkpoint.execution
            existing = s.executions.get(ex.id)
            current = WorkflowExecution.model_validate_json(existing).revision if existing else 0
            if current != checkpoint.expected_revision:
                raise ConcurrencyConflict(
                    f"Execution {ex.id}: expected revision {checkpoint.expected_revision}, found {current}")
            duplicates = [k for k in checkpoint.inbox_keys if k in s.inbox]
            if duplicates:
                raise DuplicateDelivery(f"Already processed: {duplicates}")
            # serialise everything first: a failure leaves the store untouched (atomicity)
            frozen_ex = _freeze(ex.model_copy(update={"revision": current + 1}))
            frozen_intents = [(i.id, _freeze(i)) for i in checkpoint.intents]
            s.executions[ex.id] = frozen_ex
            s.index(ex)
            s.intents.update(frozen_intents)
            s.append_journal(checkpoint.journal)
            s.inbox.update(checkpoint.inbox_keys)
            ex.revision = current + 1
            return ex.revision

    async def get(self, execution_id: str) -> WorkflowExecution | None:
        raw = self._s.executions.get(execution_id)
        return WorkflowExecution.model_validate_json(raw) if raw else None

    async def list(self, *, status: ExecutionStatus | None = None, definition_id: str | None = None,
                   correlation_id: str | None = None, limit: int = 100,
                   offset: int = 0) -> list[WorkflowExecution]:
        s = self._s
        ids = s.by_correlation.get(correlation_id, set()) if correlation_id is not None else s.meta.keys()
        matching = [(s.meta[i][4] or _MIN, i) for i in ids
                    if (status is None or s.meta[i][0] == status.value)
                    and (definition_id is None or s.meta[i][1] == definition_id)]
        matching.sort()
        return [WorkflowExecution.model_validate_json(s.executions[i]) for _, i in matching[offset:offset + limit]]

    async def due(self, now: datetime, limit: int = 100) -> list[str]:
        due = sorted((m[3], i) for i, m in self._s.meta.items() if m[3] is not None and m[3] <= now)
        return [i for _, i in due[:limit]]

    async def find_bookmarks(self, stimulus_type: str, keys: Sequence[str],
                             correlation_id: str | None) -> list[tuple[str, Bookmark]]:
        found = []
        for key in keys:
            for execution_id, bm_corr, raw in self._s.bookmarks.get((stimulus_type, key), {}).values():
                if bm_corr is None or bm_corr == correlation_id:
                    found.append((execution_id, Bookmark.model_validate_json(raw)))
        return found

    async def find_parked(self, *, dependency: str | None = None,
                          hold_id: str | None = None) -> list[tuple[str, str]]:
        return [(execution_id, wi) for execution_id, items in self._s.parked.items() for wi, dep, hold in items
                if (dependency is None or dep == dependency) and (hold_id is None or hold == hold_id)]

    async def count_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for status, *_ in self._s.meta.values():
            counts[status] = counts.get(status, 0) + 1
        return counts


class InMemoryOutboxStore(OutboxStore):
    def __init__(self, state: InMemoryState) -> None:
        self._s = state

    def _all(self) -> list[OutboxIntent]:
        return [OutboxIntent.model_validate_json(r) for r in self._s.intents.values()]

    async def due(self, now: datetime, limit: int = 100) -> list[OutboxIntent]:
        items = [i for i in self._all() if i.status is IntentStatus.PENDING
                 and (i.next_attempt_at is None or i.next_attempt_at <= now)]
        items.sort(key=lambda i: (i.next_attempt_at or i.created_at or now, i.id))
        return items[:limit]

    async def get(self, intent_id: str) -> OutboxIntent | None:
        raw = self._s.intents.get(intent_id)
        return OutboxIntent.model_validate_json(raw) if raw else None

    async def update(self, intent: OutboxIntent) -> None:
        self._s.intents[intent.id] = _freeze(intent)

    async def list(self, *, status: IntentStatus | None = None, dependency: str | None = None,
                   hold_id: str | None = None, limit: int = 100) -> list[OutboxIntent]:
        items = [i for i in self._all() if (status is None or i.status == status)
                 and (dependency is None or i.dependency == dependency)
                 and (hold_id is None or i.hold_id == hold_id)]
        items.sort(key=lambda i: (i.created_at or datetime.min.replace(tzinfo=UTC), i.id))
        return items[:limit]

    async def count_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for i in self._all():
            counts[i.status.value] = counts.get(i.status.value, 0) + 1
        return counts


class InMemoryJournalStore(JournalStore):
    def __init__(self, state: InMemoryState) -> None:
        self._s = state

    async def append(self, events: Sequence[JournalEvent]) -> None:
        async with self._s.lock:
            self._s.append_journal(events)

    async def read(self, after_seq: int, limit: int = 500) -> list[JournalEvent]:
        events = (JournalEvent.model_validate_json(r) for r in self._s.journal)
        return [e for e in events if e.seq > after_seq][:limit]

    async def query(self, *, execution_id: str | None = None, correlation_id: str | None = None,
                    trace_id: str | None = None, kinds: Sequence[str] | None = None,
                    limit: int = 1000) -> list[JournalEvent]:
        events = (JournalEvent.model_validate_json(r) for r in self._s.journal)
        return [e for e in events
                if journal_matches(e, execution_id, correlation_id, trace_id, kinds)][:limit]


class InMemoryDefinitionStore(DefinitionStore):
    def __init__(self, state: InMemoryState) -> None:
        self._s = state

    async def save(self, definition: WorkflowDefinition) -> None:
        self._s.definitions[(definition.id, definition.version)] = _freeze(definition)

    async def get(self, definition_id: str, version: int | None = None) -> WorkflowDefinition | None:
        if version is None:
            versions = [v for (i, v) in self._s.definitions if i == definition_id]
            if not versions:
                return None
            version = max(versions)
        raw = self._s.definitions.get((definition_id, version))
        return WorkflowDefinition.model_validate_json(raw) if raw else None

    async def list(self) -> list[WorkflowDefinition]:
        return [WorkflowDefinition.model_validate_json(r) for _, r in sorted(self._s.definitions.items())]


class InMemoryKeyValueStore(KeyValueStore):
    def __init__(self, state: InMemoryState) -> None:
        self._s = state

    async def put(self, collection: str, key: str, value: dict[str, Any]) -> None:
        self._s.kv[(collection, key)] = json.dumps(value, default=str)

    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        raw = self._s.kv.get((collection, key))
        return json.loads(raw) if raw else None

    async def delete(self, collection: str, key: str) -> None:
        self._s.kv.pop((collection, key), None)

    async def list(self, collection: str) -> list[dict[str, Any]]:
        return [json.loads(v) for (c, _), v in sorted(self._s.kv.items()) if c == collection]


class InMemoryPersistenceFeature(Feature):
    """Volatile persistence. Fine for tests and demos; state is lost on restart."""

    name = "persistence.memory"

    def configure(self, services: ServiceCollection) -> None:
        state = InMemoryState()
        services.replace(InMemoryState, lambda p: state)
        services.replace(ExecutionStore, lambda p: InMemoryExecutionStore(state))
        services.replace(OutboxStore, lambda p: InMemoryOutboxStore(state))
        services.replace(JournalStore, lambda p: InMemoryJournalStore(state))
        services.replace(DefinitionStore, lambda p: InMemoryDefinitionStore(state))
        services.replace(KeyValueStore, lambda p: InMemoryKeyValueStore(state))
