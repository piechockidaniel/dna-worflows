"""Persistence contracts (plan item P1.11).

Providers implement these protocols (Override axis). The only hard atomicity requirement is
:meth:`ExecutionStore.commit`: execution state, outbox intents, journal events and inbox keys of one
checkpoint are written all-or-nothing, guarded by optimistic concurrency on ``revision``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from DnA Workflows.runtime.models import (Bookmark, Checkpoint, ExecutionStatus, IntentStatus, JournalEvent,
                                     OutboxIntent, WorkflowDefinition, WorkflowExecution)


class ConcurrencyConflict(RuntimeError):
    """The execution was modified by someone else since it was loaded (fencing)."""


class DuplicateDelivery(RuntimeError):
    """An inbox key of the checkpoint was already consumed – the stimulus is a duplicate."""


@runtime_checkable
class ExecutionStore(Protocol):
    async def commit(self, checkpoint: Checkpoint) -> int: ...

    async def get(self, execution_id: str) -> WorkflowExecution | None: ...

    async def list(self, *, status: ExecutionStatus | None = None, definition_id: str | None = None,
                   correlation_id: str | None = None, limit: int = 100,
                   offset: int = 0) -> list[WorkflowExecution]: ...

    async def due(self, now: datetime, limit: int = 100) -> list[str]: ...

    async def find_bookmarks(self, stimulus_type: str, keys: Sequence[str],
                             correlation_id: str | None) -> list[tuple[str, Bookmark]]: ...

    async def find_parked(self, *, dependency: str | None = None, hold_id: str | None = None,
                          ) -> list[tuple[str, str]]: ...

    async def count_by_status(self) -> dict[str, int]: ...


@runtime_checkable
class OutboxStore(Protocol):
    async def due(self, now: datetime, limit: int = 100) -> list[OutboxIntent]: ...

    async def get(self, intent_id: str) -> OutboxIntent | None: ...

    async def update(self, intent: OutboxIntent) -> None: ...

    async def list(self, *, status: IntentStatus | None = None, dependency: str | None = None,
                   hold_id: str | None = None, limit: int = 100) -> list[OutboxIntent]: ...

    async def count_by_status(self) -> dict[str, int]: ...


@runtime_checkable
class JournalStore(Protocol):
    async def append(self, events: Sequence[JournalEvent]) -> None: ...

    async def read(self, after_seq: int, limit: int = 500) -> list[JournalEvent]: ...

    async def query(self, *, execution_id: str | None = None, correlation_id: str | None = None,
                    trace_id: str | None = None, kinds: Sequence[str] | None = None,
                    limit: int = 1000) -> list[JournalEvent]: ...


@runtime_checkable
class DefinitionStore(Protocol):
    async def save(self, definition: WorkflowDefinition) -> None: ...

    async def get(self, definition_id: str, version: int | None = None) -> WorkflowDefinition | None: ...

    async def list(self) -> list[WorkflowDefinition]: ...


@runtime_checkable
class KeyValueStore(Protocol):
    """Small document store for control-plane records (holds, proposals, cursors).

    Deliberately generic: control-plane features own their own record shapes and must not force
    a schema change on every persistence provider.
    """

    async def put(self, collection: str, key: str, value: dict[str, Any]) -> None: ...

    async def get(self, collection: str, key: str) -> dict[str, Any] | None: ...

    async def delete(self, collection: str, key: str) -> None: ...

    async def list(self, collection: str) -> list[dict[str, Any]]: ...
