"""SQLite persistence provider (plan item P1.11) – the first durable provider.

* One file, WAL mode, ``BEGIN IMMEDIATE`` transactions and a busy timeout, so several processes
  ("nodes") on one machine can share it safely. Optimistic concurrency on ``revision`` fences
  concurrent writers of the same execution.
* State is stored as JSON documents plus a few *index tables* (bookmarks, parked work, wake time)
  that are rewritten inside the same transaction as the checkpoint.
* Blocking sqlite calls run in a worker thread so the event loop stays responsive.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.persistence.contracts import (ConcurrencyConflict, DefinitionStore, DuplicateDelivery,
                                            ExecutionStore, JournalStore, KeyValueStore, OutboxStore)
from DnA Workflows.runtime.models import (Bookmark, Checkpoint, ExecutionStatus, IntentStatus, JournalEvent,
                                     OutboxIntent, WorkflowDefinition, WorkflowExecution, WorkState)

T = TypeVar("T")

SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, status TEXT NOT NULL, correlation_id TEXT,
    wake_at TEXT, created_at TEXT, revision INTEGER NOT NULL, data TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_exec_wake ON executions(wake_at) WHERE wake_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_exec_status ON executions(status);
CREATE INDEX IF NOT EXISTS ix_exec_corr ON executions(correlation_id);
CREATE TABLE IF NOT EXISTS bookmarks (
    id TEXT PRIMARY KEY, execution_id TEXT NOT NULL, stimulus_type TEXT NOT NULL, key TEXT NOT NULL,
    correlation_id TEXT, data TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_bm_lookup ON bookmarks(stimulus_type, key);
CREATE INDEX IF NOT EXISTS ix_bm_exec ON bookmarks(execution_id);
CREATE TABLE IF NOT EXISTS parked (
    execution_id TEXT NOT NULL, work_item_id TEXT NOT NULL, dependency TEXT, hold_id TEXT,
    PRIMARY KEY (execution_id, work_item_id));
CREATE INDEX IF NOT EXISTS ix_parked_dep ON parked(dependency);
CREATE INDEX IF NOT EXISTS ix_parked_hold ON parked(hold_id);
CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY, status TEXT NOT NULL, next_attempt_at TEXT, dependency TEXT, hold_id TEXT,
    created_at TEXT, data TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_intent_due ON intents(status, next_attempt_at);
CREATE TABLE IF NOT EXISTS journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL, kind TEXT NOT NULL, execution_id TEXT,
    correlation_id TEXT, trace_id TEXT, data TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_journal_exec ON journal(execution_id);
CREATE INDEX IF NOT EXISTS ix_journal_corr ON journal(correlation_id);
CREATE INDEX IF NOT EXISTS ix_journal_trace ON journal(trace_id);
CREATE TABLE IF NOT EXISTS definitions (
    id TEXT NOT NULL, version INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (id, version));
CREATE TABLE IF NOT EXISTS inbox (key TEXT PRIMARY KEY, created_at TEXT);
CREATE TABLE IF NOT EXISTS kv (
    collection TEXT NOT NULL, key TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY (collection, key));
"""


def _ts(value: datetime | None) -> str | None:
    """Sortable UTC timestamp text."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")


def _dump(model: Any) -> str:
    return json.dumps(model.model_dump(mode="json"))


class SqliteDatabase:
    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5000) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None,
                                     timeout=busy_timeout_ms / 1000)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            if self.path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
            self._conn.executescript(SCHEMA)

    async def run(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        def call() -> T:
            with self._lock:
                return fn(self._conn)
        return await asyncio.to_thread(call)

    async def transaction(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        def call(conn: sqlite3.Connection) -> T:
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = fn(conn)
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
            return result
        return await self.run(call)

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class SqliteExecutionStore(ExecutionStore):
    def __init__(self, db: SqliteDatabase) -> None:
        self._db = db

    async def commit(self, checkpoint: Checkpoint) -> int:
        ex = checkpoint.execution
        new_revision = checkpoint.expected_revision + 1
        stored = ex.model_copy(update={"revision": new_revision})
        data = _dump(stored)
        intents = [(i.id, i.status.value, _ts(i.next_attempt_at), i.dependency, i.hold_id,
                    _ts(i.created_at), _dump(i)) for i in checkpoint.intents]
        events = [(e.id, e.kind, e.execution_id, e.correlation_id, e.trace_id, _dump(e))
                  for e in checkpoint.journal]
        live = not ex.status.is_terminal  # terminal executions are never resumed: no index rows
        bookmarks = [(b.id, ex.id, b.stimulus_type, b.key, b.correlation_id, _dump(b))
                     for b in ex.bookmarks] if live else []
        parked = [(ex.id, w.id, w.dependency, w.hold_id)
                  for w in ex.work if w.state is WorkState.PARKED] if live else []
        row = (ex.id, ex.definition_id, ex.status.value, ex.correlation_id, _ts(ex.wake_at()),
               _ts(ex.created_at), new_revision, data)

        def tx(conn: sqlite3.Connection) -> int:
            current = conn.execute("SELECT revision FROM executions WHERE id=?", (ex.id,)).fetchone()
            current_rev = current["revision"] if current else 0
            if current_rev != checkpoint.expected_revision:
                raise ConcurrencyConflict(
                    f"Execution {ex.id}: expected revision {checkpoint.expected_revision}, found {current_rev}")
            for key in checkpoint.inbox_keys:
                try:
                    conn.execute("INSERT INTO inbox(key, created_at) VALUES (?, ?)",
                                 (key, _ts(ex.updated_at)))
                except sqlite3.IntegrityError:
                    raise DuplicateDelivery(f"Already processed: {key}") from None
            conn.execute(
                "INSERT INTO executions(id, definition_id, status, correlation_id, wake_at, created_at,"
                " revision, data) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET"
                " status=excluded.status, correlation_id=excluded.correlation_id,"
                " wake_at=excluded.wake_at, revision=excluded.revision, data=excluded.data", row)
            conn.execute("DELETE FROM bookmarks WHERE execution_id=?", (ex.id,))
            conn.executemany("INSERT INTO bookmarks VALUES (?,?,?,?,?,?)", bookmarks)
            conn.execute("DELETE FROM parked WHERE execution_id=?", (ex.id,))
            conn.executemany("INSERT INTO parked VALUES (?,?,?,?)", parked)
            conn.executemany("INSERT INTO intents VALUES (?,?,?,?,?,?,?)", intents)
            conn.executemany("INSERT INTO journal(id, kind, execution_id, correlation_id, trace_id, data)"
                             " VALUES (?,?,?,?,?,?)", events)
            return new_revision

        result = await self._db.transaction(tx)
        ex.revision = result
        return result

    async def get(self, execution_id: str) -> WorkflowExecution | None:
        row = await self._db.run(lambda c: c.execute(
            "SELECT data FROM executions WHERE id=?", (execution_id,)).fetchone())
        return WorkflowExecution.model_validate_json(row["data"]) if row else None

    async def list(self, *, status: ExecutionStatus | None = None, definition_id: str | None = None,
                   correlation_id: str | None = None, limit: int = 100,
                   offset: int = 0) -> list[WorkflowExecution]:
        clauses, args = [], []
        for col, val in (("status", status.value if status else None), ("definition_id", definition_id),
                         ("correlation_id", correlation_id)):
            if val is not None:
                clauses.append(f"{col}=?")
                args.append(val)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT data FROM executions {where} ORDER BY created_at, id LIMIT ? OFFSET ?"
        rows = await self._db.run(lambda c: c.execute(sql, (*args, limit, offset)).fetchall())
        return [WorkflowExecution.model_validate_json(r["data"]) for r in rows]

    async def due(self, now: datetime, limit: int = 100) -> list[str]:
        rows = await self._db.run(lambda c: c.execute(
            "SELECT id FROM executions WHERE wake_at IS NOT NULL AND wake_at <= ? ORDER BY wake_at LIMIT ?",
            (_ts(now), limit)).fetchall())
        return [r["id"] for r in rows]

    async def find_bookmarks(self, stimulus_type: str, keys: Sequence[str],
                             correlation_id: str | None) -> list[tuple[str, Bookmark]]:
        if not keys:
            return []
        marks = ",".join("?" for _ in keys)
        sql = (f"SELECT execution_id, data FROM bookmarks WHERE stimulus_type=? AND key IN ({marks})"
               " AND (correlation_id IS NULL OR correlation_id=?) ORDER BY rowid")
        rows = await self._db.run(lambda c: c.execute(sql, (stimulus_type, *keys, correlation_id)).fetchall())
        return [(r["execution_id"], Bookmark.model_validate_json(r["data"])) for r in rows]

    async def find_parked(self, *, dependency: str | None = None,
                          hold_id: str | None = None) -> list[tuple[str, str]]:
        clauses, args = [], []
        if dependency is not None:
            clauses.append("dependency=?")
            args.append(dependency)
        if hold_id is not None:
            clauses.append("hold_id=?")
            args.append(hold_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = await self._db.run(lambda c: c.execute(
            f"SELECT execution_id, work_item_id FROM parked {where} ORDER BY rowid", args).fetchall())
        return [(r["execution_id"], r["work_item_id"]) for r in rows]

    async def count_by_status(self) -> dict[str, int]:
        rows = await self._db.run(lambda c: c.execute(
            "SELECT status, COUNT(*) AS n FROM executions GROUP BY status").fetchall())
        return {r["status"]: r["n"] for r in rows}


class SqliteOutboxStore(OutboxStore):
    def __init__(self, db: SqliteDatabase) -> None:
        self._db = db

    async def due(self, now: datetime, limit: int = 100) -> list[OutboxIntent]:
        rows = await self._db.run(lambda c: c.execute(
            "SELECT data FROM intents WHERE status='pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)"
            " ORDER BY COALESCE(next_attempt_at, created_at), id LIMIT ?", (_ts(now), limit)).fetchall())
        return [OutboxIntent.model_validate_json(r["data"]) for r in rows]

    async def get(self, intent_id: str) -> OutboxIntent | None:
        row = await self._db.run(lambda c: c.execute(
            "SELECT data FROM intents WHERE id=?", (intent_id,)).fetchone())
        return OutboxIntent.model_validate_json(row["data"]) if row else None

    async def update(self, intent: OutboxIntent) -> None:
        args = (intent.status.value, _ts(intent.next_attempt_at), intent.dependency, intent.hold_id,
                _dump(intent), intent.id)
        await self._db.run(lambda c: c.execute(
            "UPDATE intents SET status=?, next_attempt_at=?, dependency=?, hold_id=?, data=? WHERE id=?", args))

    async def list(self, *, status: IntentStatus | None = None, dependency: str | None = None,
                   hold_id: str | None = None, limit: int = 100) -> list[OutboxIntent]:
        clauses, args = [], []
        for col, val in (("status", status.value if status else None), ("dependency", dependency),
                         ("hold_id", hold_id)):
            if val is not None:
                clauses.append(f"{col}=?")
                args.append(val)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = await self._db.run(lambda c: c.execute(
            f"SELECT data FROM intents {where} ORDER BY created_at, id LIMIT ?", (*args, limit)).fetchall())
        return [OutboxIntent.model_validate_json(r["data"]) for r in rows]

    async def count_by_status(self) -> dict[str, int]:
        rows = await self._db.run(lambda c: c.execute(
            "SELECT status, COUNT(*) AS n FROM intents GROUP BY status").fetchall())
        return {r["status"]: r["n"] for r in rows}


class SqliteJournalStore(JournalStore):
    def __init__(self, db: SqliteDatabase) -> None:
        self._db = db

    async def append(self, events: Sequence[JournalEvent]) -> None:
        rows = [(e.id, e.kind, e.execution_id, e.correlation_id, e.trace_id, _dump(e)) for e in events]

        def tx(conn: sqlite3.Connection) -> None:
            conn.executemany("INSERT INTO journal(id, kind, execution_id, correlation_id, trace_id, data)"
                             " VALUES (?,?,?,?,?,?)", rows)
        await self._db.transaction(tx)

    @staticmethod
    def _event(row: sqlite3.Row) -> JournalEvent:
        e = JournalEvent.model_validate_json(row["data"])
        e.seq = row["seq"]
        return e

    async def read(self, after_seq: int, limit: int = 500) -> list[JournalEvent]:
        rows = await self._db.run(lambda c: c.execute(
            "SELECT seq, data FROM journal WHERE seq > ? ORDER BY seq LIMIT ?", (after_seq, limit)).fetchall())
        return [self._event(r) for r in rows]

    async def query(self, *, execution_id: str | None = None, correlation_id: str | None = None,
                    trace_id: str | None = None, kinds: Sequence[str] | None = None,
                    limit: int = 1000) -> list[JournalEvent]:
        clauses, args = [], []
        for col, val in (("execution_id", execution_id), ("correlation_id", correlation_id),
                         ("trace_id", trace_id)):
            if val is not None:
                clauses.append(f"{col}=?")
                args.append(val)
        if kinds:
            clauses.append(f"kind IN ({','.join('?' for _ in kinds)})")
            args.extend(kinds)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = await self._db.run(lambda c: c.execute(
            f"SELECT seq, data FROM journal {where} ORDER BY seq LIMIT ?", (*args, limit)).fetchall())
        return [self._event(r) for r in rows]


class SqliteDefinitionStore(DefinitionStore):
    def __init__(self, db: SqliteDatabase) -> None:
        self._db = db

    async def save(self, definition: WorkflowDefinition) -> None:
        args = (definition.id, definition.version, _dump(definition))
        await self._db.run(lambda c: c.execute(
            "INSERT INTO definitions VALUES (?,?,?) ON CONFLICT(id, version) DO UPDATE SET data=excluded.data",
            args))

    async def get(self, definition_id: str, version: int | None = None) -> WorkflowDefinition | None:
        if version is None:
            row = await self._db.run(lambda c: c.execute(
                "SELECT data FROM definitions WHERE id=? ORDER BY version DESC LIMIT 1",
                (definition_id,)).fetchone())
        else:
            row = await self._db.run(lambda c: c.execute(
                "SELECT data FROM definitions WHERE id=? AND version=?", (definition_id, version)).fetchone())
        return WorkflowDefinition.model_validate_json(row["data"]) if row else None

    async def list(self) -> list[WorkflowDefinition]:
        rows = await self._db.run(lambda c: c.execute(
            "SELECT data FROM definitions ORDER BY id, version").fetchall())
        return [WorkflowDefinition.model_validate_json(r["data"]) for r in rows]


class SqliteKeyValueStore(KeyValueStore):
    def __init__(self, db: SqliteDatabase) -> None:
        self._db = db

    async def put(self, collection: str, key: str, value: dict[str, Any]) -> None:
        args = (collection, key, json.dumps(value, default=str))
        await self._db.run(lambda c: c.execute(
            "INSERT INTO kv VALUES (?,?,?) ON CONFLICT(collection, key) DO UPDATE SET data=excluded.data", args))

    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        row = await self._db.run(lambda c: c.execute(
            "SELECT data FROM kv WHERE collection=? AND key=?", (collection, key)).fetchone())
        return json.loads(row["data"]) if row else None

    async def delete(self, collection: str, key: str) -> None:
        await self._db.run(lambda c: c.execute("DELETE FROM kv WHERE collection=? AND key=?", (collection, key)))

    async def list(self, collection: str) -> list[dict[str, Any]]:
        rows = await self._db.run(lambda c: c.execute(
            "SELECT data FROM kv WHERE collection=? ORDER BY key", (collection,)).fetchall())
        return [json.loads(r["data"]) for r in rows]


class SqlitePersistenceOptions(FeatureOptions):
    path: str = "DnA Workflows.db"
    busy_timeout_ms: int = 5000


class SqlitePersistenceFeature(Feature):
    """Durable single-file persistence (WAL). Several local processes may share the file."""

    name = "persistence.sqlite"
    Options = SqlitePersistenceOptions

    def configure(self, services: ServiceCollection) -> None:
        o: SqlitePersistenceOptions = self.options  # type: ignore[assignment]
        services.replace(SqliteDatabase, lambda p: SqliteDatabase(o.path, busy_timeout_ms=o.busy_timeout_ms))
        services.replace(ExecutionStore, lambda p: SqliteExecutionStore(p.get(SqliteDatabase)))
        services.replace(OutboxStore, lambda p: SqliteOutboxStore(p.get(SqliteDatabase)))
        services.replace(JournalStore, lambda p: SqliteJournalStore(p.get(SqliteDatabase)))
        services.replace(DefinitionStore, lambda p: SqliteDefinitionStore(p.get(SqliteDatabase)))
        services.replace(KeyValueStore, lambda p: SqliteKeyValueStore(p.get(SqliteDatabase)))
