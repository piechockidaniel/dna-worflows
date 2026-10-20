"""PostgreSQL persistence provider (plan item P15.1, Q-006) – for clusters of DnA Workflows nodes.

* psycopg 3 (async) with a connection pool; every checkpoint is one transaction.
* Fencing: ``SELECT … FOR UPDATE`` on the execution row + revision check; a first insert that
  races another node fails on the primary key and is reported as a concurrency conflict.
* Inbox dedup keys are inserted in the same transaction (``ON CONFLICT DO NOTHING`` + row count).
* Documents are JSONB; index tables (bookmarks, parked work, wake time) make routing and timers
  cheap queries. ``schema`` isolates tenants/environments (one schema per installation, Q-009).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any

from psycopg import AsyncConnection, errors, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import CallbackService, HostedService
from DnA Workflows.persistence.contracts import (ConcurrencyConflict, DefinitionStore, DuplicateDelivery,
                                            ExecutionStore, JournalStore, KeyValueStore, OutboxStore)
from DnA Workflows.runtime.models import (Bookmark, Checkpoint, ExecutionStatus, IntentStatus, JournalEvent,
                                     OutboxIntent, WorkflowDefinition, WorkflowExecution, WorkState)

SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, status TEXT NOT NULL, correlation_id TEXT,
    wake_at TIMESTAMPTZ, created_at TIMESTAMPTZ, revision BIGINT NOT NULL, data JSONB NOT NULL);
CREATE INDEX IF NOT EXISTS ix_exec_wake ON executions(wake_at) WHERE wake_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_exec_status ON executions(status);
CREATE INDEX IF NOT EXISTS ix_exec_corr ON executions(correlation_id);
CREATE TABLE IF NOT EXISTS bookmarks (
    id TEXT PRIMARY KEY, execution_id TEXT NOT NULL, stimulus_type TEXT NOT NULL, key TEXT NOT NULL,
    correlation_id TEXT, data JSONB NOT NULL, seq BIGSERIAL);
CREATE INDEX IF NOT EXISTS ix_bm_lookup ON bookmarks(stimulus_type, key);
CREATE INDEX IF NOT EXISTS ix_bm_exec ON bookmarks(execution_id);
CREATE TABLE IF NOT EXISTS parked (
    execution_id TEXT NOT NULL, work_item_id TEXT NOT NULL, dependency TEXT, hold_id TEXT, seq BIGSERIAL,
    PRIMARY KEY (execution_id, work_item_id));
CREATE INDEX IF NOT EXISTS ix_parked_dep ON parked(dependency);
CREATE INDEX IF NOT EXISTS ix_parked_hold ON parked(hold_id);
CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY, status TEXT NOT NULL, next_attempt_at TIMESTAMPTZ, dependency TEXT, hold_id TEXT,
    created_at TIMESTAMPTZ, data JSONB NOT NULL);
CREATE INDEX IF NOT EXISTS ix_intent_due ON intents(status, next_attempt_at);
CREATE TABLE IF NOT EXISTS journal (
    seq BIGSERIAL PRIMARY KEY, id TEXT NOT NULL, kind TEXT NOT NULL, execution_id TEXT, correlation_id TEXT,
    trace_id TEXT, data JSONB NOT NULL);
CREATE INDEX IF NOT EXISTS ix_journal_exec ON journal(execution_id);
CREATE INDEX IF NOT EXISTS ix_journal_corr ON journal(correlation_id);
CREATE INDEX IF NOT EXISTS ix_journal_trace ON journal(trace_id);
CREATE TABLE IF NOT EXISTS definitions (
    id TEXT NOT NULL, version INTEGER NOT NULL, data JSONB NOT NULL, PRIMARY KEY (id, version));
CREATE TABLE IF NOT EXISTS inbox (key TEXT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS kv (
    collection TEXT NOT NULL, key TEXT NOT NULL, data JSONB NOT NULL, PRIMARY KEY (collection, key));
"""


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _json(model: Any) -> str:
    return json.dumps(model.model_dump(mode="json"))


class PostgresDatabase:
    def __init__(self, dsn: str, *, schema: str = "DnA Workflows", min_size: int = 1, max_size: int = 10) -> None:
        self.dsn = dsn
        self.schema = schema
        self._sizes = (min_size, max_size)
        self.pool = self._new_pool()
        self._ready = False
        self._lock = asyncio.Lock()

    def _new_pool(self) -> AsyncConnectionPool:
        return AsyncConnectionPool(self.dsn, min_size=self._sizes[0], max_size=self._sizes[1], open=False,
                                   kwargs={"row_factory": dict_row, "autocommit": False}, configure=self._configure)

    async def _configure(self, conn: AsyncConnection) -> None:
        await conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema)))
        await conn.commit()

    async def open(self) -> None:
        async with self._lock:
            if not self._ready:
                await self._open()

    async def _open(self) -> None:
        async with await AsyncConnection.connect(self.dsn, autocommit=True) as conn:
            await conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
            await conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema)))
            await conn.execute(SCHEMA)
        await self.pool.open()
        self._ready = True

    async def close(self) -> None:
        if self._ready:
            await self.pool.close()
            self.pool = self._new_pool()   # psycopg pools cannot be reopened; a restarted shell gets a new one
            self._ready = False

    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection]:
        """A pooled connection; the pool (and schema) is created on first use."""
        if not self._ready:
            await self.open()
        async with self.pool.connection() as conn:
            yield conn


class PgExecutionStore(ExecutionStore):
    def __init__(self, db: PostgresDatabase) -> None:
        self._db = db

    async def commit(self, checkpoint: Checkpoint) -> int:
        ex = checkpoint.execution
        new_revision = checkpoint.expected_revision + 1
        data = _json(ex.model_copy(update={"revision": new_revision}))
        live = not ex.status.is_terminal
        async with self._db.connection() as conn:
            try:
                async with conn.transaction():
                    row = await (await conn.execute("SELECT revision FROM executions WHERE id=%s FOR UPDATE",
                                                    (ex.id,))).fetchone()
                    current = row["revision"] if row else 0
                    if current != checkpoint.expected_revision:
                        raise ConcurrencyConflict(f"Execution {ex.id}: expected revision "
                                                  f"{checkpoint.expected_revision}, found {current}")
                    for key in checkpoint.inbox_keys:
                        cur = await conn.execute("INSERT INTO inbox(key) VALUES (%s) ON CONFLICT DO NOTHING", (key,))
                        if cur.rowcount == 0:
                            raise DuplicateDelivery(f"Already processed: {key}")
                    if row is None:
                        await conn.execute(
                            "INSERT INTO executions(id, definition_id, status, correlation_id, wake_at, created_at,"
                            " revision, data) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (ex.id, ex.definition_id, ex.status.value, ex.correlation_id, _utc(ex.wake_at()),
                             _utc(ex.created_at), new_revision, data))
                    else:
                        await conn.execute(
                            "UPDATE executions SET status=%s, correlation_id=%s, wake_at=%s, revision=%s, data=%s"
                            " WHERE id=%s", (ex.status.value, ex.correlation_id, _utc(ex.wake_at()), new_revision,
                                             data, ex.id))
                    await conn.execute("DELETE FROM bookmarks WHERE execution_id=%s", (ex.id,))
                    await conn.execute("DELETE FROM parked WHERE execution_id=%s", (ex.id,))
                    async with conn.cursor() as cur:
                        if live and ex.bookmarks:
                            await cur.executemany(
                                "INSERT INTO bookmarks(id, execution_id, stimulus_type, key, correlation_id, data)"
                                " VALUES (%s,%s,%s,%s,%s,%s)",
                                [(b.id, ex.id, b.stimulus_type, b.key, b.correlation_id, _json(b)) for b in ex.bookmarks])
                        parked = [(ex.id, w.id, w.dependency, w.hold_id) for w in ex.work
                                  if w.state is WorkState.PARKED] if live else []
                        if parked:
                            await cur.executemany("INSERT INTO parked(execution_id, work_item_id, dependency, hold_id)"
                                                  " VALUES (%s,%s,%s,%s)", parked)
                        if checkpoint.intents:
                            await cur.executemany(
                                "INSERT INTO intents VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                [(i.id, i.status.value, _utc(i.next_attempt_at), i.dependency, i.hold_id,
                                  _utc(i.created_at), _json(i)) for i in checkpoint.intents])
                        if checkpoint.journal:
                            await cur.executemany(
                                "INSERT INTO journal(id, kind, execution_id, correlation_id, trace_id, data)"
                                " VALUES (%s,%s,%s,%s,%s,%s)",
                                [(e.id, e.kind, e.execution_id, e.correlation_id, e.trace_id, _json(e))
                                 for e in checkpoint.journal])
            except errors.UniqueViolation as exc:   # a concurrent first insert of the same execution
                raise ConcurrencyConflict(f"Execution {ex.id} was created concurrently") from exc
        ex.revision = new_revision
        return new_revision

    async def get(self, execution_id: str) -> WorkflowExecution | None:
        async with self._db.connection() as conn:
            row = await (await conn.execute("SELECT data FROM executions WHERE id=%s", (execution_id,))).fetchone()
        return WorkflowExecution.model_validate(row["data"]) if row else None

    async def list(self, *, status: ExecutionStatus | None = None, definition_id: str | None = None,
                   correlation_id: str | None = None, limit: int = 100,
                   offset: int = 0) -> list[WorkflowExecution]:
        clauses, args = _filters((("status", status.value if status else None), ("definition_id", definition_id),
                                  ("correlation_id", correlation_id)))
        async with self._db.connection() as conn:
            rows = await (await conn.execute(f"SELECT data FROM executions {clauses} ORDER BY created_at, id"
                                             " LIMIT %s OFFSET %s", (*args, limit, offset))).fetchall()
        return [WorkflowExecution.model_validate(r["data"]) for r in rows]

    async def due(self, now: datetime, limit: int = 100) -> list[str]:
        async with self._db.connection() as conn:
            rows = await (await conn.execute("SELECT id FROM executions WHERE wake_at IS NOT NULL AND wake_at <= %s"
                                             " ORDER BY wake_at LIMIT %s", (_utc(now), limit))).fetchall()
        return [r["id"] for r in rows]

    async def find_bookmarks(self, stimulus_type: str, keys: Sequence[str],
                             correlation_id: str | None) -> list[tuple[str, Bookmark]]:
        if not keys:
            return []
        async with self._db.connection() as conn:
            rows = await (await conn.execute(
                "SELECT execution_id, data FROM bookmarks WHERE stimulus_type=%s AND key = ANY(%s)"
                " AND (correlation_id IS NULL OR correlation_id=%s) ORDER BY seq",
                (stimulus_type, list(keys), correlation_id))).fetchall()
        return [(r["execution_id"], Bookmark.model_validate(r["data"])) for r in rows]

    async def find_parked(self, *, dependency: str | None = None,
                          hold_id: str | None = None) -> list[tuple[str, str]]:
        clauses, args = _filters((("dependency", dependency), ("hold_id", hold_id)))
        async with self._db.connection() as conn:
            rows = await (await conn.execute(f"SELECT execution_id, work_item_id FROM parked {clauses} ORDER BY seq",
                                             args)).fetchall()
        return [(r["execution_id"], r["work_item_id"]) for r in rows]

    async def count_by_status(self) -> dict[str, int]:
        async with self._db.connection() as conn:
            rows = await (await conn.execute("SELECT status, COUNT(*) AS n FROM executions GROUP BY status")).fetchall()
        return {r["status"]: r["n"] for r in rows}


class PgOutboxStore(OutboxStore):
    def __init__(self, db: PostgresDatabase) -> None:
        self._db = db

    async def due(self, now: datetime, limit: int = 100) -> list[OutboxIntent]:
        async with self._db.connection() as conn:
            rows = await (await conn.execute(
                "SELECT data FROM intents WHERE status='pending' AND (next_attempt_at IS NULL OR next_attempt_at <= %s)"
                " ORDER BY COALESCE(next_attempt_at, created_at), id LIMIT %s", (_utc(now), limit))).fetchall()
        return [OutboxIntent.model_validate(r["data"]) for r in rows]

    async def get(self, intent_id: str) -> OutboxIntent | None:
        async with self._db.connection() as conn:
            row = await (await conn.execute("SELECT data FROM intents WHERE id=%s", (intent_id,))).fetchone()
        return OutboxIntent.model_validate(row["data"]) if row else None

    async def update(self, intent: OutboxIntent) -> None:
        async with self._db.connection() as conn:
            await conn.execute("UPDATE intents SET status=%s, next_attempt_at=%s, dependency=%s, hold_id=%s, data=%s"
                               " WHERE id=%s", (intent.status.value, _utc(intent.next_attempt_at), intent.dependency,
                                                intent.hold_id, _json(intent), intent.id))
            await conn.commit()

    async def list(self, *, status: IntentStatus | None = None, dependency: str | None = None,
                   hold_id: str | None = None, limit: int = 100) -> list[OutboxIntent]:
        clauses, args = _filters((("status", status.value if status else None), ("dependency", dependency),
                                  ("hold_id", hold_id)))
        async with self._db.connection() as conn:
            rows = await (await conn.execute(f"SELECT data FROM intents {clauses} ORDER BY created_at, id LIMIT %s",
                                             (*args, limit))).fetchall()
        return [OutboxIntent.model_validate(r["data"]) for r in rows]

    async def count_by_status(self) -> dict[str, int]:
        async with self._db.connection() as conn:
            rows = await (await conn.execute("SELECT status, COUNT(*) AS n FROM intents GROUP BY status")).fetchall()
        return {r["status"]: r["n"] for r in rows}


class PgJournalStore(JournalStore):
    def __init__(self, db: PostgresDatabase) -> None:
        self._db = db

    async def append(self, events: Sequence[JournalEvent]) -> None:
        if not events:
            return
        async with self._db.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany("INSERT INTO journal(id, kind, execution_id, correlation_id, trace_id, data)"
                                      " VALUES (%s,%s,%s,%s,%s,%s)",
                                      [(e.id, e.kind, e.execution_id, e.correlation_id, e.trace_id, _json(e))
                                       for e in events])
            await conn.commit()

    @staticmethod
    def _event(row: dict[str, Any]) -> JournalEvent:
        return JournalEvent.model_validate({**row["data"], "seq": row["seq"]})

    async def read(self, after_seq: int, limit: int = 500) -> list[JournalEvent]:
        async with self._db.connection() as conn:
            rows = await (await conn.execute("SELECT seq, data FROM journal WHERE seq > %s ORDER BY seq LIMIT %s",
                                             (after_seq, limit))).fetchall()
        return [self._event(r) for r in rows]

    async def query(self, *, execution_id: str | None = None, correlation_id: str | None = None,
                    trace_id: str | None = None, kinds: Sequence[str] | None = None,
                    limit: int = 1000) -> list[JournalEvent]:
        clauses, args = _filters((("execution_id", execution_id), ("correlation_id", correlation_id),
                                  ("trace_id", trace_id)))
        if kinds:
            clauses = f"{clauses} {'AND' if clauses else 'WHERE'} kind = ANY(%s)"
            args.append(list(kinds))
        async with self._db.connection() as conn:
            rows = await (await conn.execute(f"SELECT seq, data FROM journal {clauses} ORDER BY seq LIMIT %s",
                                             (*args, limit))).fetchall()
        return [self._event(r) for r in rows]


class PgDefinitionStore(DefinitionStore):
    def __init__(self, db: PostgresDatabase) -> None:
        self._db = db

    async def save(self, definition: WorkflowDefinition) -> None:
        async with self._db.connection() as conn:
            await conn.execute("INSERT INTO definitions VALUES (%s,%s,%s) ON CONFLICT (id, version)"
                               " DO UPDATE SET data=EXCLUDED.data", (definition.id, definition.version, _json(definition)))
            await conn.commit()

    async def get(self, definition_id: str, version: int | None = None) -> WorkflowDefinition | None:
        async with self._db.connection() as conn:
            if version is None:
                cur = await conn.execute("SELECT data FROM definitions WHERE id=%s ORDER BY version DESC LIMIT 1",
                                         (definition_id,))
            else:
                cur = await conn.execute("SELECT data FROM definitions WHERE id=%s AND version=%s",
                                         (definition_id, version))
            row = await cur.fetchone()
        return WorkflowDefinition.model_validate(row["data"]) if row else None

    async def list(self) -> list[WorkflowDefinition]:
        async with self._db.connection() as conn:
            rows = await (await conn.execute("SELECT data FROM definitions ORDER BY id, version")).fetchall()
        return [WorkflowDefinition.model_validate(r["data"]) for r in rows]


class PgKeyValueStore(KeyValueStore):
    def __init__(self, db: PostgresDatabase) -> None:
        self._db = db

    async def put(self, collection: str, key: str, value: dict[str, Any]) -> None:
        async with self._db.connection() as conn:
            await conn.execute("INSERT INTO kv VALUES (%s,%s,%s) ON CONFLICT (collection, key)"
                               " DO UPDATE SET data=EXCLUDED.data", (collection, key, json.dumps(value, default=str)))
            await conn.commit()

    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        async with self._db.connection() as conn:
            row = await (await conn.execute("SELECT data FROM kv WHERE collection=%s AND key=%s",
                                            (collection, key))).fetchone()
        return row["data"] if row else None

    async def delete(self, collection: str, key: str) -> None:
        async with self._db.connection() as conn:
            await conn.execute("DELETE FROM kv WHERE collection=%s AND key=%s", (collection, key))
            await conn.commit()

    async def list(self, collection: str) -> list[dict[str, Any]]:
        async with self._db.connection() as conn:
            rows = await (await conn.execute("SELECT data FROM kv WHERE collection=%s ORDER BY key",
                                             (collection,))).fetchall()
        return [r["data"] for r in rows]


def _filters(pairs: Sequence[tuple[str, Any]]) -> tuple[str, list[Any]]:
    clauses, args = [], []
    for column, value in pairs:
        if value is not None:
            clauses.append(f"{column}=%s")
            args.append(value)
    return (f"WHERE {' AND '.join(clauses)}" if clauses else ""), args


class PostgresPersistenceOptions(FeatureOptions):
    #: libpq connection string, e.g. ``postgresql://DnA Workflows:secret@db:5432/DnA Workflows``
    dsn: str
    schema_name: str = "DnA Workflows"
    min_pool_size: int = 1
    max_pool_size: int = 10


class PostgresPersistenceFeature(Feature):
    """Durable, multi-node persistence on PostgreSQL. ``pip install DnA Workflows[postgres]``."""

    name = "persistence.postgres"
    Options = PostgresPersistenceOptions

    def configure(self, services: ServiceCollection) -> None:
        o: PostgresPersistenceOptions = self.options  # type: ignore[assignment]
        db = PostgresDatabase(o.dsn, schema=o.schema_name, min_size=o.min_pool_size, max_size=o.max_pool_size)
        services.replace(PostgresDatabase, lambda p: db)
        services.replace(ExecutionStore, lambda p: PgExecutionStore(db))
        services.replace(OutboxStore, lambda p: PgOutboxStore(db))
        services.replace(JournalStore, lambda p: PgJournalStore(db))
        services.replace(DefinitionStore, lambda p: PgDefinitionStore(db))
        services.replace(KeyValueStore, lambda p: PgKeyValueStore(db))
        # the pool opens lazily on first use; this closes it gracefully with the shell
        services.add_contributor(HostedService, CallbackService(db.open, db.close), key="postgres.pool")
