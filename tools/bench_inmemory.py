"""Benchmark for plan item P15.2: should in-memory execution storage use DuckDB?

Compares, for N executions, the operations the engine performs constantly:
commit (write one execution), get (by id), due (timer scan), find_bookmarks (message routing),
list by correlation id. Candidates: the JSON-dict store as shipped in iteration 1, a DuckDB in-memory
table (JSON document + index columns, as the SQLite provider does), and the indexed in-memory store.

    python tools/bench_inmemory.py 1000 10000
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
from datetime import UTC, datetime, timedelta

import duckdb

from DnA Workflows.persistence.memory import InMemoryExecutionStore, InMemoryState
from DnA Workflows.runtime.models import Bookmark, Checkpoint, ExecutionStatus, WorkflowExecution, WorkItem

T0 = datetime(2030, 1, 1, tzinfo=UTC)


def make(i: int) -> WorkflowExecution:
    waiting = i % 2 == 0
    return WorkflowExecution(
        id=f"wf-{i}", definition_id=f"d{i % 10}", definition_version=1, created_at=T0,
        status=ExecutionStatus.WAITING if waiting else ExecutionStatus.RUNNING, correlation_id=f"order-{i}",
        variables={"payload": "x" * 200, "n": i},
        bookmarks=[Bookmark(id=f"b{i}", node_id="n", stimulus_type="message", key=f"orders|{i % 50}")] if waiting else [],
        work=[] if waiting else [WorkItem(id=f"w{i}", node_id="n", created_at=T0, not_before=T0 + timedelta(seconds=i))])


class DuckDbExecutionStore:
    """Prototype with the same contract semantics as the SQLite provider, on DuckDB in-memory."""

    def __init__(self) -> None:
        self.c = duckdb.connect(":memory:")
        self.c.execute("CREATE TABLE ex (id VARCHAR PRIMARY KEY, status VARCHAR, corr VARCHAR, wake TIMESTAMP,"
                       " revision INTEGER, data VARCHAR)")
        self.c.execute("CREATE TABLE bm (id VARCHAR, execution_id VARCHAR, type VARCHAR, key VARCHAR, corr VARCHAR)")

    async def commit(self, cp: Checkpoint) -> int:
        ex = cp.execution
        wake = ex.wake_at()
        self.c.execute("INSERT OR REPLACE INTO ex VALUES (?,?,?,?,?,?)",
                       (ex.id, ex.status.value, ex.correlation_id, wake.replace(tzinfo=None) if wake else None,
                        cp.expected_revision + 1, json.dumps(ex.model_dump(mode="json"))))
        self.c.execute("DELETE FROM bm WHERE execution_id=?", (ex.id,))
        for b in ex.bookmarks:
            self.c.execute("INSERT INTO bm VALUES (?,?,?,?,?)", (b.id, ex.id, b.stimulus_type, b.key, b.correlation_id))
        return cp.expected_revision + 1

    async def get(self, execution_id: str) -> WorkflowExecution | None:
        row = self.c.execute("SELECT data FROM ex WHERE id=?", (execution_id,)).fetchone()
        return WorkflowExecution.model_validate_json(row[0]) if row else None

    async def due(self, now: datetime, limit: int = 100) -> list[str]:
        return [r[0] for r in self.c.execute("SELECT id FROM ex WHERE wake <= ? ORDER BY wake LIMIT ?",
                                             (now.replace(tzinfo=None), limit)).fetchall()]

    async def find_bookmarks(self, t: str, keys: list[str], corr: str | None) -> list:
        return self.c.execute("SELECT execution_id FROM bm WHERE type=? AND key IN (SELECT unnest(?))",
                              (t, keys)).fetchall()

    async def list(self, *, correlation_id: str | None = None, **_: object) -> list:
        return [WorkflowExecution.model_validate_json(r[0]) for r in
                self.c.execute("SELECT data FROM ex WHERE corr=?", (correlation_id,)).fetchall()]


async def measure(store: object, n: int) -> dict[str, float]:
    results: dict[str, list[float]] = {k: [] for k in ("commit", "get", "due", "find_bookmarks", "list_corr")}
    for i in range(n):
        ex = make(i)
        t = time.perf_counter()
        await store.commit(Checkpoint(execution=ex, expected_revision=0))
        results["commit"].append(time.perf_counter() - t)
    for i in range(0, n, max(1, n // 50)):
        for name, call in (("get", lambda i=i: store.get(f"wf-{i}")),
                           ("due", lambda: store.due(T0 + timedelta(seconds=n // 2))),
                           ("find_bookmarks", lambda i=i: store.find_bookmarks("message", [f"orders|{i % 50}"], None)),
                           ("list_corr", lambda i=i: store.list(correlation_id=f"order-{i}"))):
            t = time.perf_counter()
            await call()
            results[name].append(time.perf_counter() - t)
    return {k: statistics.median(v) * 1000 for k, v in results.items()}


async def main(sizes: list[int]) -> None:
    Indexed = InMemoryExecutionStore
    print(f"{'N':>7} {'store':<26}" + "".join(f"{k:>16}" for k in ("commit", "get", "due", "find_bookmarks",
                                                                  "list_corr")) + "   (median ms)")
    for n in sizes:
        for label, factory in (("in-memory (indexed)", lambda: Indexed(InMemoryState())),
                               ("duckdb :memory:", DuckDbExecutionStore)):
            r = await measure(factory(), n)
            print(f"{n:>7} {label:<26}" + "".join(f"{r[k]:>16.3f}" for k in r))


if __name__ == "__main__":
    asyncio.run(main([int(a) for a in sys.argv[1:]] or [1000, 10000]))
