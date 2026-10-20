"""Observation store on DuckDB (plan item P9.4).

Why DuckDB here (Q-006: "DuckDB whenever justified"): forecasting re-reads history constantly with
changing filters (per node, per condition, per time window), needs quantiles and linear regression,
and only appends. That is exactly an embedded columnar OLAP workload: DuckDB does quantiles,
``regr_slope``/``regr_intercept`` and filtered re-aggregation in SQL, over millions of rows, in-process,
with no server. History can live in a file (persistent, portable, queryable with any DuckDB client) or
in memory.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeVar

import duckdb

T = TypeVar("T")

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS steps (
        ts TIMESTAMP, workflow VARCHAR, version INTEGER, execution_id VARCHAR, node VARCHAR,
        activity_type VARCHAR, dependency VARCHAR, outcome VARCHAR, duration DOUBLE, latency DOUBLE,
        attempts INTEGER)""",
    """CREATE TABLE IF NOT EXISTS features (
        execution_id VARCHAR, workflow VARCHAR, name VARCHAR, num DOUBLE, str VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS executions (
        execution_id VARCHAR PRIMARY KEY, workflow VARCHAR, version INTEGER, started_at TIMESTAMP,
        finished_at TIMESTAMP, status VARCHAR, lead_time DOUBLE)""",
    """CREATE TABLE IF NOT EXISTS parks (
        ts TIMESTAMP, workflow VARCHAR, node VARCHAR, dependency VARCHAR, parked DOUBLE)""",
    """CREATE TABLE IF NOT EXISTS forecasts (
        execution_id VARCHAR PRIMARY KEY, workflow VARCHAR, issued_at TIMESTAMP, mean DOUBLE, p50 DOUBLE,
        p90 DOUBLE, basis VARCHAR, actual DOUBLE, abs_pct_error DOUBLE, within_p90 BOOLEAN)""",
    "CREATE INDEX IF NOT EXISTS ix_steps_node ON steps(workflow, node)",
    "CREATE INDEX IF NOT EXISTS ix_feat ON features(execution_id)",
]


def utc(ts: datetime | None) -> datetime | None:
    """DuckDB TIMESTAMP is naive: store and compare everything as naive UTC."""
    if ts is None or ts.tzinfo is None:
        return ts
    return ts.astimezone(UTC).replace(tzinfo=None)


@dataclass(frozen=True, slots=True)
class Sample:
    value: float
    ts: datetime


class ObservationStore:
    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self._conn = duckdb.connect(path)
        self._lock = threading.Lock()
        for statement in SCHEMA:
            self._conn.execute(statement)

    async def run(self, fn: Callable[[duckdb.DuckDBPyConnection], T]) -> T:
        def call() -> T:
            with self._lock:
                return fn(self._conn)
        return await asyncio.to_thread(call)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- writes ---------------------------------------------------------------------------------
    async def add_steps(self, rows: Sequence[tuple[Any, ...]]) -> None:
        rows = [(utc(r[0]), *r[1:]) for r in rows]
        if rows:
            await self.run(lambda c: c.executemany("INSERT INTO steps VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows))

    async def add_features(self, execution_id: str, workflow: str, features: dict[str, Any]) -> None:
        rows = [(execution_id, workflow, name, float(v) if isinstance(v, (int, float)) and not isinstance(v, bool)
                 else None, None if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v))
                for name, v in features.items()]
        if rows:
            await self.run(lambda c: c.executemany("INSERT INTO features VALUES (?,?,?,?,?)", rows))

    async def execution_started(self, execution_id: str, workflow: str, version: int, ts: datetime) -> None:
        await self.run(lambda c: c.execute(
            "INSERT INTO executions VALUES (?,?,?,?,NULL,'running',NULL) ON CONFLICT DO NOTHING",
            (execution_id, workflow, version, utc(ts))))

    async def execution_finished(self, execution_id: str, status: str, ts: datetime) -> float | None:
        ts = utc(ts)

        def tx(c: duckdb.DuckDBPyConnection) -> float | None:
            row = c.execute("SELECT started_at FROM executions WHERE execution_id=?", (execution_id,)).fetchone()
            if row is None:
                return None
            lead = (ts - row[0]).total_seconds() if row[0] else None
            c.execute("UPDATE executions SET finished_at=?, status=?, lead_time=? WHERE execution_id=?",
                      (ts, status, lead, execution_id))
            return lead
        return await self.run(tx)

    async def add_park(self, ts: datetime, workflow: str | None, node: str | None, dependency: str | None,
                       parked: float) -> None:
        await self.run(lambda c: c.execute("INSERT INTO parks VALUES (?,?,?,?,?)",
                                           (utc(ts), workflow, node, dependency, parked)))

    async def record_forecast(self, execution_id: str, workflow: str, issued_at: datetime, mean: float, p50: float,
                              p90: float, basis: str) -> None:
        await self.run(lambda c: c.execute(
            "INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,NULL,NULL,NULL) ON CONFLICT DO NOTHING",
            (execution_id, workflow, utc(issued_at), mean, p50, p90, basis)))

    async def settle_forecast(self, execution_id: str, actual: float) -> None:
        await self.run(lambda c: c.execute(
            "UPDATE forecasts SET actual=?, abs_pct_error=CASE WHEN ? > 0 THEN abs(mean-?)/? END, within_p90=(?<=p90)"
            " WHERE execution_id=?", (actual, actual, actual, actual, actual, execution_id)))

    # -- reads ----------------------------------------------------------------------------------
    async def latencies(self, *, workflow: str | None = None, node: str | None = None,
                        activity_type: str | None = None, dependency: str | None = None,
                        conditions: dict[str, Any] | None = None, limit: int = 500) -> list[Sample]:
        """Most recent node latencies matching the filters (and execution feature conditions)."""
        clauses, args = ["latency IS NOT NULL"], []
        for col, val in (("workflow", workflow), ("node", node), ("activity_type", activity_type),
                         ("dependency", dependency)):
            if val is not None:
                clauses.append(f"{col}=?")
                args.append(val)
        for name, value in (conditions or {}).items():
            column = "num" if isinstance(value, (int, float)) and not isinstance(value, bool) else "str"
            clauses.append(f"execution_id IN (SELECT execution_id FROM features WHERE name=? AND {column}=?)")
            args.extend([name, value if column == "num" else str(value)])
        sql = (f"SELECT latency, ts FROM steps WHERE {' AND '.join(clauses)} "
               f"ORDER BY ts DESC LIMIT {int(limit)}")
        rows = await self.run(lambda c: c.execute(sql, args).fetchall())
        return [Sample(r[0], r[1]) for r in rows]

    async def regression(self, workflow: str, node: str, feature: str) -> tuple[float, float, int] | None:
        """Least squares ``latency = a + b * feature`` (DuckDB ``regr_*``). Returns (a, b, n)."""
        row = await self.run(lambda c: c.execute(
            "SELECT regr_intercept(s.latency, f.num), regr_slope(s.latency, f.num), count(*) FROM steps s"
            " JOIN features f ON f.execution_id = s.execution_id AND f.name = ?"
            " WHERE s.workflow = ? AND s.node = ? AND f.num IS NOT NULL AND s.latency IS NOT NULL",
            (feature, workflow, node)).fetchone())
        if row is None or row[0] is None or row[1] is None:
            return None
        return float(row[0]), float(row[1]), int(row[2])

    async def outcome_counts(self, workflow: str, node: str) -> dict[str, int]:
        rows = await self.run(lambda c: c.execute(
            "SELECT outcome, count(*) FROM steps WHERE workflow=? AND node=? AND outcome IS NOT NULL GROUP BY outcome",
            (workflow, node)).fetchall())
        return {r[0]: int(r[1]) for r in rows}

    async def park_durations(self, dependency: str | None, limit: int = 200) -> list[Sample]:
        sql = "SELECT parked, ts FROM parks" + (" WHERE dependency=?" if dependency else "") + \
            f" ORDER BY ts DESC LIMIT {int(limit)}"
        rows = await self.run(lambda c: c.execute(sql, [dependency] if dependency else []).fetchall())
        return [Sample(r[0], r[1]) for r in rows]

    async def node_stats(self, workflow: str) -> list[dict[str, Any]]:
        rows = await self.run(lambda c: c.execute(
            "SELECT node, any_value(activity_type), count(*), avg(latency), quantile_cont(latency, 0.5),"
            " quantile_cont(latency, 0.9), avg(duration), avg(attempts) FROM steps WHERE workflow=?"
            " GROUP BY node ORDER BY node", (workflow,)).fetchall())
        keys = ("node", "activity_type", "samples", "mean", "p50", "p90", "mean_duration", "mean_attempts")
        return [dict(zip(keys, r, strict=True)) for r in rows]

    async def lead_times(self, workflow: str, limit: int = 500) -> list[Sample]:
        rows = await self.run(lambda c: c.execute(
            "SELECT lead_time, finished_at FROM executions WHERE workflow=? AND status='completed'"
            f" AND lead_time IS NOT NULL ORDER BY finished_at DESC LIMIT {int(limit)}", (workflow,)).fetchall())
        return [Sample(r[0], r[1]) for r in rows]

    async def accuracy(self, workflow: str | None = None) -> list[dict[str, Any]]:
        where = "WHERE actual IS NOT NULL" + (" AND workflow=?" if workflow else "")
        rows = await self.run(lambda c: c.execute(
            "SELECT workflow, count(*), avg(abs_pct_error), avg(CASE WHEN within_p90 THEN 1.0 ELSE 0.0 END),"
            f" avg(actual), avg(mean) FROM forecasts {where} GROUP BY workflow ORDER BY workflow",
            [workflow] if workflow else []).fetchall())
        keys = ("workflow", "settled", "mape", "p90_coverage", "mean_actual", "mean_forecast")
        return [dict(zip(keys, r, strict=True)) for r in rows]

    async def forecast_of(self, execution_id: str) -> dict[str, Any] | None:
        row = await self.run(lambda c: c.execute(
            "SELECT workflow, issued_at, mean, p50, p90, basis, actual, abs_pct_error, within_p90 FROM forecasts"
            " WHERE execution_id=?", (execution_id,)).fetchone())
        if row is None:
            return None
        keys = ("workflow", "issued_at", "mean", "p50", "p90", "basis", "actual", "abs_pct_error", "within_p90")
        return dict(zip(keys, row, strict=True))
