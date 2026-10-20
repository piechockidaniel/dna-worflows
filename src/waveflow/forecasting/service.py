"""Forecasting service, journal sink and feature (plan items P9.4, P9.8, P9.9)."""

from __future__ import annotations

import logging
from typing import Any

from DnA Workflows.forecasting.model import Forecaster, extract_features
from DnA Workflows.forecasting.store import ObservationStore
from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.container import ServiceCollection, ServiceProvider
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.observability.journal import JournalSink
from DnA Workflows.persistence.contracts import ExecutionStore
from DnA Workflows.runtime.activity import ActivityRegistry, UnknownActivityError
from DnA Workflows.runtime.models import JournalEvent, WorkflowDefinition
from DnA Workflows.runtime.registry import WorkflowRegistry

log = logging.getLogger(__name__)


class ForecastService:
    """What the API, MCP and the Studio talk to."""

    def __init__(self, forecaster: Forecaster, store: ObservationStore, workflows: WorkflowRegistry,
                 activities: ActivityRegistry, executions: ExecutionStore, clock: Clock, services: ServiceProvider) -> None:
        self.forecaster = forecaster
        self.store = store
        self.workflows = workflows
        self.activities = activities
        self.executions = executions
        self.clock = clock
        self.services = services

    def outcomes(self, definition: WorkflowDefinition) -> dict[str, tuple[str, ...]]:
        result = {}
        for node_id, node in definition.nodes.items():
            try:
                result[node_id] = self.activities.get(node.type).outcomes
            except UnknownActivityError:
                result[node_id] = ("Done",)
        return result

    async def forecast_workflow(self, workflow_id: str, *, input: dict[str, Any] | None = None,
                                conditions: dict[str, Any] | None = None, version: int | None = None,
                                definition: WorkflowDefinition | None = None) -> dict[str, Any]:
        definition = definition or await self.workflows.get(workflow_id, version)
        forecast = await self.forecaster.forecast_workflow(definition, input=input, conditions=conditions,
                                                           outcomes=self.outcomes(definition))
        return forecast.as_dict()

    async def forecast_execution(self, execution_id: str) -> dict[str, Any]:
        execution = await self.executions.get(execution_id)
        if execution is None:
            raise LookupError(f"Execution '{execution_id}' not found")
        definition = await self.workflows.get(execution.definition_id, execution.definition_version)
        holds = await self._holds()
        result = await self.forecaster.forecast_execution(execution, definition, self.clock.now(),
                                                          outcomes=self.outcomes(definition), holds=holds)
        result["forecast_at_start"] = _jsonable(await self.store.forecast_of(execution_id))
        return result

    async def node_stats(self, workflow_id: str) -> list[dict[str, Any]]:
        return await self.store.node_stats(workflow_id)

    async def accuracy(self, workflow_id: str | None = None) -> list[dict[str, Any]]:
        return await self.store.accuracy(workflow_id)

    async def _holds(self) -> dict[str, Any]:
        from DnA Workflows.resilience.holds import HoldService
        holds = self.services.get_optional(HoldService)
        if holds is None:
            return {}
        return {h.id: {"scope": h.scope.value, "target": h.target, "reason": h.reason,
                       "release_strategy": h.release_strategy} for h in await holds.list(active_only=True)}


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in value.items()}
    return value


class ForecastJournalSink:
    """Turns the committed journal into observations (and forecast snapshots for accuracy)."""

    name = "forecasting"

    def __init__(self, store: ObservationStore, service: ForecastService) -> None:
        self.store = store
        self.service = service
        self._versions: dict[str, int] = {}

    async def write(self, events: list[JournalEvent]) -> None:
        rows = []
        for e in events:
            d = e.data
            if e.kind == "execution.started" and e.execution_id:
                await self._started(e)
            elif e.kind == "activity.completed" and d.get("latency_ms") is not None:
                outcomes = d.get("outcomes") or []
                rows.append((e.timestamp, e.definition_id, self._versions.get(e.execution_id or ""), e.execution_id,
                             e.node_id, e.activity_type, d.get("dependency"), outcomes[0] if outcomes else None,
                             (d.get("duration_ms") or 0) / 1000, d["latency_ms"] / 1000, int(d.get("attempt", 0)) + 1))
            elif e.kind == "work.resumed" and d.get("parked_ms") is not None:
                await self.store.add_park(e.timestamp, e.definition_id, e.node_id, d.get("dependency"),
                                          d["parked_ms"] / 1000)
            elif e.kind in ("execution.completed", "execution.faulted", "execution.cancelled") and e.execution_id:
                await self.store.add_steps(rows)
                rows = []
                lead = await self.store.execution_finished(e.execution_id, e.kind.split(".")[1], e.timestamp)
                if lead is not None and e.kind == "execution.completed":
                    await self.store.settle_forecast(e.execution_id, lead)
        await self.store.add_steps(rows)

    async def _started(self, e: JournalEvent) -> None:
        execution = await self.service.executions.get(e.execution_id or "")
        if execution is None:
            return
        self._versions[execution.id] = execution.definition_version
        await self.store.execution_started(execution.id, execution.definition_id, execution.definition_version,
                                           e.timestamp)
        try:
            definition = await self.service.workflows.get(execution.definition_id, execution.definition_version)
        except LookupError:
            return
        await self.store.add_features(execution.id, definition.id,
                                      extract_features(definition.forecast.features, execution.input))
        forecast = await self.service.forecast_workflow(definition.id, input=execution.input, definition=definition)
        await self.store.record_forecast(execution.id, definition.id, e.timestamp, forecast["mean"], forecast["p50"],
                                         forecast["p90"], forecast["basis"])


class ForecastingOptions(FeatureOptions):
    #: DuckDB database file (history survives restarts) or ":memory:".
    database: str = ":memory:"
    prior_strength: float = 3.0
    half_life: float = 50.0
    window: int = 500
    min_level_samples: int = 3
    default_seconds: float = 1.0
    park_default_seconds: float = 3600.0


class ForecastingFeature(Feature):
    """Processing-time forecasting: estimates refined by history (DuckDB)."""

    name = "forecasting"
    depends_on = ("runtime", "observability")
    Options = ForecastingOptions

    def configure(self, services: ServiceCollection) -> None:
        o: ForecastingOptions = self.options  # type: ignore[assignment]
        services.add(ObservationStore, lambda p: ObservationStore(o.database))
        services.add(Forecaster, lambda p: Forecaster(
            p.get(ObservationStore), prior_strength=o.prior_strength, half_life=o.half_life, window=o.window,
            min_level_samples=o.min_level_samples, default_seconds=o.default_seconds,
            park_default_seconds=o.park_default_seconds))
        services.add(ForecastService, lambda p: ForecastService(
            p.get(Forecaster), p.get(ObservationStore), p.get(WorkflowRegistry), p.get(ActivityRegistry),
            p.get(ExecutionStore), p.get(Clock), p))
        services.add_contributor(JournalSink, lambda p: ForecastJournalSink(p.get(ObservationStore),
                                                                           p.get(ForecastService)), key="forecasting")
