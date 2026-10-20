"""Prometheus metrics (plan item P4.6) – the open-source / Grafana path.

* **Counters are derived from the journal** (a journal sink), so metrics and history can never
  disagree.
* Activity **durations** come from an activity middleware (wall-clock time of each attempt).
* **Gauges** (executions by status, outbox by status, active holds, parked work, export lag) are
  refreshed periodically from the stores.

Scrape ``/metrics`` from the API feature, or set ``port`` to start a standalone exporter. Azure
Monitor managed Prometheus can scrape the same endpoint, so this also serves the Azure path.
"""

from __future__ import annotations

import time
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, start_http_server

from DnA Workflows.kernel.container import ServiceCollection, ServiceProvider
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import CallbackService, HostedService, PeriodicService
from DnA Workflows.observability.journal import JournalExporter, JournalSink
from DnA Workflows.persistence.contracts import ExecutionStore, OutboxStore
from DnA Workflows.runtime.activity import ActivityContext, ActivityResult, Done, Sleep, Suspend
from DnA Workflows.runtime.contracts import ActivityMiddleware, Next
from DnA Workflows.runtime.models import JournalEvent


class DnA WorkflowsMetrics:
    def __init__(self, registry: CollectorRegistry | None = None, namespace: str = "DnA Workflows") -> None:
        self.registry = registry or CollectorRegistry()
        r, ns = self.registry, namespace
        self.executions = Counter("executions", "Workflow execution lifecycle events",
                                  ["workflow", "status"], namespace=ns, registry=r)
        self.activities = Counter("activities", "Activity step results",
                                  ["activity_type", "result"], namespace=ns, registry=r)
        self.retries = Counter("work_retries", "Retries scheduled by the failure policy",
                               ["dependency", "fault_kind"], namespace=ns, registry=r)
        self.parked = Counter("work_parked", "Work items parked", ["dependency"], namespace=ns, registry=r)
        self.intents = Counter("outbox_intents", "Outbox delivery results", ["intent_kind", "result"],
                               namespace=ns, registry=r)
        self.holds = Counter("holds", "Holds placed/released", ["scope", "source", "action"], namespace=ns, registry=r)
        self.unmatched = Counter("messages_unmatched", "Inbound messages no workflow matched", ["endpoint"],
                                 namespace=ns, registry=r)
        self.ingress = Counter("ingress_transitions", "Consumer pause/resume events", ["endpoint", "action"],
                               namespace=ns, registry=r)
        self.duration = Histogram("activity_duration_seconds", "Wall-clock time of one activity attempt",
                                  ["activity_type", "result"], namespace=ns, registry=r,
                                  buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60))
        self.executions_by_status = Gauge("executions_current", "Executions by status", ["status"],
                                          namespace=ns, registry=r)
        self.intents_by_status = Gauge("outbox_current", "Outbox intents by status", ["status"],
                                       namespace=ns, registry=r)
        self.holds_active = Gauge("holds_active", "Active holds", ["scope"], namespace=ns, registry=r)
        self.parked_now = Gauge("work_parked_current", "Work items currently parked", namespace=ns, registry=r)
        self.export_lag = Gauge("journal_export_lag", "Journal events not yet exported", ["sink"],
                                namespace=ns, registry=r)

    def render(self) -> bytes:
        return generate_latest(self.registry)


_ACTIVITY_RESULTS = {"activity.completed": "completed", "activity.suspended": "suspended",
                     "activity.sleeping": "sleeping", "work.retry_scheduled": "retry_scheduled",
                     "work.parked": "parked", "work.faulted": "faulted"}
_EXECUTION_STATUS = {"execution.started": "started", "execution.completed": "completed",
                     "execution.faulted": "faulted", "execution.cancelled": "cancelled",
                     "execution.parked": "parked"}
_INTENT_RESULTS = {"intent.delivered": "delivered", "intent.failed": "failed", "intent.held": "held",
                   "intent.retry_scheduled": "retry_scheduled"}


class PrometheusJournalSink:
    """Turns journal events into counters."""

    name = "prometheus"

    def __init__(self, metrics: DnA WorkflowsMetrics) -> None:
        self.m = metrics

    async def write(self, events: list[JournalEvent]) -> None:
        for e in events:
            self.observe(e)

    def observe(self, e: JournalEvent) -> None:
        m, d, kind = self.m, e.data, e.kind
        if kind in _EXECUTION_STATUS:
            m.executions.labels(e.definition_id or "", _EXECUTION_STATUS[kind]).inc()
        if kind in _ACTIVITY_RESULTS:
            m.activities.labels(e.activity_type or "", _ACTIVITY_RESULTS[kind]).inc()
        if kind == "work.retry_scheduled":
            fault = d.get("fault") or {}
            m.retries.labels(fault.get("dependency") or "", fault.get("kind") or "").inc()
        elif kind == "work.parked":
            m.parked.labels(d.get("dependency") or "").inc()
        elif kind in _INTENT_RESULTS:
            m.intents.labels(d.get("intent_kind") or "", _INTENT_RESULTS[kind]).inc()
        elif kind in ("hold.placed", "hold.released"):
            hold = d.get("hold") or {}
            m.holds.labels(hold.get("scope") or "", hold.get("source") or "", kind.split(".")[1]).inc()
        elif kind == "message.unmatched":
            m.unmatched.labels(d.get("endpoint") or "").inc()
        elif kind in ("ingress.paused", "ingress.resumed"):
            m.ingress.labels(d.get("endpoint") or "", kind.split(".")[1]).inc()


class MetricsMiddleware:
    """Measures each activity attempt (outermost after tracing)."""

    order = -50

    def __init__(self, metrics: DnA WorkflowsMetrics) -> None:
        self.m = metrics

    async def invoke(self, context: ActivityContext, next_: Next) -> ActivityResult:
        start = time.perf_counter()
        result_label = "error"
        try:
            result = await next_(context)
            result_label = ("completed" if isinstance(result, Done) else
                            "suspended" if isinstance(result, Suspend) else
                            "sleeping" if isinstance(result, Sleep) else "other")
            return result
        finally:
            self.m.duration.labels(context.activity_type, result_label).observe(time.perf_counter() - start)


class StateGauges(PeriodicService):
    name = "prometheus-gauges"

    def __init__(self, metrics: DnA WorkflowsMetrics, executions: ExecutionStore, outbox: OutboxStore,
                 services: ServiceProvider, interval: float = 15.0) -> None:
        super().__init__()
        self.m = metrics
        self.executions = executions
        self.outbox = outbox
        self.services = services
        self.interval = interval

    async def tick(self) -> None:
        await self.refresh()

    async def refresh(self) -> None:
        m = self.m
        counts = await self.executions.count_by_status()
        for status in ("running", "waiting", "parked", "completed", "faulted", "cancelled"):
            m.executions_by_status.labels(status).set(counts.get(status, 0))
        intents = await self.outbox.count_by_status()
        for status in ("pending", "delivered", "held", "failed"):
            m.intents_by_status.labels(status).set(intents.get(status, 0))
        m.parked_now.set(len(await self.executions.find_parked()))
        from DnA Workflows.resilience.holds import HoldScope, HoldService
        holds = self.services.get_optional(HoldService)
        if holds is not None:
            active = await holds.list(active_only=True)
            for scope in HoldScope:
                m.holds_active.labels(scope.value).set(sum(1 for h in active if h.scope is scope))
        exporter = self.services.get_optional(JournalExporter)
        if exporter is not None:
            for sink, lag in (await exporter.lag()).items():
                m.export_lag.labels(sink).set(lag)


class PrometheusOptions(FeatureOptions):
    namespace: str = "DnA Workflows"
    #: Start a standalone ``/metrics`` HTTP server on this port (not needed with the API feature).
    port: int | None = None
    gauge_interval: float = 15.0
    background_services: bool = True


class PrometheusFeature(Feature):
    """Prometheus metrics derived from the journal + activity timings + state gauges."""

    name = "observability.prometheus"
    depends_on = ("observability",)
    Options = PrometheusOptions

    def configure(self, services: ServiceCollection) -> None:
        o: PrometheusOptions = self.options  # type: ignore[assignment]
        services.add(DnA WorkflowsMetrics, lambda p: DnA WorkflowsMetrics(namespace=o.namespace))
        services.add_contributor(JournalSink, lambda p: PrometheusJournalSink(p.get(DnA WorkflowsMetrics)),
                                 key="prometheus")
        services.add_contributor(ActivityMiddleware, lambda p: MetricsMiddleware(p.get(DnA WorkflowsMetrics)),
                                 key="prometheus")
        services.add(StateGauges, lambda p: StateGauges(p.get(DnA WorkflowsMetrics), p.get(ExecutionStore),
                                                        p.get(OutboxStore), p, interval=o.gauge_interval))
        if o.background_services:
            services.add_contributor(HostedService, lambda p: p.get(StateGauges), key="prometheus.gauges")
        if o.port is not None:
            port = o.port

            def server(p: ServiceProvider) -> Any:
                async def start() -> None:
                    start_http_server(port, registry=p.get(DnA WorkflowsMetrics).registry)
                return CallbackService(start)
            services.add_contributor(HostedService, server, key="prometheus.server")
