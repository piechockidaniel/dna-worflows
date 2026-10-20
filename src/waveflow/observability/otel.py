"""OpenTelemetry tracing and logs (plan item P4.5; exporters for P4.6 Tempo and P4.7 Azure).

What gets a real span:

* ``workflow.start`` / ``workflow.resume`` – instant spans that anchor an execution in the caller's
  trace (the producer of the message, the HTTP request, ...),
* ``activity <type>`` – every activity attempt, with fault status and DnA Workflows attributes,
* ``send <intent kind>`` – every outbox delivery (PRODUCER); outgoing messages carry this span as
  their ``traceparent``, so the consumer side on another node continues the same trace.

Journal events are optionally emitted as OpenTelemetry **log records** correlated to their span:
Loki (via OTLP/Alloy) and Azure Monitor (``traces`` table, ``operation_Id`` = trace id) can then
show the whole business transaction by correlation id.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from opentelemetry import trace as otel_trace
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, SimpleLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import NonRecordingSpan, SpanContext, SpanKind, Status, StatusCode, TraceFlags
from pydantic import BaseModel, ConfigDict, Field

from DnA Workflows.kernel.container import ServiceCollection, ServiceProvider
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import CallbackService, HostedService
from DnA Workflows.observability.journal import JournalSink
from DnA Workflows.runtime.activity import ActivityContext, ActivityResult, Done, Suspend
from DnA Workflows.runtime.contracts import ActivityMiddleware, DeliveryMiddleware, DeliveryNext, Next, SpanFactory
from DnA Workflows.runtime.engine import EngineOptions
from DnA Workflows.runtime.models import JournalEvent, OutboxIntent
from DnA Workflows.tracing import TraceContext, reset_trace, use_trace

log = logging.getLogger(__name__)


class SpanExporterRef(BaseModel):
    """Contributor (Extend axis): an additional span exporter (e.g. Azure Monitor)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    exporter: Any
    batch: bool = True


class LogExporterRef(BaseModel):
    """Contributor (Extend axis): an additional log-record exporter."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    exporter: Any
    batch: bool = True


# -- conversions ---------------------------------------------------------------------------------------

def to_otel_context(parent: TraceContext | None) -> Any:
    if parent is None:
        return None
    span_context = SpanContext(trace_id=int(parent.trace_id, 16), span_id=int(parent.span_id, 16), is_remote=True,
                               trace_flags=TraceFlags(TraceFlags.SAMPLED if parent.sampled else TraceFlags.DEFAULT))
    return otel_trace.set_span_in_context(NonRecordingSpan(span_context))


def from_span(span: Any, tracestate: str | None = None) -> TraceContext:
    sc = span.get_span_context()
    return TraceContext(format(sc.trace_id, "032x"), format(sc.span_id, "016x"), sc.trace_flags.sampled, tracestate)


def _clean(attributes: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in attributes.items() if v is not None and v != ""}


# -- spans ---------------------------------------------------------------------------------------------

class OtelSpanFactory:
    def __init__(self, tracer: Any) -> None:
        self.tracer = tracer

    def instant(self, name: str, parent: TraceContext | None, attributes: dict[str, Any]) -> TraceContext:
        span = self.tracer.start_span(name, context=to_otel_context(parent), kind=SpanKind.CONSUMER if parent
                                      else SpanKind.INTERNAL, attributes=_clean(attributes))
        span.end()
        return from_span(span, parent.tracestate if parent else None)


class OtelActivityMiddleware:
    order = -100  # outermost: the span covers everything else

    def __init__(self, tracer: Any) -> None:
        self.tracer = tracer

    async def invoke(self, context: ActivityContext, next_: Next) -> ActivityResult:
        parent = TraceContext.parse(context.work_item.traceparent) or context.trace
        attributes = _clean({
            "DnA Workflows.execution_id": context.execution.id, "DnA Workflows.workflow": context.definition.id,
            "DnA Workflows.node": context.node.id, "DnA Workflows.activity_type": context.activity_type,
            "DnA Workflows.correlation_id": context.correlation_id, "DnA Workflows.attempt": context.attempt,
            "DnA Workflows.dependency": context.dependency, "DnA Workflows.work_item_id": context.work_item.id})
        with self.tracer.start_as_current_span(f"activity {context.activity_type}", context=to_otel_context(parent),
                                               kind=SpanKind.INTERNAL, attributes=attributes,
                                               record_exception=False, set_status_on_exception=False) as span:
            context.trace = from_span(span, parent.tracestate)
            token = use_trace(context.trace)
            try:
                result = await next_(context)
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, f"{type(exc).__name__}: {exc}"))
                raise
            finally:
                reset_trace(token)
            if isinstance(result, Done):
                span.set_attribute("DnA Workflows.outcomes", list(result.outcomes))
            elif isinstance(result, Suspend):
                span.set_attribute("DnA Workflows.suspended_on", f"{result.stimulus_type}:{result.key}")
            return result


class OtelDeliveryMiddleware:
    order = -100

    def __init__(self, tracer: Any) -> None:
        self.tracer = tracer

    async def invoke(self, intent: OutboxIntent, next_: DeliveryNext) -> None:
        parent = TraceContext.parse(intent.traceparent)
        system, _, destination = (intent.dependency or "").partition(":")
        attributes = _clean({"DnA Workflows.intent_id": intent.id, "DnA Workflows.intent_kind": intent.kind,
                             "DnA Workflows.execution_id": intent.execution_id,
                             "DnA Workflows.correlation_id": intent.correlation_id, "DnA Workflows.dependency": intent.dependency,
                             "messaging.system": system or None, "messaging.destination.name": destination or None})
        with self.tracer.start_as_current_span(f"send {intent.kind}", context=to_otel_context(parent),
                                               kind=SpanKind.PRODUCER, attributes=attributes,
                                               record_exception=False, set_status_on_exception=False) as span:
            token = use_trace(from_span(span, parent.tracestate if parent else None))
            try:
                await next_(intent)
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, f"{type(exc).__name__}: {exc}"))
                raise
            finally:
                reset_trace(token)


# -- logs ---------------------------------------------------------------------------------------------

def severity_for(kind: str) -> tuple[SeverityNumber, str]:
    if kind.endswith(("faulted", "failed")):
        return SeverityNumber.ERROR, "ERROR"
    if kind.endswith(("parked", "held", "retry_scheduled", "placed", "paused", "unmatched")):
        return SeverityNumber.WARN, "WARN"
    return SeverityNumber.INFO, "INFO"


class OtelLogJournalSink:
    """Journal → OpenTelemetry log records (correlated with the span that produced them)."""

    name = "otel-logs"

    def __init__(self, provider: LoggerProvider | None) -> None:
        self.provider = provider
        self.logger = provider.get_logger("DnA Workflows.journal") if provider else None

    async def write(self, events: list[JournalEvent]) -> None:
        if self.logger is None:
            return
        for e in events:
            severity, text = severity_for(e.kind)
            parent = TraceContext.parse(f"00-{e.trace_id}-{e.span_id}-01") if e.trace_id and e.span_id else None
            self.logger.emit(
                timestamp=int(e.timestamp.timestamp() * 1e9), severity_number=severity, severity_text=text,
                body=f"{e.kind} {e.definition_id or ''}/{e.node_id or ''}".strip(),
                context=to_otel_context(parent), event_name=f"DnA Workflows.{e.kind}",
                attributes=_clean({
                    "DnA Workflows.kind": e.kind, "DnA Workflows.execution_id": e.execution_id,
                    "DnA Workflows.workflow": e.definition_id, "DnA Workflows.node": e.node_id,
                    "DnA Workflows.activity_type": e.activity_type, "DnA Workflows.correlation_id": e.correlation_id,
                    "DnA Workflows.host": e.host, "DnA Workflows.seq": e.seq,
                    "DnA Workflows.data": json.dumps(e.data, default=str) if e.data else None}))


# -- composition --------------------------------------------------------------------------------------

class OpenTelemetryOptions(FeatureOptions):
    service_name: str = "DnA Workflows"
    service_namespace: str | None = None
    #: OTLP/HTTP base endpoint, e.g. http://tempo:4318 or http://alloy:4318 (traces → /v1/traces, logs → /v1/logs)
    otlp_endpoint: str | None = None
    otlp_headers: dict[str, str] = Field(default_factory=dict)
    #: Seconds per export attempt (the exporter retries within this budget, then drops the batch).
    otlp_timeout: float = 10.0
    otlp_logs: bool = True
    console: bool = False
    sampling_ratio: float = 1.0
    #: Register as the global tracer provider (lets other OTel instrumentations join the same traces).
    set_global: bool = False
    #: Use synchronous processors (tests, short-lived CLIs).
    synchronous: bool = False


class OtelRuntime:
    """Holds the SDK providers built for this shell."""

    def __init__(self, tracer_provider: TracerProvider, logger_provider: LoggerProvider | None) -> None:
        self.tracer_provider = tracer_provider
        self.logger_provider = logger_provider
        self.tracer = tracer_provider.get_tracer("DnA Workflows")

    def shutdown(self) -> None:
        self.tracer_provider.force_flush()
        self.tracer_provider.shutdown()
        if self.logger_provider is not None:
            self.logger_provider.force_flush()
            self.logger_provider.shutdown()


class OpenTelemetryFeature(Feature):
    """OpenTelemetry spans for workflows, activities and deliveries; journal as OTel logs."""

    name = "observability.otel"
    depends_on = ("observability",)
    Options = OpenTelemetryOptions

    def configure(self, services: ServiceCollection) -> None:
        o: OpenTelemetryOptions = self.options  # type: ignore[assignment]
        services.add(OtelRuntime, lambda p: self._build(p, o))
        services.replace(SpanFactory, lambda p: OtelSpanFactory(p.get(OtelRuntime).tracer))
        services.add_contributor(ActivityMiddleware, lambda p: OtelActivityMiddleware(p.get(OtelRuntime).tracer),
                                 key="otel")
        services.add_contributor(DeliveryMiddleware, lambda p: OtelDeliveryMiddleware(p.get(OtelRuntime).tracer),
                                 key="otel")
        services.add_contributor(JournalSink, lambda p: OtelLogJournalSink(p.get(OtelRuntime).logger_provider),
                                 key="otel-logs")

        def lifecycle(p: ServiceProvider) -> CallbackService:
            async def noop() -> None:
                p.get(OtelRuntime)

            async def stop() -> None:
                p.get(OtelRuntime).shutdown()
            return CallbackService(noop, stop)
        services.add_contributor(HostedService, lifecycle, key="otel.lifecycle")

    def _build(self, p: ServiceProvider, o: OpenTelemetryOptions) -> OtelRuntime:
        attributes = {"service.name": o.service_name, "service.instance.id": p.get(EngineOptions).host_id}
        if o.service_namespace:
            attributes["service.namespace"] = o.service_namespace
        resource = Resource.create(attributes)
        tracer_provider = TracerProvider(resource=resource, sampler=ParentBased(TraceIdRatioBased(o.sampling_ratio)))
        span_exporters = [r for r in p.get_all(SpanExporterRef)]
        log_exporters = [r for r in p.get_all(LogExporterRef)]
        if o.otlp_endpoint:
            from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            base = o.otlp_endpoint.rstrip("/")
            span_exporters.append(SpanExporterRef(exporter=OTLPSpanExporter(
                endpoint=f"{base}/v1/traces", headers=o.otlp_headers, timeout=o.otlp_timeout)))
            if o.otlp_logs:
                log_exporters.append(LogExporterRef(exporter=OTLPLogExporter(
                    endpoint=f"{base}/v1/logs", headers=o.otlp_headers, timeout=o.otlp_timeout)))
        if o.console:
            span_exporters.append(SpanExporterRef(exporter=ConsoleSpanExporter(), batch=False))
        for ref in span_exporters:
            batch = ref.batch and not o.synchronous
            tracer_provider.add_span_processor(BatchSpanProcessor(ref.exporter) if batch
                                               else SimpleSpanProcessor(ref.exporter))
        logger_provider = None
        if log_exporters:
            logger_provider = LoggerProvider(resource=resource)
            for ref in log_exporters:
                batch = ref.batch and not o.synchronous
                logger_provider.add_log_record_processor(BatchLogRecordProcessor(ref.exporter) if batch
                                                         else SimpleLogRecordProcessor(ref.exporter))
        if o.set_global:
            otel_trace.set_tracer_provider(tracer_provider)
        return OtelRuntime(tracer_provider, logger_provider)
