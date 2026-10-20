import json
import logging

import pytest
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from DnA Workflows.kernel import ManualClock
from DnA Workflows.kernel.hosting import HostedService
from DnA Workflows.messaging import ConsumerHost, MessageBus
from DnA Workflows.messaging.providers.inmemory import InMemoryBroker
from DnA Workflows.observability import JournalExporter, JsonLogFormatter, TraceContextFilter, configure_logging
from DnA Workflows.observability.journal import LoggingJournalSink
from DnA Workflows.observability.otel import LogExporterRef, OtelRuntime, SpanExporterRef, severity_for
from DnA Workflows.observability.prometheus import DnA WorkflowsMetrics, StateGauges
from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore
from DnA Workflows.resilience import HoldService
from DnA Workflows.runtime import (Activity, ActivityInputs, Done, OutboxProcessor, TimerService, WorkflowBuilder,
                              WorkflowRegistry, WorkflowRuntime, add_activity)
from DnA Workflows.tracing import (TraceContext, current_correlation_id, current_trace, reset_correlation, reset_trace,
                              use_correlation, use_trace)

ENDPOINTS = {"orders": {"transport": "inmemory", "address": "q.orders"},
             "invoices": {"transport": "inmemory", "address": "q.invoices"},
             "journal": {"transport": "inmemory", "address": "q.journal"}}


@pytest.fixture(autouse=True)
def broker():
    InMemoryBroker.reset()
    yield InMemoryBroker.get()
    InMemoryBroker.reset()


class Unstable(Activity):
    type_name = "Unstable"
    fail = True

    class Inputs(ActivityInputs):
        pass

    def dependency(self, inputs, node):
        return "http:erp"

    async def execute(self, context):
        if Unstable.fail:
            raise ConnectionError("erp down")
        return Done()


# ------------------------------------------------------------------------------------------------------
# tracing primitive
# ------------------------------------------------------------------------------------------------------

def test_trace_context_parsing_and_propagation():
    root = TraceContext.new_root()
    assert len(root.trace_id) == 32 and len(root.span_id) == 16 and root.sampled
    child = root.child()
    assert child.trace_id == root.trace_id and child.span_id != root.span_id
    parsed = TraceContext.parse(child.traceparent, "a=b")
    assert parsed == TraceContext(child.trace_id, child.span_id, True, "a=b")
    unsampled = TraceContext.parse(f"00-{'a' * 32}-{'b' * 16}-00")
    assert unsampled.sampled is False and unsampled.traceparent.endswith("-00")
    for bad in (None, "", "garbage", f"ff-{'a' * 32}-{'b' * 16}-01", f"00-{'0' * 32}-{'b' * 16}-01",
                f"00-{'a' * 32}-{'0' * 16}-01"):
        assert TraceContext.parse(bad) is None
    assert TraceContext.continue_or_new("garbage").trace_id != root.trace_id
    assert TraceContext.continue_or_new(root.traceparent).trace_id == root.trace_id
    carrier = TraceContext(root.trace_id, root.span_id, tracestate="v=1").inject({})
    assert carrier == {"traceparent": root.traceparent, "tracestate": "v=1"}
    assert TraceContext.extract({"TraceParent": root.traceparent, "x": 1}) == root
    assert TraceContext.extract(None) is None

    token = use_trace(root)
    ctoken = use_correlation("c-1")
    assert current_trace() == root and current_correlation_id() == "c-1"
    reset_correlation(ctoken)
    reset_trace(token)
    assert current_trace() is None and current_correlation_id() is None


def test_log_filter_and_json_formatter():
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello %s", ("world",), None)
    root = TraceContext.new_root()
    token, ctoken = use_trace(root), use_correlation("corr-9")
    try:
        TraceContextFilter().filter(record)
    finally:
        reset_correlation(ctoken)
        reset_trace(token)
    payload = json.loads(JsonLogFormatter().format(record))
    assert payload["message"] == "hello world" and payload["trace_id"] == root.trace_id
    assert payload["correlation_id"] == "corr-9"
    try:
        raise ValueError("boom")
    except ValueError:
        import sys
        err = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    assert "ValueError: boom" in json.loads(JsonLogFormatter().format(err))["exception"]

    handler = configure_logging("warning", json_format=False)
    try:
        assert configure_logging("error") is handler            # idempotent
        assert logging.getLogger().level == logging.ERROR
    finally:
        logging.getLogger().removeHandler(handler)
        logging.getLogger().setLevel(logging.WARNING)


# ------------------------------------------------------------------------------------------------------
# journal export
# ------------------------------------------------------------------------------------------------------

class RecordingSink:
    def __init__(self, name, fail=False):
        self.name = name
        self.fail = fail
        self.events = []

    async def write(self, events):
        if self.fail:
            raise RuntimeError("sink down")
        self.events.extend(events)


async def test_exporter_cursors_isolation_and_lag(make_shell):
    shell = make_shell({"observability": {"sinks": [], "batch_size": 2}})
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("w").add("a", "Start").then("b", "Start").build())
    await shell.get(WorkflowRuntime).start("w")
    good, bad = RecordingSink("good"), RecordingSink("bad", fail=True)
    exporter = JournalExporter(shell.get(JournalStore), shell.get(__import__(
        "DnA Workflows.persistence.contracts", fromlist=["KeyValueStore"]).KeyValueStore), [good, bad],
        __import__("DnA Workflows.runtime.contracts", fromlist=["RuntimeSignals"]).RuntimeSignals(), batch_size=2)
    total = len(await shell.get(JournalStore).read(0))
    written = await exporter.export()
    assert written == {"good": total, "bad": 0} and "sink down" in exporter.errors["bad"]
    assert [e.kind for e in good.events][0] == "execution.started"
    assert await exporter.lag() == {"good": 0, "bad": total}
    assert await exporter.export() == {"good": 0, "bad": 0}   # nothing new, cursor persisted
    bad.fail = False
    assert (await exporter.export())["bad"] == total and exporter.errors == {}


async def test_logging_and_stream_sinks(make_shell, caplog, broker):
    shell = make_shell({
        "messaging": {"endpoints": ENDPOINTS}, "messaging.inmemory": {},
        "observability": {"stream_endpoint": "journal"}})
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("w").add("a", "Log", message="hi").build())
    ex = await shell.get(WorkflowRuntime).start("w", correlation_id="c-77")
    with caplog.at_level(logging.INFO, logger="DnA Workflows.journal"):
        written = await shell.get(JournalExporter).export()
    assert written["logging"] == written["messaging"] > 0
    lines = [r.journal for r in caplog.records if r.name == "DnA Workflows.journal"]
    assert {line["correlation_id"] for line in lines} == {"c-77"}
    formatted = json.loads(JsonLogFormatter().format(caplog.records[0]))
    assert formatted["journal"]["kind"] == "execution.started" and formatted["message"].startswith("execution.started")
    streamed = broker.history["q.journal"]
    assert streamed[0].type == "DnA Workflows.journal.execution.started" and streamed[0].subject == ex.id
    assert streamed[0].trace.trace_id == ex.trace_id and streamed[0].data["workflow"] == "w"
    assert any(isinstance(h, JournalExporter) for h in shell.provider.get_all(HostedService)) is False


async def test_logging_sink_levels(caplog):
    from DnA Workflows.runtime.models import JournalEvent
    from datetime import UTC, datetime
    now = datetime.now(UTC)
    with caplog.at_level(logging.INFO, logger="DnA Workflows.journal"):
        await LoggingJournalSink().write([JournalEvent(id="1", kind="work.faulted", timestamp=now),
                                          JournalEvent(id="2", kind="work.parked", timestamp=now),
                                          JournalEvent(id="3", kind="activity.completed", timestamp=now)])
    assert [r.levelno for r in caplog.records] == [logging.ERROR, logging.WARNING, logging.INFO]


# ------------------------------------------------------------------------------------------------------
# prometheus
# ------------------------------------------------------------------------------------------------------

def metric(text: str, line_start: str) -> float:
    for line in text.splitlines():
        if line.startswith(line_start):
            return float(line.rsplit(" ", 1)[1])
    raise AssertionError(f"{line_start} not found in metrics:\n{text}")


async def test_prometheus_metrics_from_journal_middleware_and_gauges(make_shell):
    clock = ManualClock()
    Unstable.fail = True
    shell = make_shell({"observability": {"sinks": []}, "observability.prometheus": {},
                        "resilience": {"retry": {"dependency_unavailable": {"max_attempts": 2, "jitter": 0}}}},
                       clock=clock, configure=lambda s: add_activity(s, Unstable))
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("sync").add("call", "Unstable").build())
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("ok").add("a", "Log", message="x").build())
    rt = shell.get(WorkflowRuntime)
    await rt.start("ok")
    await rt.start("sync")
    clock.advance(10)
    await shell.get(TimerService).run_due()                   # budget exhausted -> trip -> parked
    await shell.get(JournalExporter).export()
    await shell.get(StateGauges).refresh()
    text = shell.get(DnA WorkflowsMetrics).render().decode()

    assert metric(text, 'DnA Workflows_executions_total{status="started",workflow="ok"}') == 1
    assert metric(text, 'DnA Workflows_executions_total{status="completed",workflow="ok"}') == 1
    assert metric(text, 'DnA Workflows_executions_total{status="parked",workflow="sync"}') == 1
    assert metric(text, 'DnA Workflows_activities_total{activity_type="Unstable",result="retry_scheduled"}') == 1
    assert metric(text, 'DnA Workflows_activities_total{activity_type="Unstable",result="parked"}') == 1
    assert metric(text, 'DnA Workflows_work_retries_total{dependency="http:erp",fault_kind="dependency_unavailable"}') == 1
    assert metric(text, 'DnA Workflows_work_parked_total{dependency="http:erp"}') == 1
    assert metric(text, 'DnA Workflows_holds_total{action="placed",scope="dependency",source="auto-trip"}') == 1
    assert metric(text, 'DnA Workflows_activity_duration_seconds_count{activity_type="Log",result="completed"}') == 1
    assert metric(text, 'DnA Workflows_activity_duration_seconds_count{activity_type="Unstable",result="error"}') == 2
    assert metric(text, 'DnA Workflows_executions_current{status="parked"}') == 1
    assert metric(text, 'DnA Workflows_holds_active{scope="dependency"}') == 1
    assert metric(text, "DnA Workflows_work_parked_current") == 1
    assert metric(text, 'DnA Workflows_journal_export_lag{sink="prometheus"}') == 0

    hold = (await shell.get(HoldService).list(active_only=True))[0]
    Unstable.fail = False
    await shell.get(HoldService).release(hold.id, actor="ops")
    await shell.get(JournalExporter).export()
    text = shell.get(DnA WorkflowsMetrics).render().decode()
    assert metric(text, 'DnA Workflows_holds_total{action="released",scope="dependency",source="auto-trip"}') == 1
    assert metric(text, 'DnA Workflows_executions_total{status="completed",workflow="sync"}') == 1


async def test_prometheus_message_and_intent_counters(make_shell, broker):
    shell = make_shell({"messaging": {"endpoints": {**ENDPOINTS, "orders": {**ENDPOINTS["orders"], "consume": True}}},
                        "messaging.inmemory": {}, "observability": {"sinks": []}, "observability.prometheus": {},
                        "resilience": {}})
    await shell.get(ConsumerHost).start()
    await shell.get(WorkflowRegistry).publish(
        WorkflowBuilder("s").add("s", "SendMessage", endpoint="invoices", type="t").build())
    await shell.get(WorkflowRuntime).start("s")
    await shell.get(OutboxProcessor).process_due()
    await shell.get(MessageBus).publish("orders", "nobody", {})
    await broker.idle()
    hold = await shell.get(HoldService).place("ingress", "orders", "x")
    await shell.get(ConsumerHost).wait_idle()
    await shell.get(JournalExporter).export()
    text = shell.get(DnA WorkflowsMetrics).render().decode()
    assert metric(text, 'DnA Workflows_outbox_intents_total{intent_kind="messaging.send",result="delivered"}') == 1
    assert metric(text, 'DnA Workflows_messages_unmatched_total{endpoint="orders"}') == 1
    assert metric(text, 'DnA Workflows_ingress_transitions_total{action="paused",endpoint="orders"}') == 1
    assert hold.scope.value == "ingress"
    await shell.get(ConsumerHost).stop()


async def test_prometheus_standalone_server_option(make_shell, monkeypatch):
    started = []
    import DnA Workflows.observability.prometheus as prom
    monkeypatch.setattr(prom, "start_http_server", lambda port, registry: started.append((port, registry)))
    shell = make_shell({"observability.prometheus": {"port": 9464}})
    for service in shell.provider.get_all(HostedService):
        if type(service).__name__ == "CallbackService":
            await service.start()
    assert started and started[0][0] == 9464


# ------------------------------------------------------------------------------------------------------
# OpenTelemetry: one trace across two nodes and a broker
# ------------------------------------------------------------------------------------------------------

@pytest.fixture
def otel_node(make_shell):
    async def build(consume, spans, logs, extra=None):
        def configure(services):
            services.add_contributor(SpanExporterRef, SpanExporterRef(exporter=spans, batch=False))
            services.add_contributor(LogExporterRef, LogExporterRef(exporter=logs, batch=False))
        endpoints = {n: {**c, "consume": n in consume} for n, c in ENDPOINTS.items()}
        shell = make_shell({"messaging": {"endpoints": endpoints}, "messaging.inmemory": {},
                            "observability": {"sinks": []}, "observability.otel": {"synchronous": True},
                            **(extra or {})}, configure=configure)
        await shell.get(ConsumerHost).start()
        return shell
    return build


async def test_one_trace_across_nodes_with_real_parent_chain(otel_node, broker):
    spans, logs = InMemorySpanExporter(), InMemoryLogRecordExporter()
    sales = await otel_node(["orders"], spans, logs)
    billing = await otel_node(["invoices"], spans, logs)
    await sales.get(WorkflowRegistry).publish(
        WorkflowBuilder("take").add("r", "MessageReceived", endpoint="orders")
        .then("s", "SendMessage", endpoint="invoices", type="invoice", data={"$output": "r.data"}).build())
    await billing.get(WorkflowRegistry).publish(
        WorkflowBuilder("bill").add("r", "MessageReceived", endpoint="invoices").then("l", "Log", message="ok").build())

    upstream = TraceContext.new_root()   # e.g. the web shop's HTTP request
    bus = sales.get(MessageBus)
    await bus.send("orders", bus.envelope("order.placed", {"id": 1}, trace=upstream, correlation_id="o-1"))
    for _ in range(3):
        await broker.idle()
        await sales.get(OutboxProcessor).process_due()
    await broker.idle()

    finished = spans.get_finished_spans()
    by_name = {}
    for s in finished:
        by_name.setdefault(s.name, []).append(s)
    assert {s.context.trace_id for s in finished} == {int(upstream.trace_id, 16)}   # ONE trace
    start_take = by_name["workflow.start take"][0]
    assert format(start_take.parent.span_id, "016x") == upstream.span_id
    send = by_name["send messaging.send"][0]
    start_bill = by_name["workflow.start bill"][0]
    assert start_bill.parent.span_id == send.context.span_id           # consumer is child of producer
    log_bill = next(s for s in by_name["activity Log"])
    assert log_bill.attributes["DnA Workflows.workflow"] == "bill"
    assert log_bill.attributes["DnA Workflows.correlation_id"] == "o-1"
    assert send.attributes["messaging.system"] == "inmemory" and send.kind.name == "PRODUCER"

    [ex_bill] = await billing.get(ExecutionStore).list(definition_id="bill")
    await billing.get(JournalExporter).export()
    records = [r.log_record for r in logs.get_finished_logs()]
    started = next(r for r in records if r.attributes.get("DnA Workflows.kind") == "execution.started"
                   and r.attributes.get("DnA Workflows.workflow") == "bill")
    assert format(started.trace_id, "032x") == upstream.trace_id
    assert started.attributes["DnA Workflows.correlation_id"] == "o-1" and ex_bill.correlation_id == "o-1"
    for shell in (sales, billing):
        await shell.get(ConsumerHost).stop()
        shell.get(OtelRuntime).shutdown()


async def test_activity_errors_and_delivery_errors_are_recorded(make_shell, broker):
    spans = InMemorySpanExporter()
    Unstable.fail = True
    shell = make_shell({"messaging": {"endpoints": ENDPOINTS}, "messaging.inmemory": {},
                        "observability": {"sinks": []}, "observability.otel": {"synchronous": True},
                        "resilience": {}},
                       configure=lambda s: (add_activity(s, Unstable),
                                            s.add_contributor(SpanExporterRef, SpanExporterRef(exporter=spans))))
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("u").add("c", "Unstable").build())
    await shell.get(WorkflowRegistry).publish(
        WorkflowBuilder("e").add("w", "Event", name="go").then("s", "SendMessage", endpoint="orders", type="t").build())
    rt = shell.get(WorkflowRuntime)
    await rt.start("u")
    ex = await rt.start("e")
    from DnA Workflows.runtime import StimulusRouter
    await StimulusRouter.publish_event(shell.get(StimulusRouter), "go", {})
    broker.set_available(False)
    await shell.get(OutboxProcessor).process_due()
    by_name = {s.name: s for s in spans.get_finished_spans()}
    act = by_name["activity Unstable"]
    assert act.status.status_code.name == "ERROR" and act.events[0].name == "exception"
    assert by_name["send messaging.send"].status.status_code.name == "ERROR"
    assert by_name["activity Event"].attributes["DnA Workflows.outcomes"] == ("Done",)
    assert "workflow.resume e" in by_name
    resume = by_name["workflow.resume e"]
    assert format(resume.context.trace_id, "032x") == ex.trace_id
    shell.get(OtelRuntime).shutdown()


async def test_suspend_attribute_console_global_and_otlp_configuration(make_shell, monkeypatch):
    spans = InMemorySpanExporter()
    shell = make_shell({"observability": {"sinks": []},
                        "observability.otel": {"synchronous": True, "console": True, "service_namespace": "acme",
                                               "otlp_endpoint": "http://127.0.0.1:9/", "otlp_timeout": 0.2,
                                               "set_global": True}},
                       configure=lambda s: s.add_contributor(SpanExporterRef, SpanExporterRef(exporter=spans)))
    runtime = shell.get(OtelRuntime)
    assert runtime.logger_provider is not None   # OTLP logs enabled
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("w").add("e", "Event", name="x").build())
    await shell.get(WorkflowRuntime).start("w")
    [span] = [s for s in spans.get_finished_spans() if s.name == "activity Event"]
    assert span.attributes["DnA Workflows.suspended_on"] == "event:x"
    assert span.resource.attributes["service.namespace"] == "acme"
    for hosted in shell.provider.get_all(HostedService):
        if type(hosted).__name__ == "CallbackService":
            await hosted.start()
            await hosted.stop()   # lifecycle shutdown flushes and closes providers


def test_severity_mapping():
    assert severity_for("work.faulted")[1] == "ERROR"
    assert severity_for("intent.held")[1] == "WARN"
    assert severity_for("activity.completed")[1] == "INFO"


async def test_otel_logs_noop_without_log_exporters(make_shell):
    from DnA Workflows.observability.otel import OtelLogJournalSink
    sink = OtelLogJournalSink(None)
    await sink.write([object()])  # no provider: ignored


# ------------------------------------------------------------------------------------------------------
# Azure Monitor
# ------------------------------------------------------------------------------------------------------

async def test_azure_monitor_feature_adds_exporters(make_shell, monkeypatch):
    from DnA Workflows.observability.azure_monitor import AzureMonitorFeature
    spans, logs = InMemorySpanExporter(), InMemoryLogRecordExporter()
    seen = []
    monkeypatch.setattr(AzureMonitorFeature, "trace_exporter_factory", staticmethod(lambda cs: seen.append(cs) or spans))
    monkeypatch.setattr(AzureMonitorFeature, "log_exporter_factory", staticmethod(lambda cs: logs))
    shell = make_shell({"observability": {"sinks": []},
                        "observability.otel": {"synchronous": True},
                        "observability.azure_monitor": {"connection_string": "InstrumentationKey=abc"}})
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("w").add("a", "Log", message="x").build())
    await shell.get(WorkflowRuntime).start("w", correlation_id="az-1")
    await shell.get(JournalExporter).export()
    assert seen == ["InstrumentationKey=abc"]
    assert any(s.name == "activity Log" for s in spans.get_finished_spans())
    assert any(r.log_record.attributes.get("DnA Workflows.correlation_id") == "az-1" for r in logs.get_finished_logs())


def test_real_azure_exporters_construct_offline(monkeypatch):
    monkeypatch.setenv("APPLICATIONINSIGHTS_STATSBEAT_DISABLED_ALL", "true")  # no self-telemetry calls home
    from DnA Workflows.observability.azure_monitor import _default_log_exporter, _default_trace_exporter
    cs = "InstrumentationKey=00000000-0000-0000-0000-000000000000;IngestionEndpoint=https://westeurope-5.in.applicationinsights.azure.com/"
    assert type(_default_trace_exporter(cs)).__name__ == "AzureMonitorTraceExporter"
    assert type(_default_log_exporter(cs)).__name__ == "AzureMonitorLogExporter"
