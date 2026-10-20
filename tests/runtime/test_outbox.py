from datetime import timedelta

import pytest

from DnA Workflows.kernel import ManualClock
from DnA Workflows.kernel.hosting import HostedService
from DnA Workflows.persistence.contracts import JournalStore, OutboxStore
from DnA Workflows.runtime import (Activity, Done, IntentHandler, OutboxProcessor, WorkflowBuilder,
                              WorkflowRegistry, WorkflowRuntime, add_activity)
from DnA Workflows.runtime.contracts import (Fail, FailurePolicy, GateDecision, Park, RetryAfter,
                                        RuntimeSignals, WorkGate)
from DnA Workflows.runtime.models import IntentStatus
from DnA Workflows.tracing import current_trace


class Emit(Activity):
    type_name = "Emit"

    async def execute(self, context):
        context.emit("test.send", {"n": 1}, dependency="rabbitmq:orders")
        return Done()


class Recorder:
    kind = "test.send"

    def __init__(self):
        self.fail_with: Exception | None = None
        self.delivered = []
        self.traces = []

    async def handle(self, intent):
        self.traces.append(current_trace())
        if self.fail_with is not None:
            raise self.fail_with
        self.delivered.append(intent)


class Policy:
    def __init__(self, *decisions):
        self.decisions = list(decisions)
        self.seen = []

    async def decide(self, ctx):
        self.seen.append(ctx)
        return self.decisions.pop(0)


class Gate:
    held = False

    async def evaluate(self, request):
        return GateDecision("h1", "held") if self.held and request.subject == "intent" else None


@pytest.fixture
def env(make_shell):
    def build(policy=None, gate=None):
        clock = ManualClock()
        recorder = Recorder()

        def configure(services):
            add_activity(services, Emit)
            services.add_contributor(IntentHandler, recorder)
            if policy:
                services.add_instance(FailurePolicy, policy)
            if gate:
                services.add_instance(WorkGate, gate)
        shell = make_shell({"runtime": {}}, clock=clock, configure=configure)
        return shell, recorder, clock
    return build


async def run_emit(shell):
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("emit").add("e", "Emit").build())
    return await shell.get(WorkflowRuntime).start("emit", correlation_id="corr")


async def test_intent_committed_with_checkpoint_and_delivered_with_trace(env):
    shell, recorder, _ = env()
    ex = await run_emit(shell)
    outbox = shell.get(OutboxStore)
    [intent] = await outbox.list()
    assert intent.status is IntentStatus.PENDING and intent.execution_id == ex.id
    assert intent.correlation_id == "corr" and intent.traceparent.split("-")[1] == ex.trace_id

    assert await shell.get(OutboxProcessor).process_due() == 1
    assert recorder.delivered[0].id == intent.id
    assert recorder.traces[0].trace_id == ex.trace_id  # ambient trace set during delivery
    assert (await outbox.get(intent.id)).status is IntentStatus.DELIVERED
    kinds = [e.kind for e in await shell.get(JournalStore).query(correlation_id="corr")]
    assert "intent.delivered" in kinds
    assert await shell.get(OutboxProcessor).process_due() == 0


async def test_failed_delivery_retry_then_final_failure_and_redelivery(env):
    shell, recorder, clock = env(Policy(RetryAfter(timedelta(seconds=5)), Fail("gave up")))
    await run_emit(shell)
    proc = shell.get(OutboxProcessor)
    recorder.fail_with = ConnectionError("down")
    await proc.process_due()
    [intent] = await shell.get(OutboxStore).list()
    assert intent.status is IntentStatus.PENDING and intent.attempts == 1
    assert intent.last_fault.error_type == "ConnectionError"
    assert await proc.process_due() == 0  # not due yet
    clock.advance(5)
    await proc.process_due()
    intent = await shell.get(OutboxStore).get(intent.id)
    assert intent.status is IntentStatus.FAILED

    recorder.fail_with = None
    assert await proc.redeliver_failed(intent.id, actor="ops") is True
    assert await proc.redeliver_failed(intent.id) is False
    assert await proc.redeliver_failed("missing") is False
    assert await proc.process_due() == 1


async def test_gate_holds_intents_until_released(env):
    gate = Gate()
    gate.held = True
    shell, recorder, _ = env(gate=gate)
    await run_emit(shell)
    proc = shell.get(OutboxProcessor)
    await proc.process_due()
    [intent] = await shell.get(OutboxStore).list()
    assert intent.status is IntentStatus.HELD and intent.hold_id == "h1" and recorder.delivered == []
    assert await proc.release(hold_id="other") == []
    assert await proc.release(intent_ids=["x"]) == []
    gate.held = False
    assert await proc.release(hold_id="h1", actor="ops") == [intent.id]
    assert await proc.process_due() == 1


async def test_park_decision_holds_intent_and_unknown_kind_fails(env):
    shell, recorder, _ = env(Policy(Park("dependency down", hold_id="auto-1")))
    await run_emit(shell)
    recorder.fail_with = ConnectionError("down")
    proc = shell.get(OutboxProcessor)
    await proc.process_due()
    [intent] = await shell.get(OutboxStore).list()
    assert intent.status is IntentStatus.HELD and intent.hold_id == "auto-1"

    store = shell.get(OutboxStore)
    intent.kind = "unknown.kind"
    intent.status = IntentStatus.PENDING
    await store.update(intent)
    await proc.process_due()
    assert (await store.get(intent.id)).status is IntentStatus.FAILED


async def test_duplicate_handler_kinds_rejected_and_wake_signal(make_shell):
    class Other:
        kind = "test.send"

        async def handle(self, intent):
            pass

    signals = RuntimeSignals()
    with pytest.raises(ValueError, match="Two handlers"):
        OutboxProcessor(store=None, journal=None, handlers=[Recorder(), Other()], policy=None,
                        gate=None, classifiers=[], clock=None, ids=None, signals=signals)
    proc = OutboxProcessor(store=None, journal=None, handlers=[Recorder(), Recorder()], policy=None,
                           gate=None, classifiers=[], clock=None, ids=None, signals=signals)
    signals.notify("outbox")  # wake before start is a harmless no-op
    assert len(proc.handlers) == 1


async def test_background_services_deliver_without_manual_ticks(make_shell):
    import asyncio
    recorder = Recorder()
    shell = make_shell({"runtime": {"background_services": True, "outbox_interval": 5}},
                       configure=lambda s: (add_activity(s, Emit), s.add_contributor(IntentHandler, recorder)))
    async with shell:
        assert any(isinstance(h, OutboxProcessor) for h in shell.provider.get_all(HostedService))
        await run_emit(shell)
        for _ in range(50):  # woken by the commit signal, far before the 5s interval
            if recorder.delivered:
                break
            await asyncio.sleep(0.01)
    assert len(recorder.delivered) == 1
