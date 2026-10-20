"""End-to-end behaviour of the operational lifecycle (P3): no hammering, no dropping."""


import pytest

from DnA Workflows.kernel import ManualClock
from DnA Workflows.persistence.contracts import JournalStore, OutboxStore
from DnA Workflows.resilience import (HoldScope, HoldService, HoldStatus, ProbeResult, ProposalService,
                                 ProposalStatus, ReleaseAction, ReleaseCoordinator, ReleaseRule)
from DnA Workflows.resilience.feature import add_health_probe, add_release_rule
from DnA Workflows.runtime import (Activity, ActivityInputs, Done, ExecutionStatus, IntentHandler, OutboxProcessor,
                              TimerService, WorkflowBuilder, WorkflowRegistry, WorkflowRuntime, WorkState,
                              add_activity)
from DnA Workflows.runtime.models import IntentStatus


class Dependency:
    """A fake downstream system that can be switched off."""

    def __init__(self):
        self.up = True
        self.calls = 0

    def call(self):
        self.calls += 1
        if not self.up:
            raise ConnectionError("connection refused")


DEP = Dependency()


class CallSap(Activity):
    type_name = "CallSap"

    class Inputs(ActivityInputs):
        order: int = 0

    def dependency(self, inputs, node):
        return "http:sap-prod"

    async def execute(self, context):
        DEP.call()
        context.emit("notify", {"order": context.inputs.order}, dependency="rabbitmq:notifications")
        return Done(output={"booked": context.inputs.order})


class Notifier:
    kind = "notify"

    def __init__(self):
        self.up = True
        self.sent = []

    async def handle(self, intent):
        if not self.up:
            raise ConnectionError("broker down")
        self.sent.append(intent.payload)


class Probe:
    def __init__(self, dependency: Dependency, prefix="http:sap"):
        self.dep = dependency
        self.prefix = prefix
        self.checks = 0

    def supports(self, dependency):
        return dependency.startswith(self.prefix)

    async def check(self, dependency):
        self.checks += 1
        return ProbeResult(self.dep.up, "200 OK" if self.dep.up else "refused")


@pytest.fixture(autouse=True)
def reset_dep():
    DEP.up = True
    DEP.calls = 0


@pytest.fixture
def env(make_shell):
    def build(resilience=None, probe=True, rules=()):
        clock = ManualClock()
        notifier = Notifier()
        probe_obj = Probe(DEP)

        def configure(services):
            add_activity(services, CallSap)
            services.add_contributor(IntentHandler, notifier)
            if probe:
                add_health_probe(services, probe_obj)
            for rule in rules:
                add_release_rule(services, rule)
        cfg = {"retry": {"dependency_unavailable": {"max_attempts": 3, "base_delay": 10, "jitter": 0}}}
        cfg.update(resilience or {})
        shell = make_shell({"resilience": cfg}, clock=clock, configure=configure)
        return shell, clock, notifier, probe_obj
    return build


async def start_orders(shell, n=2):
    await shell.get(WorkflowRegistry).publish(
        WorkflowBuilder("book").add("sap", "CallSap", order={"$input": "order"}).build())
    rt = shell.get(WorkflowRuntime)
    return [await rt.start("book", input={"order": i}) for i in range(1, n + 1)]


async def drive(shell, clock, seconds):
    clock.advance(seconds)
    await shell.get(TimerService).run_due()


async def test_budget_then_trip_then_park_everything_then_probe_auto_release(env):
    shell, clock, notifier, probe = env(resilience={"release": {"rules": [
        {"pattern": "http:sap-*", "strategy": "probe-auto", "options": {"successes_required": 2}}]}})
    rt = shell.get(WorkflowRuntime)
    holds = shell.get(HoldService)
    DEP.up = False

    [first] = await start_orders(shell, 1)
    assert first.status is ExecutionStatus.RUNNING and first.work[0].state is WorkState.RETRYING
    await drive(shell, clock, 10)          # attempt 2
    await drive(shell, clock, 20)          # attempt 3 -> budget exhausted -> trip
    first = await rt.get(first.id)
    assert first.status is ExecutionStatus.PARKED and DEP.calls == 3
    [hold] = await holds.list(active_only=True)
    assert hold.scope is HoldScope.DEPENDENCY and hold.target == "http:sap-prod"
    assert hold.source == "auto-trip" and hold.release_strategy == "probe-auto"
    assert first.parked()[0].hold_id == hold.id

    # a new order does not even touch the dependency: the gate parks it immediately
    rt2 = await rt.start("book", input={"order": 99})
    assert rt2.status is ExecutionStatus.PARKED and DEP.calls == 3

    coordinator = shell.get(ReleaseCoordinator)
    [(_, decision)] = await coordinator.evaluate_all()
    assert decision.action is ReleaseAction.KEEP and "streak 0/2" in decision.reason
    DEP.up = True
    [(_, decision)] = await coordinator.evaluate_all()
    assert decision.action is ReleaseAction.KEEP and "streak 1/2" in decision.reason
    [(_, decision)] = await coordinator.evaluate_all()
    assert decision.action is ReleaseAction.RELEASE

    hold = await holds.get(hold.id)
    assert hold.status is HoldStatus.RELEASED and hold.released_by == "strategy:probe-auto"
    assert hold.resumed_work == 2
    assert (await rt.get(first.id)).status is ExecutionStatus.COMPLETED
    assert (await rt.get(rt2.id)).status is ExecutionStatus.COMPLETED
    assert DEP.calls == 5  # exactly one more call per order: no hammering

    kinds = [e.kind for e in await shell.get(JournalStore).query(execution_id=first.id)]
    assert kinds.count("work.retry_scheduled") == 2
    assert "work.parked" in kinds and "work.resumed" in kinds and kinds[-1] == "execution.completed"
    control = [e.kind for e in await shell.get(JournalStore).query(kinds=["hold.placed", "hold.released"])]
    assert control == ["hold.placed", "hold.released"]


async def test_outbox_intents_are_held_and_released_with_the_dependency(env):
    shell, clock, notifier, _ = env(resilience={"retry": {
        "dependency_unavailable": {"max_attempts": 1, "jitter": 0}}})
    holds = shell.get(HoldService)
    outbox = shell.get(OutboxProcessor)
    await start_orders(shell, 2)
    notifier.up = False
    await outbox.process_due()                   # first failure trips rabbitmq hold, second is gated
    intents = await shell.get(OutboxStore).list()
    assert {i.status for i in intents} == {IntentStatus.HELD}
    [hold] = await holds.list(active_only=True)
    assert hold.target == "rabbitmq:notifications" and hold.release_strategy == "manual"
    assert {i.hold_id for i in intents} == {hold.id}

    notifier.up = True
    hold = await holds.release(hold.id, actor="ops", reason="broker back")
    assert hold.released_intents == 2
    assert await outbox.process_due() == 2
    assert sorted(p["order"] for p in notifier.sent) == [1, 2]
    assert (await holds.release(hold.id)).status is HoldStatus.RELEASED  # idempotent


async def test_probe_recommend_creates_one_proposal_and_approval_releases(env):
    shell, clock, _, _ = env(rules=[ReleaseRule("http:sap-*", "probe-recommend", {"successes_required": 1})])
    holds = shell.get(HoldService)
    proposals = shell.get(ProposalService)
    DEP.up = False
    await start_orders(shell, 1)
    await drive(shell, clock, 10)
    await drive(shell, clock, 20)
    [hold] = await holds.list(active_only=True)
    assert hold.release_strategy == "probe-recommend"  # contributed by a "module" rule

    DEP.up = True
    coordinator = shell.get(ReleaseCoordinator)
    await coordinator.evaluate_all()
    await coordinator.evaluate_all()               # still recommending: no duplicate proposal
    [proposal] = await proposals.list(status=ProposalStatus.PENDING)
    assert proposal.kind == "hold.release" and proposal.target == {"hold_id": hold.id}
    assert (await holds.get(hold.id)).status is HoldStatus.ACTIVE  # nothing happens without approval

    approved = await proposals.approve(proposal.id, actor="alice", note="checked SAP status page")
    assert approved.status is ProposalStatus.EXECUTED and approved.result["resumed_work"] == 1
    assert (await holds.get(hold.id)).released_by == "alice"


async def test_configured_rules_win_over_module_rules_and_ttl_release(env):
    shell, clock, _, _ = env(
        resilience={"release": {"rules": [{"pattern": "http:*", "strategy": "ttl", "options": {"seconds": 60}}]}},
        rules=[ReleaseRule("http:sap-*", "probe-auto")])
    holds = shell.get(HoldService)
    hold = await holds.place("dependency", "http:sap-prod", "maintenance window", actor="ops")
    assert hold.release_strategy == "ttl"
    coordinator = shell.get(ReleaseCoordinator)
    assert (await coordinator.evaluate(hold)).action is ReleaseAction.KEEP
    clock.advance(61)
    hold = await holds.get(hold.id)
    assert (await coordinator.evaluate(hold)).action is ReleaseAction.RELEASE
    assert (await holds.get(hold.id)).status is HoldStatus.RELEASED


async def test_manual_holds_on_every_scope(env):
    shell, clock, notifier, _ = env()
    rt = shell.get(WorkflowRuntime)
    holds = shell.get(HoldService)
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("book").add("sap", "CallSap").build())

    for scope, target in [("global", "*"), ("workflow", "bo*"), ("activity_type", "CallSap")]:
        hold = await holds.place(scope, target, "investigating", actor="ops")
        again = await holds.place(scope, target, "dup")
        assert again.id == hold.id   # idempotent
        ex = await rt.start("book")
        assert ex.status is ExecutionStatus.PARKED and ex.parked()[0].hold_id == hold.id
        await holds.release(hold.id, actor="ops")
        assert (await rt.get(ex.id)).status is ExecutionStatus.COMPLETED

    ex = await rt.start("book", run=False)
    hold = await holds.place("execution", ex.id, "look at this one")
    assert (await rt.run(ex.id)).status is ExecutionStatus.PARKED
    await holds.release(hold.id)
    assert (await rt.get(ex.id)).status is ExecutionStatus.COMPLETED

    # ingress holds never park work, other scopes never block ingress
    from DnA Workflows.runtime.contracts import GateRequest
    ingress = await holds.place("ingress", "orders.*", "pause consumption")
    assert (await holds.match(GateRequest(subject="ingress", endpoint="orders.created"))).id == ingress.id
    assert await holds.match(GateRequest(subject="work", definition_id="book")) is None
    wf_hold = await holds.place("workflow", "book", "x")
    assert await holds.match(GateRequest(subject="ingress", endpoint="other")) is None
    assert await holds.match(GateRequest(subject="intent", execution_id="e")) is None
    await holds.release(wf_hold.id)
    with pytest.raises(LookupError):
        await holds.get("missing")


async def test_resume_failure_of_one_execution_does_not_block_release(env, monkeypatch):
    shell, clock, _, _ = env()
    holds = shell.get(HoldService)
    rt = shell.get(WorkflowRuntime)
    hold = await holds.place("global", "*", "freeze")
    await start_orders(shell, 2)

    original = rt.resume_parked
    calls = []

    async def flaky_resume(execution_id, *args, **kwargs):
        calls.append(execution_id)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return await original(execution_id, *args, **kwargs)

    monkeypatch.setattr(rt, "resume_parked", flaky_resume)
    released = await holds.release(hold.id)
    assert released.resumed_work == 1 and len(calls) == 2


async def test_proposal_lifecycle_errors(env):
    shell, _, _, _ = env()
    proposals = shell.get(ProposalService)
    rt = shell.get(WorkflowRuntime)
    with pytest.raises(ValueError, match="No handler"):
        await proposals.propose("nope", {}, "x", proposed_by="me")
    p = await proposals.propose("hold.release", {"hold_id": "missing"}, "try", proposed_by="ai:triage",
                                confidence=0.4, evidence={"why": "probe ok"})
    failed = await proposals.approve(p.id, actor="bob")
    assert failed.status is ProposalStatus.FAILED and "HoldNotFound" in failed.result["error"]
    from DnA Workflows.resilience.proposals import ProposalNotFound, ProposalStateError
    with pytest.raises(ProposalStateError):
        await proposals.reject(p.id, actor="bob")
    with pytest.raises(ProposalNotFound):
        await proposals.get("nope")

    holds = shell.get(HoldService)
    await holds.place("global", "*", "freeze")
    [ex] = await start_orders(shell, 1)
    item = ex.parked()[0]
    p2 = await proposals.propose("work.abandon", {"execution_id": ex.id, "work_item_id": item.id},
                                 "duplicate order", proposed_by="ai:triage")
    rejected = await proposals.reject(p2.id, actor="bob", note="not a duplicate")
    assert rejected.status is ProposalStatus.REJECTED
    p3 = await proposals.propose("work.resume", {"execution_id": ex.id}, "resume it", proposed_by="me")
    assert (await proposals.approve(p3.id, actor="bob")).result == {"resumed": [item.id]}
    # still held globally -> parked again by the gate; abandon it via a proposal
    ex = await rt.get(ex.id)
    p4 = await proposals.propose("work.abandon", {"execution_id": ex.id, "work_item_id": ex.parked()[0].id},
                                 "give up", proposed_by="me")
    assert (await proposals.approve(p4.id, actor="bob")).result == {"execution_status": "cancelled"}
    assert len(await proposals.list()) == 4


async def test_unknown_strategy_is_rejected_and_kept(env, make_shell):
    with pytest.raises(ValueError, match="Unknown release strategy"):
        shell = make_shell({"resilience": {"release": {"default": "nope"}}})
        shell.get(HoldService)
        await shell.get(HoldService).place("global", "*", "x")
    shell, _, _, _ = env()
    holds = shell.get(HoldService)
    hold = await holds.place("dependency", "x:y", "r", release_strategy="custom-missing")
    decision = await shell.get(ReleaseCoordinator).evaluate(hold)
    assert decision.action is ReleaseAction.KEEP and "unknown strategy" in decision.reason


async def test_probe_strategy_edge_cases_and_interval(env):
    shell, clock, _, probe = env(probe=False)
    holds = shell.get(HoldService)
    coordinator = shell.get(ReleaseCoordinator)
    no_probe = await holds.place("dependency", "http:other", "x", release_strategy="probe-auto",
                                 release_options={"interval": 30})
    assert "no health probe" in (await coordinator.evaluate(no_probe)).reason
    assert (await coordinator.evaluate(await holds.get(no_probe.id))).reason == "not due"
    clock.advance(31)
    assert "no health probe" in (await coordinator.evaluate(await holds.get(no_probe.id))).reason
    wrong_scope = await holds.place("workflow", "w", "x", release_strategy="probe-auto")
    assert "only apply" in (await coordinator.evaluate(wrong_scope)).reason

    class Crashing:
        def supports(self, dependency):
            return True

        async def check(self, dependency):
            raise RuntimeError("dns")

    from DnA Workflows.resilience.release import ProbeRegistry
    result = await ProbeRegistry([Crashing()]).check("x")
    assert not result.healthy and "RuntimeError" in result.detail


async def test_crash_between_release_and_resume_is_healed_by_the_coordinator(env):
    """The hold is marked released, but the process 'dies' before resuming the parked work."""
    shell, clock, _, _ = env()
    holds = shell.get(HoldService)
    hold = await holds.place("global", "*", "freeze")
    [ex] = await start_orders(shell, 1)
    assert ex.status is ExecutionStatus.PARKED
    stored = await holds.get(hold.id)
    stored.status = HoldStatus.RELEASED                     # simulate: status saved, resume never ran
    await holds.update_strategy_state(stored)
    coordinator = shell.get(ReleaseCoordinator)
    await coordinator.evaluate_all()
    assert coordinator.orphans_resumed == 1
    assert (await shell.get(WorkflowRuntime).get(ex.id)).status is ExecutionStatus.COMPLETED
    await coordinator.evaluate_all()
    assert coordinator.orphans_resumed == 0


async def test_release_resumes_in_parking_order(env):
    shell, clock, _, _ = env()
    holds = shell.get(HoldService)
    hold = await holds.place("dependency", "http:sap-prod", "freeze")
    executions = []
    for order in (1, 2, 3):
        executions.extend(await start_orders(shell, 1) if order == 1 else [
            await shell.get(WorkflowRuntime).start("book", input={"order": order})])
        clock.advance(1)
    await holds.release(hold.id)
    completed = [e for e in await shell.get(JournalStore).query(kinds=["execution.completed"])]
    assert [e.execution_id for e in completed] == [e.id for e in executions]
