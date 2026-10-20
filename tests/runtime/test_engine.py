from datetime import timedelta

import pytest

from DnA Workflows.kernel import ManualClock
from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore
from DnA Workflows.runtime import (Activity, ActivityContext, Done, ExecutionNotFound,
                              ExecutionStatus, FaultKind, Sleep, StimulusRouter, Suspend,
                              TimerService, WorkflowBuilder, WorkflowRegistry, WorkflowRuntime,
                              WorkState, add_activity, code)
from DnA Workflows.runtime.contracts import (Fail, FailurePolicy, GateDecision, GateRequest, Park,
                                        RetryAfter, WorkGate)
from DnA Workflows.runtime.lifecycle import InvalidTransition
from DnA Workflows.runtime.registry import DefinitionValidationError


class Flaky(Activity):
    """Fails `failures` times, then succeeds."""

    type_name = "Flaky"
    calls: dict[str, int] = {}

    from DnA Workflows.runtime.activity import ActivityInputs as _I

    class Inputs(_I):
        key: str
        failures: int = 1
        error: str = "connection"

    def dependency(self, inputs, node):
        return "http:flaky"

    async def execute(self, context: ActivityContext):
        k = context.inputs.key
        Flaky.calls[k] = Flaky.calls.get(k, 0) + 1
        if Flaky.calls[k] <= context.inputs.failures:
            if context.inputs.error == "connection":
                raise ConnectionError("refused")
            raise ValueError("bad data")
        return Done(output={"calls": Flaky.calls[k]})


class ScriptedPolicy:
    """Failure policy returning pre-programmed decisions."""

    def __init__(self, *decisions):
        self.decisions = list(decisions)
        self.seen = []

    async def decide(self, context):
        self.seen.append(context)
        return self.decisions.pop(0) if self.decisions else Fail("no more decisions")


class HoldGate:
    def __init__(self):
        self.held: set[str] = set()

    async def evaluate(self, request: GateRequest):
        if request.dependency in self.held:
            return GateDecision(hold_id="hold-1", reason=f"dependency {request.dependency} is held")
        return None


@pytest.fixture
def shell_with(make_shell):
    def factory(policy=None, gate=None, clock=None, runtime=None):
        def configure(services):
            add_activity(services, Flaky)
            if policy is not None:
                services.add_instance(FailurePolicy, policy)
            if gate is not None:
                services.add_instance(WorkGate, gate)
        return make_shell({"runtime": runtime or {}}, clock=clock, configure=configure)
    return factory


@pytest.fixture(autouse=True)
def reset_flaky():
    Flaky.calls.clear()


async def publish(shell, definition):
    return await shell.get(WorkflowRegistry).publish(definition)


async def test_linear_workflow_with_bindings_completes(shell_with):
    shell = shell_with()
    rt = shell.get(WorkflowRuntime)
    wf = (WorkflowBuilder("greet")
          .add("start", "Start")
          .then("name", "SetVariable", name="greeting", value={"$tmpl": "Hello {input.who}!"})
          .then("log", "Log", message={"$var": "greeting"}, data={"n": 1})
          .then("out", "SetOutput", value={"greeting": {"$output": "name"}, "corr": {"$ctx": "correlation_id"}})
          .build())
    await publish(shell, wf)
    ex = await rt.start("greet", input={"who": "Ada"}, correlation_id="order-1")
    assert ex.status is ExecutionStatus.COMPLETED
    assert ex.output == {"greeting": "Hello Ada!", "corr": "order-1"}
    assert ex.work == [] and ex.revision > 1

    journal = await shell.get(JournalStore).query(execution_id=ex.id)
    kinds = [e.kind for e in journal]
    assert kinds[0] == "execution.started" and kinds[-1] == "execution.completed"
    assert kinds.count("activity.completed") == 4
    log_event = next(e for e in journal if e.kind == "activity.log")
    assert log_event.data["message"] == "Hello Ada!" and log_event.data["n"] == 1
    # one trace id for the whole execution, every event correlated
    assert {e.trace_id for e in journal} == {ex.trace_id}
    assert {e.correlation_id for e in journal} == {"order-1"}
    assert {e.host for e in journal} == {"test-host"}


async def test_branching_switch_join_and_end(shell_with):
    shell = shell_with()
    rt = shell.get(WorkflowRuntime)
    wf = (WorkflowBuilder("branch")
          .add("start", "Start")
          .then("if", "If", condition={"$input": "vip"})
          .then("vip", "SetVariable", outcome="True", name="tier", value="gold")
          .then("std", "SetVariable", after="if", outcome="False", name="tier", value="basic")
          .add("switch", "Switch", value={"$var": "tier"}, cases=["gold"])
          .connect("vip", "switch").connect("std", "switch")
          .then("gold", "SetOutput", outcome="gold", value="G")
          .then("other", "SetOutput", after="switch", outcome="Default", value="O")
          .build())
    await publish(shell, wf)
    assert (await rt.start("branch", input={"vip": True})).output == "G"
    assert (await rt.start("branch", input={"vip": False})).output == "O"

    fork = (WorkflowBuilder("fork")
            .add("start", "Start")
            .then("a", "SetVariable", name="a", value=1)
            .then("b", "SetVariable", after="start", name="b", value=2)
            .add("join", "Join")
            .connect("a", "join").connect("b", "join")
            .then("end", "End", output={"$tmpl": "{var.a}+{var.b}"})
            .build())
    await publish(shell, fork)
    ex = await rt.start("fork")
    assert ex.status is ExecutionStatus.COMPLETED and ex.output == "1+2"
    assert ex.outputs["join"] == ["a", "b"]

    anyjoin = (WorkflowBuilder("anyjoin")
               .add("start", "Start")
               .then("a", "Start").then("b", "Start", after="start")
               .add("join", "Join", mode="any")
               .connect("a", "join").connect("b", "join")
               .then("count", "Code", fn=code(lambda c: c.set_var("n", c.get_var("n", 0) + 1)))
               .build())
    await publish(shell, anyjoin)
    ex = await rt.start("anyjoin")
    assert ex.variables["n"] == 1 and ex.join_arrivals == {}


async def test_event_bookmark_resume_and_correlation(shell_with):
    shell = shell_with()
    rt = shell.get(WorkflowRuntime)
    router = shell.get(StimulusRouter)
    wf = (WorkflowBuilder("approval")
          .add("start", "Start")
          .then("wait", "Event", name="approved", correlate=True)
          .then("done", "SetOutput", value={"$output": "wait"})
          .build())
    await publish(shell, wf)
    ex1 = await rt.start("approval", correlation_id="c1")
    ex2 = await rt.start("approval", correlation_id="c2")
    assert ex1.status is ExecutionStatus.WAITING and len(ex1.bookmarks) == 1

    result = await router.publish_event("approved", {"by": "bob"}, correlation_id="c2")
    assert result.resumed == [ex2.id] and result.started == []
    assert (await rt.get(ex2.id)).output == {"by": "bob"}
    assert (await rt.get(ex1.id)).status is ExecutionStatus.WAITING
    assert await rt.resume(ex2.id, "nope") is None  # bookmark gone / execution terminal


async def test_trigger_starts_workflow_and_inbox_dedups_redelivery(shell_with):
    shell = shell_with()
    router = shell.get(StimulusRouter)
    wf = (WorkflowBuilder("on-order")
          .add("trigger", "Event", name="order.created")
          .then("out", "SetOutput", value={"$output": "trigger.id"})
          .build())
    await publish(shell, wf)
    r1 = await router.publish_event("order.created", {"id": 7}, message_id="m-1", correlation_id="o7")
    r2 = await router.publish_event("order.created", {"id": 7}, message_id="m-1")
    assert len(r1.started) == 1 and r2.started == [] and r2.duplicates == 1
    ex = await shell.get(WorkflowRuntime).get(r1.started[0])
    assert ex.output == 7 and ex.correlation_id == "o7" and ex.input == {"id": 7}
    assert not (await router.publish_event("unknown")).matched

    # started directly, a root trigger waits for its event instead
    direct = await shell.get(WorkflowRuntime).start("on-order")
    assert direct.status is ExecutionStatus.WAITING


async def test_delay_is_a_durable_timer_driven_by_the_timer_service(shell_with):
    clock = ManualClock()
    shell = shell_with(clock=clock)
    rt = shell.get(WorkflowRuntime)
    timers = shell.get(TimerService)
    wf = WorkflowBuilder("delay").add("d", "Delay", seconds=30).then("o", "SetOutput", value="late").build()
    await publish(shell, wf)
    ex = await rt.start("delay")
    assert ex.status is ExecutionStatus.RUNNING and ex.work[0].not_before is not None
    assert await timers.run_due() == []
    clock.advance(31)
    assert await timers.run_due() == [ex.id]
    assert (await rt.get(ex.id)).output == "late"


async def test_retry_after_then_success_uses_timer_and_records_attempts(shell_with):
    clock = ManualClock()
    policy = ScriptedPolicy(RetryAfter(timedelta(seconds=10), "transient"))
    shell = shell_with(policy=policy, clock=clock)
    rt = shell.get(WorkflowRuntime)
    await publish(shell, WorkflowBuilder("f").add("call", "Flaky", key="a", failures=1).build())
    ex = await rt.start("f")
    item = ex.work[0]
    assert item.state is WorkState.RETRYING and item.attempt == 1
    assert item.last_fault.error_type == "ConnectionError"
    assert policy.seen[0].dependency == "http:flaky" and policy.seen[0].subject == "work"
    clock.advance(10)
    await shell.get(TimerService).run_due()
    ex = await rt.get(ex.id)
    assert ex.status is ExecutionStatus.COMPLETED and ex.outputs["call"] == {"calls": 2}
    kinds = [e.kind for e in await shell.get(JournalStore).query(execution_id=ex.id)]
    assert "work.retry_scheduled" in kinds


async def test_park_and_resume_parked_and_execution_status(shell_with):
    policy = ScriptedPolicy(Park("dependency unavailable", hold_id="h1"))
    shell = shell_with(policy=policy)
    rt = shell.get(WorkflowRuntime)
    await publish(shell, WorkflowBuilder("p").add("call", "Flaky", key="b", failures=1).build())
    ex = await rt.start("p")
    assert ex.status is ExecutionStatus.PARKED
    parked = ex.parked()[0]
    assert parked.hold_id == "h1" and parked.parked_reason == "dependency unavailable"
    assert await shell.get(ExecutionStore).find_parked(dependency="http:flaky") == [(ex.id, parked.id)]
    assert await shell.get(ExecutionStore).find_parked(hold_id="other") == []

    assert await rt.resume_parked(ex.id, hold_id="other") == []
    assert await rt.resume_parked(ex.id, hold_id="h1", reason="fixed", actor="ops") == [parked.id]
    ex = await rt.get(ex.id)
    assert ex.status is ExecutionStatus.COMPLETED
    events = await shell.get(JournalStore).query(execution_id=ex.id, kinds=["work.resumed"])
    assert events[0].data["actor"] == "ops" and events[0].data["hold_id"] == "h1"


async def test_gate_parks_before_execution_without_running_the_activity(shell_with):
    gate = HoldGate()
    gate.held.add("http:flaky")
    shell = shell_with(gate=gate)
    rt = shell.get(WorkflowRuntime)
    await publish(shell, WorkflowBuilder("g").add("call", "Flaky", key="c", failures=0).build())
    ex = await rt.start("g")
    assert ex.status is ExecutionStatus.PARKED and Flaky.calls == {}
    assert ex.parked()[0].hold_id == "hold-1" and ex.parked()[0].attempt == 0
    gate.held.clear()
    await rt.resume_parked(ex.id)
    assert (await rt.get(ex.id)).status is ExecutionStatus.COMPLETED


async def test_fault_then_operator_retry_and_abandon(shell_with):
    shell = shell_with()  # default policy: fault immediately
    rt = shell.get(WorkflowRuntime)
    await publish(shell, WorkflowBuilder("x").add("call", "Flaky", key="d", failures=1, error="value").build())
    ex = await rt.start("x")
    assert ex.status is ExecutionStatus.FAULTED
    assert ex.fault.error_type == "ValueError" and ex.fault.kind is FaultKind.UNKNOWN
    assert await rt.retry_faulted(ex.id, actor="ops") == [ex.work[0].id]
    assert (await rt.get(ex.id)).status is ExecutionStatus.COMPLETED
    assert await rt.retry_faulted(ex.id) == []

    ex = await rt.start("x")  # Flaky key d now succeeds -> completes; use new key
    await publish(shell, WorkflowBuilder("y").add("call", "Flaky", key="e", failures=5, error="value").build())
    ex = await rt.start("y")
    ex = await rt.abandon(ex.id, ex.work[0].id, reason="bad data", actor="ops")
    assert ex.status is ExecutionStatus.CANCELLED and ex.fault.message == "bad data"
    with pytest.raises(LookupError):
        await rt.abandon(ex.id, "missing")


async def test_abandon_one_branch_keeps_execution_alive(shell_with):
    shell = shell_with(policy=ScriptedPolicy(Park("down")))
    rt = shell.get(WorkflowRuntime)
    wf = (WorkflowBuilder("two")
          .add("start", "Start")
          .then("call", "Flaky", key="f", failures=1)
          .then("wait", "Event", after="start", name="go")
          .build())
    await publish(shell, wf)
    ex = await rt.start("two")
    assert ex.status is ExecutionStatus.PARKED
    ex = await rt.abandon(ex.id, ex.parked()[0].id)
    assert ex.status is ExecutionStatus.WAITING


async def test_cancel_and_terminal_noops(shell_with):
    shell = shell_with()
    rt = shell.get(WorkflowRuntime)
    await publish(shell, WorkflowBuilder("w").add("wait", "Event", name="never").build())
    ex = await rt.start("w")
    ex = await rt.cancel(ex.id, reason="not needed", actor="ops")
    assert ex.status is ExecutionStatus.CANCELLED and ex.bookmarks == []
    assert (await rt.cancel(ex.id)).status is ExecutionStatus.CANCELLED
    assert (await rt.run(ex.id)).status is ExecutionStatus.CANCELLED
    with pytest.raises(ExecutionNotFound):
        await rt.get("missing")


async def test_activity_failures_are_permanent_for_bad_inputs_and_bad_results(shell_with):
    shell = shell_with()
    rt = shell.get(WorkflowRuntime)

    class BadResult(Activity):
        type_name = "BadResult"

        async def execute(self, context):
            return "not a result"

    class BadOutput(Activity):
        type_name = "BadOutput"

        async def execute(self, context):
            context.set_var("leak", "should be rolled back")
            return Done(output=object())

    rt.activities.register(BadResult)
    rt.activities.register(BadOutput)
    wf = WorkflowBuilder("bad").add("s", "SetVariable", name={"$input": "missing"}).build()
    await publish(shell, wf)
    ex = await rt.start("bad")
    assert ex.status is ExecutionStatus.FAULTED and "Invalid inputs" in ex.fault.message
    assert ex.fault.kind is FaultKind.PERMANENT

    await publish(shell, WorkflowBuilder("bad2").add("r", "BadResult").build())
    assert "returned str" in (await rt.start("bad2")).fault.message

    await publish(shell, WorkflowBuilder("bad3").add("r", "BadOutput").build())
    ex = await rt.start("bad3")
    assert "not JSON-serialisable" in ex.fault.message and "leak" not in ex.variables

    await publish(shell, WorkflowBuilder("bad4").add("f", "Fail", message="nope", kind="throttled").build())
    assert (await rt.start("bad4")).fault.kind is FaultKind.THROTTLED


async def test_code_activity_can_return_results_and_await(shell_with):
    shell = shell_with()
    rt = shell.get(WorkflowRuntime)

    async def wait(ctx):
        return Suspend("event", "k") if not ctx.payload.get("stimulus") else Done(output="resumed")

    def nap(ctx):
        return Done(output="slept") if ctx.payload.get("woke") else Sleep.seconds(0)

    wf = (WorkflowBuilder("code")
          .add("c", "Code", fn=code(wait))
          .then("n", "Code", fn=code(nap))
          .then("bad", "Code", fn="not callable")
          .build())
    await publish(shell, wf)
    ex = await rt.start("code")
    assert ex.status is ExecutionStatus.WAITING
    await shell.get(StimulusRouter).publish_event("k", {"x": 1})
    ex = await rt.get(ex.id)
    assert ex.outputs["n"] == "slept" and ex.status is ExecutionStatus.FAULTED
    assert "callable" in ex.fault.message


async def test_validation_errors_on_publish(shell_with):
    shell = shell_with()
    registry = shell.get(WorkflowRegistry)
    from DnA Workflows.runtime.models import EdgeSpec, NodeSpec, WorkflowDefinition
    bad = WorkflowDefinition(id="bad", nodes={
        "a": NodeSpec(id="a", type="Nope"),
        "b": NodeSpec(id="x", type="Log", inputs={"message": "m", "extra": 1}),
        "c": NodeSpec(id="c", type="If", inputs={"condition": True}),
    }, edges=[EdgeSpec(source="c", target="zzz", outcome="Maybe"), EdgeSpec(source="c", target="c", outcome="True")])
    with pytest.raises(DefinitionValidationError) as err:
        await registry.publish(bad)
    text = str(err.value)
    for fragment in ("unknown activity type 'Nope'", "does not match", "Invalid inputs", "unknown node 'zzz'",
                     "outcome 'Maybe'"):
        assert fragment in text
    with pytest.raises(DefinitionValidationError, match="no nodes"):
        await registry.publish(WorkflowDefinition(id="empty"))
    cyc = WorkflowDefinition(id="cyc", nodes={"a": NodeSpec(id="a", type="Start")},
                             edges=[EdgeSpec(source="a", target="a")])
    with pytest.raises(DefinitionValidationError, match="no root"):
        await registry.publish(cyc)
    with pytest.raises(LookupError):
        await registry.get("missing")
    with pytest.raises(LookupError):
        await shell.get(WorkflowRuntime).start("missing")


async def test_versions_persistence_reload_and_start_node_validation(shell_with):
    shell = shell_with()
    registry = shell.get(WorkflowRegistry)
    v1 = WorkflowBuilder("v", version=1).add("o", "SetOutput", value=1).build()
    v2 = WorkflowBuilder("v", version=2).add("o", "SetOutput", value=2).build()
    await registry.publish(v1)
    await registry.publish(v2)
    rt = shell.get(WorkflowRuntime)
    assert (await rt.start("v")).output == 2
    assert (await rt.start("v", version=1)).output == 1
    assert [d.version for d in registry.list()] == [2]
    with pytest.raises(LookupError):
        await rt.start("v", start_node="nope")

    # a fresh registry over the same store reloads persisted JSON definitions
    fresh = WorkflowRegistry(rt.activities, registry._store)
    assert await fresh.load_persisted() == 2
    assert (await fresh.get("v")).version == 2
    # code-bound definitions are not persisted
    await registry.publish(WorkflowBuilder("code-only").add("c", "Code", fn=code(lambda c: 1)).build())
    assert await registry._store.get("code-only") is None
    # lookup falls back to the store for definitions unknown in memory
    other = WorkflowRegistry(rt.activities, registry._store)
    assert (await other.get("v", 1)).version == 1


async def test_lifecycle_rejects_invalid_transitions():
    from DnA Workflows.runtime.lifecycle import can_transition, transition
    from DnA Workflows.runtime.models import WorkItem
    item = WorkItem(id="w", node_id="n")
    assert can_transition(WorkState.PENDING, WorkState.RUNNING)
    with pytest.raises(InvalidTransition) as err:
        transition(item, WorkState.COMPLETED)
    assert err.value.source is WorkState.PENDING
    transition(item, WorkState.RUNNING)
    transition(item, WorkState.COMPLETED)
    with pytest.raises(InvalidTransition):
        transition(item, WorkState.RUNNING)


async def test_max_steps_per_drain_leaves_work_for_the_timer(shell_with):
    shell = shell_with(runtime={"max_steps_per_drain": 2})
    rt = shell.get(WorkflowRuntime)
    b = WorkflowBuilder("long").add("n0", "Start")
    for i in range(1, 5):
        b.then(f"n{i}", "Start")
    await shell.get(WorkflowRegistry).publish(b.build())
    ex = await rt.start("long")
    assert ex.status is ExecutionStatus.RUNNING
    await shell.get(TimerService).run_due()
    await shell.get(TimerService).run_due()
    assert (await rt.get(ex.id)).status is ExecutionStatus.COMPLETED


async def test_concurrent_runs_are_serialised_per_execution(shell_with):
    import asyncio
    shell = shell_with()
    rt = shell.get(WorkflowRuntime)
    await publish(shell, WorkflowBuilder("c").add("a", "Start").then("b", "Start").build())
    ex = await rt.start("c", run=False)
    results = await asyncio.gather(*(rt.run(ex.id) for _ in range(5)))
    assert all(r.status is ExecutionStatus.COMPLETED for r in results)
    assert rt._locks == {} and rt._lock_users == {}


def test_builder_guards_and_helpers():
    b = WorkflowBuilder("b", name="Named", description="d")
    with pytest.raises(ValueError, match="previous node"):
        b.then("x", "Start")
    b.add("a", "Start", display_name="Alpha", dependency_override="http:x", retry_override={"max_attempts": 9})
    with pytest.raises(ValueError, match="Duplicate"):
        b.add("a", "Start")
    with pytest.raises(ValueError, match="Unknown node"):
        b.at("zzz")
    b.then("b1", "Start").at("a").then("b2", "Start").variable("v", 1)
    d = b.build()
    assert d.name == "Named" and d.nodes["a"].name == "Alpha" and d.nodes["a"].dependency == "http:x"
    assert d.nodes["a"].retry == {"max_attempts": 9} and d.variables == {"v": 1}
    assert [(e.source, e.target) for e in d.edges] == [("a", "b1"), ("a", "b2")]


async def test_router_counts_duplicates_on_resume(shell_with):
    from DnA Workflows.persistence.contracts import DuplicateDelivery
    shell = shell_with()
    router = shell.get(StimulusRouter)
    await publish(shell, WorkflowBuilder("w").add("start", "Start").then("e", "Event", name="k").build())
    await shell.get(WorkflowRuntime).start("w")
    runtime = shell.get(WorkflowRuntime)

    async def dup(*args, **kwargs):
        raise DuplicateDelivery("seen")
    runtime.resume = dup
    result = await router.publish_event("k", {}, message_id="m1")
    assert result.duplicates == 1 and result.resumed == []
