"""Sub-workflows and compensation (P14)."""

import pytest

from DnA Workflows.persistence.contracts import JournalStore, OutboxStore
from DnA Workflows.resilience import HoldService
from DnA Workflows.runtime import (Activity, ActivityInputs, Done, ExecutionStatus, OutboxProcessor, StimulusRouter,
                              WorkflowBuilder, WorkflowRegistry, WorkflowRuntime, WorkState, add_activity)
from DnA Workflows.runtime.models import CompensationSpec, FaultKind

LOG: list[str] = []

POSTGRES = False  # TestClient/ASGI app: see tests/conftest.py


class Step(Activity):
    """Records its action; can be told to fail."""

    type_name = "Step"
    broken: set[str] = set()

    class Inputs(ActivityInputs):
        action: str
        ref: str | None = None
        fail: bool = False
        dependency: str | None = None

    def dependency(self, inputs, node):
        return inputs.dependency

    async def execute(self, context):
        i = context.inputs
        if i.fail or i.action in Step.broken:
            raise ValueError(f"{i.action} failed")
        LOG.append(f"{i.action}{':' + i.ref if i.ref else ''}")
        return Done(output={"ref": f"{i.action}-ref"})


def undo(action, **inputs):
    return CompensationSpec(type="Step", inputs={"action": action, "ref": {"$payload": "original.output.ref"},
                                                 **inputs})


@pytest.fixture(autouse=True)
def reset():
    LOG.clear()
    Step.broken = set()


@pytest.fixture
def shell(make_shell):
    return make_shell({"resilience": {"retry": {"unknown": {"max_attempts": 1, "on_exhausted": "fail"}}}},
                      configure=lambda s: add_activity(s, Step))


async def deliver(shell, rounds=6):
    for _ in range(rounds):
        if not await shell.get(OutboxProcessor).process_due():
            return


async def publish(shell, *builders):
    for b in builders:
        await shell.get(WorkflowRegistry).publish(b.build())


# ------------------------------------------------------------------------------------------------------
# sub-workflows
# ------------------------------------------------------------------------------------------------------

async def test_parent_waits_for_child_and_gets_its_output(shell):
    await publish(shell,
                  WorkflowBuilder("child").add("s", "SetOutput", value={"$tmpl": "hello {input.name}"}),
                  WorkflowBuilder("parent").add("call", "RunWorkflow", workflow="child", input={"name": {"$input": "who"}})
                  .then("out", "SetOutput", value={"$output": "call"}))
    rt = shell.get(WorkflowRuntime)
    parent = await rt.start("parent", input={"who": "Ada"}, correlation_id="order-1")
    assert parent.status is ExecutionStatus.WAITING and parent.bookmarks[0].stimulus_type == "child"
    await deliver(shell)
    parent = await rt.get(parent.id)
    assert parent.status is ExecutionStatus.COMPLETED and parent.output == "hello Ada"
    [child_id] = parent.children
    child = await rt.get(child_id)
    assert child.parent_execution_id == parent.id and child.correlation_id == "order-1"
    assert child.trace_id == parent.trace_id                # one trace across parent and child
    # redelivering the start intent is idempotent
    start = next(i for i in await shell.get(OutboxStore).list() if i.kind == "workflow.start_child")
    await shell.get(OutboxProcessor).handlers["workflow.start_child"].handle(start)


async def test_failing_child_fails_the_parent_step_with_its_classification(shell):
    await publish(shell, WorkflowBuilder("child").add("s", "Step", action="x", fail=True),
                  WorkflowBuilder("parent").add("call", "RunWorkflow", workflow="child"))
    rt = shell.get(WorkflowRuntime)
    parent = await rt.start("parent")
    await deliver(shell)
    parent = await rt.get(parent.id)
    assert parent.status is ExecutionStatus.FAULTED
    assert "ended faulted" in parent.fault.message and parent.fault.kind is FaultKind.PERMANENT


async def test_fire_and_forget_and_unknown_child(shell):
    await publish(shell, WorkflowBuilder("child").add("s", "Step", action="bg"),
                  WorkflowBuilder("ff").add("call", "RunWorkflow", workflow="child", wait=False),
                  WorkflowBuilder("broken").add("call", "RunWorkflow", workflow="does-not-exist"))
    rt = shell.get(WorkflowRuntime)
    ff = await rt.start("ff")
    assert ff.status is ExecutionStatus.COMPLETED and ff.outputs["call"]["execution_id"] == ff.children[0]
    await deliver(shell)
    assert LOG == ["bg"]
    broken = await rt.start("broken")
    await deliver(shell)
    broken = await rt.get(broken.id)
    assert broken.status is ExecutionStatus.FAULTED and "not found" in broken.fault.message


async def test_cancelling_the_parent_cancels_children(shell):
    await publish(shell, WorkflowBuilder("child").add("w", "Event", name="never"),
                  WorkflowBuilder("parent").add("call", "RunWorkflow", workflow="child"))
    rt = shell.get(WorkflowRuntime)
    parent = await rt.start("parent")
    await deliver(shell)
    child_id = (await rt.get(parent.id)).children[0]
    assert (await rt.get(child_id)).status is ExecutionStatus.WAITING
    await rt.cancel(parent.id, reason="customer cancelled", actor="ops")
    await deliver(shell)
    assert (await rt.get(child_id)).status is ExecutionStatus.CANCELLED
    # cancelling a child that was never started is harmless
    handler = shell.get(OutboxProcessor).handlers["workflow.cancel_child"]
    from DnA Workflows.runtime.models import OutboxIntent
    await handler.handle(OutboxIntent(id="i", kind="workflow.cancel_child", payload={"child_execution_id": "nope"}))


# ------------------------------------------------------------------------------------------------------
# compensation
# ------------------------------------------------------------------------------------------------------

def saga(check_fails: bool):
    b = (WorkflowBuilder("saga")
         .add("reserve", "Step", action="reserve")
         .then("charge", "Step", action="charge")
         .then("ship", "Step", action="ship", fail=check_fails))
    d = b.build()
    d.nodes["reserve"].compensate = undo("release")
    d.nodes["charge"].compensate = undo("refund")
    return d


async def test_compensate_activity_undoes_in_reverse_order_then_continues(shell):
    d = (WorkflowBuilder("saga")
         .add("reserve", "Step", action="reserve")
         .then("charge", "Step", action="charge")
         .then("decide", "If", condition=False)
         .then("ok", "Step", outcome="True", action="ship")
         .then("undo", "Compensate", after="decide", outcome="False")
         .then("notify", "Step", action="tell-customer")).build()
    d.nodes["reserve"].compensate = undo("release")
    d.nodes["charge"].compensate = undo("refund")
    await shell.get(WorkflowRegistry).publish(d)
    ex = await shell.get(WorkflowRuntime).start("saga")
    assert LOG == ["reserve", "charge", "refund:charge-ref", "release:reserve-ref", "tell-customer"]
    assert ex.status is ExecutionStatus.COMPLETED and ex.compensations == [] and ex.compensation_mode is None
    kinds = [e.kind for e in await shell.get(JournalStore).query(execution_id=ex.id, kinds=[
        "compensation.started", "compensation.step_completed", "compensation.completed"])]
    assert kinds == ["compensation.started", "compensation.step_completed", "compensation.step_completed",
                     "compensation.completed"]


async def test_compensate_on_fault(shell):
    d = saga(check_fails=True)
    d.compensate_on_fault = True
    await shell.get(WorkflowRegistry).publish(d)
    ex = await shell.get(WorkflowRuntime).start("saga")
    assert LOG == ["reserve", "charge", "refund:charge-ref", "release:reserve-ref"]
    assert ex.status is ExecutionStatus.COMPENSATED and "ship failed" in ex.fault.message


async def test_operator_compensates_a_completed_run(shell):
    await shell.get(WorkflowRegistry).publish(saga(check_fails=False))
    rt = shell.get(WorkflowRuntime)
    ex = await rt.start("saga")
    assert ex.status is ExecutionStatus.COMPLETED
    ex = await rt.compensate(ex.id, reason="order returned", actor="ops")
    assert LOG[-2:] == ["refund:charge-ref", "release:reserve-ref"]
    assert ex.status is ExecutionStatus.COMPENSATED
    assert (await rt.compensate(ex.id)).status is ExecutionStatus.COMPENSATED   # no-op afterwards


async def test_failing_undo_stops_for_an_operator_and_can_be_retried(shell):
    d = saga(check_fails=True)
    d.compensate_on_fault = True
    await shell.get(WorkflowRegistry).publish(d)
    Step.broken = {"refund"}
    rt = shell.get(WorkflowRuntime)
    ex = await rt.start("saga")
    assert ex.status is ExecutionStatus.FAULTED and ex.compensation_mode == "fault"
    assert ex.fault.message == "refund failed"
    Step.broken = set()
    await rt.retry_faulted(ex.id, actor="ops")
    ex = await rt.get(ex.id)
    assert ex.status is ExecutionStatus.COMPENSATED and LOG[-2:] == ["refund:charge-ref", "release:reserve-ref"]


async def test_undo_steps_respect_holds(shell):
    d = saga(check_fails=True)
    d.compensate_on_fault = True
    d.nodes["charge"].compensate = undo("refund", dependency="http:payments")
    await shell.get(WorkflowRegistry).publish(d)
    holds = shell.get(HoldService)
    hold = await holds.place("dependency", "http:payments", "payment provider maintenance")
    rt = shell.get(WorkflowRuntime)
    ex = await rt.start("saga")
    assert ex.status is ExecutionStatus.PARKED and ex.work[-1].kind == "compensate"
    assert [w.state for w in ex.work if w.kind == "compensate"] == [WorkState.PARKED]
    await holds.release(hold.id)
    assert (await rt.get(ex.id)).status is ExecutionStatus.COMPENSATED


async def test_operator_compensation_abandons_open_work_and_children(shell):
    await publish(shell, WorkflowBuilder("child").add("w", "Event", name="never"))
    d = (WorkflowBuilder("long").add("reserve", "Step", action="reserve")
         .then("sub", "RunWorkflow", workflow="child")
         .then("wait", "Event", after="reserve", name="approval")).build()
    d.nodes["reserve"].compensate = undo("release")
    await shell.get(WorkflowRegistry).publish(d)
    rt = shell.get(WorkflowRuntime)
    ex = await rt.start("long")
    await deliver(shell)
    ex = await rt.compensate(ex.id, reason="stop")
    await deliver(shell)
    assert ex.status is ExecutionStatus.COMPENSATED and ex.bookmarks == [] and LOG[-1] == "release:reserve-ref"
    assert (await rt.get(ex.children[0])).status is ExecutionStatus.CANCELLED
    await shell.get(StimulusRouter).publish_event("approval", {})   # nothing left to resume


async def test_compensation_via_api_and_approved_proposal(make_shell):
    from fastapi.testclient import TestClient

    from DnA Workflows.api.feature import ApiApp
    from DnA Workflows.resilience import ProposalService
    shell = make_shell({"api": {"serve": False}, "resilience": {}}, configure=lambda s: add_activity(s, Step))
    await shell.get(WorkflowRegistry).publish(saga(check_fails=False))
    rt = shell.get(WorkflowRuntime)
    first, second = await rt.start("saga"), await rt.start("saga")
    proposals = shell.get(ProposalService)
    p = await proposals.propose("execution.compensate", {"execution_id": first.id}, "duplicate order",
                                proposed_by="ai:triage")
    assert (await proposals.approve(p.id, actor="alice")).result == {"execution_status": "compensated"}
    with TestClient(shell.get(ApiApp).app) as c:
        r = c.post(f"/executions/{second.id}/compensate", json={"reason": "returned"}, headers={"X-Actor": "bob"})
        assert r.json()["status"] == "compensated"
    events = await shell.get(JournalStore).query(execution_id=second.id, kinds=["compensation.started"])
    assert events[0].data["actor"] == "bob" and events[0].data["reason"] == "returned"
