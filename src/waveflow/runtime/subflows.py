"""Sub-workflows and compensation (plan phase P14).

``RunWorkflow`` starts a child execution *through the outbox* with a deterministic id, so a retried
or re-delivered start is idempotent. The parent waits on a ``child`` bookmark; when the child
finishes, a ``workflow.child_finished`` intent (durable, also via the outbox) resumes the parent.
A faulted or cancelled child becomes a classified failure of the ``RunWorkflow`` step – so the
parent's retry budget, parking and compensation apply as usual. Cancelling the parent cancels
its children.
"""

from __future__ import annotations

from typing import Any

from DnA Workflows.persistence.contracts import ConcurrencyConflict
from DnA Workflows.runtime.activity import (Activity, ActivityContext, ActivityFault, ActivityInputs, ActivityResult,
                                       Compensate, Done, Suspend)
from DnA Workflows.runtime.engine import ExecutionNotFound, WorkflowRuntime
from DnA Workflows.runtime.models import FaultKind, OutboxIntent, Stimulus
from DnA Workflows.runtime.router import StimulusRouter


class RunWorkflow(Activity):
    """Run another workflow as a step. Waits for its result unless ``wait`` is false."""

    type_name = "RunWorkflow"
    category = "Control"
    output_schema = {"type": "object", "description": "output of the child workflow"}

    class Inputs(ActivityInputs):
        workflow: str
        version: int | None = None
        input: dict[str, Any] = {}
        wait: bool = True

    async def execute(self, context: ActivityContext) -> ActivityResult:
        i = context.inputs
        child_id = f"{context.execution.id}.{context.node.id}.{context.work_item.id.rsplit('-', 1)[-1][:12]}"
        context.emit("workflow.start_child", {
            "child_execution_id": child_id, "workflow": i.workflow, "version": i.version, "input": i.input,
            "parent_execution_id": context.execution.id, "parent_node_id": context.node.id, "wait": i.wait,
            "correlation_id": context.correlation_id})
        if child_id not in context.execution.children:
            context.execution.children.append(child_id)
        if not i.wait:
            return Done(output={"execution_id": child_id})
        return Suspend("child", child_id, payload={"child_execution_id": child_id})

    async def resume(self, context: ActivityContext) -> ActivityResult:
        result = context.payload.get("stimulus") or {}
        if result.get("status") == "completed":
            return Done(output=result.get("output"))
        fault = result.get("fault") or {}
        kind = FaultKind(fault.get("kind", "permanent")) if fault.get("kind") in FaultKind._value2member_map_ \
            else FaultKind.PERMANENT
        raise ActivityFault(f"child workflow {result.get('child_execution_id')} ended {result.get('status')}: "
                            f"{fault.get('message', '')}".rstrip(": "), kind)


class CompensateStep(Activity):
    """Undo all completed undoable steps of this run (newest first), then continue."""

    type_name = "Compensate"
    category = "Control"

    async def execute(self, context: ActivityContext) -> ActivityResult:
        return Compensate()


class StartChildHandler:
    kind = "workflow.start_child"

    def __init__(self, runtime: WorkflowRuntime, router: StimulusRouter) -> None:
        self.runtime = runtime
        self.router = router

    async def handle(self, intent: OutboxIntent) -> None:
        p = intent.payload
        try:
            await self.runtime.start(
                p["workflow"], version=p.get("version"), input=p.get("input") or {},
                correlation_id=p.get("correlation_id"), traceparent=intent.traceparent,
                execution_id=p["child_execution_id"], parent_execution_id=p["parent_execution_id"],
                parent_node_id=p.get("parent_node_id"), notify_parent=bool(p.get("wait", True)))
        except ConcurrencyConflict:
            return   # already started by an earlier delivery of this intent (idempotent)
        except LookupError as exc:   # unknown child workflow: fail the waiting parent step
            if p.get("wait", True):
                await self.router.dispatch(_child_stimulus(p["child_execution_id"], "faulted", None, {
                    "kind": "permanent", "message": str(exc)}))
            else:
                raise


class ChildFinishedHandler:
    kind = "workflow.child_finished"

    def __init__(self, router: StimulusRouter) -> None:
        self.router = router

    async def handle(self, intent: OutboxIntent) -> None:
        p = intent.payload
        await self.router.dispatch(_child_stimulus(p["child_execution_id"], p["status"], p.get("output"),
                                                   p.get("fault"), traceparent=intent.traceparent))


class CancelChildHandler:
    kind = "workflow.cancel_child"

    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime

    async def handle(self, intent: OutboxIntent) -> None:
        try:
            await self.runtime.cancel(intent.payload["child_execution_id"], reason=intent.payload.get("reason", ""),
                                      actor="parent")
        except ExecutionNotFound:
            return   # the child was never started


def _child_stimulus(child_id: str, status: str, output: Any, fault: Any, traceparent: str | None = None) -> Stimulus:
    return Stimulus(type="child", keys=[child_id], traceparent=traceparent, message_id=f"child-finished:{child_id}",
                    payload={"child_execution_id": child_id, "status": status, "output": output, "fault": fault})
