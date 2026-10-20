"""Test runs of drafts (plan item P11.3).

A test run executes a (draft) definition in an **isolated runtime**: its own in-memory stores, its
own registry, no outbox delivery, no holds. Nodes that touch external dependencies are *simulated*
unless the analyst provides sample outputs (``mocks``) or explicitly allows live calls. The result
shows the path taken, every step's output, the messages that *would* have been sent and a forecast –
so an analyst can check a workflow end to end without a developer and without side effects.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.container import ServiceProvider
from DnA Workflows.kernel.ids import IdGenerator, UuidIdGenerator
from DnA Workflows.persistence.memory import (InMemoryDefinitionStore, InMemoryExecutionStore, InMemoryJournalStore,
                                         InMemoryOutboxStore, InMemoryState)
from DnA Workflows.runtime.activity import ActivityContext, ActivityRegistry, ActivityResult, Done
from DnA Workflows.runtime.contracts import FaultClassifier, FaultImmediatelyPolicy, Next, OpenGate, RuntimeSignals
from DnA Workflows.runtime.engine import EngineOptions, WorkflowRuntime
from DnA Workflows.runtime.models import WorkflowDefinition
from DnA Workflows.runtime.registry import WorkflowRegistry


class MockOutput(BaseModel):
    output: Any = None
    outcome: str | list[str] = "Done"


class TestRunRequest(BaseModel):
    __test__ = False  # not a pytest test class
    input: dict[str, Any] = Field(default_factory=dict)
    #: Sample results per node id – replaces the real activity.
    mocks: dict[str, MockOutput] = Field(default_factory=dict)
    #: Let nodes with external dependencies (HTTP, AI, ...) really run. Off by default.
    allow_live: bool = False
    max_steps: int = 200


class _SimulationMiddleware:
    order = -1000

    def __init__(self, request: TestRunRequest) -> None:
        self.request = request
        self.simulated: list[str] = []

    async def invoke(self, context: ActivityContext, next_: Next) -> ActivityResult:
        mock = self.request.mocks.get(context.node.id)
        if mock is not None:
            self.simulated.append(context.node.id)
            return Done(outcome=mock.outcome, output=mock.output)
        external = context.dependency is not None and not context.dependency.startswith(("inmemory:",))
        if external and not self.request.allow_live and context.activity_type != "SendMessage":
            self.simulated.append(context.node.id)
            return Done(output={"simulated": True, "dependency": context.dependency})
        return await next_(context)


class TestRunner:
    __test__ = False  # not a pytest test class

    def __init__(self, services: ServiceProvider, activities: ActivityRegistry, clock: Clock,
                 classifiers: list[FaultClassifier]) -> None:
        self.services = services
        self.activities = activities
        self.clock = clock
        self.classifiers = classifiers

    async def run(self, definition: WorkflowDefinition, request: TestRunRequest) -> dict[str, Any]:
        state = InMemoryState()
        registry = WorkflowRegistry(self.activities, InMemoryDefinitionStore(state))
        problems = registry.validate(definition)
        if problems:
            return {"status": "invalid", "problems": problems}
        await registry.publish(definition, persist=False)
        simulation = _SimulationMiddleware(request)
        ids: IdGenerator = UuidIdGenerator()
        runtime = WorkflowRuntime(
            InMemoryExecutionStore(state), registry, self.activities, self.services, self.clock, ids,
            FaultImmediatelyPolicy(), OpenGate(), self.classifiers, [simulation], RuntimeSignals(),
            EngineOptions(host_id="test-run", max_steps_per_drain=request.max_steps))
        execution = await runtime.start(definition.id, input=request.input, correlation_id="test-run")
        events = await InMemoryJournalStore(state).query(execution_id=execution.id)
        intents = await InMemoryOutboxStore(state).list(limit=1000)
        path = [e.node_id for e in events if e.kind in ("activity.completed", "activity.suspended")]
        return {
            "status": execution.status.value,
            "output": execution.output,
            "outputs": execution.outputs,
            "variables": execution.variables,
            "path": path,
            "simulated": simulation.simulated,
            "waiting_for": [{"node": b.node_id, "stimulus": f"{b.stimulus_type}:{b.key}"} for b in execution.bookmarks],
            "fault": execution.fault.model_dump(mode="json") if execution.fault else None,
            "would_send": [{"kind": i.kind, "payload": i.payload, "node": i.node_id} for i in intents],
            "journal": [{"kind": e.kind, "node": e.node_id, "data": e.data} for e in events],
        }
