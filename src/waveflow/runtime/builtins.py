"""Built-in activities (plan item P1.10)."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from DnA Workflows.runtime.activity import (Activity, ActivityContext, ActivityFault, ActivityInputs,
                                       ActivityResult, Done, Sleep, Suspend)
from DnA Workflows.runtime.bindings import is_static
from DnA Workflows.runtime.models import FaultKind, NodeSpec


class Start(Activity):
    """No-op entry point."""

    type_name = "Start"
    category = "Control"

    async def execute(self, context: ActivityContext) -> ActivityResult:
        return Done()


class End(Activity):
    """Ends a path; optionally sets the workflow output."""

    type_name = "End"
    category = "Control"
    outcomes = ()

    class Inputs(ActivityInputs):
        output: Any = None

    async def execute(self, context: ActivityContext) -> ActivityResult:
        if context.inputs.output is not None:
            context.set_output(context.inputs.output)
        return Done(outcome=())


class Log(Activity):
    """Writes a message to the execution journal (and the Python log)."""

    type_name = "Log"
    category = "Diagnostics"

    class Inputs(ActivityInputs):
        message: Any
        level: Literal["debug", "info", "warning", "error"] = "info"
        data: dict[str, Any] = {}

    async def execute(self, context: ActivityContext) -> ActivityResult:
        i = context.inputs
        context.log(str(i.message), i.level, **i.data)
        return Done(output=i.message)


class SetVariable(Activity):
    type_name = "SetVariable"
    category = "Data"

    class Inputs(ActivityInputs):
        name: str
        value: Any = None

    async def execute(self, context: ActivityContext) -> ActivityResult:
        context.set_var(context.inputs.name, context.inputs.value)
        return Done(output=context.inputs.value)


class SetOutput(Activity):
    type_name = "SetOutput"
    category = "Data"

    class Inputs(ActivityInputs):
        value: Any = None

    async def execute(self, context: ActivityContext) -> ActivityResult:
        context.set_output(context.inputs.value)
        return Done(output=context.inputs.value)


class If(Activity):
    type_name = "If"
    category = "Control"
    outcomes = ("True", "False")

    class Inputs(ActivityInputs):
        condition: Any

    async def execute(self, context: ActivityContext) -> ActivityResult:
        return Done(outcome="True" if context.inputs.condition else "False")


class Switch(Activity):
    """Routes by value: outcome is the matching case, otherwise ``Default``."""

    type_name = "Switch"
    category = "Control"
    outcomes = ("*",)

    class Inputs(ActivityInputs):
        value: Any
        cases: list[str]

    async def execute(self, context: ActivityContext) -> ActivityResult:
        value = str(context.inputs.value)
        return Done(outcome=value if value in context.inputs.cases else "Default", output=value)


class Delay(Activity):
    """Durable delay (survives restarts – it is a timer in the store, not a sleep)."""

    type_name = "Delay"
    category = "Timing"

    class Inputs(ActivityInputs):
        seconds: float

    async def execute(self, context: ActivityContext) -> ActivityResult:
        if context.payload.get("woke"):
            return Done()
        return Sleep.seconds(context.inputs.seconds)


class Join(Activity):
    """Merges parallel branches. ``all`` waits for every inbound branch, ``any`` for the first."""

    type_name = "Join"
    output_schema = {"type": "array", "items": {"type": "string"}}
    category = "Control"

    class Inputs(ActivityInputs):
        mode: Literal["all", "any"] = "all"

    async def execute(self, context: ActivityContext) -> ActivityResult:
        node_id = context.node.id
        expected = context.definition.incoming_sources(node_id)
        arrivals = context.execution.join_arrivals.setdefault(node_id, [])
        source = context.payload.get("from")
        if source and source not in arrivals:
            arrivals.append(source)
        if context.inputs.mode == "any":
            first = len(arrivals) == 1
            if set(arrivals) >= expected:
                context.execution.join_arrivals.pop(node_id, None)
            return Done() if first else Done(outcome=())
        if set(arrivals) >= expected:
            context.execution.join_arrivals.pop(node_id, None)
            return Done(output=sorted(arrivals))
        return Done(outcome=())


class Event(Activity):
    """Trigger on a named event. As a root node it starts workflows, mid-flow it waits."""

    type_name = "Event"
    output_schema = {"type": "object", "description": "payload of the event"}
    category = "Triggers"
    is_trigger = True

    class Inputs(ActivityInputs):
        name: str
        #: Only resume for events carrying this execution's correlation id.
        correlate: bool = False

    async def execute(self, context: ActivityContext) -> ActivityResult:
        if "stimulus" in context.payload:
            return Done(output=context.payload["stimulus"])
        corr = context.correlation_id if context.inputs.correlate else None
        return Suspend("event", context.inputs.name, correlation_id=corr)

    @classmethod
    def trigger_keys(cls, node: NodeSpec) -> list[tuple[str, str]]:
        name = node.inputs.get("name")
        return [("event", name)] if isinstance(name, str) and is_static(name) else []


class Fail(Activity):
    """Fails deliberately (useful for validation branches)."""

    type_name = "Fail"
    category = "Control"
    outcomes = ()

    class Inputs(ActivityInputs):
        message: str
        kind: FaultKind = FaultKind.PERMANENT

    async def execute(self, context: ActivityContext) -> ActivityResult:
        raise ActivityFault(context.inputs.message, context.inputs.kind)


@dataclass(frozen=True)
class CodeRef:
    """Wraps a Python function for the ``Code`` activity (code-first workflows)."""

    fn: Callable[[ActivityContext], Any]


def code(fn: Callable[[ActivityContext], Any]) -> CodeRef:
    return CodeRef(fn)


class Code(Activity):
    """Runs a Python function ``fn(context)``. Returning an activity result controls flow;
    any other value becomes the node output."""

    type_name = "Code"
    category = "Code"
    outcomes = ("*",)

    class Inputs(ActivityInputs):
        fn: Any
        dependency: str | None = None

    def dependency(self, inputs: Any, node: NodeSpec) -> str | None:
        return inputs.dependency

    async def execute(self, context: ActivityContext) -> ActivityResult:
        fn = context.inputs.fn
        fn = fn.fn if isinstance(fn, CodeRef) else fn
        if not callable(fn):
            raise ActivityFault("Code activity requires a callable 'fn'")
        result = fn(context)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, (Done, Suspend, Sleep)):
            return result
        return Done(output=result)


BUILTIN_ACTIVITIES: list[type[Activity]] = [Start, End, Log, SetVariable, SetOutput, If, Switch, Delay,
                                            Join, Event, Fail, Code]
