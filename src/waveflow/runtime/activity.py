"""Activity contract, results, execution context and registry (plan items P1.3, P1.4).

An activity is a small class with typed inputs (a pydantic model). Instances are created per
execution step by the container, so activities can ask for services in their constructor (for
example a message bus or a chat client registry) – the plugin mechanism reaches all the way down.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ConfigDict, ValidationError

from DnA Workflows.runtime.models import FaultKind, NodeSpec, OutboxIntent, WorkflowDefinition, WorkflowExecution, WorkItem
from DnA Workflows.tracing import TraceContext

if TYPE_CHECKING:
    from DnA Workflows.kernel.container import ServiceProvider

log = logging.getLogger("DnA Workflows.activity")
_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING,
           "error": logging.ERROR, "critical": logging.CRITICAL}


class NoInputs(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ActivityInputs(BaseModel):
    """Base for activity inputs. Unknown inputs are errors (catches typos in definitions)."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


# -- results ----------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Done:
    """Completed. ``outcome`` selects outgoing edges; an empty tuple schedules nothing."""

    outcome: str | Sequence[str] = "Done"
    output: Any = None

    @property
    def outcomes(self) -> tuple[str, ...]:
        return (self.outcome,) if isinstance(self.outcome, str) else tuple(self.outcome)


@dataclass(frozen=True, slots=True)
class Suspend:
    """Wait for a stimulus (creates a bookmark)."""

    stimulus_type: str
    key: str
    correlation_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Sleep:
    """Come back later (durable timer). The work item is re-run with ``payload['woke'] = True``."""

    delay: timedelta

    @classmethod
    def seconds(cls, value: float) -> "Sleep":
        return cls(timedelta(seconds=value))


@dataclass(frozen=True, slots=True)
class Compensate:
    """Undo the completed undoable steps (reverse order), then continue after this step."""


ActivityResult = Done | Suspend | Sleep | Compensate


class ActivityFault(Exception):
    """Raised by activities to fail with an explicit classification."""

    def __init__(self, message: str, kind: FaultKind = FaultKind.PERMANENT, *,
                 dependency: str | None = None, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.kind = kind
        self.dependency = dependency
        self.retry_after = retry_after


class InputValidationError(ActivityFault):
    def __init__(self, message: str) -> None:
        super().__init__(message, FaultKind.PERMANENT)


# -- context ----------------------------------------------------------------------------------------

class ActivityContext:
    """What an activity sees while it runs. Mutations are staged into the current checkpoint."""

    def __init__(self, *, execution: WorkflowExecution, definition: WorkflowDefinition, node: NodeSpec,
                 work_item: WorkItem, inputs: BaseModel, services: "ServiceProvider", now: datetime,
                 trace: TraceContext, activity_type: str, dependency: str | None,
                 new_id: Any) -> None:
        self.execution = execution
        self.definition = definition
        self.node = node
        self.work_item = work_item
        self.inputs = inputs
        self.services = services
        self.now = now
        self.trace = trace
        self.activity_type = activity_type
        self.dependency = dependency
        self._new_id = new_id
        self.intents: list[OutboxIntent] = []
        self.logs: list[tuple[str, str, dict[str, Any]]] = []
        self.items: dict[str, Any] = {}   # scratch space for middleware

    # -- data access ----------------------------------------------------------------------------
    @property
    def payload(self) -> dict[str, Any]:
        return self.work_item.payload

    @property
    def attempt(self) -> int:
        return self.work_item.attempt

    @property
    def correlation_id(self) -> str | None:
        return self.execution.correlation_id

    def get_var(self, name: str, default: Any = None) -> Any:
        return self.execution.variables.get(name, default)

    def set_var(self, name: str, value: Any) -> None:
        self.execution.variables[name] = value

    def set_output(self, value: Any) -> None:
        """Set the workflow's final output."""
        self.execution.output = value

    def output_of(self, node_id: str) -> Any:
        return self.execution.outputs.get(node_id)

    # -- side effects -----------------------------------------------------------------------------
    def emit(self, kind: str, payload: dict[str, Any], *, dependency: str | None = None) -> OutboxIntent:
        """Stage a post-commit intent (transactional outbox, P1.8)."""
        intent = OutboxIntent(
            id=self._new_id("intent"), kind=kind, payload=payload, execution_id=self.execution.id,
            node_id=self.node.id, dependency=dependency, traceparent=self.trace.traceparent,
            correlation_id=self.execution.correlation_id, causation_id=self.work_item.id,
            created_at=self.now, next_attempt_at=self.now)
        self.intents.append(intent)
        return intent

    def log(self, message: str, level: str = "info", **data: Any) -> None:
        self.logs.append((level, message, data))
        log.log(_LEVELS.get(level.lower(), logging.INFO), "[%s/%s] %s", self.execution.id,
                self.node.id, message,
                extra={"trace_id": self.trace.trace_id, "correlation_id": self.correlation_id})


# -- activity base ----------------------------------------------------------------------------------

class Activity:
    type_name: ClassVar[str] = ""
    category: ClassVar[str] = "General"
    description: ClassVar[str] = ""
    Inputs: ClassVar[type[BaseModel]] = NoInputs
    outcomes: ClassVar[tuple[str, ...]] = ("Done",)
    #: Triggers can start workflows (P1.7); when reached mid-flow they wait (bookmark).
    is_trigger: ClassVar[bool] = False
    #: Default dependency kind; see :meth:`dependency`.
    dependency_kind: ClassVar[str | None] = None
    #: Analyst-facing name (defaults to the type name split into words).
    display_name: ClassVar[str] = ""
    #: JSON Schema of the output – lets the Studio offer "pick a field" instead of typed bindings.
    output_schema: ClassVar[dict[str, Any] | None] = None

    async def execute(self, context: ActivityContext) -> ActivityResult:  # pragma: no cover
        raise NotImplementedError

    async def resume(self, context: ActivityContext) -> ActivityResult:
        """Called when a bookmark of this activity is resumed. Default: complete with the payload."""
        return Done(output=context.payload.get("stimulus"))

    def dependency(self, inputs: BaseModel, node: NodeSpec) -> str | None:
        """Dependency key this step touches (P3.2). ``node.dependency`` always wins."""
        return None

    @classmethod
    def trigger_keys(cls, node: NodeSpec) -> list[tuple[str, str]]:
        """Publish-time stimulus identities ``(type, key)`` for trigger nodes."""
        return []

    @classmethod
    def describe(cls) -> dict[str, Any]:
        doc = " ".join((cls.__doc__ or "").replace("``", "").split())
        return {
            "type": cls.type_name,
            "display_name": cls.display_name or re.sub(r"(?<!^)(?=[A-Z])", " ", cls.type_name),
            "category": cls.category,
            "description": cls.description or doc.split(". ")[0].rstrip(".") + ("." if doc else ""),
            "help": doc,
            "inputs_schema": cls.Inputs.model_json_schema(),
            "output_schema": cls.output_schema,
            "outcomes": list(cls.outcomes),
            "is_trigger": cls.is_trigger,
        }


def validate_inputs(activity_type: type[Activity], values: dict[str, Any]) -> BaseModel:
    try:
        return activity_type.Inputs.model_validate(values)
    except ValidationError as exc:
        raise InputValidationError(
            f"Invalid inputs for {activity_type.type_name}: {exc.errors(include_url=False)}") from exc


class UnknownActivityError(LookupError):
    pass


class ActivityRegistry:
    """Activity types by name (P1.4). Activities are contributed by features."""

    def __init__(self, activities: list[type[Activity]] | None = None) -> None:
        self._types: dict[str, type[Activity]] = {}
        for a in activities or []:
            self.register(a)

    def register(self, activity: type[Activity]) -> type[Activity]:
        if not activity.type_name:
            raise ValueError(f"{activity.__qualname__} has no type_name")
        existing = self._types.get(activity.type_name)
        if existing is not None and existing is not activity:
            raise ValueError(f"Activity type '{activity.type_name}' already registered")
        self._types[activity.type_name] = activity
        return activity

    def get(self, type_name: str) -> type[Activity]:
        try:
            return self._types[type_name]
        except KeyError:
            raise UnknownActivityError(f"Unknown activity type '{type_name}'") from None

    def __contains__(self, type_name: str) -> bool:
        return type_name in self._types

    def describe(self) -> list[dict[str, Any]]:
        return [a.describe() for _, a in sorted(self._types.items())]


@dataclass(frozen=True, slots=True)
class ActivityType:
    """Wrapper used to contribute activity types through the container (Extend axis)."""

    activity: type[Activity]
