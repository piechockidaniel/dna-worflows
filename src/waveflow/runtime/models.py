"""Runtime data model (plan items P1.1, P1.5, P1.7, P1.8, P3.5, P4.3).

Everything that is persisted is a pydantic model so that it round-trips through JSON, can be
stored by any persistence provider, and publishes a JSON Schema (useful for tooling and AI agents).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", validate_assignment=False)


# ---------------------------------------------------------------------------------------------------
# Definitions (P1.1)
# ---------------------------------------------------------------------------------------------------

class Driver(_Model):
    """Duration that scales with an input feature, e.g. 2 s per order line."""

    feature: str
    per_unit: float                     # seconds per unit of the feature


class Estimate(_Model):
    """Designer's three-point estimate in seconds (PERT). Used until history exists, and blended with
    history afterwards. ``likely`` alone is enough; the range expresses uncertainty."""

    likely: float
    optimistic: float | None = None
    pessimistic: float | None = None
    driver: Driver | None = None


class FeatureSpec(_Model):
    """An input feature used for forecasting (conditions and drivers)."""

    path: str                           # path in the workflow input, e.g. "order.lines"
    transform: Literal["value", "count", "length", "bucket"] = "value"
    #: Upper bounds for ``bucket`` (e.g. [10, 100] → "<=10", "<=100", ">100").
    buckets: list[float] = Field(default_factory=list)


class ForecastSpec(_Model):
    features: dict[str, FeatureSpec] = Field(default_factory=dict)


class CompensationSpec(_Model):
    """How to undo a completed step (saga compensation, P14.2). Bindings may use
    ``{"$payload": "original.output.<path>"}`` to refer to what the step produced."""

    type: str
    inputs: dict[str, Any] = Field(default_factory=dict)
    name: str | None = None


class NodeSpec(_Model):
    id: str
    type: str
    name: str | None = None
    #: Input bindings: literal JSON values, ``{"$var": ...}``-style references, or Python callables
    #: (code-first workflows only – such definitions are not persisted as JSON).
    inputs: dict[str, Any] = Field(default_factory=dict)
    #: Explicit dependency key override (P3.2), e.g. ``http:sap-prod``.
    dependency: str | None = None
    #: Per-node retry override passed to the failure policy (P3.3), e.g. ``{"max_attempts": 5}``.
    retry: dict[str, Any] | None = None
    #: Designer estimate for forecasting (P9.2).
    estimate: Estimate | None = None
    #: Undo action for this step (P14.2).
    compensate: CompensationSpec | None = None
    #: Studio layout and analyst notes – opaque to the engine.
    ui: dict[str, Any] = Field(default_factory=dict)


class EdgeSpec(_Model):
    source: str
    target: str
    outcome: str = "Done"


class WorkflowDefinition(_Model):
    id: str
    version: int = 1
    name: str | None = None
    description: str | None = None
    nodes: dict[str, NodeSpec] = Field(default_factory=dict)
    edges: list[EdgeSpec] = Field(default_factory=list)
    variables: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    forecast: ForecastSpec = Field(default_factory=ForecastSpec)
    #: When the workflow would fault, undo completed steps first (saga, P14.2).
    compensate_on_fault: bool = False

    def roots(self) -> list[str]:
        targets = {e.target for e in self.edges}
        return [n for n in self.nodes if n not in targets]

    def outgoing(self, node_id: str, outcome: str) -> list[EdgeSpec]:
        return [e for e in self.edges if e.source == node_id and e.outcome == outcome]

    def incoming_sources(self, node_id: str) -> set[str]:
        return {e.source for e in self.edges if e.target == node_id}

    def to_json_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


# ---------------------------------------------------------------------------------------------------
# Faults (P3.1)
# ---------------------------------------------------------------------------------------------------

class FaultKind(StrEnum):
    TRANSIENT = "transient"                  # retry soon
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"  # retry briefly, then park + hold
    THROTTLED = "throttled"                  # retry with longer backoff
    PERMANENT = "permanent"                  # retrying will not help
    UNKNOWN = "unknown"


class FaultInfo(_Model):
    kind: FaultKind = FaultKind.UNKNOWN
    error_type: str = ""
    message: str = ""
    dependency: str | None = None
    retry_after: float | None = None         # seconds hinted by the dependency (e.g. Retry-After)
    at: datetime | None = None
    details: dict[str, Any] = Field(default_factory=dict)

    def summary(self) -> str:
        return f"{self.kind.value}: {self.error_type}: {self.message}"


# ---------------------------------------------------------------------------------------------------
# Execution state (P1.5) + operational lifecycle (P3.5)
# ---------------------------------------------------------------------------------------------------

class ExecutionStatus(StrEnum):
    RUNNING = "running"      # has runnable or delayed work
    WAITING = "waiting"      # only bookmarks remain (waiting for a stimulus)
    PARKED = "parked"        # nothing can run: at least one work item is parked
    COMPLETED = "completed"
    FAULTED = "faulted"
    CANCELLED = "cancelled"
    COMPENSATED = "compensated"   # completed steps were undone (saga)

    @property
    def is_terminal(self) -> bool:
        return self in (ExecutionStatus.COMPLETED, ExecutionStatus.FAULTED, ExecutionStatus.CANCELLED,
                        ExecutionStatus.COMPENSATED)


class WorkState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    PARKED = "parked"
    RESUMING = "resuming"
    COMPLETED = "completed"
    FAULTED = "faulted"
    ABANDONED = "abandoned"


class WorkItem(_Model):
    id: str
    node_id: str
    activity_type: str | None = None
    kind: str = "execute"                    # execute | resume
    payload: dict[str, Any] = Field(default_factory=dict)
    state: WorkState = WorkState.PENDING
    attempt: int = 0
    seq: int = 0                             # FIFO ordering within the execution
    not_before: datetime | None = None
    dependency: str | None = None
    parked_reason: str | None = None
    hold_id: str | None = None
    parked_at: datetime | None = None
    last_fault: FaultInfo | None = None
    traceparent: str | None = None           # parent span of this work (P4.2)
    causation_id: str | None = None
    created_at: datetime | None = None


class CompensationRecord(_Model):
    node_id: str
    output: Any = None
    completed_at: datetime | None = None


class Bookmark(_Model):
    id: str
    node_id: str
    stimulus_type: str
    key: str
    correlation_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


class WorkflowExecution(_Model):
    id: str
    definition_id: str
    definition_version: int
    status: ExecutionStatus = ExecutionStatus.RUNNING
    input: dict[str, Any] = Field(default_factory=dict)
    variables: dict[str, Any] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)
    output: Any = None
    work: list[WorkItem] = Field(default_factory=list)
    bookmarks: list[Bookmark] = Field(default_factory=list)
    join_arrivals: dict[str, list[str]] = Field(default_factory=dict)
    fault: FaultInfo | None = None
    correlation_id: str | None = None
    trace_id: str | None = None
    traceparent: str | None = None           # root span context of the execution
    next_seq: int = 0
    # sub-workflows (P14.1)
    parent_execution_id: str | None = None
    parent_node_id: str | None = None
    notify_parent: bool = False
    children: list[str] = Field(default_factory=list)
    # compensation (P14.2): completed undoable steps, oldest first; mode while compensating
    compensations: list[CompensationRecord] = Field(default_factory=list)
    compensation_mode: str | None = None
    revision: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    completed_at: datetime | None = None

    # -- convenience ----------------------------------------------------------------------------
    def item(self, work_item_id: str) -> WorkItem | None:
        return next((w for w in self.work if w.id == work_item_id), None)

    def parked(self) -> list[WorkItem]:
        return [w for w in self.work if w.state is WorkState.PARKED]

    def wake_at(self) -> datetime | None:
        """Earliest moment at which some work becomes runnable (index used by the timer, P1.9)."""
        if self.status.is_terminal:
            return None
        times = [w.not_before or w.created_at for w in self.work
                 if w.state in (WorkState.PENDING, WorkState.RETRYING, WorkState.RESUMING,
                                WorkState.RUNNING)]
        times = [t for t in times if t is not None]
        return min(times) if times else None


# ---------------------------------------------------------------------------------------------------
# Outbox (P1.8) and journal (P4.3)
# ---------------------------------------------------------------------------------------------------

class IntentStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    HELD = "held"          # blocked by a hold; released together with the hold (P3.8)
    FAILED = "failed"      # final – dead-lettered


class OutboxIntent(_Model):
    id: str
    kind: str
    payload: dict[str, Any] = Field(default_factory=dict)
    execution_id: str | None = None
    node_id: str | None = None
    dependency: str | None = None
    status: IntentStatus = IntentStatus.PENDING
    attempts: int = 0
    next_attempt_at: datetime | None = None
    hold_id: str | None = None
    last_fault: FaultInfo | None = None
    traceparent: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    created_at: datetime | None = None
    delivered_at: datetime | None = None


class JournalEvent(_Model):
    seq: int = 0                             # assigned by the store, strictly increasing
    id: str
    kind: str
    timestamp: datetime
    execution_id: str | None = None
    definition_id: str | None = None
    node_id: str | None = None
    work_item_id: str | None = None
    activity_type: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    correlation_id: str | None = None
    host: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class Checkpoint(_Model):
    """Everything one engine step produces, committed atomically (P1.6)."""

    execution: WorkflowExecution
    expected_revision: int
    intents: list[OutboxIntent] = Field(default_factory=list)
    journal: list[JournalEvent] = Field(default_factory=list)
    #: Inbox keys consumed by this commit; a duplicate key aborts the commit (dedup, P1.7).
    inbox_keys: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------------------------------
# Stimuli (P1.7)
# ---------------------------------------------------------------------------------------------------

class Stimulus(_Model):
    type: str
    keys: list[str]
    payload: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    message_id: str | None = None
    causation_id: str | None = None
    traceparent: str | None = None
    tracestate: str | None = None
    source: str | None = None


class DispatchResult(_Model):
    started: list[str] = Field(default_factory=list)
    resumed: list[str] = Field(default_factory=list)
    duplicates: int = 0

    @property
    def matched(self) -> bool:
        return bool(self.started or self.resumed or self.duplicates)
