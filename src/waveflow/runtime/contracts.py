"""Contracts the runtime consumes but does not implement itself (plan items P1.3, P3.x, P4.5).

The runtime ships safe, minimal defaults (fault immediately, never hold). The ``resilience`` feature
overrides them; observability features add middleware.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from DnA Workflows.runtime.models import FaultInfo, FaultKind

if TYPE_CHECKING:
    from DnA Workflows.runtime.activity import ActivityContext, ActivityResult
    from DnA Workflows.runtime.models import OutboxIntent
    from DnA Workflows.tracing import TraceContext


# -- fault classification (P3.1) --------------------------------------------------------------------

@runtime_checkable
class FaultClassifier(Protocol):
    """Contributor (Extend axis). Return ``None`` when the exception is not recognised."""

    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo | None: ...


# -- failure policy (P3.3 / P3.6) ---------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class FailureContext:
    fault: FaultInfo
    attempt: int                      # attempts already made, including the failed one
    subject: str                      # "work" | "intent"
    dependency: str | None
    execution_id: str | None = None
    definition_id: str | None = None
    node_id: str | None = None
    activity_type: str | None = None
    retry_override: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class RetryAfter:
    delay: timedelta
    reason: str = ""


@dataclass(frozen=True, slots=True)
class Park:
    reason: str
    hold_id: str | None = None


@dataclass(frozen=True, slots=True)
class Fail:
    reason: str


FailureDecision = RetryAfter | Park | Fail


@runtime_checkable
class FailurePolicy(Protocol):
    """Override axis: exactly one policy decides what happens after a failure."""

    async def decide(self, context: FailureContext) -> FailureDecision: ...


class FaultImmediatelyPolicy:
    """Runtime default when the resilience feature is not composed."""

    async def decide(self, context: FailureContext) -> FailureDecision:
        return Fail(context.fault.summary())


class DefaultFaultClassifier:
    """Last-resort classifier used when no contributor recognises an error."""

    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo:
        return FaultInfo(kind=FaultKind.UNKNOWN, error_type=type(error).__name__, message=str(error),
                         dependency=dependency)


# -- gate (P3.4) -----------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class GateRequest:
    subject: str                      # "work" | "intent" | "ingress"
    definition_id: str | None = None
    execution_id: str | None = None
    activity_type: str | None = None
    node_id: str | None = None
    dependency: str | None = None
    endpoint: str | None = None


@dataclass(frozen=True, slots=True)
class GateDecision:
    hold_id: str
    reason: str


@runtime_checkable
class WorkGate(Protocol):
    """Override axis: decides whether work may cross a boundary right now."""

    async def evaluate(self, request: GateRequest) -> GateDecision | None: ...


class OpenGate:
    async def evaluate(self, request: GateRequest) -> GateDecision | None:
        return None


# -- activity middleware (P4.5) ---------------------------------------------------------------------

Next = Callable[["ActivityContext"], Awaitable["ActivityResult"]]


@runtime_checkable
class ActivityMiddleware(Protocol):
    """Contributor (Extend axis) wrapping every activity execution. Lower ``order`` runs outermost."""

    order: int

    async def invoke(self, context: "ActivityContext", next_: Next) -> "ActivityResult": ...


# -- spans (P4.5) ----------------------------------------------------------------------------------------

@runtime_checkable
class SpanFactory(Protocol):
    """Override axis: decides the span for engine-level moments (execution start, resume).

    The default only generates ids; the OpenTelemetry feature emits real (instant) spans so tracing
    back-ends see a complete parent chain.
    """

    def instant(self, name: str, parent: "TraceContext | None", attributes: dict[str, Any]) -> "TraceContext": ...


class IdOnlySpanFactory:
    def instant(self, name: str, parent: "TraceContext | None", attributes: dict[str, Any]) -> "TraceContext":
        from DnA Workflows.tracing import TraceContext
        return parent.child() if parent is not None else TraceContext.new_root()


# -- outbox delivery middleware (P4.5) ---------------------------------------------------------------------

DeliveryNext = Callable[["OutboxIntent"], Awaitable[None]]


@runtime_checkable
class DeliveryMiddleware(Protocol):
    """Contributor (Extend axis) wrapping every outbox delivery. Lower ``order`` runs outermost."""

    order: int

    async def invoke(self, intent: "OutboxIntent", next_: DeliveryNext) -> None: ...


# -- signals --------------------------------------------------------------------------------------

@dataclass
class RuntimeSignals:
    """In-process wake-up hints so background processors react immediately instead of polling.

    Purely an optimisation: every processor still polls, so a lost signal only adds latency.
    """

    listeners: dict[str, list[Callable[[], None]]] = field(default_factory=dict)

    def subscribe(self, topic: str, callback: Callable[[], None]) -> None:
        self.listeners.setdefault(topic, []).append(callback)

    def notify(self, topic: str) -> None:
        for cb in self.listeners.get(topic, []):
            cb()
