"""Post-commit outbox processing (plan items P1.8, P3.4, P3.6, P3.8).

Intents are written in the same atomic checkpoint as the workflow state that produced them, then
delivered here. Delivery failures go through the same classifier + failure policy as activity
failures, and intents respect holds: a held dependency keeps its intents (status ``held``) until the
hold is released – they are neither dropped nor retried in a loop.
"""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.hosting import PeriodicService
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.persistence.contracts import JournalStore, OutboxStore
from DnA Workflows.runtime.contracts import (DefaultFaultClassifier, DeliveryMiddleware, Fail, FailureContext,
                                        FailurePolicy, FaultClassifier, GateRequest, Park, RetryAfter,
                                        RuntimeSignals, WorkGate)
from DnA Workflows.runtime.engine import EngineOptions
from DnA Workflows.runtime.models import FaultInfo, FaultKind, IntentStatus, JournalEvent, OutboxIntent
from DnA Workflows.tracing import TraceContext, reset_trace, use_trace

log = logging.getLogger(__name__)


@runtime_checkable
class IntentHandler(Protocol):
    """Contributor (Extend axis): delivers one intent kind, e.g. ``messaging.send``."""

    kind: str

    async def handle(self, intent: OutboxIntent) -> None: ...


class OutboxProcessor(PeriodicService):
    name = "outbox-processor"

    def __init__(self, store: OutboxStore, journal: JournalStore, handlers: list[IntentHandler],
                 policy: FailurePolicy, gate: WorkGate, classifiers: list[FaultClassifier],
                 clock: Clock, ids: IdGenerator, signals: RuntimeSignals,
                 options: EngineOptions | None = None, interval: float = 0.5,
                 middleware: list[DeliveryMiddleware] | None = None) -> None:
        super().__init__()
        self.middleware = sorted(middleware or [], key=lambda m: getattr(m, "order", 0))
        self.store = store
        self.journal = journal
        self.handlers: dict[str, IntentHandler] = {}
        for h in handlers:
            if h.kind in self.handlers and type(self.handlers[h.kind]) is not type(h):
                raise ValueError(f"Two handlers claim intent kind '{h.kind}'")
            self.handlers[h.kind] = h
        self.policy = policy
        self.gate = gate
        self.classifiers = classifiers
        self.clock = clock
        self.ids = ids
        self.host_id = (options or EngineOptions()).host_id
        self.interval = interval
        signals.subscribe("outbox", self.wake)

    async def tick(self) -> None:
        await self.process_due()

    async def process_due(self, limit: int = 100) -> int:
        delivered = 0
        for intent in await self.store.due(self.clock.now(), limit):
            if await self.deliver(intent):
                delivered += 1
        return delivered

    async def deliver(self, intent: OutboxIntent) -> bool:
        now = self.clock.now()
        handler = self.handlers.get(intent.kind)
        trace = TraceContext.parse(intent.traceparent)
        if handler is None:
            intent.status = IntentStatus.FAILED
            intent.last_fault = FaultInfo(kind=FaultKind.PERMANENT, error_type="NoHandler",
                                          message=f"No handler for intent kind '{intent.kind}'", at=now)
            await self._save(intent, "intent.failed", trace, {"reason": intent.last_fault.message})
            return False

        held = await self.gate.evaluate(GateRequest(subject="intent", execution_id=intent.execution_id,
                                                    node_id=intent.node_id, dependency=intent.dependency))
        if held is not None:
            intent.status = IntentStatus.HELD
            intent.hold_id = held.hold_id
            await self._save(intent, "intent.held", trace, {"hold_id": held.hold_id, "reason": held.reason})
            return False

        token = use_trace(trace)
        try:
            await self._invoke(handler, intent)
        except Exception as exc:
            await self._failed(intent, exc, trace)
            return False
        finally:
            reset_trace(token)
        intent.status = IntentStatus.DELIVERED
        intent.delivered_at = now
        intent.attempts += 1
        await self._save(intent, "intent.delivered", trace, {"attempts": intent.attempts})
        return True

    async def _invoke(self, handler: IntentHandler, intent: OutboxIntent) -> None:
        async def terminal(i: OutboxIntent) -> None:
            await handler.handle(i)

        pipeline = terminal
        for mw in reversed(self.middleware):
            pipeline = _wrap(mw, pipeline)
        await pipeline(intent)

    async def release(self, *, hold_id: str | None = None, intent_ids: list[str] | None = None,
                      reason: str = "released", actor: str | None = None) -> list[str]:
        """Move held intents back to pending (used when a hold is released, P3.8)."""
        released = []
        for intent in await self.store.list(status=IntentStatus.HELD, limit=10_000):
            if hold_id is not None and intent.hold_id != hold_id:
                continue
            if intent_ids is not None and intent.id not in intent_ids:
                continue
            intent.status = IntentStatus.PENDING
            intent.hold_id = None
            intent.attempts = 0
            intent.next_attempt_at = self.clock.now()
            await self._save(intent, "intent.released", TraceContext.parse(intent.traceparent),
                             {"reason": reason, "actor": actor, "hold_id": hold_id})
            released.append(intent.id)
        if released:
            self.wake()
        return released

    async def redeliver_failed(self, intent_id: str, *, actor: str | None = None) -> bool:
        intent = await self.store.get(intent_id)
        if intent is None or intent.status is not IntentStatus.FAILED:
            return False
        intent.status = IntentStatus.PENDING
        intent.attempts = 0
        intent.next_attempt_at = self.clock.now()
        await self._save(intent, "intent.redelivery_requested", TraceContext.parse(intent.traceparent),
                         {"actor": actor})
        self.wake()
        return True

    async def _failed(self, intent: OutboxIntent, error: Exception, trace: TraceContext | None) -> None:
        now = self.clock.now()
        fault = self._classify(error, intent.dependency)
        fault.at = now
        intent.attempts += 1
        intent.last_fault = fault
        decision = await self.policy.decide(FailureContext(
            fault=fault, attempt=intent.attempts, subject="intent", dependency=intent.dependency,
            execution_id=intent.execution_id, node_id=intent.node_id))
        data = {"attempt": intent.attempts, "fault": fault.model_dump(mode="json")}
        if isinstance(decision, RetryAfter):
            intent.next_attempt_at = now + decision.delay
            await self._save(intent, "intent.retry_scheduled", trace,
                             {**data, "delay_seconds": decision.delay.total_seconds()})
        elif isinstance(decision, Park):
            intent.status = IntentStatus.HELD
            intent.hold_id = decision.hold_id
            await self._save(intent, "intent.held", trace, {**data, "hold_id": decision.hold_id,
                                                            "reason": decision.reason})
        else:
            assert isinstance(decision, Fail)
            intent.status = IntentStatus.FAILED
            await self._save(intent, "intent.failed", trace, {**data, "reason": decision.reason})

    def _classify(self, error: Exception, dependency: str | None) -> FaultInfo:
        for classifier in self.classifiers:
            fault = classifier.classify(error, dependency)
            if fault is not None:
                return fault
        return DefaultFaultClassifier().classify(error, dependency)

    async def _save(self, intent: OutboxIntent, kind: str, trace: TraceContext | None,
                    data: dict) -> None:
        await self.store.update(intent)
        await self.journal.append([JournalEvent(
            id=self.ids.new_id("ev"), kind=kind, timestamp=self.clock.now(),
            execution_id=intent.execution_id, node_id=intent.node_id,
            trace_id=trace.trace_id if trace else None, span_id=trace.span_id if trace else None,
            correlation_id=intent.correlation_id, host=self.host_id,
            data={"intent_id": intent.id, "intent_kind": intent.kind, "dependency": intent.dependency,
                  **data})])


def _wrap(mw: DeliveryMiddleware, next_):  # type: ignore[no-untyped-def]
    async def call(intent: OutboxIntent) -> None:
        await mw.invoke(intent, next_)
    return call
