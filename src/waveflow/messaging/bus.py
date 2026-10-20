"""Logical message bus, transactional send and transport classification (plan items P2.3, P3.1, P3.7)."""

from __future__ import annotations

import logging
from typing import Any

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.messaging.core import (EndpointBinding, MessageEnvelope, MessageTransport, TransportPermanentError,
                                     TransportThrottled, TransportUnavailable)
from DnA Workflows.resilience.release import ProbeResult
from DnA Workflows.runtime.models import FaultInfo, FaultKind, OutboxIntent
from DnA Workflows.tracing import TraceContext, current_trace

log = logging.getLogger(__name__)

SEND_INTENT = "messaging.send"


class UnknownEndpoint(LookupError):
    pass


class EndpointRegistry:
    def __init__(self, bindings: dict[str, EndpointBinding]) -> None:
        self._bindings = {}
        for name, binding in bindings.items():
            self._bindings[name] = binding.model_copy(update={"name": name})

    def get(self, name: str) -> EndpointBinding:
        try:
            return self._bindings[name]
        except KeyError:
            raise UnknownEndpoint(f"Unknown endpoint '{name}'. Known: {sorted(self._bindings)}") from None

    def all(self) -> list[EndpointBinding]:
        return list(self._bindings.values())

    def consumed(self) -> list[EndpointBinding]:
        return [b for b in self._bindings.values() if b.consume]

    def __contains__(self, name: str) -> bool:
        return name in self._bindings


class MessageBus:
    """Send to *logical* endpoints. Used by the outbox handler and by host code."""

    def __init__(self, endpoints: EndpointRegistry, transports: list[MessageTransport], clock: Clock,
                 ids: IdGenerator, source: str = "DnA Workflows") -> None:
        self.endpoints = endpoints
        self.transports: dict[str, MessageTransport] = {}
        for t in transports:
            if t.name in self.transports:
                raise ValueError(f"Two transports named '{t.name}'")
            self.transports[t.name] = t
        for binding in endpoints.all():
            if binding.transport not in self.transports:
                raise ValueError(f"Endpoint '{binding.name}' uses unknown transport '{binding.transport}'. "
                                 f"Composed transports: {sorted(self.transports)}")
        self.clock = clock
        self.ids = ids
        self.source = source

    def transport_for(self, endpoint: str) -> tuple[EndpointBinding, MessageTransport]:
        binding = self.endpoints.get(endpoint)
        return binding, self.transports[binding.transport]

    def dependency_key(self, endpoint: str) -> str:
        """``<transport>:<endpoint>`` – the unit holds and retry budgets apply to."""
        binding = self.endpoints.get(endpoint)
        return f"{binding.transport}:{binding.name}"

    def envelope(self, type: str, data: Any, *, subject: str | None = None, correlation_id: str | None = None,
                 causation_id: str | None = None, headers: dict[str, str] | None = None,
                 trace: TraceContext | None = None, source: str | None = None) -> MessageEnvelope:
        trace = trace or current_trace()
        return MessageEnvelope(
            id=self.ids.new_id("msg"), source=source or self.source, type=type, data=data, subject=subject,
            time=self.clock.now(), correlation_id=correlation_id, causation_id=causation_id,
            traceparent=trace.traceparent if trace else None, tracestate=trace.tracestate if trace else None,
            headers=dict(headers or {}))

    async def send(self, endpoint: str, envelope: MessageEnvelope) -> None:
        binding, transport = self.transport_for(endpoint)
        await transport.send(binding, envelope)

    async def publish(self, endpoint: str, type: str, data: Any, **kwargs: Any) -> MessageEnvelope:
        """Direct (non-transactional) send, for host code outside workflows."""
        envelope = self.envelope(type, data, **kwargs)
        await self.send(endpoint, envelope)
        return envelope


class SendMessageIntentHandler:
    """Delivers ``messaging.send`` outbox intents (transactional send, P2.3)."""

    kind = SEND_INTENT

    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus

    async def handle(self, intent: OutboxIntent) -> None:
        envelope = MessageEnvelope.model_validate(intent.payload["envelope"])
        delivery = current_trace()
        if delivery is not None and envelope.trace is not None and delivery.trace_id == envelope.trace.trace_id:
            # a producer span is active (OpenTelemetry delivery middleware): consumers become its children
            envelope.traceparent = delivery.traceparent
        await self.bus.send(intent.payload["endpoint"], envelope)


class TransportErrorClassifier:
    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo | None:
        kind = {TransportUnavailable: FaultKind.DEPENDENCY_UNAVAILABLE, TransportThrottled: FaultKind.THROTTLED,
                TransportPermanentError: FaultKind.PERMANENT}.get(type(error))
        if kind is None:
            for cls, k in ((TransportUnavailable, FaultKind.DEPENDENCY_UNAVAILABLE),
                           (TransportThrottled, FaultKind.THROTTLED),
                           (TransportPermanentError, FaultKind.PERMANENT)):
                if isinstance(error, cls):
                    kind = k
                    break
        if kind is None:
            return None
        return FaultInfo(kind=kind, error_type=type(error).__name__, message=str(error), dependency=dependency,
                         retry_after=getattr(error, "retry_after", None),
                         details={"transport": getattr(error, "transport", None)})


class TransportHealthProbe:
    """Health probe for ``<transport>:*`` dependencies – asks the transport itself (P3.7)."""

    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus

    def supports(self, dependency: str) -> bool:
        return dependency.split(":", 1)[0] in self.bus.transports

    async def check(self, dependency: str) -> ProbeResult:
        transport = self.bus.transports[dependency.split(":", 1)[0]]
        healthy, detail = await transport.probe()
        return ProbeResult(healthy, detail)


def stimulus_keys(endpoint: str, message_type: str | None) -> list[str]:
    keys = [f"{endpoint}|*"]
    if message_type:
        keys.insert(0, f"{endpoint}|{message_type}")
    return keys


def describe_bus(bus: MessageBus) -> dict[str, Any]:
    return {"transports": {n: t.capabilities.model_dump() for n, t in bus.transports.items()},
            "endpoints": [b.model_dump() for b in bus.endpoints.all()]}
