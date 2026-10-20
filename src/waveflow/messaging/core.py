"""Broker-neutral messaging contracts (plan items P2.1, P2.2).

* :class:`MessageEnvelope` – CloudEvents 1.0 shaped, plus correlation/causation and W3C trace
  context. Every transport maps it to its native message in *binary content mode*: the body is the
  data, attributes travel as headers/properties.
* :class:`EndpointBinding` – workflows only know **logical endpoints** (``orders.created``);
  configuration binds them to a transport and a physical address. Moving from RabbitMQ to Service
  Bus is a configuration change.
* :class:`MessageTransport` – what a provider plugin implements.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from DnA Workflows.tracing import TraceContext

CE_PREFIX = "cloudEvents:"
_CE_ATTRIBUTES = ("id", "source", "type", "subject", "time", "specversion", "datacontenttype")


class MessageEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    source: str
    type: str
    data: Any = None
    subject: str | None = None
    time: datetime | None = None
    specversion: str = "1.0"
    datacontenttype: str = "application/json"
    correlation_id: str | None = None
    causation_id: str | None = None
    traceparent: str | None = None
    tracestate: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)

    # -- trace helpers -----------------------------------------------------------------------------
    @property
    def trace(self) -> TraceContext | None:
        return TraceContext.parse(self.traceparent, self.tracestate)

    # -- wire mapping (binary content mode) ---------------------------------------------------------
    def body(self) -> bytes:
        if self.datacontenttype.startswith("application/json") or self.datacontenttype.endswith("+json"):
            return json.dumps(self.data).encode("utf-8")
        if isinstance(self.data, bytes):
            return self.data
        return str(self.data if self.data is not None else "").encode("utf-8")

    def wire_headers(self) -> dict[str, str]:
        """Attributes as headers. Native properties (message id, correlation id, content type)
        are *also* set by transports; headers keep the envelope lossless across brokers."""
        h = dict(self.headers)
        for attr in _CE_ATTRIBUTES:
            value = getattr(self, attr)
            if value is not None:
                h[CE_PREFIX + attr] = value.isoformat() if isinstance(value, datetime) else str(value)
        if self.correlation_id:
            h[CE_PREFIX + "correlationid"] = self.correlation_id
        if self.causation_id:
            h[CE_PREFIX + "causationid"] = self.causation_id
        if self.traceparent:
            h["traceparent"] = self.traceparent
        if self.tracestate:
            h["tracestate"] = self.tracestate
        return h

    @classmethod
    def from_wire(cls, body: bytes | str | None, headers: dict[str, Any] | None, *,
                  message_id: str | None = None, correlation_id: str | None = None,
                  content_type: str | None = None, subject: str | None = None) -> "MessageEnvelope":
        """Rebuild an envelope. Messages from non-DnA Workflows producers (no CloudEvents headers) are
        accepted too: native properties fill the gaps, so DnA Workflows can consume any message."""
        hdr = {str(k): _text(v) for k, v in (headers or {}).items() if v is not None}
        ce = {k[len(CE_PREFIX):]: v for k, v in hdr.items() if k.startswith(CE_PREFIX)}
        rest = {k: v for k, v in hdr.items()
                if not k.startswith(CE_PREFIX) and k not in ("traceparent", "tracestate")}
        ctype = ce.get("datacontenttype") or content_type or "application/json"
        raw = body.decode("utf-8") if isinstance(body, bytes) else body
        if raw is None or raw == "":
            data: Any = None
        elif ctype.startswith("application/json") or ctype.endswith("+json"):
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                data = raw
        else:
            data = raw
        time_value = ce.get("time")
        return cls(
            id=ce.get("id") or message_id or "",
            source=ce.get("source") or "unknown",
            type=ce.get("type") or rest.get("type") or "unknown",
            subject=ce.get("subject") or subject,
            time=datetime.fromisoformat(time_value) if time_value else None,
            specversion=ce.get("specversion", "1.0"),
            datacontenttype=ctype,
            data=data,
            correlation_id=ce.get("correlationid") or correlation_id,
            causation_id=ce.get("causationid"),
            traceparent=hdr.get("traceparent"),
            tracestate=hdr.get("tracestate"),
            headers=rest,
        )

    def to_stimulus_payload(self) -> dict[str, Any]:
        """What a workflow sees as the trigger/bookmark payload."""
        return {"id": self.id, "type": self.type, "source": self.source, "subject": self.subject,
                "data": self.data, "correlation_id": self.correlation_id, "headers": self.headers,
                "time": self.time.isoformat() if self.time else None}


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


class EndpointBinding(BaseModel):
    """Logical endpoint → transport + physical address."""

    model_config = ConfigDict(extra="forbid")

    name: str = ""
    transport: str
    #: Transport-specific physical address (queue name, routing key, topic/subscription, ...).
    address: str
    #: Start a consumer for this endpoint (inbound messages become workflow stimuli).
    consume: bool = False
    options: dict[str, Any] = Field(default_factory=dict)


class TransportCapabilities(BaseModel):
    publish_subscribe: bool = False
    competing_consumers: bool = True
    ordering: Literal["none", "per_key", "session"] = "none"
    delayed_delivery: bool = False
    dead_letter: bool = False
    max_message_bytes: int | None = None


# -- errors (classified by the messaging feature's classifier) -------------------------------------------

class TransportError(Exception):
    def __init__(self, message: str, *, transport: str | None = None, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.transport = transport
        self.retry_after = retry_after


class TransportUnavailable(TransportError):
    """Broker unreachable / connection lost / timeout: the dependency is unavailable."""


class TransportThrottled(TransportError):
    """Broker asks us to slow down (e.g. Service Bus server busy)."""


class TransportPermanentError(TransportError):
    """Retrying will not help (unknown entity, message too large, not authorised, ...)."""


class RejectMessage(Exception):
    """Raised by a consumer handler: do not redeliver; dead-letter the message if supported."""


# -- transport contract ---------------------------------------------------------------------------------

class IncomingMessage(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    envelope: MessageEnvelope
    endpoint: str
    address: str
    delivery_count: int = 1
    raw: Any = None


MessageHandler = Callable[[IncomingMessage], Awaitable[None]]


@runtime_checkable
class Subscription(Protocol):
    async def close(self) -> None: ...


@runtime_checkable
class MessageTransport(Protocol):
    """Implemented by provider plugins (RabbitMQ, Service Bus, in-memory, ...).

    Handler semantics for ``subscribe``: return normally → ack/complete; raise
    :class:`RejectMessage` → dead-letter (no redelivery); raise anything else → negative ack so
    the broker redelivers later.
    """

    name: str
    capabilities: TransportCapabilities

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send(self, binding: EndpointBinding, envelope: MessageEnvelope) -> None: ...

    async def subscribe(self, binding: EndpointBinding, handler: MessageHandler) -> Subscription: ...

    async def probe(self) -> tuple[bool, str]: ...


class TransportRef(BaseModel):
    """Contributor wrapper so provider features can register transports (Extend axis)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    transport: Any
