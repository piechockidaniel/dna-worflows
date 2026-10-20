"""W3C Trace Context primitive (plan item P4.1).

Kept dependency-free on purpose: the runtime, the messaging layer and every plugin propagate this
value, while OpenTelemetry is an *optional* feature layered on top (P4.5). When OpenTelemetry is
enabled it produces the span ids; otherwise ids are generated here. Either way one trace id follows a
unit of business work across work items, outbox intents, messages, nodes and services.
"""

from __future__ import annotations

import contextvars
import re
import secrets
from dataclasses import dataclass

_TRACEPARENT = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")
_INVALID_TRACE = "0" * 32
_INVALID_SPAN = "0" * 16


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str
    span_id: str
    sampled: bool = True
    tracestate: str | None = None

    # -- construction ---------------------------------------------------------------------------
    @classmethod
    def new_root(cls) -> "TraceContext":
        return cls(secrets.token_hex(16), secrets.token_hex(8))

    @classmethod
    def parse(cls, traceparent: str | None, tracestate: str | None = None) -> "TraceContext | None":
        """Parse a ``traceparent`` header. Invalid values yield ``None`` (per the W3C spec the
        receiver then starts a new trace instead of failing)."""
        if not traceparent:
            return None
        m = _TRACEPARENT.match(traceparent.strip().lower())
        if not m:
            return None
        version, trace_id, span_id, flags = m.groups()
        if version == "ff" or trace_id == _INVALID_TRACE or span_id == _INVALID_SPAN:
            return None
        return cls(trace_id, span_id, bool(int(flags, 16) & 0x01), tracestate)

    @classmethod
    def continue_or_new(cls, traceparent: str | None, tracestate: str | None = None) -> "TraceContext":
        return cls.parse(traceparent, tracestate) or cls.new_root()

    def child(self) -> "TraceContext":
        """A new span in the same trace (its parent is ``self``)."""
        return TraceContext(self.trace_id, secrets.token_hex(8), self.sampled, self.tracestate)

    # -- serialisation --------------------------------------------------------------------------
    @property
    def traceparent(self) -> str:
        return f"00-{self.trace_id}-{self.span_id}-{'01' if self.sampled else '00'}"

    def inject(self, carrier: dict[str, str]) -> dict[str, str]:
        carrier["traceparent"] = self.traceparent
        if self.tracestate:
            carrier["tracestate"] = self.tracestate
        return carrier

    @classmethod
    def extract(cls, carrier: dict[str, str] | None) -> "TraceContext | None":
        if not carrier:
            return None
        lowered = {k.lower(): v for k, v in carrier.items() if isinstance(v, str)}
        return cls.parse(lowered.get("traceparent"), lowered.get("tracestate"))


_current: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar(
    "DnA Workflows_trace", default=None)


def current_trace() -> TraceContext | None:
    return _current.get()


def use_trace(ctx: TraceContext | None) -> contextvars.Token[TraceContext | None]:
    return _current.set(ctx)


def reset_trace(token: contextvars.Token[TraceContext | None]) -> None:
    _current.reset(token)


_correlation: contextvars.ContextVar[str | None] = contextvars.ContextVar("DnA Workflows_correlation", default=None)


def current_correlation_id() -> str | None:
    return _correlation.get()


def use_correlation(correlation_id: str | None) -> contextvars.Token[str | None]:
    return _correlation.set(correlation_id)


def reset_correlation(token: contextvars.Token[str | None]) -> None:
    _correlation.reset(token)
