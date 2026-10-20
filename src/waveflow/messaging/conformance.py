"""Transport conformance kit (plan item P2.7).

Every transport plugin must pass :func:`run_transport_conformance`. It is shipped inside the package
so third-party providers (Kafka, Event Hubs, MassTransit, ...) can run the exact same contract
tests. It needs a transport and a factory for a *fresh, empty* endpoint binding.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from DnA Workflows.messaging.core import (EndpointBinding, IncomingMessage, MessageEnvelope, MessageTransport,
                                     RejectMessage)


@dataclass
class ConformanceReport:
    passed: list[str] = field(default_factory=list)

    def ok(self, name: str) -> None:
        self.passed.append(name)


def _envelope(n: int) -> MessageEnvelope:
    return MessageEnvelope(
        id=f"conf-{n}", source="DnA Workflows://conformance", type="conformance.test", subject=f"subject-{n}",
        data={"n": n, "nested": {"list": [1, "two", None], "unicode": "zażółć"}},
        correlation_id=f"corr-{n}", causation_id=f"cause-{n}",
        traceparent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", tracestate="vendor=x",
        headers={"tenant": "acme"})


async def _collect(queue: asyncio.Queue[IncomingMessage], timeout: float) -> IncomingMessage:
    return await asyncio.wait_for(queue.get(), timeout)


async def run_transport_conformance(transport: MessageTransport, new_binding: Callable[[], EndpointBinding], *,
                                    timeout: float = 10.0) -> ConformanceReport:
    report = ConformanceReport()
    await transport.start()
    try:
        healthy, detail = await transport.probe()
        assert healthy, f"probe should be healthy: {detail}"
        report.ok("probe")

        # 1. round trip preserves the envelope
        binding = new_binding()
        received: asyncio.Queue[IncomingMessage] = asyncio.Queue()

        async def collect(message: IncomingMessage) -> None:
            await received.put(message)
        sub = await transport.subscribe(binding, collect)
        sent = _envelope(1)
        await transport.send(binding, sent)
        got = await _collect(received, timeout)
        e = got.envelope
        assert got.endpoint == binding.name, got.endpoint
        for attr in ("id", "type", "source", "subject", "data", "correlation_id", "causation_id", "traceparent",
                     "tracestate"):
            assert getattr(e, attr) == getattr(sent, attr), f"{attr}: {getattr(e, attr)!r} != {getattr(sent, attr)!r}"
        assert e.headers.get("tenant") == "acme", e.headers
        report.ok("roundtrip")
        await sub.close()

        # 2. a failing handler leads to redelivery
        binding = new_binding()
        attempts: list[int] = []
        done = asyncio.Event()

        async def fail_once(message: IncomingMessage) -> None:
            attempts.append(message.delivery_count)
            if len(attempts) == 1:
                raise RuntimeError("simulated handler failure")
            done.set()
        sub = await transport.subscribe(binding, fail_once)
        await transport.send(binding, _envelope(2))
        await asyncio.wait_for(done.wait(), timeout)
        assert len(attempts) == 2 and attempts[1] > 1, attempts
        report.ok("redelivery")
        await sub.close()

        # 3. RejectMessage is not redelivered
        binding = new_binding()
        rejected: list[str] = []
        follow_up = asyncio.Event()

        async def reject_first(message: IncomingMessage) -> None:
            if message.envelope.id == "conf-3":
                rejected.append(message.envelope.id)
                raise RejectMessage("poison")
            follow_up.set()
        sub = await transport.subscribe(binding, reject_first)
        await transport.send(binding, _envelope(3))
        await transport.send(binding, _envelope(4))
        await asyncio.wait_for(follow_up.wait(), timeout)
        await asyncio.sleep(min(1.0, timeout / 10))
        assert rejected == ["conf-3"], rejected
        report.ok("reject")
        await sub.close()

        # 4. closing a subscription stops delivery (message stays for the next consumer)
        binding = new_binding()
        late: asyncio.Queue[IncomingMessage] = asyncio.Queue()

        async def never(message: IncomingMessage) -> None:  # pragma: no cover - must not be called
            raise AssertionError("closed subscription received a message")
        sub = await transport.subscribe(binding, never)
        await sub.close()
        await transport.send(binding, _envelope(5))

        async def collect_late(message: IncomingMessage) -> None:
            await late.put(message)
        sub = await transport.subscribe(binding, collect_late)
        assert (await _collect(late, timeout)).envelope.id == "conf-5"
        report.ok("close-keeps-messages")
        await sub.close()
    finally:
        await transport.stop()
    return report
