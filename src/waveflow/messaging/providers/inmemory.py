"""In-memory transport (plan item P2.6): tests, demos, single-process development.

Behaves like a real broker where it matters: messages cross a wire format (body + headers),
consumers compete, failed deliveries are redelivered with a delay and dead-lettered after
``max_deliveries``. Brokers are shared by name inside the process, so two shells ("nodes") can talk
to each other. ``available = False`` simulates an outage (P3 scenarios).
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import ClassVar

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.messaging.core import (EndpointBinding, IncomingMessage, MessageEnvelope, MessageHandler,
                                     RejectMessage, TransportCapabilities, TransportPermanentError,
                                     TransportUnavailable)
from DnA Workflows.messaging.feature import add_transport
from DnA Workflows.resilience.feature import add_release_rule
from DnA Workflows.resilience.release import ReleaseRule

log = logging.getLogger(__name__)


@dataclass
class _WireMessage:
    body: bytes
    headers: dict[str, str]
    message_id: str
    delivery_count: int = 0
    not_before: float = 0.0


class InMemoryBroker:
    _brokers: ClassVar[dict[str, "InMemoryBroker"]] = {}

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self.available = True
        self.permanent_failures: set[str] = set()
        self.queues: dict[str, deque[_WireMessage]] = {}
        self.dead_letters: dict[str, list[MessageEnvelope]] = {}
        self.history: dict[str, list[MessageEnvelope]] = {}
        self.acked: dict[str, list[MessageEnvelope]] = {}
        self.in_flight = 0
        self.consumers: dict[str, int] = {}
        self._waiters: set[asyncio.Event] = set()

    @classmethod
    def get(cls, name: str = "default") -> "InMemoryBroker":
        if name not in cls._brokers:
            cls._brokers[name] = cls(name)
        return cls._brokers[name]

    @classmethod
    def reset(cls) -> None:
        cls._brokers.clear()

    def set_available(self, available: bool) -> None:
        self.available = available
        self._notify()

    def put(self, address: str, envelope: MessageEnvelope) -> None:
        self.history.setdefault(address, []).append(envelope)
        self.queues.setdefault(address, deque()).append(
            _WireMessage(envelope.body(), envelope.wire_headers(), envelope.id))
        self._notify()

    def pending(self, address: str) -> int:
        return len(self.queues.get(address, ()))

    def _notify(self) -> None:
        for event in list(self._waiters):
            event.set()

    async def idle(self, timeout: float = 5.0) -> None:
        """Wait until nothing is queued (and deliverable) or in flight."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            busy = self.in_flight or (self.available and any(
                q for address, q in self.queues.items() if self.consumers.get(address)))
            if not busy:
                return
            await asyncio.sleep(0.005)
        raise TimeoutError(f"Broker '{self.name}' not idle: {[(a, len(q)) for a, q in self.queues.items()]}")


class _Subscription:
    def __init__(self, task: asyncio.Task[None]) -> None:
        self._task = task

    async def close(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass


class InMemoryTransport:
    capabilities = TransportCapabilities(publish_subscribe=False, competing_consumers=True, dead_letter=True)

    def __init__(self, name: str = "inmemory", broker: InMemoryBroker | None = None, *,
                 max_deliveries: int = 10, redelivery_delay: float = 0.05) -> None:
        self.name = name
        self.broker = broker or InMemoryBroker.get()
        self.max_deliveries = max_deliveries
        self.redelivery_delay = redelivery_delay

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, binding: EndpointBinding, envelope: MessageEnvelope) -> None:
        if not self.broker.available:
            raise TransportUnavailable(f"in-memory broker '{self.broker.name}' is unavailable", transport=self.name)
        if binding.address in self.broker.permanent_failures:
            raise TransportPermanentError(f"address '{binding.address}' rejects messages", transport=self.name)
        self.broker.put(binding.address, envelope)

    async def probe(self) -> tuple[bool, str]:
        return self.broker.available, "available" if self.broker.available else "broker unavailable"

    async def subscribe(self, binding: EndpointBinding, handler: MessageHandler) -> _Subscription:
        # registered synchronously so `idle()` already knows this queue is consumed
        self.broker.consumers[binding.address] = self.broker.consumers.get(binding.address, 0) + 1
        task = asyncio.create_task(self._consume(binding, handler), name=f"inmemory:{binding.name}")
        return _Subscription(task)

    async def _consume(self, binding: EndpointBinding, handler: MessageHandler) -> None:
        broker = self.broker
        queue = broker.queues.setdefault(binding.address, deque())
        wake = asyncio.Event()
        broker._waiters.add(wake)
        loop = asyncio.get_running_loop()
        try:
            while True:
                now = loop.time()
                msg = None
                if broker.available:
                    for candidate in list(queue):
                        if candidate.not_before <= now:
                            msg = candidate
                            queue.remove(candidate)
                            break
                if msg is None:
                    wake.clear()
                    delays = [m.not_before - now for m in queue if m.not_before > now]
                    timeout = min(delays) if delays and broker.available else None
                    try:
                        await asyncio.wait_for(wake.wait(), timeout=timeout)
                    except TimeoutError:
                        pass
                    continue
                await self._deliver(binding, handler, msg, queue, loop)
        finally:
            broker._waiters.discard(wake)
            broker.consumers[binding.address] -= 1

    async def _deliver(self, binding: EndpointBinding, handler: MessageHandler, msg: _WireMessage,
                       queue: deque[_WireMessage], loop: asyncio.AbstractEventLoop) -> None:
        broker = self.broker
        msg.delivery_count += 1
        envelope = MessageEnvelope.from_wire(msg.body, msg.headers, message_id=msg.message_id)
        broker.in_flight += 1
        try:
            await handler(IncomingMessage(envelope=envelope, endpoint=binding.name, address=binding.address,
                                          delivery_count=msg.delivery_count))
            broker.acked.setdefault(binding.address, []).append(envelope)
        except RejectMessage as exc:
            log.info("Message %s dead-lettered: %s", msg.message_id, exc)
            broker.dead_letters.setdefault(binding.address, []).append(envelope)
        except asyncio.CancelledError:
            queue.appendleft(msg)  # consumer stopped mid-delivery: hand the message back
            raise
        except Exception as exc:
            if msg.delivery_count >= self.max_deliveries:
                log.warning("Message %s dead-lettered after %s deliveries: %s", msg.message_id,
                            msg.delivery_count, exc)
                broker.dead_letters.setdefault(binding.address, []).append(envelope)
            else:
                msg.not_before = loop.time() + self.redelivery_delay
                queue.append(msg)
        finally:
            broker.in_flight -= 1


class InMemoryTransportOptions(FeatureOptions):
    name: str = "inmemory"
    broker: str = "default"
    max_deliveries: int = 10
    redelivery_delay: float = 0.05
    #: Release strategy contributed for holds on this transport's endpoints (None = no opinion).
    release_strategy: str | None = "probe-auto"


class InMemoryTransportFeature(Feature):
    """In-process broker with fault injection (tests, demos)."""

    name = "messaging.inmemory"
    depends_on = ("messaging",)
    Options = InMemoryTransportOptions

    def configure(self, services: ServiceCollection) -> None:
        o: InMemoryTransportOptions = self.options  # type: ignore[assignment]
        add_transport(services, o.name, lambda p: InMemoryTransport(
            o.name, InMemoryBroker.get(o.broker), max_deliveries=o.max_deliveries,
            redelivery_delay=o.redelivery_delay))
        if o.release_strategy:
            add_release_rule(services, ReleaseRule(f"{o.name}:*", o.release_strategy, {"successes_required": 1}))
