"""Faithful fakes of the aio-pika and azure-servicebus client surfaces used by the providers.

They model broker semantics (routing, topic wildcards, ack/nack/reject, peek-lock settlement,
redelivery counts, dead-lettering, outages) so the *real* provider code is exercised end to end.
"""

from __future__ import annotations

import asyncio
import itertools
import re
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import aio_pika.exceptions as amqp_ex
from azure.servicebus import exceptions as sb_ex
from pamqp.commands import Basic


# =====================================================================================================
# aio-pika
# =====================================================================================================

def topic_match(pattern: str, key: str) -> bool:
    regex = "^" + re.escape(pattern).replace(r"\#", ".*").replace(r"\*", "[^.]+") + "$"
    return re.match(regex, key) is not None


@dataclass
class FakeAmqpBroker:
    down: bool = False
    queues: dict[str, "FakeQueue"] = field(default_factory=dict)
    exchanges: dict[str, "FakeExchange"] = field(default_factory=dict)
    dead_lettered: list[Any] = field(default_factory=list)
    connections: int = 0

    def check(self) -> None:
        if self.down:
            raise amqp_ex.AMQPConnectionError("connection refused")

    async def connect(self, url: str) -> "FakeConnection":
        self.check()
        self.connections += 1
        return FakeConnection(self)


def publish_error(exchange: str, routing_key: str) -> amqp_ex.PublishError:
    """What aiormq raises when a mandatory message is returned unroutable."""
    frame = Basic.Return(reply_code=312, reply_text="NO_ROUTE", exchange=exchange, routing_key=routing_key)
    return amqp_ex.PublishError(SimpleNamespace(delivery=frame), frame)


class FakeConnection:
    def __init__(self, broker: FakeAmqpBroker) -> None:
        self.broker = broker
        self.is_closed = False

    async def channel(self, publisher_confirms: bool = True) -> "FakeChannel":
        self.broker.check()
        return FakeChannel(self.broker)

    async def close(self) -> None:
        self.is_closed = True


class FakeChannel:
    def __init__(self, broker: FakeAmqpBroker) -> None:
        self.broker = broker
        self.is_closed = False
        self.prefetch = None
        self.default_exchange = FakeExchange(broker, "", "direct")

    async def set_qos(self, prefetch_count: int) -> None:
        self.prefetch = prefetch_count

    async def declare_exchange(self, name, type="direct", *, durable=False, passive=False, **kw):
        self.broker.check()
        if passive and name not in self.broker.exchanges:
            raise amqp_ex.ChannelNotFoundEntity(f"NOT_FOUND - no exchange '{name}'")
        return self.broker.exchanges.setdefault(name, FakeExchange(self.broker, name, str(type)))

    async def declare_queue(self, name, *, durable=False, passive=False, arguments=None, **kw):
        self.broker.check()
        if passive and name not in self.broker.queues:
            raise amqp_ex.ChannelNotFoundEntity(f"NOT_FOUND - no queue '{name}'")
        queue = self.broker.queues.setdefault(name, FakeQueue(self.broker, name))
        queue.arguments = arguments
        return queue


class FakeExchange:
    def __init__(self, broker: FakeAmqpBroker, name: str, type: str) -> None:
        self.broker = broker
        self.name = name
        self.type = type
        self.bindings: list[tuple[str, FakeQueue]] = []

    async def publish(self, message, routing_key, *, mandatory=True, timeout=None):
        self.broker.check()
        if self.name == "":
            targets = [self.broker.queues[routing_key]] if routing_key in self.broker.queues else []
        else:
            targets = [q for key, q in self.bindings if topic_match(key, routing_key)]
        if not targets and mandatory:
            raise publish_error(self.name, routing_key)
        for q in targets:
            q.put(message)


class FakeIncoming:
    def __init__(self, queue: "FakeQueue", message: Any, redelivered: bool) -> None:
        self.queue = queue
        self._m = message
        self.body = message.body
        self.headers = message.headers
        self.message_id = message.message_id
        self.correlation_id = message.correlation_id
        self.content_type = message.content_type
        self.type = message.type
        self.redelivered = redelivered
        self.settled: str | None = None

    async def ack(self):
        self.settled = "ack"

    async def nack(self, requeue=True):
        self.settled = "nack"
        if requeue:
            self.queue.put(self._m, redelivered=True)

    async def reject(self, requeue=False):
        self.settled = "reject"
        if requeue:
            self.queue.put(self._m, redelivered=True)
        else:
            self.queue.broker.dead_lettered.append(self._m)


class FakeQueue:
    _tags = itertools.count(1)

    def __init__(self, broker: FakeAmqpBroker, name: str) -> None:
        self.broker = broker
        self.name = name
        self.arguments = None
        self.pending: list[tuple[Any, bool]] = []
        self.consumers: dict[str, Any] = {}
        self.tasks: set[asyncio.Task] = set()

    async def bind(self, exchange, routing_key=None, **kw):
        exchange.bindings.append((routing_key, self))

    def put(self, message, redelivered=False):
        self.pending.append((message, redelivered))
        self._pump()

    def _pump(self):
        while self.pending and self.consumers:
            message, redelivered = self.pending.pop(0)
            callback = next(iter(self.consumers.values()))
            task = asyncio.get_running_loop().create_task(callback(FakeIncoming(self, message, redelivered)))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

    async def consume(self, callback, no_ack=False, **kw):
        tag = f"ctag-{next(self._tags)}"
        self.consumers[tag] = callback
        self._pump()
        return tag

    async def cancel(self, tag):
        self.consumers.pop(tag, None)


# =====================================================================================================
# azure-servicebus
# =====================================================================================================

@dataclass
class FakeSbMessage:
    body: bytes
    application_properties: dict
    message_id: str | None
    correlation_id: str | None
    content_type: str | None
    subject: str | None
    delivery_count: int = 0


@dataclass
class FakeSbNamespace:
    down: bool = False
    busy: bool = False
    entities: dict[str, list[FakeSbMessage]] = field(default_factory=dict)   # queue or topic/subscriptions/sub
    topics: dict[str, list[str]] = field(default_factory=dict)               # topic -> subscriptions
    dead_letters: list[FakeSbMessage] = field(default_factory=list)
    missing: set[str] = field(default_factory=set)
    closed_clients: int = 0

    def check(self, entity: str | None = None) -> None:
        if self.down:
            raise sb_ex.ServiceBusConnectionError(message="connection lost")
        if self.busy:
            raise sb_ex.ServiceBusServerBusyError(message="server busy")
        if entity in self.missing:
            raise sb_ex.MessagingEntityNotFoundError(message=f"{entity} not found")

    def client(self) -> "FakeSbClient":
        return FakeSbClient(self)


class FakeSbClient:
    def __init__(self, ns: FakeSbNamespace) -> None:
        self.ns = ns

    def get_queue_sender(self, name):
        return FakeSbSender(self.ns, name, topic=False)

    def get_topic_sender(self, name):
        return FakeSbSender(self.ns, name, topic=True)

    def get_queue_receiver(self, name, **kw):
        return FakeSbReceiver(self.ns, name, **kw)

    def get_subscription_receiver(self, topic, sub, **kw):
        subs = self.ns.topics.setdefault(topic, [])
        if sub not in subs:
            subs.append(sub)
        return FakeSbReceiver(self.ns, f"{topic}/subscriptions/{sub}", **kw)

    async def close(self):
        self.ns.closed_clients += 1


class FakeSbSender:
    def __init__(self, ns: FakeSbNamespace, name: str, topic: bool) -> None:
        self.ns = ns
        self.name = name
        self.topic = topic

    async def send_messages(self, message):
        self.ns.check(self.name)
        body = message.body if isinstance(message.body, bytes) else b"".join(message.body)
        fake = FakeSbMessage(body, dict(message.application_properties or {}), message.message_id,
                             message.correlation_id, message.content_type, message.subject)
        if self.topic:
            for sub in self.ns.topics.get(self.name, []):
                self.ns.entities.setdefault(f"{self.name}/subscriptions/{sub}", []).append(
                    FakeSbMessage(**fake.__dict__))
        else:
            self.ns.entities.setdefault(self.name, []).append(fake)

    async def close(self):
        pass


class FakeSbReceiver:
    def __init__(self, ns: FakeSbNamespace, entity: str, **kw) -> None:
        self.ns = ns
        self.entity = entity
        self.kwargs = kw
        self.locked: list[FakeSbMessage] = []
        self.fail_next_receive = 0

    async def __aenter__(self):
        self.ns.check(self.entity)
        return self

    async def __aexit__(self, *exc):
        return None

    async def close(self):
        pass

    async def receive_messages(self, max_message_count=1, max_wait_time=None):
        if self.fail_next_receive:
            self.fail_next_receive -= 1
            raise sb_ex.ServiceBusCommunicationError(message="network blip")
        deadline = asyncio.get_running_loop().time() + (max_wait_time or 0.05)
        while True:
            queue = self.ns.entities.setdefault(self.entity, [])
            if queue and not self.ns.down:
                batch = queue[:max_message_count]
                del queue[:max_message_count]
                for m in batch:
                    m.delivery_count += 1
                self.locked.extend(batch)
                return batch
            if asyncio.get_running_loop().time() >= deadline:
                return []
            await asyncio.sleep(0.005)

    async def complete_message(self, message):
        self.locked.remove(message)

    async def abandon_message(self, message):
        self.locked.remove(message)
        self.ns.entities.setdefault(self.entity, []).append(message)

    async def dead_letter_message(self, message, reason=None, error_description=None):
        self.locked.remove(message)
        self.ns.dead_letters.append(message)

    async def peek_messages(self, max_message_count=1, **kw):
        self.ns.check(self.entity)
        return list(self.ns.entities.get(self.entity, []))[:max_message_count]
