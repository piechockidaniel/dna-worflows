"""RabbitMQ transport on aio-pika (plan item P2.6) – the on-premise representative.

Endpoint binding for RabbitMQ::

    orders.created:
      transport: rabbitmq
      address: DnA Workflows.orders          # the queue this node consumes from
      consume: true
      options:
        exchange: orders                # "" = default exchange (address is then the routing key)
        exchange_type: topic
        routing_key: order.created      # used when sending (default: address)
        bind_keys: [order.created]      # queue bindings when consuming (default: [routing_key])
        dead_letter_exchange: orders.dlx
        queue_arguments: {x-queue-type: quorum}

Reliability choices: publisher confirms + ``mandatory`` (unroutable = permanent error, not silent
loss), persistent delivery, manual acknowledgements after the workflow committed, and a short delay
before requeueing a failed delivery so a poison message cannot spin the CPU.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.messaging.core import (EndpointBinding, IncomingMessage, MessageEnvelope, MessageHandler,
                                     RejectMessage, TransportCapabilities, TransportError, TransportPermanentError,
                                     TransportUnavailable)
from DnA Workflows.messaging.feature import add_transport
from DnA Workflows.resilience.feature import add_release_rule
from DnA Workflows.resilience.release import ReleaseRule

log = logging.getLogger(__name__)

Connect = Callable[..., Awaitable[Any]]


def map_rabbitmq_error(exc: BaseException, transport: str) -> TransportError:
    """aio-pika / aiormq exception → DnA Workflows transport error (→ fault kind, P3.1)."""
    if isinstance(exc, TransportError):
        return exc
    from aio_pika import exceptions as ex
    message = f"{type(exc).__name__}: {exc}"
    permanent = (ex.ChannelNotFoundEntity, ex.ChannelPreconditionFailed, ex.PublishError,
                 ex.ProbableAuthenticationError, ex.AuthenticationError, ex.IncompatibleProtocolError)
    if isinstance(exc, permanent):
        return TransportPermanentError(message, transport=transport)
    return TransportUnavailable(message, transport=transport)


class _RabbitSubscription:
    def __init__(self, queue: Any, tag: str) -> None:
        self._queue = queue
        self._tag = tag

    async def close(self) -> None:
        try:
            await self._queue.cancel(self._tag)
        except Exception as exc:  # channel may already be gone; nothing left to cancel
            log.debug("Cancel consumer %s: %s", self._tag, exc)


class RabbitMqTransport:
    capabilities = TransportCapabilities(publish_subscribe=True, competing_consumers=True, ordering="none",
                                         delayed_delivery=False, dead_letter=True)

    def __init__(self, name: str = "rabbitmq", url: str = "amqp://guest:guest@localhost/", *,
                 prefetch: int = 20, declare_topology: bool = True, connect_timeout: float = 10.0,
                 publish_timeout: float = 10.0, redelivery_delay: float = 1.0,
                 connect: Connect | None = None) -> None:
        self.name = name
        self.url = url
        self.prefetch = prefetch
        self.declare_topology = declare_topology
        self.connect_timeout = connect_timeout
        self.publish_timeout = publish_timeout
        self.redelivery_delay = redelivery_delay
        self._connect = connect
        self._connection: Any = None
        self._publish_channel: Any = None
        self._exchanges: dict[str, Any] = {}
        self._declared_queues: set[str] = set()
        self._lock = asyncio.Lock()

    # -- connection management ------------------------------------------------------------------
    async def _connection_or_connect(self) -> Any:
        if self._connection is not None and not getattr(self._connection, "is_closed", False):
            return self._connection
        async with self._lock:
            if self._connection is None or getattr(self._connection, "is_closed", False):
                connect = self._connect
                if connect is None:
                    import aio_pika
                    connect = aio_pika.connect_robust
                try:
                    self._connection = await asyncio.wait_for(connect(self.url), self.connect_timeout)
                except Exception as exc:
                    raise map_rabbitmq_error(exc, self.name) from exc
                self._publish_channel = None
                self._exchanges.clear()
        return self._connection

    async def _channel(self) -> Any:
        conn = await self._connection_or_connect()
        if self._publish_channel is None or getattr(self._publish_channel, "is_closed", False):
            self._publish_channel = await conn.channel(publisher_confirms=True)
            self._exchanges.clear()
        return self._publish_channel

    async def _exchange(self, channel: Any, binding: EndpointBinding) -> Any:
        name = binding.options.get("exchange", "")
        if not name:
            return channel.default_exchange
        if name not in self._exchanges:
            self._exchanges[name] = await channel.declare_exchange(
                name, binding.options.get("exchange_type", "topic"), durable=True,
                passive=not self.declare_topology)
        return self._exchanges[name]

    async def start(self) -> None:
        await self._connection_or_connect()

    async def stop(self) -> None:
        conn, self._connection = self._connection, None
        self._publish_channel = None
        self._exchanges.clear()
        if conn is not None:
            await conn.close()

    async def probe(self) -> tuple[bool, str]:
        try:
            await self._connection_or_connect()
            return True, "connected"
        except TransportError as exc:
            return False, str(exc)

    # -- send -----------------------------------------------------------------------------------
    @staticmethod
    def routing_key(binding: EndpointBinding) -> str:
        if not binding.options.get("exchange"):
            return binding.address
        return binding.options.get("routing_key", binding.address)

    async def send(self, binding: EndpointBinding, envelope: MessageEnvelope) -> None:
        import aio_pika
        try:
            channel = await self._channel()
            exchange = await self._exchange(channel, binding)
            message = aio_pika.Message(
                envelope.body(), headers=envelope.wire_headers(), content_type=envelope.datacontenttype,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT, message_id=envelope.id,
                correlation_id=envelope.correlation_id, type=envelope.type,
                timestamp=envelope.time, app_id=envelope.source[:255])
            await exchange.publish(message, routing_key=self.routing_key(binding), mandatory=True,
                                   timeout=self.publish_timeout)
        except Exception as exc:
            raise map_rabbitmq_error(exc, self.name) from exc

    # -- consume --------------------------------------------------------------------------------
    async def subscribe(self, binding: EndpointBinding, handler: MessageHandler) -> _RabbitSubscription:
        try:
            conn = await self._connection_or_connect()
            channel = await conn.channel()
            await channel.set_qos(prefetch_count=int(binding.options.get("prefetch", self.prefetch)))
            queue = await self._declare_queue(channel, binding)
            tag = await queue.consume(self._callback(binding, handler), no_ack=False)
        except Exception as exc:
            raise map_rabbitmq_error(exc, self.name) from exc
        return _RabbitSubscription(queue, tag)

    async def _declare_queue(self, channel: Any, binding: EndpointBinding) -> Any:
        opts = binding.options
        if not self.declare_topology:
            return await channel.declare_queue(binding.address, passive=True)
        arguments = dict(opts.get("queue_arguments", {}))
        if opts.get("dead_letter_exchange"):
            arguments["x-dead-letter-exchange"] = opts["dead_letter_exchange"]
        queue = await channel.declare_queue(binding.address, durable=opts.get("durable", True),
                                            arguments=arguments or None)
        exchange_name = opts.get("exchange", "")
        if exchange_name:
            exchange = await channel.declare_exchange(exchange_name, opts.get("exchange_type", "topic"), durable=True)
            for key in opts.get("bind_keys") or [self.routing_key(binding)]:
                await queue.bind(exchange, routing_key=key)
        return queue

    def _callback(self, binding: EndpointBinding, handler: MessageHandler) -> Callable[[Any], Awaitable[None]]:
        async def on_message(raw: Any) -> None:
            envelope = MessageEnvelope.from_wire(
                raw.body, raw.headers or {}, message_id=raw.message_id, correlation_id=raw.correlation_id,
                content_type=raw.content_type)
            if envelope.type == "unknown" and raw.type:
                envelope.type = raw.type
            incoming = IncomingMessage(envelope=envelope, endpoint=binding.name, address=binding.address,
                                       delivery_count=2 if raw.redelivered else 1, raw=raw)
            try:
                await handler(incoming)
            except RejectMessage as exc:
                log.info("Rejecting message %s on %s: %s", envelope.id, binding.name, exc)
                await raw.reject(requeue=False)   # → dead-letter exchange when configured
            except Exception as exc:
                log.warning("Delivery of %s on %s failed, requeueing: %s", envelope.id, binding.name, exc)
                await asyncio.sleep(self.redelivery_delay)
                await raw.nack(requeue=True)
            else:
                await raw.ack()
        return on_message


class RabbitMqOptions(FeatureOptions):
    name: str = "rabbitmq"
    url: str = "amqp://guest:guest@localhost/"
    prefetch: int = 20
    declare_topology: bool = True
    connect_timeout: float = 10.0
    publish_timeout: float = 10.0
    redelivery_delay: float = 1.0
    #: Release strategy this module contributes for holds on ``<name>:*`` dependencies (P3.7).
    release_strategy: str | None = "probe-auto"
    release_options: dict[str, Any] = {"successes_required": 2, "interval": 15}


class RabbitMqTransportFeature(Feature):
    """RabbitMQ transport (aio-pika). Install with ``pip install DnA Workflows[rabbitmq]``."""

    name = "messaging.rabbitmq"
    depends_on = ("messaging",)
    Options = RabbitMqOptions

    def configure(self, services: ServiceCollection) -> None:
        o: RabbitMqOptions = self.options  # type: ignore[assignment]
        add_transport(services, o.name, lambda p: RabbitMqTransport(
            o.name, o.url, prefetch=o.prefetch, declare_topology=o.declare_topology,
            connect_timeout=o.connect_timeout, publish_timeout=o.publish_timeout,
            redelivery_delay=o.redelivery_delay))
        if o.release_strategy:
            add_release_rule(services, ReleaseRule(f"{o.name}:*", o.release_strategy, dict(o.release_options)))
