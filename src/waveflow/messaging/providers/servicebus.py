"""Azure Service Bus transport (plan item P2.6) – the cloud representative.

Endpoint binding for Service Bus::

    invoices:                       # queue
      transport: servicebus
      address: invoices
      consume: true
    order-events:                   # topic + subscription
      transport: servicebus
      address: order-events/subscriptions/DnA Workflows   # send goes to the topic, consume from the subscription
      consume: true
      options: {max_wait_time: 5, prefetch: 20, session_id: null}

Authentication: a connection string, or ``namespace`` + Azure AD (``DefaultAzureCredential``,
requires ``azure-identity``). Settlement: complete after the workflow committed, abandon on failure
(Service Bus redelivers and dead-letters after MaxDeliveryCount), dead-letter on reject.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.messaging.core import (EndpointBinding, IncomingMessage, MessageEnvelope, MessageHandler,
                                     RejectMessage, TransportCapabilities, TransportError, TransportPermanentError,
                                     TransportThrottled, TransportUnavailable)
from DnA Workflows.messaging.feature import add_transport
from DnA Workflows.resilience.feature import add_release_rule
from DnA Workflows.resilience.release import ReleaseRule

log = logging.getLogger(__name__)


def map_servicebus_error(exc: BaseException, transport: str) -> TransportError:
    if isinstance(exc, TransportError):
        return exc
    from azure.servicebus import exceptions as ex
    message = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, (ex.ServiceBusServerBusyError, ex.ServiceBusQuotaExceededError)):
        return TransportThrottled(message, transport=transport, retry_after=10)
    if isinstance(exc, (ex.MessagingEntityNotFoundError, ex.MessagingEntityDisabledError,
                        ex.MessageSizeExceededError, ex.ServiceBusAuthenticationError,
                        ex.ServiceBusAuthorizationError)):
        return TransportPermanentError(message, transport=transport)
    return TransportUnavailable(message, transport=transport)


def parse_address(address: str) -> tuple[str, str | None]:
    """``queue`` → (queue, None); ``topic/subscriptions/sub`` → (topic, sub)."""
    parts = address.split("/subscriptions/", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (address, None)


class _ServiceBusSubscription:
    def __init__(self, task: asyncio.Task[None], receiver: Any) -> None:
        self._task = task
        self._receiver = receiver

    async def close(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        try:
            await self._receiver.close()
        except Exception as exc:  # pragma: no cover - best effort
            log.debug("Closing receiver: %s", exc)


class ServiceBusTransport:
    capabilities = TransportCapabilities(publish_subscribe=True, competing_consumers=True, ordering="session",
                                         delayed_delivery=True, dead_letter=True, max_message_bytes=256 * 1024)

    def __init__(self, name: str = "servicebus", *, connection_string: str | None = None,
                 namespace: str | None = None, client_factory: Callable[[], Any] | None = None,
                 max_wait_time: float = 5.0, prefetch: int = 20, batch_size: int = 10,
                 probe_entity: str | None = None, error_backoff: float = 5.0) -> None:
        self.name = name
        self.connection_string = connection_string
        self.namespace = namespace
        self._client_factory = client_factory
        self.max_wait_time = max_wait_time
        self.prefetch = prefetch
        self.batch_size = batch_size
        self.probe_entity = probe_entity
        self.error_backoff = error_backoff
        self._client: Any = None
        self._senders: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    def _create_client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory()
        from azure.servicebus.aio import ServiceBusClient
        if self.connection_string:
            return ServiceBusClient.from_connection_string(self.connection_string)
        if self.namespace:
            from azure.identity.aio import DefaultAzureCredential  # optional dependency
            return ServiceBusClient(self.namespace, DefaultAzureCredential())
        raise TransportPermanentError("Service Bus needs 'connection_string' or 'namespace'", transport=self.name)

    async def _get_client(self) -> Any:
        async with self._lock:
            if self._client is None:
                self._client = self._create_client()
            return self._client

    async def start(self) -> None:
        await self._get_client()

    async def stop(self) -> None:
        for sender in self._senders.values():
            try:
                await sender.close()
            except Exception:  # pragma: no cover - best effort
                pass
        self._senders.clear()
        client, self._client = self._client, None
        if client is not None:
            await client.close()

    async def send(self, binding: EndpointBinding, envelope: MessageEnvelope) -> None:
        from azure.servicebus import ServiceBusMessage
        entity, subscription = parse_address(binding.address)
        try:
            client = await self._get_client()
            if entity not in self._senders:
                # a consumed subscription address means the entity is a topic
                is_topic = subscription is not None or binding.options.get("topic", False)
                self._senders[entity] = client.get_topic_sender(entity) if is_topic else client.get_queue_sender(entity)
            sender = self._senders[entity]
            message = ServiceBusMessage(
                envelope.body(), application_properties=envelope.wire_headers(), message_id=envelope.id,
                correlation_id=envelope.correlation_id, subject=envelope.subject or envelope.type,
                content_type=envelope.datacontenttype,
                session_id=binding.options.get("session_id"))
            await sender.send_messages(message)
        except Exception as exc:
            self._senders.pop(entity, None)  # reopen a fresh link next time
            raise map_servicebus_error(exc, self.name) from exc

    async def probe(self) -> tuple[bool, str]:
        entity = self.probe_entity
        if entity is None:
            return False, "no probe_entity configured"
        topic, sub = parse_address(entity)
        try:
            client = await self._get_client()
            receiver = (client.get_subscription_receiver(topic, sub) if sub else client.get_queue_receiver(topic))
            async with receiver:
                await receiver.peek_messages(max_message_count=1)
            return True, f"peeked '{entity}'"
        except Exception as exc:
            return False, str(map_servicebus_error(exc, self.name))

    async def subscribe(self, binding: EndpointBinding, handler: MessageHandler) -> _ServiceBusSubscription:
        topic, sub = parse_address(binding.address)
        opts = binding.options
        try:
            client = await self._get_client()
            kwargs = {"max_wait_time": opts.get("max_wait_time", self.max_wait_time),
                      "prefetch_count": opts.get("prefetch", self.prefetch)}
            if opts.get("session_id"):
                kwargs["session_id"] = opts["session_id"]
            receiver = (client.get_subscription_receiver(topic, sub, **kwargs) if sub
                        else client.get_queue_receiver(topic, **kwargs))
            await receiver.__aenter__()
        except Exception as exc:
            raise map_servicebus_error(exc, self.name) from exc
        task = asyncio.create_task(self._receive_loop(binding, receiver, handler), name=f"servicebus:{binding.name}")
        return _ServiceBusSubscription(task, receiver)

    async def _receive_loop(self, binding: EndpointBinding, receiver: Any, handler: MessageHandler) -> None:
        while True:
            try:
                batch = await receiver.receive_messages(max_message_count=self.batch_size,
                                                        max_wait_time=self.max_wait_time)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Service Bus receive on %s failed: %s", binding.name, map_servicebus_error(exc, self.name))
                await asyncio.sleep(self.error_backoff)
                continue
            for raw in batch:
                await self._dispatch(binding, receiver, raw, handler)

    async def _dispatch(self, binding: EndpointBinding, receiver: Any, raw: Any, handler: MessageHandler) -> None:
        body = raw.body
        if not isinstance(body, (bytes, str)):
            body = b"".join(body)
        headers = {(k.decode() if isinstance(k, bytes) else k): v
                   for k, v in (raw.application_properties or {}).items()}
        envelope = MessageEnvelope.from_wire(body, headers, message_id=raw.message_id,
                                             correlation_id=raw.correlation_id, content_type=raw.content_type,
                                             subject=raw.subject)
        if envelope.type == "unknown" and raw.subject:
            envelope.type = raw.subject
        incoming = IncomingMessage(envelope=envelope, endpoint=binding.name, address=binding.address,
                                   delivery_count=raw.delivery_count or 1, raw=raw)
        try:
            await handler(incoming)
        except RejectMessage as exc:
            await receiver.dead_letter_message(raw, reason="rejected", error_description=str(exc)[:1024])
        except Exception as exc:
            log.warning("Delivery of %s on %s failed, abandoning: %s", envelope.id, binding.name, exc)
            await receiver.abandon_message(raw)
        else:
            await receiver.complete_message(raw)


class ServiceBusOptions(FeatureOptions):
    name: str = "servicebus"
    connection_string: str | None = None
    namespace: str | None = None          # e.g. "mybus.servicebus.windows.net" (Azure AD auth)
    max_wait_time: float = 5.0
    prefetch: int = 20
    batch_size: int = 10
    probe_entity: str | None = None       # queue or topic/subscriptions/sub to peek for health
    release_strategy: str | None = "probe-auto"
    release_options: dict[str, Any] = {"successes_required": 2, "interval": 30}


class ServiceBusTransportFeature(Feature):
    """Azure Service Bus transport. Install with ``pip install DnA Workflows[servicebus]``."""

    name = "messaging.servicebus"
    depends_on = ("messaging",)
    Options = ServiceBusOptions

    def configure(self, services: ServiceCollection) -> None:
        o: ServiceBusOptions = self.options  # type: ignore[assignment]
        add_transport(services, o.name, lambda p: ServiceBusTransport(
            o.name, connection_string=o.connection_string, namespace=o.namespace, max_wait_time=o.max_wait_time,
            prefetch=o.prefetch, batch_size=o.batch_size, probe_entity=o.probe_entity))
        if o.release_strategy:
            add_release_rule(services, ReleaseRule(f"{o.name}:*", o.release_strategy, dict(o.release_options)))
