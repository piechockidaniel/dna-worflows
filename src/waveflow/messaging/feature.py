"""Messaging feature (plan phase P2).

Example::

    messaging:
      endpoints:
        orders.created: {transport: rabbitmq, address: DnA Workflows.orders, consume: true,
                         options: {exchange: orders, bind_keys: ["order.created"]}}
        invoices:       {transport: servicebus, address: invoices}
      unmatched: dead_letter
    messaging.rabbitmq: {url: "${RABBITMQ_URL}"}
    messaging.servicebus: {connection_string: "${SERVICEBUS_CONNECTION}"}
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import HostedService
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.messaging.activities import MessageReceived, SendMessage
from DnA Workflows.messaging.bus import (EndpointRegistry, MessageBus, SendMessageIntentHandler, TransportErrorClassifier,
                                    TransportHealthProbe)
from DnA Workflows.messaging.consumer import ConsumerHost
from DnA Workflows.messaging.core import EndpointBinding, TransportRef
from DnA Workflows.persistence.contracts import JournalStore
from DnA Workflows.resilience.release import HealthProbe
from DnA Workflows.runtime.contracts import FaultClassifier, RuntimeSignals, WorkGate
from DnA Workflows.runtime.feature import add_activity
from DnA Workflows.runtime.outbox import IntentHandler
from DnA Workflows.runtime.router import StimulusRouter


class MessagingOptions(FeatureOptions):
    endpoints: dict[str, EndpointBinding] = Field(default_factory=dict)
    unmatched: Literal["dead_letter", "requeue", "ack"] = "dead_letter"
    source: str = "DnA Workflows"
    #: How often consumers are re-reconciled (retries failed subscriptions).
    consumer_retry_interval: float = 5.0
    background_services: bool = True


class MessagingFeature(Feature):
    """Broker-neutral messaging: logical endpoints, transactional send, consumers → stimuli."""

    name = "messaging"
    depends_on = ("runtime",)
    Options = MessagingOptions

    def configure(self, services: ServiceCollection) -> None:
        o: MessagingOptions = self.options  # type: ignore[assignment]
        services.add(EndpointRegistry, lambda p: EndpointRegistry(dict(o.endpoints)))
        services.add(MessageBus, lambda p: MessageBus(
            p.get(EndpointRegistry), [r.transport for r in p.get_all(TransportRef)], p.get(Clock),
            p.get(IdGenerator), source=o.source))
        services.add_contributor(IntentHandler, lambda p: SendMessageIntentHandler(p.get(MessageBus)),
                                 key="messaging.send")
        services.add_contributor(FaultClassifier, TransportErrorClassifier, key="messaging.transport")
        services.add_contributor(HealthProbe, lambda p: TransportHealthProbe(p.get(MessageBus)),
                                 key="messaging.transport")
        add_activity(services, SendMessage, MessageReceived)
        services.add(ConsumerHost, lambda p: ConsumerHost(
            p.get(MessageBus), p.get(StimulusRouter), p.get(WorkGate), p.get(RuntimeSignals),
            p.get(JournalStore), p.get(Clock), p.get(IdGenerator), unmatched=o.unmatched,
            interval=o.consumer_retry_interval))
        if o.background_services:
            services.add_contributor(HostedService, lambda p: p.get(ConsumerHost), key="messaging.consumers")


def add_transport(services: ServiceCollection, name: str, factory) -> None:
    """Helper for provider features: contribute a transport (Extend axis)."""
    services.add_contributor(TransportRef, lambda p: TransportRef(transport=factory(p)), key=f"transport:{name}")
