"""Provider tests: conformance kit on every transport + provider-specific behaviour (P2.6, P2.7)."""

import asyncio
import itertools
import os

import pytest

from DnA Workflows.messaging.conformance import run_transport_conformance
from DnA Workflows.messaging.core import (EndpointBinding, MessageEnvelope, TransportPermanentError, TransportThrottled,
                                     TransportUnavailable)
from DnA Workflows.messaging.providers.inmemory import InMemoryBroker, InMemoryTransport
from DnA Workflows.messaging.providers.rabbitmq import RabbitMqTransport, map_rabbitmq_error
from DnA Workflows.messaging.providers.servicebus import ServiceBusTransport, map_servicebus_error, parse_address

from .fakes import FakeAmqpBroker, FakeSbNamespace, publish_error

_n = itertools.count(1)


def fresh(transport="t", **options):
    n = next(_n)
    return lambda: EndpointBinding(name=f"ep{n}-{next(_n)}", transport=transport, address=f"q{n}-{next(_n)}",
                                   options=options)


def env(n=1, **kw):
    return MessageEnvelope(id=f"m{n}", source="s", type="t", data={"n": n}, **kw)


# ----------------------------------------------------------------------------------------------------
# conformance on all transports
# ----------------------------------------------------------------------------------------------------

async def test_inmemory_conformance():
    InMemoryBroker.reset()
    report = await run_transport_conformance(InMemoryTransport(redelivery_delay=0.01), fresh("inmemory"), timeout=3)
    assert report.passed == ["probe", "roundtrip", "redelivery", "reject", "close-keeps-messages"]


async def test_rabbitmq_conformance_default_exchange():
    broker = FakeAmqpBroker()
    t = RabbitMqTransport(connect=broker.connect, redelivery_delay=0.01)
    report = await run_transport_conformance(t, fresh("rabbitmq"), timeout=3)
    assert len(report.passed) == 5 and len(broker.dead_lettered) == 1


async def test_rabbitmq_conformance_topic_exchange():
    broker = FakeAmqpBroker()
    t = RabbitMqTransport(connect=broker.connect, redelivery_delay=0.01)

    def binding():
        n = next(_n)
        return EndpointBinding(name=f"orders{n}", transport="rabbitmq", address=f"DnA Workflows.orders{n}",
                               options={"exchange": "orders", "routing_key": f"order.created.{n}",
                                        "bind_keys": [f"order.*.{n}"], "dead_letter_exchange": "orders.dlx",
                                        "queue_arguments": {"x-queue-type": "quorum"}})
    await run_transport_conformance(t, binding, timeout=3)
    q = next(iter(broker.queues.values()))
    assert q.arguments == {"x-queue-type": "quorum", "x-dead-letter-exchange": "orders.dlx"}
    assert broker.exchanges["orders"].type == "topic"


@pytest.mark.parametrize("topic", [False, True])
async def test_servicebus_conformance(topic):
    ns = FakeSbNamespace()

    def binding():
        n = next(_n)
        address = f"events{n}/subscriptions/DnA Workflows" if topic else f"queue{n}"
        return EndpointBinding(name=f"sb{n}", transport="servicebus", address=address)

    # the conformance probe needs an entity to peek
    t = ServiceBusTransport(client_factory=ns.client, probe_entity="health", max_wait_time=0.02)
    report = await run_transport_conformance(t, binding, timeout=3)
    assert len(report.passed) == 5 and len(ns.dead_letters) == 1
    assert ns.closed_clients == 1


# ----------------------------------------------------------------------------------------------------
# RabbitMQ specifics
# ----------------------------------------------------------------------------------------------------

async def test_rabbitmq_error_mapping_and_outage():
    import aio_pika.exceptions as ex
    assert isinstance(map_rabbitmq_error(ex.AMQPConnectionError("x"), "r"), TransportUnavailable)
    assert isinstance(map_rabbitmq_error(ex.ChannelNotFoundEntity("x"), "r"), TransportPermanentError)
    assert isinstance(map_rabbitmq_error(publish_error("x", "k"), "r"), TransportPermanentError)
    assert isinstance(map_rabbitmq_error(asyncio.TimeoutError(), "r"), TransportUnavailable)
    already = TransportUnavailable("x")
    assert map_rabbitmq_error(already, "r") is already

    broker = FakeAmqpBroker(down=True)
    t = RabbitMqTransport(connect=broker.connect)
    assert await t.probe() == (False, "AMQPConnectionError: connection refused")
    b = EndpointBinding(name="e", transport="rabbitmq", address="q")
    with pytest.raises(TransportUnavailable):
        await t.send(b, env())
    with pytest.raises(TransportUnavailable):
        await t.subscribe(b, None)
    broker.down = False
    assert (await t.probe())[0] is True
    with pytest.raises(TransportPermanentError, match="NO_ROUTE"):   # unroutable: queue does not exist
        await t.send(b, env())
    await t.stop()
    await t.stop()


async def test_rabbitmq_reconnects_after_connection_closed_and_passive_topology():
    broker = FakeAmqpBroker()
    t = RabbitMqTransport(connect=broker.connect, declare_topology=False)
    b = EndpointBinding(name="e", transport="rabbitmq", address="q", options={"exchange": "ex"})
    with pytest.raises(TransportPermanentError, match="no exchange"):
        await t.send(b, env())
    with pytest.raises(TransportPermanentError, match="no queue"):
        await t.subscribe(b, None)
    t._connection.is_closed = True                          # robust connection died for good
    await t.start()
    assert broker.connections == 2
    assert RabbitMqTransport.routing_key(b) == "q"
    assert RabbitMqTransport.routing_key(EndpointBinding(name="e", transport="r", address="q",
                                                         options={"exchange": "x", "routing_key": "k"})) == "k"


async def test_rabbitmq_foreign_message_type_from_amqp_property():
    broker = FakeAmqpBroker()
    t = RabbitMqTransport(connect=broker.connect)
    b = EndpointBinding(name="legacy", transport="rabbitmq", address="legacy")
    got = asyncio.Queue()

    async def handler(m):
        await got.put(m)
    await t.subscribe(b, handler)
    import aio_pika
    broker.queues["legacy"].put(aio_pika.Message(b'{"x": 1}', type="legacy.created", message_id="native"))
    m = await asyncio.wait_for(got.get(), 2)
    assert (m.envelope.type, m.envelope.id, m.envelope.data) == ("legacy.created", "native", {"x": 1})


# ----------------------------------------------------------------------------------------------------
# Service Bus specifics
# ----------------------------------------------------------------------------------------------------

async def test_servicebus_error_mapping():
    from azure.servicebus import exceptions as ex
    cases = {
        ex.ServiceBusServerBusyError: TransportThrottled, ex.ServiceBusQuotaExceededError: TransportThrottled,
        ex.MessagingEntityNotFoundError: TransportPermanentError, ex.MessageSizeExceededError: TransportPermanentError,
        ex.ServiceBusAuthorizationError: TransportPermanentError, ex.ServiceBusConnectionError: TransportUnavailable,
        ex.OperationTimeoutError: TransportUnavailable,
    }
    for err, expected in cases.items():
        assert isinstance(map_servicebus_error(err(message="x"), "sb"), expected), err
    assert map_servicebus_error(TransportUnavailable("x"), "sb").__class__ is TransportUnavailable
    assert parse_address("q") == ("q", None)
    assert parse_address("topic/subscriptions/sub") == ("topic", "sub")


async def test_servicebus_send_failures_reopen_sender_and_probe():
    ns = FakeSbNamespace()
    t = ServiceBusTransport(client_factory=ns.client)
    b = EndpointBinding(name="e", transport="servicebus", address="orders")
    assert await t.probe() == (False, "no probe_entity configured")
    ns.down = True
    with pytest.raises(TransportUnavailable):
        await t.send(b, env())
    assert "orders" not in t._senders
    ns.down, ns.busy = False, True
    with pytest.raises(TransportThrottled) as err:
        await t.send(b, env())
    assert err.value.retry_after == 10
    ns.busy = False
    ns.missing.add("orders")
    with pytest.raises(TransportPermanentError):
        await t.send(b, env())
    ns.missing.clear()
    await t.send(b, env(subject="sub"))
    assert ns.entities["orders"][0].subject == "sub"
    await t.send(EndpointBinding(name="t", transport="servicebus", address="topic", options={"topic": True}), env())
    t.probe_entity = "orders"
    assert (await t.probe())[0] is True
    t.probe_entity = "topic/subscriptions/s"
    assert (await t.probe())[0] is True
    ns.down = True
    assert (await t.probe())[0] is False
    with pytest.raises(TransportUnavailable):
        await t.subscribe(b, None)
    await t.stop()


async def test_servicebus_receive_errors_are_survived_and_foreign_messages_read():
    ns = FakeSbNamespace()
    t = ServiceBusTransport(client_factory=ns.client, max_wait_time=0.01, error_backoff=0.01)
    b = EndpointBinding(name="e", transport="servicebus", address="legacy", options={"session_id": "s1"})
    got = asyncio.Queue()

    async def handler(m):
        await got.put(m)
    sub = await t.subscribe(b, handler)
    sub._receiver.fail_next_receive = 2
    from .fakes import FakeSbMessage
    ns.entities.setdefault("legacy", []).append(FakeSbMessage(
        b'{"x": 1}', {b"tenant": "acme"}, "native-1", "c-1", "application/json", "legacy.created"))
    m = await asyncio.wait_for(got.get(), 2)
    assert (m.envelope.type, m.envelope.id, m.envelope.correlation_id) == ("legacy.created", "native-1", "c-1")
    assert m.envelope.headers == {"tenant": "acme"} and sub._receiver.kwargs["session_id"] == "s1"
    await sub.close()
    await t.stop()


async def test_servicebus_requires_credentials():
    t = ServiceBusTransport()
    with pytest.raises(TransportPermanentError, match="connection_string"):
        await t.start()
    t2 = ServiceBusTransport(connection_string="Endpoint=sb://x.servicebus.windows.net/;"
                                               "SharedAccessKeyName=k;SharedAccessKey=dGVzdA==")
    await t2.start()        # building the client does not connect
    assert t2._client is not None
    await t2.stop()


# ----------------------------------------------------------------------------------------------------
# feature composition
# ----------------------------------------------------------------------------------------------------

async def test_provider_features_register_transports_and_release_rules(make_shell):
    from DnA Workflows.messaging import MessageBus
    from DnA Workflows.resilience.holds import HoldService
    shell = make_shell({
        "messaging": {"endpoints": {
            "a": {"transport": "rabbitmq", "address": "qa"},
            "b": {"transport": "servicebus", "address": "qb"}}},
        "messaging.rabbitmq": {"url": "amqp://u:p@rabbit/"},
        "messaging.servicebus": {"connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=a2V5",
                                 "release_strategy": "probe-recommend"},
        "resilience": {},
    })
    bus = shell.get(MessageBus)
    assert set(bus.transports) == {"rabbitmq", "servicebus"}
    assert bus.dependency_key("a") == "rabbitmq:a"
    holds = shell.get(HoldService)
    assert (await holds.place("dependency", "rabbitmq:a", "x")).release_strategy == "probe-auto"
    hold = await holds.place("dependency", "servicebus:b", "x")
    assert hold.release_strategy == "probe-recommend" and hold.release_options["interval"] == 30


# ----------------------------------------------------------------------------------------------------
# real brokers (opt-in)
# ----------------------------------------------------------------------------------------------------

@pytest.mark.rabbitmq
@pytest.mark.skipif(not os.environ.get("DnA Workflows_TEST_RABBITMQ_URL"), reason="DnA Workflows_TEST_RABBITMQ_URL not set")
async def test_real_rabbitmq_conformance():  # pragma: no cover - needs a broker
    t = RabbitMqTransport(url=os.environ["DnA Workflows_TEST_RABBITMQ_URL"], redelivery_delay=0.1)
    await run_transport_conformance(t, fresh("rabbitmq", exchange="DnA Workflows.conformance",
                                             queue_arguments={"x-expires": 60000}))


@pytest.mark.servicebus
@pytest.mark.skipif(not os.environ.get("DnA Workflows_TEST_SERVICEBUS_CONNECTION"),
                    reason="DnA Workflows_TEST_SERVICEBUS_CONNECTION not set")
async def test_real_servicebus_conformance():  # pragma: no cover - needs a namespace
    queue = os.environ.get("DnA Workflows_TEST_SERVICEBUS_QUEUE", "DnA Workflows-conformance")
    t = ServiceBusTransport(connection_string=os.environ["DnA Workflows_TEST_SERVICEBUS_CONNECTION"], probe_entity=queue)
    await run_transport_conformance(
        t, lambda: EndpointBinding(name=f"sb{next(_n)}", transport="servicebus", address=queue), timeout=60)
