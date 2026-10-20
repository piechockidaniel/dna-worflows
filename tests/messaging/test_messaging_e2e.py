import asyncio

import pytest

from DnA Workflows.kernel import ManualClock
from DnA Workflows.messaging import ConsumerHost, MessageBus, MessageEnvelope
from DnA Workflows.messaging.bus import UnknownEndpoint
from DnA Workflows.messaging.providers.inmemory import InMemoryBroker
from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore, OutboxStore
from DnA Workflows.resilience import HoldService, HoldStatus, ReleaseCoordinator
from DnA Workflows.runtime import ExecutionStatus, OutboxProcessor, WorkflowBuilder, WorkflowRegistry, WorkflowRuntime
from DnA Workflows.runtime.models import IntentStatus


@pytest.fixture(autouse=True)
def fresh_broker():
    InMemoryBroker.reset()
    yield
    InMemoryBroker.reset()


ENDPOINTS = {
    "orders": {"transport": "inmemory", "address": "q.orders", "consume": True},
    "invoices": {"transport": "inmemory", "address": "q.invoices", "consume": True},
    "replies": {"transport": "inmemory", "address": "q.replies", "consume": True},
    "outbound": {"transport": "inmemory", "address": "q.outbound"},
}


@pytest.fixture
def node(make_shell):
    """A DnA Workflows 'node': its own shell (own store), sharing the in-process broker."""
    async def build(endpoints=None, resilience=None, clock=None, unmatched="dead_letter"):
        shell = make_shell({
            "messaging": {"endpoints": endpoints or ENDPOINTS, "unmatched": unmatched},
            "messaging.inmemory": {"redelivery_delay": 0.01},
            "resilience": resilience or {"retry": {"dependency_unavailable": {"max_attempts": 2, "jitter": 0}}},
        }, clock=clock)
        await shell.get(ConsumerHost).start()
        return shell
    return build


async def settle(*shells):
    """Deliver outboxes and let the broker drain until everything is quiet."""
    broker = InMemoryBroker.get()
    for _ in range(20):
        await broker.idle()                      # consumers finish -> may create new intents
        delivered = 0
        for shell in shells:
            delivered += await shell.get(OutboxProcessor).process_due()
        if not delivered:
            return


def consuming(*names):
    """Endpoint config where only ``names`` are consumed by this node."""
    return {n: {**cfg, "consume": n in names} for n, cfg in ENDPOINTS.items()}


async def test_two_nodes_one_trace_one_correlation(node):
    # memory backend: two nodes with separate stores; sqlite backend: two nodes sharing one store file
    sales = await node(endpoints=consuming("orders"))
    billing = await node(endpoints=consuming("invoices"))
    await sales.get(WorkflowRegistry).publish(
        WorkflowBuilder("take-order")
        .add("receive", "MessageReceived", endpoint="orders", type="order.placed")
        .then("bill", "SendMessage", endpoint="invoices", type="invoice.requested",
              data={"order": {"$output": "receive.data.id"}, "amount": {"$output": "receive.data.amount"}})
        .build())
    await billing.get(WorkflowRegistry).publish(
        WorkflowBuilder("invoice")
        .add("receive", "MessageReceived", endpoint="invoices")
        .then("out", "SetOutput", value={"$tmpl": "invoice for order {output.receive.data.order}"})
        .build())

    bus = sales.get(MessageBus)
    sent = await bus.publish("orders", "order.placed", {"id": 42, "amount": 99.5}, correlation_id="order-42")
    await settle(sales, billing)

    [ex_a] = await sales.get(ExecutionStore).list(definition_id="take-order")
    [ex_b] = await billing.get(ExecutionStore).list(definition_id="invoice")
    assert ex_a.status is ExecutionStatus.COMPLETED and ex_b.status is ExecutionStatus.COMPLETED
    assert ex_b.output == "invoice for order 42"
    # end-to-end tracking across two nodes and a broker hop
    assert ex_a.correlation_id == ex_b.correlation_id == "order-42"
    assert ex_a.trace_id == ex_b.trace_id
    assert ex_a.input["data"] == {"id": 42, "amount": 99.5} and ex_a.input["endpoint"] == "orders"
    invoice_msg = InMemoryBroker.get().history["q.invoices"][0]
    assert invoice_msg.causation_id is not None and invoice_msg.source == "DnA Workflows://take-order/bill"
    assert invoice_msg.trace.trace_id == ex_a.trace_id
    # the sales journal shows the send; the billing journal shows the start – same trace id
    sales_events = await sales.get(JournalStore).query(trace_id=ex_a.trace_id)
    billing_events = [e for e in await billing.get(JournalStore).query(trace_id=ex_a.trace_id)
                      if e.execution_id == ex_b.id]
    assert any(e.kind == "intent.delivered" for e in sales_events)
    assert billing_events[0].kind == "execution.started"
    assert billing_events[0].data["causation_id"] == invoice_msg.causation_id
    assert sent.id in [e.id for e in InMemoryBroker.get().acked["q.orders"]]


async def test_request_reply_with_correlation(node):
    shell = await node()
    await shell.get(WorkflowRegistry).publish(
        WorkflowBuilder("ask")
        .add("send", "SendMessage", endpoint="outbound", type="quote.requested", data={"sku": "A1"})
        .then("wait", "MessageReceived", endpoint="replies", type="quote.given", correlate=True)
        .then("out", "SetOutput", value={"$output": "wait.data.price"})
        .build())
    rt = shell.get(WorkflowRuntime)
    mine = await rt.start("ask", correlation_id="q-1")
    other = await rt.start("ask", correlation_id="q-2")
    assert mine.status is ExecutionStatus.WAITING
    await settle(shell)
    requests = {m.correlation_id: m for m in InMemoryBroker.get().history["q.outbound"]}
    assert set(requests) == {"q-1", "q-2"} and requests["q-1"].type == "quote.requested"

    bus = shell.get(MessageBus)
    await bus.publish("replies", "quote.given", {"price": 10}, correlation_id="q-1")
    await bus.publish("replies", "quote.other", {"price": 99}, correlation_id="q-2")  # wrong type
    await settle(shell)
    assert (await rt.get(mine.id)).output == 10
    assert (await rt.get(other.id)).status is ExecutionStatus.WAITING
    assert len(InMemoryBroker.get().dead_letters["q.replies"]) == 1  # unmatched -> dead letter


async def test_redelivered_message_is_deduplicated(node):
    shell = await node()
    await shell.get(WorkflowRegistry).publish(
        WorkflowBuilder("once").add("r", "MessageReceived", endpoint="orders").build())
    bus = shell.get(MessageBus)
    env = bus.envelope("order.placed", {"id": 1})
    await bus.send("orders", env)
    await bus.send("orders", env)   # same id: the broker redelivers / producer retried
    await settle(shell)
    assert len(await shell.get(ExecutionStore).list(definition_id="once")) == 1
    assert len(InMemoryBroker.get().acked["q.orders"]) == 2   # duplicate acked, not dead-lettered


async def test_unmatched_policies(node):
    ack_shell = await node(unmatched="ack")
    await ack_shell.get(MessageBus).publish("orders", "nobody.cares", {})
    await settle(ack_shell)
    assert InMemoryBroker.get().acked["q.orders"] and not InMemoryBroker.get().dead_letters
    kinds = [e.kind for e in await ack_shell.get(JournalStore).query(kinds=["message.unmatched"])]
    assert kinds == ["message.unmatched"]
    await ack_shell.get(ConsumerHost).stop()

    InMemoryBroker.reset()
    requeue = await node(unmatched="requeue")
    await requeue.get(MessageBus).publish("orders", "later", {})
    await asyncio.sleep(0.05)
    # requeued until a workflow exists for it (then consumed)
    await requeue.get(WorkflowRegistry).publish(
        WorkflowBuilder("late").add("r", "MessageReceived", endpoint="orders", type="later").build())
    await settle(requeue)
    assert len(await requeue.get(ExecutionStore).list(definition_id="late")) == 1


async def test_ingress_hold_pauses_consumption_and_release_resumes(node):
    shell = await node()
    await shell.get(WorkflowRegistry).publish(
        WorkflowBuilder("in").add("r", "MessageReceived", endpoint="orders").build())
    holds = shell.get(HoldService)
    consumers = shell.get(ConsumerHost)
    hold = await holds.place("ingress", "orders", "downstream maintenance", actor="ops")
    await consumers.wait_idle()
    assert "orders" in consumers.paused and "orders" not in consumers.subscriptions
    await shell.get(MessageBus).publish("orders", "order.placed", {"id": 1})
    await asyncio.sleep(0.05)
    broker = InMemoryBroker.get()
    assert broker.pending("q.orders") == 1       # waiting safely in the broker
    assert await shell.get(ExecutionStore).list() == []

    await holds.release(hold.id, actor="ops")
    await consumers.wait_idle()
    await settle(shell)
    assert broker.pending("q.orders") == 0 and len(await shell.get(ExecutionStore).list()) == 1
    kinds = [e.kind for e in await shell.get(JournalStore).query(kinds=["ingress.paused", "ingress.resumed"])]
    assert kinds == ["ingress.paused", "ingress.resumed"]


async def test_message_arriving_while_held_is_handed_back(node):
    shell = await node()
    consumers = shell.get(ConsumerHost)
    from DnA Workflows.messaging.consumer import IngressHeld
    from DnA Workflows.messaging.core import IncomingMessage
    await shell.get(HoldService).place("ingress", "orders", "x")
    with pytest.raises(IngressHeld):
        await consumers.handle(IncomingMessage(envelope=MessageEnvelope(id="m", source="s", type="t"),
                                               endpoint="orders", address="q.orders"))
    await consumers.wait_idle()
    assert "orders" in consumers.paused


async def test_broker_outage_trips_hold_and_module_release_strategy_recovers(node):
    clock = ManualClock()
    shell = await node(clock=clock)
    await shell.get(WorkflowRegistry).publish(
        WorkflowBuilder("notify").add("s", "SendMessage", endpoint="outbound", type="x", data={"n": 1}).build())
    rt = shell.get(WorkflowRuntime)
    broker = InMemoryBroker.get()
    broker.set_available(False)
    for _ in range(2):
        await rt.start("notify")
    outbox = shell.get(OutboxProcessor)
    await outbox.process_due()           # attempt 1 each: retry scheduled
    clock.advance(60)
    await outbox.process_due()           # attempt 2: budget exhausted -> trip + hold
    intents = await shell.get(OutboxStore).list()
    assert {i.status for i in intents} == {IntentStatus.HELD}
    [hold] = await shell.get(HoldService).list(active_only=True)
    assert hold.target == "inmemory:outbound"
    assert hold.release_strategy == "probe-auto"        # contributed by the transport module

    coordinator = shell.get(ReleaseCoordinator)
    await coordinator.evaluate_all()
    assert (await shell.get(HoldService).get(hold.id)).status is HoldStatus.ACTIVE
    broker.set_available(True)
    await coordinator.evaluate_all()                   # probe healthy -> released
    assert (await shell.get(HoldService).get(hold.id)).status is HoldStatus.RELEASED
    await settle(shell)
    assert len(broker.history["q.outbound"]) == 2


async def test_envelope_wire_roundtrip_and_foreign_messages():
    env = MessageEnvelope(id="1", source="s", type="t", data={"a": 1}, subject="sub", correlation_id="c",
                          causation_id="k", traceparent="00-" + "a" * 32 + "-" + "b" * 16 + "-01",
                          tracestate="x=y", headers={"tenant": "acme"})
    back = MessageEnvelope.from_wire(env.body(), env.wire_headers())
    assert back == env
    foreign = MessageEnvelope.from_wire(b'{"legacy": true}', {"type": "legacy.event", "x-custom": b"bytes"},
                                        message_id="native-1", correlation_id="nc", subject="s2")
    assert (foreign.id, foreign.type, foreign.correlation_id, foreign.subject) == ("native-1", "legacy.event", "nc", "s2")
    assert foreign.data == {"legacy": True} and foreign.headers["x-custom"] == "bytes"
    text = MessageEnvelope.from_wire(b"plain", {}, content_type="text/plain")
    assert text.data == "plain" and text.body() == b"plain"
    assert MessageEnvelope.from_wire(b"{not json", {}).data == "{not json"
    assert MessageEnvelope.from_wire(None, None).data is None
    assert MessageEnvelope(id="b", source="s", type="t", data=b"raw", datacontenttype="application/octet-stream").body() == b"raw"
    assert MessageEnvelope(id="n", source="s", type="t", datacontenttype="text/plain").body() == b""


async def test_configuration_errors(make_shell):
    shell = make_shell({"messaging": {"endpoints": {"x": {"transport": "kafka", "address": "t"}}},
                        "messaging.inmemory": {}})
    with pytest.raises(ValueError, match="unknown transport 'kafka'"):
        shell.get(MessageBus)
    shell = make_shell({"messaging": {"endpoints": ENDPOINTS}, "messaging.inmemory": {}})
    with pytest.raises(UnknownEndpoint):
        shell.get(MessageBus).dependency_key("nope")
    from DnA Workflows.messaging.providers.inmemory import InMemoryTransport
    with pytest.raises(ValueError, match="Two transports"):
        MessageBus(shell.get(MessageBus).endpoints, [InMemoryTransport(), InMemoryTransport()], None, None)
    from DnA Workflows.messaging.bus import describe_bus
    assert "inmemory" in describe_bus(shell.get(MessageBus))["transports"]


async def test_consumer_survives_broker_down_at_startup_and_recovers(node, make_shell):
    """Subscriptions that fail (broker down when the host starts) are journaled and retried."""
    from DnA Workflows.messaging.providers.inmemory import InMemoryTransport

    class Flaky(InMemoryTransport):
        fail_subscribe = True
        fail_start = True

        async def start(self):
            if Flaky.fail_start:
                raise ConnectionError("broker down at startup")

        async def subscribe(self, binding, handler):
            if Flaky.fail_subscribe:
                raise ConnectionError("cannot subscribe")
            return await super().subscribe(binding, handler)

    from DnA Workflows.messaging.feature import add_transport
    shell = make_shell({"messaging": {"endpoints": {"orders": {"transport": "flaky", "address": "q.f",
                                                               "consume": True}}}},
                       configure=lambda s: add_transport(s, "flaky", lambda p: Flaky("flaky")))
    consumers = shell.get(ConsumerHost)
    await consumers.start()
    assert "orders" in consumers.failed and "orders" not in consumers.subscriptions
    await consumers.reconcile()                       # still failing: journaled only once
    Flaky.fail_subscribe = False
    await consumers.tick()
    assert "orders" in consumers.subscriptions and consumers.failed == {}
    kinds = [e.kind for e in await shell.get(JournalStore).query(
        kinds=["ingress.subscribe_failed", "ingress.subscribed"])]
    assert kinds == ["ingress.subscribe_failed", "ingress.subscribed"]
    await consumers.stop()
    assert consumers.subscriptions == {}
    await consumers.reconcile()                       # stopped host: no-op
    consumers._schedule_reconcile()                   # stopped host: no-op
