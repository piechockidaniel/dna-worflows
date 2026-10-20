"""Consumer host: inbound messages become workflow stimuli (plan item P2.4).

* At-least-once: a message is acknowledged only after the stimulus was routed and committed. A
  crash in between means redelivery; the inbox keys committed with the start/resume make that a
  recognised duplicate instead of a second execution.
* **Ingress holds** (P3.4) stop consumption entirely: the subscription is closed and messages wait
  in the broker – the cheapest possible freeze. Releasing the hold resubscribes.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Literal

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.hosting import PeriodicService
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.messaging.bus import MessageBus, stimulus_keys
from DnA Workflows.messaging.core import EndpointBinding, IncomingMessage, RejectMessage, Subscription
from DnA Workflows.persistence.contracts import JournalStore
from DnA Workflows.runtime.contracts import GateRequest, RuntimeSignals, WorkGate
from DnA Workflows.runtime.models import JournalEvent, Stimulus
from DnA Workflows.runtime.router import StimulusRouter

log = logging.getLogger(__name__)

UnmatchedPolicy = Literal["dead_letter", "requeue", "ack"]


class IngressHeld(Exception):
    """Raised to hand a message back to the broker while the endpoint is held."""


class ConsumerHost(PeriodicService):
    """Also a periodic service: every ``interval`` seconds it re-reconciles, which retries endpoints
    whose subscription failed (e.g. the broker was down when the host started)."""

    name = "consumer-host"

    def __init__(self, bus: MessageBus, router: StimulusRouter, gate: WorkGate, signals: RuntimeSignals,
                 journal: JournalStore, clock: Clock, ids: IdGenerator,
                 unmatched: UnmatchedPolicy = "dead_letter", interval: float = 5.0) -> None:
        super().__init__()
        self.interval = interval
        self.failed: dict[str, str] = {}
        self.bus = bus
        self.router = router
        self.gate = gate
        self.journal = journal
        self.clock = clock
        self.ids = ids
        self.unmatched = unmatched
        self.subscriptions: dict[str, Subscription] = {}
        self.paused: set[str] = set()
        self._started = False
        self._reconcile_lock = asyncio.Lock()
        self._pending: set[asyncio.Task[None]] = set()
        signals.subscribe("holds", self._on_holds_changed)

    async def start(self) -> None:
        for transport in self.bus.transports.values():
            try:
                await transport.start()
            except Exception as exc:  # the periodic reconcile keeps trying
                log.warning("Transport %s failed to start: %s", transport.name, exc)
        self._started = True
        await self.reconcile()
        await super().start()

    async def tick(self) -> None:
        await self.reconcile()

    async def stop(self) -> None:
        await super().stop()
        self._started = False
        for task in list(self._pending):
            task.cancel()
        for name in list(self.subscriptions):
            await self._unsubscribe(name)
        for transport in self.bus.transports.values():
            await transport.stop()

    async def reconcile(self) -> None:
        """Subscribe to every consumed endpoint that is not held; pause the held ones."""
        if not self._started:
            return
        async with self._reconcile_lock:
            for binding in self.bus.endpoints.consumed():
                held = await self._held(binding.name)
                if held and binding.name in self.subscriptions:
                    await self._unsubscribe(binding.name)
                    self.paused.add(binding.name)
                    await self._journal("ingress.paused", binding, {"reason": held})
                elif not held and binding.name not in self.subscriptions:
                    try:
                        await self._subscribe(binding)
                    except Exception as exc:
                        first = binding.name not in self.failed
                        self.failed[binding.name] = f"{type(exc).__name__}: {exc}"
                        if first:
                            log.warning("Cannot consume endpoint '%s': %s", binding.name, exc)
                            await self._journal("ingress.subscribe_failed", binding,
                                                {"error": self.failed[binding.name]})
                        continue
                    if self.failed.pop(binding.name, None) is not None:
                        await self._journal("ingress.subscribed", binding, {"recovered": True})
                    if binding.name in self.paused:
                        self.paused.discard(binding.name)
                        await self._journal("ingress.resumed", binding, {})

    async def handle(self, message: IncomingMessage) -> None:
        """The transport callback. Raising makes the transport nack/redeliver."""
        held = await self._held(message.endpoint)
        if held:
            self._schedule_reconcile()
            raise IngressHeld(held)
        envelope = message.envelope
        stimulus = Stimulus(
            type="message", keys=stimulus_keys(message.endpoint, envelope.type),
            payload={**envelope.to_stimulus_payload(), "endpoint": message.endpoint},
            correlation_id=envelope.correlation_id, message_id=envelope.id or None,
            causation_id=envelope.causation_id or envelope.id or None, traceparent=envelope.traceparent,
            tracestate=envelope.tracestate, source=envelope.source)
        result = await self.router.dispatch(stimulus)
        if result.matched:
            return
        binding = self.bus.endpoints.get(message.endpoint)
        await self._journal("message.unmatched", binding, {
            "message_id": envelope.id, "type": envelope.type, "policy": self.unmatched,
            "correlation_id": envelope.correlation_id}, trace_id=envelope.trace.trace_id if envelope.trace else None)
        if self.unmatched == "dead_letter":
            raise RejectMessage(f"No workflow matched message {envelope.id} ({envelope.type})")
        if self.unmatched == "requeue":
            raise IngressHeld("no workflow matched; requeued")

    # -- internals -------------------------------------------------------------------------------
    async def _held(self, endpoint: str) -> str | None:
        decision = await self.gate.evaluate(GateRequest(subject="ingress", endpoint=endpoint))
        return decision.reason if decision else None

    async def _subscribe(self, binding: EndpointBinding) -> None:
        transport = self.bus.transports[binding.transport]

        async def on_message(message: IncomingMessage) -> None:
            await self.handle(message)
        self.subscriptions[binding.name] = await transport.subscribe(binding, on_message)
        log.info("Consuming endpoint '%s' via %s (%s)", binding.name, binding.transport, binding.address)

    async def _unsubscribe(self, name: str) -> None:
        sub = self.subscriptions.pop(name, None)
        if sub is not None:
            await sub.close()

    def _on_holds_changed(self) -> None:
        self._schedule_reconcile()

    def _schedule_reconcile(self) -> None:
        if not self._started:
            return
        try:
            task = asyncio.get_running_loop().create_task(self.reconcile())
        except RuntimeError:  # pragma: no cover - no loop (synchronous caller outside asyncio)
            return
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def wait_idle(self) -> None:
        """Test helper: wait for scheduled reconciliations."""
        while self._pending:
            await asyncio.gather(*list(self._pending), return_exceptions=True)

    async def _journal(self, kind: str, binding: EndpointBinding, data: dict, trace_id: str | None = None) -> None:
        await self.journal.append([JournalEvent(
            id=self.ids.new_id("ev"), kind=kind, timestamp=self.clock.now(), trace_id=trace_id,
            data={"endpoint": binding.name, "transport": binding.transport, **data})])
