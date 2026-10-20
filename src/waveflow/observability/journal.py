"""Journal streaming (plan items P4.3, P4.4).

The journal is committed atomically with workflow state, so it is the single source of truth for
"what happened". The exporter streams it to any number of sinks (Extend axis) – structured logs,
a message stream (Event Hubs/Kafka/RabbitMQ stream → ClickHouse/ADX), Prometheus counters,
OpenTelemetry logs. Each sink has its own durable cursor: a slow or failing sink never blocks the
others, and delivery to sinks is at-least-once (sinks can dedupe on ``event.id``).
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

from DnA Workflows.kernel.hosting import PeriodicService
from DnA Workflows.persistence.contracts import JournalStore, KeyValueStore
from DnA Workflows.runtime.contracts import RuntimeSignals
from DnA Workflows.runtime.models import JournalEvent

log = logging.getLogger(__name__)
CURSORS = "journal-cursors"


@runtime_checkable
class JournalSink(Protocol):
    name: str

    async def write(self, events: list[JournalEvent]) -> None: ...


def event_record(e: JournalEvent) -> dict[str, Any]:
    """Flat, stable record shape shared by all text/stream sinks."""
    return {
        "ts": e.timestamp.isoformat(), "seq": e.seq, "id": e.id, "kind": e.kind, "execution_id": e.execution_id,
        "workflow": e.definition_id, "node": e.node_id, "work_item_id": e.work_item_id,
        "activity_type": e.activity_type, "trace_id": e.trace_id, "span_id": e.span_id,
        "correlation_id": e.correlation_id, "host": e.host, "data": e.data,
    }


class JournalExporter(PeriodicService):
    name = "journal-exporter"

    def __init__(self, journal: JournalStore, cursors: KeyValueStore, sinks: list[JournalSink],
                 signals: RuntimeSignals, interval: float = 1.0, batch_size: int = 500) -> None:
        super().__init__()
        self.journal = journal
        self.cursors = cursors
        self.sinks = sinks
        self.interval = interval
        self.batch_size = batch_size
        self.errors: dict[str, str] = {}
        signals.subscribe("journal", self.wake)

    async def tick(self) -> None:
        await self.export()

    async def cursor(self, sink: str) -> int:
        data = await self.cursors.get(CURSORS, sink)
        return int(data["seq"]) if data else 0

    async def export(self) -> dict[str, int]:
        """Export everything pending. Returns the number of events written per sink."""
        written: dict[str, int] = {}
        for sink in self.sinks:
            written[sink.name] = 0
            position = await self.cursor(sink.name)
            while True:
                events = await self.journal.read(position, self.batch_size)
                if not events:
                    break
                try:
                    await sink.write(events)
                except Exception as exc:
                    self.errors[sink.name] = f"{type(exc).__name__}: {exc}"
                    log.warning("Journal sink '%s' failed at seq %s: %s", sink.name, position, exc)
                    break
                self.errors.pop(sink.name, None)
                position = events[-1].seq
                await self.cursors.put(CURSORS, sink.name, {"seq": position})
                written[sink.name] += len(events)
                if len(events) < self.batch_size:
                    break
        return written

    async def lag(self) -> dict[str, int]:
        """Events not yet exported, per sink (exposed as a metric)."""
        result = {}
        for sink in self.sinks:
            position = await self.cursor(sink.name)
            result[sink.name] = len(await self.journal.read(position, 100_000))
        return result


class LoggingJournalSink:
    """JSON lines on the ``DnA Workflows.journal`` logger – picked up by Promtail/Alloy (Loki), the Azure
    Monitor agent, or any log shipper reading stdout."""

    name = "logging"

    def __init__(self, logger_name: str = "DnA Workflows.journal") -> None:
        self.logger = logging.getLogger(logger_name)

    async def write(self, events: list[JournalEvent]) -> None:
        for e in events:
            level = logging.ERROR if e.kind.endswith(("faulted", "failed")) else (
                logging.WARNING if e.kind.endswith(("parked", "held", "retry_scheduled", "placed")) else logging.INFO)
            summary = f"{e.kind} workflow={e.definition_id} node={e.node_id} execution={e.execution_id}"
            self.logger.log(level, summary, extra={"trace_id": e.trace_id, "span_id": e.span_id,
                                                   "correlation_id": e.correlation_id, "journal": event_record(e)})


class MessagingJournalSink:
    """Streams journal events to a logical messaging endpoint as CloudEvents
    (``DnA Workflows.journal.<kind>``), e.g. an Event Hubs / Kafka / RabbitMQ stream for analytics."""

    def __init__(self, bus: Any, endpoint: str, name: str = "messaging") -> None:
        self.bus = bus
        self.endpoint = endpoint
        self.name = name

    async def write(self, events: list[JournalEvent]) -> None:
        for e in events:
            envelope = self.bus.envelope(f"DnA Workflows.journal.{e.kind}", event_record(e), subject=e.execution_id,
                                         correlation_id=e.correlation_id, source="DnA Workflows://journal")
            envelope.id = e.id  # stable id: consumers dedupe at-least-once exports
            if e.trace_id and e.span_id:
                envelope.traceparent = f"00-{e.trace_id}-{e.span_id}-01"
            await self.bus.send(self.endpoint, envelope)
