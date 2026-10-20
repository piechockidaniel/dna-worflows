"""Observability core feature (plan phase P4): journal export + log correlation.

Example::

    observability:
      sinks: [logging]                  # built-in sinks to enable
      stream_endpoint: journal.events   # also stream the journal to this messaging endpoint
      configure_logging: true           # JSON logs with trace/correlation ids on stdout
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import CallbackService, HostedService
from DnA Workflows.observability.journal import JournalExporter, JournalSink, LoggingJournalSink, MessagingJournalSink
from DnA Workflows.observability.logs import configure_logging
from DnA Workflows.persistence.contracts import JournalStore, KeyValueStore
from DnA Workflows.runtime.contracts import RuntimeSignals


class ObservabilityOptions(FeatureOptions):
    sinks: list[Literal["logging"]] = Field(default_factory=lambda: ["logging"])
    stream_endpoint: str | None = None
    export_interval: float = 1.0
    batch_size: int = 500
    configure_logging: bool = False
    log_level: str = "INFO"
    json_logs: bool = True
    #: With several nodes sharing one store, enable the exporter on one node only (or make sinks idempotent).
    exporter_enabled: bool = True
    background_services: bool = True


class ObservabilityFeature(Feature):
    """Streams the execution journal to sinks; correlates logs with traces."""

    name = "observability"
    depends_on = ("runtime",)
    Options = ObservabilityOptions

    def configure(self, services: ServiceCollection) -> None:
        o: ObservabilityOptions = self.options  # type: ignore[assignment]
        if "logging" in o.sinks:
            services.add_contributor(JournalSink, LoggingJournalSink, key="logging")
        if o.stream_endpoint:
            endpoint = o.stream_endpoint

            def stream_sink(p):  # messaging is optional: resolved lazily
                from DnA Workflows.messaging.bus import MessageBus
                return MessagingJournalSink(p.get(MessageBus), endpoint)
            services.add_contributor(JournalSink, stream_sink, key="messaging")
        services.add(JournalExporter, lambda p: JournalExporter(
            p.get(JournalStore), p.get(KeyValueStore), p.get_all(JournalSink), p.get(RuntimeSignals),
            interval=o.export_interval, batch_size=o.batch_size))
        if o.configure_logging:
            async def setup_logging() -> None:
                configure_logging(o.log_level, o.json_logs)
            services.add_contributor(HostedService, CallbackService(setup_logging), key="observability.logging")
        if o.background_services and o.exporter_enabled:
            services.add_contributor(HostedService, lambda p: p.get(JournalExporter), key="observability.exporter")
