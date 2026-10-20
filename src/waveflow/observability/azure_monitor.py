"""Azure Monitor / Application Insights (plan item P4.7) – the corporate path.

Adds Azure Monitor exporters to the OpenTelemetry feature:

* spans → Application Insights ``requests``/``dependencies`` (end-to-end transaction view),
* journal events → ``traces`` with ``customDimensions`` (``DnA Workflows.correlation_id`` etc.) and
  ``operation_Id`` = W3C trace id, so a KQL query by correlation id shows the whole business flow.

Metrics for Azure: scrape ``/metrics`` with Azure Monitor managed Prometheus (no extra code).

Example::

    observability.azure_monitor:
      connection_string: "${APPLICATIONINSIGHTS_CONNECTION_STRING}"
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.observability.otel import LogExporterRef, SpanExporterRef


def _default_trace_exporter(connection_string: str) -> Any:
    from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter
    return AzureMonitorTraceExporter(connection_string=connection_string)


def _default_log_exporter(connection_string: str) -> Any:
    from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
    return AzureMonitorLogExporter(connection_string=connection_string)


class AzureMonitorOptions(FeatureOptions):
    connection_string: str
    export_traces: bool = True
    export_logs: bool = True


class AzureMonitorFeature(Feature):
    """Application Insights exporters for traces and journal logs. ``pip install DnA Workflows[azure-monitor]``."""

    name = "observability.azure_monitor"
    depends_on = ("observability.otel",)
    Options = AzureMonitorOptions

    #: Seams for tests (the real exporters validate the connection string and talk HTTPS).
    trace_exporter_factory: Callable[[str], Any] = staticmethod(_default_trace_exporter)
    log_exporter_factory: Callable[[str], Any] = staticmethod(_default_log_exporter)

    def configure(self, services: ServiceCollection) -> None:
        o: AzureMonitorOptions = self.options  # type: ignore[assignment]
        cs = o.connection_string
        if o.export_traces:
            services.add_contributor(SpanExporterRef, lambda p: SpanExporterRef(
                exporter=type(self).trace_exporter_factory(cs)), key="azure-monitor")
        if o.export_logs:
            services.add_contributor(LogExporterRef, lambda p: LogExporterRef(
                exporter=type(self).log_exporter_factory(cs)), key="azure-monitor")
