# Azure path (plan item P4.7)

| Signal | Where it goes | How |
|---|---|---|
| Traces (workflow.start / activity / send spans) | Application Insights (`requests`, `dependencies`) | OTLP → **OpenTelemetry Collector** (`deploy/otel-collector.azure.yaml`, `azuremonitor` exporter) |
| Journal events (history, lifecycle, holds) | Application Insights `traces` with `customDimensions` | same collector pipeline (OTel logs) |
| Metrics | Azure Monitor managed Prometheus | scrape `GET /metrics` (AKS / Container Apps) |
| Messaging | Azure Service Bus | `messaging.servicebus` feature |

**Recommended (Q-005): the collector.** DnA Workflows only speaks OTLP; the collector (contrib image) holds the
Application Insights connection string, batches, limits memory and can fan out to more back-ends. The in-process
`observability.azure_monitor` feature remains available for small deployments (note: its SDK is still beta and pins
OpenTelemetry 1.44.x).

Configuration: see `examples/order_integration/shell.servicebus.yaml`. Secrets are provided through environment
variables (Key Vault references in Container Apps / CSI driver in AKS):
`SERVICEBUS_CONNECTION`, `APPLICATIONINSIGHTS_CONNECTION_STRING`, `DnA Workflows_API_KEY`, `ERP_URL`.
For Azure AD instead of connection strings use `messaging.servicebus.namespace` (needs `azure-identity`)
and an `azure_openai` AI client without `api_key` (managed identity).

## KQL: one business transaction end to end

```kusto
// every journal event of an order, across all nodes/services, in order
traces
| where customDimensions["DnA Workflows.correlation_id"] == "order-42"
| project timestamp, kind = tostring(customDimensions["DnA Workflows.kind"]),
          workflow = tostring(customDimensions["DnA Workflows.workflow"]),
          node = tostring(customDimensions["DnA Workflows.node"]), host = tostring(customDimensions["DnA Workflows.host"]),
          operation_Id
| order by timestamp asc
```

```kusto
// what is frozen right now, and why
traces
| where customDimensions["DnA Workflows.kind"] in ("hold.placed", "hold.released")
| extend hold = parse_json(tostring(customDimensions["DnA Workflows.data"])).hold
| summarize arg_max(timestamp, *) by hold_id = tostring(hold.id)
| where customDimensions["DnA Workflows.kind"] == "hold.placed"
| project timestamp, hold_id, scope = tostring(hold.scope), target = tostring(hold.target), reason = tostring(hold.reason)
```

```kusto
// retries and parking per dependency (last 24h)
traces
| where timestamp > ago(24h) and customDimensions["DnA Workflows.kind"] in ("work.retry_scheduled", "work.parked")
| extend dependency = tostring(parse_json(tostring(customDimensions["DnA Workflows.data"])).fault.dependency)
| summarize count() by dependency, kind = tostring(customDimensions["DnA Workflows.kind"]), bin(timestamp, 1h)
```

The `operation_Id` of every row is the W3C trace id: open it in *Transaction search* to see the spans of the same
business transaction, including the hops through Service Bus to other DnA Workflows nodes.
