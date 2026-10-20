# Writing DnA Workflows plugins

Everything in DnA Workflows is a feature behind a contract. A plugin is an ordinary Python package that
ships one or more `Feature` classes and advertises them through an entry point:

```toml
# your plugin's pyproject.toml
[project.entry-points."DnA Workflows.features"]
"messaging.kafka" = "DnA Workflows_kafka:KafkaTransportFeature"
```

A host selects the feature by name in its shell configuration. Nothing else has to change.

## The two axes

| You want to… | Use | Resolved as |
|---|---|---|
| replace *the* implementation (store, failure policy, gate, span factory) | `services.replace(Contract, impl)` | `provider.get(Contract)` |
| provide a default others may replace | `services.try_add(Contract, impl)` | `provider.get(Contract)` |
| add one more contributor (classifier, probe, sink, transport, activity, strategy) | `services.add_contributor(Contract, impl, key=...)` | `provider.get_all(Contract)` |

An implementation can be a class (autowired by constructor type hints, and `list[X]` receives all
contributors), a factory `lambda provider: ...`, or an instance.

## Catalogue of extension points

| Contract | Axis | Purpose | Built-ins |
|---|---|---|---|
| `Activity` (via `add_activity`) | extend | new workflow steps | runtime, messaging, http, ai activities |
| `ExecutionStore`, `OutboxStore`, `JournalStore`, `DefinitionStore`, `KeyValueStore` | override | persistence | in-memory, SQLite |
| `FaultClassifier` | extend | map errors to fault kinds | network, status code, data, transport, AI, HTTP |
| `FailurePolicy` | override | retry / park / fail decision | `RetryBudgetPolicy` |
| `WorkGate` | override | may work cross a boundary now? | `HoldGate` |
| `ReleaseStrategy` | extend | how holds get released | manual, ttl, probe-auto, probe-recommend, ai-recommend |
| `ReleaseRule` (via `add_release_rule`) | extend | default strategy for *your* dependencies | each transport/HTTP/AI module |
| `HealthProbe` (via `add_health_probe`) | extend | "is the dependency back?" | transports, HTTP |
| `ProposalHandler` | extend | executes approved proposals of one kind | hold.release, work.resume, work.abandon |
| `IntentHandler` | extend | delivers one outbox intent kind | `messaging.send` |
| `MessageTransport` (via `add_transport`) | extend | a broker | inmemory, rabbitmq, servicebus |
| `JournalSink` | extend | stream history somewhere | logging, messaging, prometheus, otel-logs |
| `ActivityMiddleware` / `DeliveryMiddleware` | extend | wrap every activity / delivery | OTel spans, Prometheus timings |
| `SpanFactory` | override | spans for execution start/resume | id-only, OpenTelemetry |
| `ChatClientProvider` / `ChatClientRef` | extend | AI model providers / ready clients | openai, azure_openai, ollama, openai_compatible |
| `HostedService` | extend | background loops with the shell lifecycle | timers, outbox, consumers, coordinator, exporter, API |
| `ApiExtension` | extend | add routes / mount sub-apps on the API | MCP at `/mcp` |
| `Authenticator` | override | who is calling (tokens, API keys) | Keycloak/OIDC, development mode |
| `JournalSink` for forecasting | extend | learning from history | `ForecastJournalSink` (DuckDB) |

## Example: a transport

```python
from DnA Workflows.kernel import Feature, FeatureOptions, ServiceCollection
from DnA Workflows.messaging.core import TransportCapabilities, TransportUnavailable
from DnA Workflows.messaging.feature import add_transport
from DnA Workflows.resilience.feature import add_release_rule
from DnA Workflows.resilience.release import ReleaseRule


class KafkaTransport:
    name = "kafka"
    capabilities = TransportCapabilities(publish_subscribe=True, ordering="per_key")

    async def start(self): ...
    async def stop(self): ...
    async def send(self, binding, envelope):        # body = envelope.body(), headers = envelope.wire_headers()
        ...                                          # raise TransportUnavailable / TransportThrottled / TransportPermanentError
    async def subscribe(self, binding, handler): ... # ack on return, no redelivery on RejectMessage, redeliver otherwise
    async def probe(self) -> tuple[bool, str]: ...


class KafkaOptions(FeatureOptions):
    bootstrap_servers: str


class KafkaTransportFeature(Feature):
    name = "messaging.kafka"
    depends_on = ("messaging",)
    Options = KafkaOptions

    def configure(self, services: ServiceCollection) -> None:
        add_transport(services, "kafka", lambda p: KafkaTransport())
        add_release_rule(services, ReleaseRule("kafka:*", "probe-auto", {"successes_required": 2}))
```

Prove it behaves like every other transport:

```python
from DnA Workflows.messaging.conformance import run_transport_conformance

async def test_kafka_conformance():
    await run_transport_conformance(KafkaTransport(...), lambda: EndpointBinding(name=..., transport="kafka", address=...))
```

## Example: an activity

```python
from DnA Workflows.runtime import Activity, ActivityInputs, Done, add_activity

class CreateTicket(Activity):
    type_name = "CreateTicket"
    category = "ITSM"

    class Inputs(ActivityInputs):          # validated; becomes the JSON Schema in /capabilities
        title: str
        priority: int = 3

    def __init__(self, client: "TicketClient") -> None:   # injected from the container
        self.client = client

    def dependency(self, inputs, node):    # holds, budgets and probes apply to this key
        return "itsm:servicenow"

    async def execute(self, context):
        ticket = await self.client.create(context.inputs.title, context.inputs.priority)
        return Done(output={"id": ticket.id})
```

Register it in your feature with `add_activity(services, CreateTicket)`. For side effects that must
not repeat when a step is retried, emit an outbox intent (`context.emit(kind, payload, dependency=...)`)
and contribute an `IntentHandler` for that kind.

## Example: a release strategy

```python
class MaintenanceWindowRelease:
    name = "maintenance-window"

    async def evaluate(self, hold, context):     # -> ReleaseDecision(KEEP | RELEASE | RECOMMEND, reason)
        ...
```

Register it with `services.add_contributor(ReleaseStrategy, MaintenanceWindowRelease, key="maintenance-window")`
and select it in configuration or through a `ReleaseRule`.

## Testing plugins

`DnA Workflows.testing.build_test_shell(...)` builds a shell with a manual clock, sequential IDs and no
background loops, so time and processing are driven explicitly. `DnA Workflows.ai.testing.ScriptedChatClient`
scripts model answers and tool calls for AI features.
