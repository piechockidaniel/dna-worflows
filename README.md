# DnA Workflows

A plugin-first integration workflow platform for moving data, processes, artifacts and events
between systems. Its focus is the **operational lifecycle of work**: when something cannot be
processed, work is **frozen, investigated and unpaused**. It is never dropped, and a failing
dependency is never hammered with retries.

DnA Workflows is inspired by the architecture of plugins to implement certain feature in runtime.
That influence shows in the contracts-only cores, the override/extend extension axes, the
checkpoint + outbox runtime, publish-time trigger indexing, holds and incidents.

> Plan: `PLAN.md` · Decisions and open questions: `CHANGELOG.md` · Plugins: `docs/plugins.md`

## The four pillars

| Pillar | What DnA Workflows provides | Where |
|---|---|---|
| **1. Communication as a plugin layer** | Workflows talk to *logical endpoints*; configuration binds each one to RabbitMQ, Azure Service Bus or in-memory. Sends are transactional (outbox); receives are deduplicated (inbox keys); every provider must pass a shared conformance kit. | `DnA Workflows.messaging` |
| **2. Streaming, distributed history** | An atomic execution **journal** is streamed to pluggable sinks (logs, a message stream, Prometheus, OpenTelemetry logs). W3C trace context plus correlation and causation IDs follow work across nodes and brokers. Grafana (Prometheus/Tempo/Loki) and Azure Monitor paths are both supported. | `DnA Workflows.observability` |
| **3. Operational lifecycle** | Faults are classified; a short **retry budget** applies; then work is **parked** behind a **hold** (auto-tripped per dependency). Holds are released by **pluggable strategies** (manual, TTL, probe-auto, probe-recommend, AI), which modules contribute. Operators can resume, retry or abandon work, and every action is audited. | `DnA Workflows.resilience` |
| **4. AI readiness** | Microsoft Agent Framework clients (OpenAI, Azure OpenAI, Ollama, OpenAI-compatible). Activities: `AgentPrompt`, `ExtractStructured`, `Classify` (the label becomes the flowchart outcome). An AI **triage agent** with read-only tools *proposes* releases; humans approve. Everything is machine-describable (`/capabilities`). | `DnA Workflows.ai` |

### Built for domain analysts (iteration 2)

| Capability | What it gives a semi-technical operator |
|---|---|
| **DnA Workflows Studio** (`studio/`, Vite + React) | Visual designer with a **data picker** (no binding syntax), schema-generated forms, estimates in hours/minutes, "undo" actions, **test runs** with sample data (nothing leaves the system), forecast preview, reviews and comments, role-aware "My work", executions with ETA, operations console. |
| **Collaborative authoring** | Shared drafts → review (four-eyes) → publish; every hand-over is a role, never a single person. |
| **Forecasting** | "Usually about 2 h, rarely more than 5 h": designer estimates refined by history (DuckDB), per input (e.g. per order line), live ETA of running work including frozen dependencies, measured accuracy. |
| **Sub-workflows & compensation** | Reuse workflows as steps; undo completed steps in reverse order (saga), automatically on failure or on request. |
| **Keycloak** | Login for people (PKCE) and machines (client credentials); roles → permissions on every route. |
| **MCP server** | AI agents (stdio or HTTP + OAuth2) can inspect, start, forecast and *propose* – never approve. |
| **PostgreSQL** | Multi-node clusters; verified against a real PostgreSQL 16 in the test suite. |

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[all,dev]"

# the narrated freeze/unpause demo (in-process: in-memory broker, SQLite, simulated ERP)
python examples/order_integration/demo.py

# tests + coverage gate (total > 70 %, critical modules ≥ 85 %)
pytest --cov=DnA Workflows --cov-report=json:coverage.json && python tools/check_coverage.py
```

Run a real host (API on :8080, `/docs` for OpenAPI):

```bash
cd examples/order_integration
uvicorn fake_erp:app --port 18080 &                  # a switchable fake ERP
python -m DnA Workflows run --config shell.inmemory.yaml
curl -XPOST localhost:8080/messages/orders -H 'content-type: application/json' \
     -d '{"type":"order.placed","data":{"order_id":1},"correlation_id":"order-1"}'
curl -XPOST localhost:18080/toggle                   # take the ERP down, send more orders, watch /holds
```

Studio locally (development mode, persona switcher instead of Keycloak):

```bash
cd studio && npm ci && npm run build && cd ..
cd examples/order_integration
uvicorn fake_erp:app --port 18080 &
python -m DnA Workflows run --config shell.studio.yaml      # → http://localhost:8080/studio/
# Studio development with hot reload: cd studio && npm run dev   (→ http://localhost:5173/studio/)
```

Full stack – PostgreSQL, RabbitMQ, Keycloak, Studio, MCP, Grafana/Tempo/Loki/Prometheus:
`docker compose -f deploy/docker-compose.yml up --build` (Keycloak users anna/ben/dora/olga/alice/admin,
password = user name). MCP for a local agent: `python -m DnA Workflows mcp --config shell.yaml`.
All quality gates: `tools/check_all.sh`.
For the Azure path, see `deploy/azure/README.md`.

CLI: `python -m DnA Workflows run|validate|mcp --config <shell.yaml>`, `describe [--config]`, `schema`.

## Core concepts

### Shells, features, plugins
A **shell** is a composition of **features** selected by configuration (YAML/JSON with `${ENV:-default}`):

```yaml
name: order-integration
features:
  persistence.sqlite: {path: /data/DnA Workflows.db}
  runtime: {definitions: [./workflows]}
  resilience: {release: {rules: [{pattern: "http:erp", strategy: probe-auto}]}}
  messaging: {endpoints: {orders: {transport: rabbitmq, address: DnA Workflows.orders, consume: true}}}
  messaging.rabbitmq: {url: "${RABBITMQ_URL}"}
  observability.prometheus: {}
  api: {port: 8080}
```

Every capability sits behind a contract. Features register implementations in two ways:
- **Override** (`replace`/`try_add`): exactly one implementation of a contract, e.g. the `FailurePolicy`.
- **Extend** (`add_contributor`): one more entry in a fan-in, e.g. another `FaultClassifier`, `HealthProbe`,
  `JournalSink` or transport.

Features are discovered through the `DnA Workflows.features` entry-point group, exactly like third-party
plugins. See [`docs/plugins.md`](docs/plugins.md).

### Workflows
A workflow is a **flowchart**: nodes (activities) plus outcome-labelled edges. Fork is implicit, merging
uses `Join`. Definitions are JSON/YAML files, or code via `WorkflowBuilder`. Inputs are bound
safely, with no code strings evaluated:
`{"$input": "order.id"}`, `{"$var": "x"}`, `{"$output": "node.path"}`, `{"$payload": ...}`, `{"$ctx": "correlation_id"}`,
`{"$tmpl": "Order {input.order.id}"}`.

Built-in activities: `Start`, `End`, `Log`, `SetVariable`, `SetOutput`, `If`, `Switch`, `Delay` (a durable timer), `Join`,
`Event` (trigger/wait), `Fail`, `Code`, `RunWorkflow` (sub-workflow), `Compensate` (undo completed steps),
`HttpRequest`, `SendMessage`, `MessageReceived`, `AgentPrompt`, `ExtractStructured`, `Classify`.

### Execution guarantees
- One **checkpoint** per step: state, outbox intents, journal events and inbox keys commit atomically, with
  optimistic concurrency fencing the writer.
- Activities run **at least once**. Side effects go through the **outbox** and are delivered after the commit.
- A failed attempt is rolled back: its variables, output and intents are discarded.
- Timers, retries and recovery after a crash are **durable**, because the store drives them.

### Operational lifecycle (pillar 3)

```
 Pending → Running → Completed
              │  ├→ Retrying ─(budget)─→ Parked ──(hold released / operator)──→ Resuming → Running
              │  └→ Faulted ──(operator retry after fix)──────────────────────→ Resuming
 Parked/Pending/Retrying/Resuming → Abandoned (operator, audited)
```

- **Classification**: `transient`, `dependency_unavailable`, `throttled` (honours `Retry-After`), `permanent`,
  `unknown`. Providers contribute classifiers (AMQP, Service Bus, HTTP, AI).
- **Retry budget** per fault kind, per dependency pattern (`http:sap-*`) or per node.
- **Auto-trip**: once the budget is exhausted on an unavailable dependency, a **dependency hold** is placed.
  Other executions then park *before* calling the dependency.
- **Hold scopes**: `global`, `workflow`, `execution`, `activity_type`, `dependency`, `ingress` (stop
  consuming; messages wait in the broker).
- **Release strategies** (pluggable, chosen per hold; modules contribute defaults for their dependencies):
  `manual`, `ttl`, `probe-auto`, `probe-recommend`, `ai-recommend`. Releasing resumes parked work in
  parking order (FIFO) and releases held outbox intents. A crash in the middle of a release is healed by
  the coordinator.
- **Proposals**: strategies and agents may only *propose*. An approval executes the action, and the
  actor is recorded.

### Observability (pillar 2)
- The **journal** is the committed history. It is queryable by execution, correlation ID or trace ID, and
  the exporter streams it to sinks with per-sink cursors (at-least-once delivery).
- **Traces**: `workflow.start`/`workflow.resume`, `activity <type>` and `send <intent>` spans. Outgoing
  messages carry the producer span, so consumers on other nodes continue the **same trace**.
- **Metrics**: counters derived from the journal, activity duration histograms, and state gauges. All are
  exposed on `/metrics`.
- **Logs**: JSON lines with `trace_id`, `span_id` and `correlation_id`. Journal events can also be
  exported as OpenTelemetry log records (to Loki or Application Insights).

## Project layout

```
src/DnA Workflows/
  kernel/          container (override/extend), features, shells, config, hosting, clock, ids
  runtime/         models, bindings, activities, registry, engine, router, outbox, timers, lifecycle
  persistence/     store contracts, in-memory (indexed), SQLite and PostgreSQL providers
  resilience/      classification, retry budget, holds, release strategies, proposals
  messaging/       envelope/endpoints/transport contracts, bus, consumer host, activities, conformance kit
    providers/     inmemory, rabbitmq (aio-pika), servicebus (azure-servicebus)
  observability/   journal export, log correlation, prometheus, otel, azure_monitor
  ai/              MAF client registry, activities, operations tools, triage, scripted test client
  activities/      http integration plugin
  forecasting/     DuckDB observation store, estimator, graph forecast, live ETA, accuracy
  authoring/       drafts, review/publish, test runs
  security/        Keycloak/OIDC resource server, roles → permissions
  mcpserver/       MCP server (stdio + streamable HTTP with OAuth2)
  operations.py    operations tools shared by AI agents and MCP
  api/             FastAPI management API (+ serves the Studio)
  hosting/         host process + CLI
studio/            DnA Workflows Studio (Vite + React + TypeScript)
examples/order_integration/   workflow, shell configs (in-memory / RabbitMQ / Service Bus), demo, fake ERP
deploy/            Dockerfile (incl. Studio), docker-compose (PostgreSQL, RabbitMQ, Keycloak, Grafana stack),
                   OTel collector (Grafana + Azure), Keycloak realm, Grafana provisioning, Azure runbook
tests/             one folder per package; engine/store tests run on in-memory, SQLite AND real PostgreSQL
tools/check_coverage.py       coverage gate
```
