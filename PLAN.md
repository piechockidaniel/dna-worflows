# DnA Workflows — Implementation Plan

> Product name **DnA Workflows** (package `DnA Workflows`), decided in Q-001.
> Python 3.12, asyncio-first.

Every plan item has an ID (`P<phase>.<item>`). Decisions in `CHANGELOG.md` reference these IDs.

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` deferred

**Iteration 1 status: all planned items done except P5.5 (deferred, Q-004).**

---

## Guiding principles (apply to every phase)

- **G1 Everything is a plugin.** Each capability lives behind a contract (a Python `Protocol` or `ABC`).
  Implementations are registered by a *Feature*. Implementations can be chosen in two ways:
  - **Override**: replace the single implementation of a contract.
  - **Extend**: add one more contributor to a fan-in list.
- **G2 Composition by configuration.** A *Shell* (a JSON, YAML or dict document) selects features and
  their options. Code does not change when, for example, RabbitMQ is swapped for Service Bus.
- **G3 Durable by default where it matters.** Checkpoint, outbox, journal and holds commit atomically. The
  in-memory stores are for tests and demos. SQLite is the first durable provider.
- **G4 No hammering, no dropping.** Failures are classified. Short retries happen inside a budget, and
  after that work is **parked** behind a **hold**, which is released by a pluggable strategy.
- **G5 Traceable end to end.** Every hop carries W3C trace context plus a correlation ID and a causation
  ID.
- **G6 Not over-engineered.** The target is hundreds of messages per second. It runs as a single process
  or as a few nodes sharing a store.
- **G7 Tested.** Total coverage must exceed 70%. Critical modules (the runtime engine, stores, outbox,
  resilience, and messaging core) must reach at least 85%. Both are enforced by `tools/check_coverage.py`.

---

## P0 — Foundations (kernel)

- [x] **P0.1** Project skeleton: `pyproject.toml`, src layout, pytest/coverage config, README.
- [x] **P0.2** `ServiceCollection` / `ServiceProvider`: singleton/transient lifetimes, factories,
  constructor autowiring by type hints, and both axes: `replace` (override) / `try_add` (default) /
  `add_contributor` (extend).
- [x] **P0.3** `Feature` contract (name, dependencies, options, `configure`) + `FeatureCatalog`.
  Discovery through the explicit register call and Python entry points (`DnA Workflows.features`).
- [x] **P0.4** `Shell`: build a composition from config, resolve dependencies, topologically order the
  features, build the provider, and start/stop `HostedService` contributors.
- [x] **P0.5** Kernel utilities: `Clock` (system/manual, for deterministic tests) and ID generation.

## P1 — Workflow runtime

- [x] **P1.1** Definition model: `WorkflowDefinition` is a flowchart of nodes and outcome-labelled edges
  (fork is built in; join is a node). It serialises to and from JSON. There is also a fluent builder.
- [x] **P1.2** Input binding: literals, `$var`, `$input`, `$output`, `$tmpl` (safe templating), and Python
  callables for code-first workflows.
- [x] **P1.3** `Activity` contract: `execute` and `resume`, typed inputs via pydantic (this also gives a JSON
  Schema, which matters for AI readiness, P5), an optional `dependency` declaration (for P3), and an
  optional trigger declaration.
- [x] **P1.4** `ActivityRegistry` + descriptors (type, category, inputs JSON Schema, outcomes).
- [x] **P1.5** Execution state: execution status, variables, node outputs, a *work queue* of work items,
  bookmarks, join state, revision (CAS fencing), and trace/correlation fields.
- [x] **P1.6** Engine: a single-writer drain per execution. Each step commits one **Checkpoint**
  (execution + outbox intents + journal events) atomically. Work items may be delayed (`not_before`),
  which is used by both retries and `Delay`.
- [x] **P1.7** Stimulus routing: published trigger index → start executions; bookmark index → resume
  executions. Optional correlation filter. Inbox dedup by message ID.
- [x] **P1.8** Post-commit outbox: intent kind → handler registry (Extend). The processor delivers
  intents, classifies failures (P3), and retries or marks them final.
- [x] **P1.9** Timer service: wakes executions whose delayed work became due. Durable, because it is
  driven by the store.
- [x] **P1.10** Built-in activities: `Start`, `Log`, `SetVariable`, `If`, `Switch`, `Delay`, `Join`,
  `Event` (trigger/bookmark), `Fail`, `End`, `Code`.
- [x] **P1.11** Stores: contracts + in-memory implementations + **SQLite** (durable, single-file, WAL).
- [x] **P1.12** *(added during implementation, D-036)* HTTP integration plugin: `HttpRequest`, named HTTP
  dependencies, health probes, error classification, trace/correlation header propagation.

## P2 — Messaging as a plugin layer (Pillar 1)

- [x] **P2.1** `MessageEnvelope` (CloudEvents 1.0 shaped + trace/correlation/causation), logical
  `Endpoint` → physical `EndpointBinding` (transport + address + options).
- [x] **P2.2** `MessageTransport` contract: capabilities, `send`, `subscribe`, `probe` (health), and the
  dependency key. Typed errors: `TransportUnavailable` / `TransportPermanentError`.
- [x] **P2.3** `MessageBus` (logical send) + `messaging.send` outbox intent handler (transactional send).
- [x] **P2.4** `ConsumerHost`: subscribes to consumable endpoints and turns each message into a stimulus.
  It acks after commit, honours **ingress holds**, and dedupes through the inbox.
- [x] **P2.5** Activities: `SendMessage`, `MessageReceived` (trigger + correlated bookmark).
- [x] **P2.6** Providers: **InMemory** (with fault injection), **RabbitMQ** (aio-pika), and
  **Azure Service Bus** (azure-servicebus aio).
- [x] **P2.7** Transport **conformance kit** (a reusable test suite every provider must pass), plus
  opt-in real-broker tests gated by environment variables.

## P3 — Operational lifecycle: freeze / investigate / unpause (Pillar 3)

- [x] **P3.1** `FaultKind` + `FaultClassifier` (Extend; composite). Default classifiers cover Python
  network errors, HTTP statuses, and transport errors.
- [x] **P3.2** Dependency keys (`<kind>:<name>`, e.g. `rabbitmq:orders`, `http:sap-prod`) declared by
  activities and intents.
- [x] **P3.3** Retry budget policy (Override): attempts and backoff → `RetryAfter` | `Park` | `Fault`.
- [x] **P3.4** **Holds**: scopes `global`, `workflow`, `execution`, `activity_type`, `dependency`,
  `ingress`. Includes the hold service and the pause gate, which is evaluated before work executes and
  before outbox delivery.
- [x] **P3.5** Work-item lifecycle state machine (`Pending → Running → Retrying → Parked → Resuming →
  Completed | Faulted | Abandoned`). It validates transitions and records every transition in the journal.
- [x] **P3.6** Auto-trip policy: after the budget is exhausted on `DependencyUnavailable`, a dependency
  hold is placed automatically, so other executions park instead of each hammering the dependency.
- [x] **P3.7** **Pluggable release strategies**, chosen per dependency pattern or contributed by a module:
  `manual`, `probe-auto`, `probe-recommend`, `ttl`. A `ReleaseCoordinator` runs in the background.
  `HealthProbe` contract: providers contribute probes for their dependencies.
- [x] **P3.8** Release → resume: parked work matching the hold goes back to `Resuming → Pending`. Manual
  resume and abandon work per item.
- [x] **P3.9** Proposals (approval workflow): used by `probe-recommend` and by AI triage (P5). An action is
  executed only on approval.

## P4 — Observability: streaming, distributed, end to end (Pillar 2)

- [x] **P4.1** `TraceContext` (W3C traceparent parse/format/child) + correlation/causation context vars.
- [x] **P4.2** Propagation through work items, outbox intents, envelopes, consumers and new executions, so
  that one trace spans nodes and services.
- [x] **P4.3** Journal: `JournalEvent`, committed with the checkpoint, and queryable by execution /
  correlation / trace.
- [x] **P4.4** `JournalExporter` (a background cursor) → `JournalSink` contributors (Extend). Built-in sinks:
  structured logging (JSON lines) and messaging (stream to an endpoint).
- [x] **P4.5** OpenTelemetry spans for activity executions and deliveries, parented by the propagated
  context.
- [x] **P4.6** **Grafana/Prometheus path**: Prometheus metrics sink + `/metrics`, OTLP exporter (Tempo),
  provisioning files (docker-compose, Prometheus config, Grafana dashboard).
- [x] **P4.7** **Azure path**: Azure Monitor OpenTelemetry exporter feature (traces), configuration
  through a connection string.

## P5 — AI readiness (Pillar 4) on Microsoft Agent Framework

- [x] **P5.1** `ChatClientRegistry`: named MAF chat clients (`SupportsChatGetResponse`). Factories for
  known providers are lazily imported.
- [x] **P5.2** Activities: `AgentPrompt`, `ExtractStructured` (JSON Schema validated), `Classify` (label
  → outcome, for routing). AI failures are classified as a dependency `ai:<client>`, so P3 applies to
  them as well.
- [x] **P5.3** Operations tools (read-only + *propose*): list holds, parked work, journal, and
  propose-release, exposed as MAF tools.
- [x] **P5.4** Triage service: a MAF agent analyses a hold (context = journal + faults) → structured
  recommendation → **Proposal** (a human approves).
- [x] **P5.5** MCP server exposing workflows and operations tools – delivered in iteration 2 (P13).

## P6 — Hosting, management API, example

- [x] **P6.1** `Host`: load the shell config file, build, start hosted services, graceful stop.
- [x] **P6.2** Management API (FastAPI): workflows, executions, journal, holds, parked work, proposals,
  `/metrics`, health.
- [x] **P6.3** Example: "order integration". A message arrives → a call to an unreliable dependency → a
  reply message. It shows parking, freezing and releasing.
- [x] **P6.4** `deploy/`: docker-compose (RabbitMQ, Prometheus, Grafana, Tempo), config samples for
  RabbitMQ and Service Bus.

## Next iteration (proposed, not started – see CHANGELOG open questions)

- [x] **P5.5** MCP server over `OperationsTools` (Q-004) → delivered as P13
- [~] **P1.13** PostgreSQL persistence delivered (P15.1); execution ownership leases still open (Q-010)
- [ ] **P2.8** Kafka / Event Hubs transports (same conformance kit)
- [x] **P6.5** AuthN/Z with Keycloak (Q-007) → delivered as P10
- [x] **P1.14** Sub-workflows and compensation (Q-008) → delivered as P14
- [ ] **P7.4** Real-broker CI stage (RabbitMQ container, Service Bus emulator) (Q-003)

## P7 — Quality gates

- [x] **P7.1** Unit + integration tests for every phase (pytest, pytest-asyncio).
- [x] **P7.2** Coverage gate: total > 70%, critical modules ≥ 85% (`tools/check_coverage.py`).
- [x] **P7.3** End-to-end scenario tests: two "nodes" (runtimes) sharing a store and a transport, with
  trace continuity across them.

---

# Iteration 2

**Iteration 2 status: all items done.** Details and evidence in `CHANGELOG.md` (D-045 … D-066).

Inputs: your answers to Q-001…Q-009 and your vision — (1) a **Vite low-/no-code GUI** so that a
*domain analyst* (semi-technical) can run the whole process without a single indispensable person,
and (2) **processing-time forecasting** that starts from estimates and refines itself with history.

Guiding principles added:

- **G8 No single-person bottleneck.** Capabilities are *roles*, not people: several people can hold each
  role, every hand-over is an explicit, visible state (draft → in review → published; hold → proposal →
  approved), and everything can be picked up by anyone with the role.
- **G9 Analyst first.** Anything an analyst does in the GUI is driven by machine-readable descriptors
  (JSON Schemas, outcome lists, output shapes) — no hand-written forms per activity, no expression syntax.
- **G10 Estimates are honest.** Every forecast carries its *basis* (designer estimate / history / mixed,
  sample size) and its uncertainty range; accuracy is measured against actuals.

## P8 — Rename
- [x] **P8.1** FlowMesh → **DnA Workflows** (`DnA Workflows` package, entry points, env vars `DnA Workflows_*`).

## P9 — Processing-time forecasting (vision #2)
- [x] **P9.1** Timing capture: the engine records per-step `duration` and queue `wait` in the journal;
      execution lead time, park and bookmark waits are derived from events.
- [x] **P9.2** Designer estimates on nodes: three-point estimate (optimistic / likely / pessimistic) and an
      optional **driver** (duration per unit of an input feature, e.g. per order line).
- [x] **P9.3** Input features per workflow (`count`, `value`, `length`, `bucket` of an input path).
- [x] **P9.4** **DuckDB** observation store (columnar; quantiles, regression and filtered re-aggregation in SQL)
      fed by a journal sink — history is kept in a form that is cheap to traverse and recompute.
- [x] **P9.5** Estimator: Bayesian blend of prior (PERT) and recency-weighted history, hierarchical
      fall-back (node+conditions → node → activity type/dependency → prior → default), driver regression.
- [x] **P9.6** Graph forecast: branch probabilities from history, fork/join (max), loops (fixed point),
      uncertainty (variance propagation → P50/P90).
- [x] **P9.7** Live ETA of running executions (elapsed, parked behind holds, waiting on bookmarks) —
      re-forecast on every step.
- [x] **P9.8** Accuracy: forecast snapshot at start vs actual at completion (error per workflow).
- [x] **P9.9** API + MCP exposure.

## P10 — Security with Keycloak (Q-007)
- [x] **P10.1** OIDC resource server: JWT (RS256) validation via issuer discovery + JWKS, audience, expiry.
- [x] **P10.2** Roles (realm + client roles) → permissions; route-level enforcement; actor = token subject.
- [x] **P10.3** Keycloak realm export (clients: studio SPA with PKCE, API, MCP service account) in `deploy/`.

## P11 — Collaborative authoring (vision #1, G8)
- [x] **P11.1** Workflow drafts (server-side), validation on save, comments.
- [x] **P11.2** Review flow: draft → in review → approved → published (4-eyes optional), version history, diff.
- [x] **P11.3** Test runs with sample input (isolated, no side effects leaving the host) + forecast preview.
- [x] **P11.4** Designer metadata for analysts: activity display names, help texts, output shape hints.

## P12 — DnA Workflows Studio (Vite GUI, vision #1)
- [x] **P12.1** Vite + React + TypeScript app, Keycloak login (PKCE), role-aware navigation.
- [x] **P12.2** Visual designer (React Flow): palette from `/capabilities`, schema-generated forms,
      **data picker** (pick fields of previous steps instead of writing bindings), validation, estimates.
- [x] **P12.3** Review/publish screens; test run with sample data; forecast preview.
- [x] **P12.4** Operations console: executions, timeline/journal, holds, parked work, proposals (approve),
      AI triage.
- [x] **P12.5** Forecast views: ETA of running work, per-step statistics, accuracy.
- [x] **P12.6** Unit tests (Vitest) for the core UI logic; build served by the API feature.

## P13 — MCP server (Q-004)
- [x] **P13.1** Tools: operations (read-only + propose), workflows (list/describe/start), forecasts.
- [x] **P13.2** Transports: **stdio** and **streamable HTTP**; HTTP protected by OAuth2 (Keycloak,
      client-credentials service accounts), tools filtered by roles.

## P14 — Sub-workflows & compensation (Q-008)
- [x] **P14.1** `RunWorkflow` activity: child execution, parent waits, output/fault propagation via outbox,
      cancellation cascades, shared correlation + trace.
- [x] **P14.2** Compensation (saga): per-node compensating action, reverse-order execution with the normal
      retry/park machinery, triggers: `Compensate` activity, operator action, `compensate_on_fault`.

## P15 — Storage (Q-006)
- [x] **P15.1** PostgreSQL provider (psycopg 3 async, pool, `SELECT … FOR UPDATE`/revision fencing),
      passing the same conformance + engine suites against a real PostgreSQL.
- [x] **P15.2** DuckDB where justified: benchmark-driven decision for in-memory operations (recorded).

## P16 — Azure via collector (Q-005)
- [x] **P16.1** Collector config with the `azuremonitor` exporter; in-process exporter kept optional.

## P17 — Quality
- [x] **P17.1** Python coverage gate unchanged (>70 % total, ≥85 % critical); new critical modules:
      forecasting estimator, security, compensation.
- [x] **P17.2** Frontend: typecheck + Vitest + production build in the test run.
