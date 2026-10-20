# ChangeLog & Decision Record (DnA Workflows)

Every decision references the plan item it belongs to (see `PLAN.md`).

- **D-xxx**: a decision I made while implementing. You can override any of them.
- **Q-xxx**: an open question or blocker that needs *your* decision. Work continued elsewhere in
  the meantime.

---

## Decisions

### Foundations (P0)

- **D-001 · P0.1** Working name *FlowMesh* used during iteration 1; renamed to **DnA Workflows** after Q-001 (D-045).
- **D-002 · P0.1** The stack is Python 3.12, asyncio everywhere, pydantic v2 for every persisted or
  configurable model (gives JSON round-trip plus JSON Schema), a `src/` layout, and setuptools. Each plugin
  is an optional *extra* (`DnA Workflows[rabbitmq]`, `DnA Workflows[servicebus]`, `DnA Workflows[ai]`, …), so a
  deployment installs only what it composes.
- **D-003 · P0.2** DnA Workflows uses its **own ~200-line IoC container** instead of a third-party one. We need
  exactly two explicit semantics that the libraries blur:
  - **Override**: `replace` / `try_add`.
  - **Extend**: `add_contributor`, idempotent per key.

  It also supports constructor autowiring by type hints (including `list[X]` for contributors and
  `X | None` for optional services). There is no magic and no global state.
- **D-004 · P0.3** Features are discovered through the Python entry-point group `DnA Workflows.features`.
  Third-party plugins ship the same way as built-ins. A plugin whose optional dependency is not
  installed is skipped with a warning and never breaks the host.
- **D-005 · P0.4** A shell auto-includes feature dependencies with default options. Strict mode is
  available via `auto_include_dependencies: false`. Feature options are validated with `extra="forbid"`,
  so config typos fail at startup.
- **D-006 · P0.4** Config files may be YAML or JSON, with `${ENV}` / `${ENV:-default}` substitution.
  Secrets such as connection strings stay out of files.

### Runtime (P1)

- **D-007 · P1.1** The execution model is a single one: a **flowchart** of nodes with outcome-labelled
  edges. Fork is implicit (several edges on one outcome), and merging is an explicit `Join` node
  (`all`/`any`). Nested composite activities are *not* implemented.
  Loops are expressed as graph cycles, and sub-workflows are left for a later iteration. This keeps the
  engine small and the state easy to inspect.
- **D-008 · P1.2** **No evaluation of code strings.** Definitions bind inputs through safe,
  JSON-serialisable forms: `$var`, `$input`, `$output`, `$payload`, `$ctx`, and `$tmpl` templates.
  Code-first workflows may use Python callables; such definitions stay in memory and are not persisted.
  See **Q-002** about a richer expression language.
- **D-009 · P1.3** Activities are instantiated per step **through the container**, so they can ask for
  services (message bus, chat clients, …) in their constructor. The plugin model reaches down to single
  activities.
- **D-010 · P1.5** Variables and outputs must be JSON. This is enforced at the step boundary and a
  violation is a permanent fault, never a crash of the engine.
- **D-011 · P1.6** Each step commits one checkpoint containing execution state, outbox intents, journal
  events and inbox keys, atomically, with optimistic concurrency (`revision`) as fencing.
  - Activities therefore run **at least once**. Side effects that must not repeat go through the outbox.
  - A **failed attempt is rolled back**: its variable/output changes and staged intents are discarded.
- **D-012 · P1.6** When no correlation ID is supplied, the execution ID becomes the correlation ID. Every
  execution is correlatable end to end.
- **D-013 · P1.7** Inbox dedup keys (`start:<def>:<node>:<msg>` / `resume:<bookmark>:<msg>`) are
  committed **in the same transaction** as the start/resume they guard. A redelivered message is therefore
  recognised exactly once per target, with no separate dedup table that could drift.
- **D-014 · P1.7** Trigger identities are computed **at publish time**. Routing
  a stimulus is an index lookup, not a scan of running executions.
- **D-015 · P1.11** Stores are **split per contract** (execution, outbox, journal, definitions,
  key-value).
  - **In-memory** is the default and round-trips through JSON, so it behaves like production storage.
  - **SQLite** (WAL, `BEGIN IMMEDIATE`, revision CAS) is the first durable provider. Several local
    processes can share one file.
  - One conformance suite runs against both; all engine tests run on both backends.
- **D-016 · P1.11** There is no distributed lease or placement service yet (**G6**, "hundreds of
  messages"). Revision fencing already prevents double commits. Leases can be added behind the store
  contract when scale requires it.

### Operational lifecycle (P3)

- **D-017 · P3.5** The work-item state machine lives in the **runtime**, which owns work items. The
  **resilience** feature only supplies policies (classifier, retry budget, holds, release strategies).
  The runtime ships safe defaults: fault immediately, never hold.
- **D-018 · P3.5** Added an operator action that was not in the original plan: **retry faulted work**
  (`Faulted → Resuming`). It is the "investigate, fix, unpause" path for permanent errors such as bad data.
- **D-019 · P3.8** Abandoning the last remaining work of an execution ends it as `cancelled`, with a fault
  record `Abandoned: <reason>`. If other branches or bookmarks remain, the execution continues.

### Observability (P4)

- **D-020 · P4.1** `TraceContext` (W3C traceparent) is a **dependency-free core primitive**.
  OpenTelemetry is an optional layer on top, so trace continuity works even without OTel installed.
- **D-021 · P4.2** On resume, the execution **continues its own trace**. An incoming stimulus span is
  adopted as parent only when it belongs to the same trace; otherwise it is recorded as a *link*
  (`link_traceparent`) in the journal.

- **D-033 · P4.5** A `SpanFactory` contract gives the start and resume of an execution **real (instant)
  spans**, so tracing UIs show a complete parent chain. The **producer span** of an outbox delivery
  becomes the outgoing message's `traceparent`, which makes the consumer on another node its child.
  Verified by test: one trace across two nodes and a broker.
- **D-034 · P4.3/P4.4** Each journal sink has its **own durable cursor**, so a failing sink never blocks
  the others. Delivery to sinks is at-least-once; sinks dedupe on the event `id`.
- **D-032 · P4.6** **Prometheus counters are derived from the journal**, so metrics and history cannot
  disagree. Durations come from a middleware and gauges from periodic store reads. Consequence: when
  several nodes share one store, enable the exporter on one node only (`observability.exporter_enabled`).
- **D-031 · P4.7** **OpenTelemetry is pinned to 1.44.x.** `azure-monitor-opentelemetry-exporter 1.0.0b45`
  is broken with OTel 1.45 despite declaring `~=1.35`. The working `1.0.0b57` requires exactly 1.44.
  See **Q-005**.

### Messaging (P2)

- **D-023 · P2.1** Envelopes are **CloudEvents 1.0 in binary content mode**: the body is the data and the
  attributes travel as `cloudEvents:*` headers, plus `traceparent`/`tracestate`. Messages from
  non-DnA Workflows producers are accepted too, with native properties (message ID, correlation ID, type,
  subject) filling the gaps. DnA Workflows can therefore consume existing traffic.
- **D-024 · P2.2** The dependency key of an endpoint is `<transport>:<endpoint>`: a hold on one endpoint
  does not freeze the whole broker. An operator can still freeze the broker with the pattern
  `rabbitmq:*`.
- **D-025 · P2.4** An inbound message no workflow matches is **dead-lettered by default**. It is neither
  acked (which would silently drop it) nor requeued forever. `ack` and `requeue` are configurable.
- **D-026 · P2.4** An ingress hold **closes the subscription**, so messages wait in the broker at no cost.
  This is better than nack-looping. The consumer host also re-reconciles periodically, so a broker that
  is down at startup does not crash the host.
- **D-027 · P2.6** **RabbitMQ is native (aio-pika), not via MassTransit.**
  - Publisher confirms plus `mandatory` mean an unroutable message is a *permanent* error, never a
    silent loss.
  - Messages are persistent and acked manually after the commit.
  - A delayed requeue (`redelivery_delay`) stops a poison message spinning the CPU.
  - A passive mode supports brokers where the application may not declare topology.
- **D-028 · P2.6** **Service Bus** settlement:
  - complete after the commit;
  - abandon on failure (Service Bus dead-letters after MaxDeliveryCount);
  - dead-letter on reject.

  `ServerBusy`/`QuotaExceeded` map to *throttled*. Health is checked by peeking `probe_entity`, which needs
  no admin rights. Authentication uses a connection string or Azure AD (`namespace`).
- **D-029 · P2.7** The **transport conformance kit ships inside the package**
  (`DnA Workflows.messaging.conformance`), so third-party providers run the same contract tests. It runs
  against in-memory, RabbitMQ (two topologies) and Service Bus (queue and topic).

### Operational lifecycle (P3), continued

- **D-030 · P3.7** This answers your point 4, "release strategy as part of module integration". Each hold
  gets its strategy when it is placed, in this precedence: **configuration rules → rules contributed by
  modules → configured default (`manual`)**. Every transport, the HTTP plugin and the AI module
  contribute defaults for their own dependencies. Built-in strategies: `manual`, `ttl`, `probe-auto`,
  `probe-recommend`, `ai-recommend`. New ones are one class each.
- **D-040 · P3.8** Release resumes parked work in **parking order (FIFO)**. A release interrupted by a
  crash (hold marked released, work not yet resumed) is **healed by the coordinator's orphan sweep**.
  Both were found by tests and fixed.

### HTTP, AI, API, hosting

- **D-036 · P1.12 (new plan item)** Added an **HTTP integration plugin**, because most ERP/SAP
  integrations are HTTP:
  - named dependencies (base URL, headers, health URL);
  - status codes classified (429 honours `Retry-After`);
  - `traceparent` and `x-correlation-id` propagated downstream;
  - an auto-release probe.
- **D-035 · P5.1** The model contract is **MAF's own `SupportsChatGetResponse`**; DnA Workflows only adds naming,
  configuration and lazily built providers (`openai`, `azure_openai` incl. Azure AD, `ollama`,
  `openai_compatible`). An AI endpoint is a dependency `ai:<client>` like any other, so model outages
  retry, park and hold too.
- **D-038 · P5.2** `ExtractStructured` and `Classify` work provider-neutrally: prompted JSON, validated with
  **JSON Schema**, with repair attempts. They do not rely on provider-native structured output. Invalid
  model output is a *transient* fault, so budget, park and operator apply. In `Classify`, the label is
  the flowchart **outcome**.
- **D-037 · P5.3/P5.4** **"AI proposes, humans dispose."** Agent tools are read-only plus `propose_*`; the
  triage agent never acts. It is also available as the release strategy `ai-recommend`. Tool calling is
  verified with a scripted MAF client.
- **D-039 · P6.2** API rules:
  - Routes exist only for composed features.
  - `X-Actor` names the person or system behind an action, for the audit trail.
  - `X-API-Key` authentication is optional (see **Q-007**).
  - `traceparent` is accepted on starts and messages.
  - `POST /messages/{endpoint}` is an HTTP→broker bridge.
  - `POST /proposals` lets external agents propose.
  - `/capabilities` describes features and activity JSON Schemas (AI readiness).
- **D-041 · P1.1** The builder's node-level options were renamed `dependency_override`/`retry_override`.
  They had silently captured `HttpRequest`'s `dependency` input; a test caught it.
- **D-042 · P0.3** Entry-point discovery is cached per process, since scanning all installed
  distributions on every shell build was slow.
- **D-043 · P6.1** `python -m DnA Workflows run` installs JSON logging on stdout. `configure_logging` is
  idempotent; duplicated log lines were found in the real-process smoke test and fixed. Journal log
  lines carry the event as a structured `journal` field, not a JSON string.

### Environment

- **D-022 · P2.6/P2.7** The sandbox has no root access and no Docker, so no real RabbitMQ/Service Bus can
  run here. The providers are tested against **fakes at the client-library boundary** (aio-pika /
  azure-servicebus objects). Real-broker conformance tests are included and run automatically when
  `DnA Workflows_TEST_RABBITMQ_URL` / `DnA Workflows_TEST_SERVICEBUS_CONNECTION` are set.
- **D-044 · P7** What was verified here:
  - 233 tests; engine and store suites run on **both** persistence backends.
  - **97.7 % coverage**; all critical modules are at ≥ 87 %, with the gate in `tools/check_coverage.py`.
  - A lint pass (ruff).
  - A **multi-process smoke test**: the CLI host, the fake ERP over real HTTP, an outage, an auto-trip, a
    probe release, `/metrics`, and graceful SIGTERM.

---

## Iteration 2 decisions (your answers + vision)

### Answers recorded

| Q | Your answer | Resolution |
|---|---|---|
| Q-001 | **DnA Workflows** | Renamed everywhere (D-045) |
| Q-002 | Not now, maybe later | No expression language; data picker + templates cover analysts (D-066) |
| Q-003 | You will run Docker/real brokers later | Still open on your side; see "Open questions" below |
| Q-004 | Both transports, OAuth2 client credentials | stdio + streamable HTTP with Keycloak tokens (D-058) |
| Q-005 | Use the collector | `deploy/otel-collector.azure.yaml`; in-process exporter optional (D-062) |
| Q-006 | PostgreSQL; DuckDB where justified | PostgreSQL provider (D-061); DuckDB decided by benchmark (D-050) |
| Q-007 | Keycloak | OIDC resource server + roles (D-052) |
| Q-008 | Yes | Sub-workflows + saga compensation (D-059, D-060) |
| Q-009 | Own resources per tenant | Isolation by deployment, no tenant dimension in code (D-065) |

### Decisions

- **D-045 · P8.1** Renamed FlowMesh → **DnA Workflows** (package `DnA Workflows`, env vars `DnA Workflows_*`).
- **D-046 · P9.1** Forecasts model a step's **latency**: time from enqueue to completion, including queueing,
  retries, parking and waiting for messages or people. This is elapsed business time, which is what an ETA
  needs. Pure execution time is recorded separately (`duration`).
- **D-047 · P9.5** The estimator works like this:
  - The designer's three-point estimate (PERT) is the **prior**. It counts as `prior_strength` = 3
    pseudo-runs.
  - It is blended with **recency-weighted history** (half-life of 50 runs).
  - Fallback levels run from "step + conditions" to "step", then "activity type + dependency", then
    "estimate", then a flagged default.
  - Drivers (e.g. per order line) are learned by linear regression (`regr_slope` in DuckDB).
  - P50/P90 come from a log-normal fit.
  - Every number carries its **basis** (estimate / history / mixed / no data) and its sample count.
- **D-048 · P9.6** Whole-workflow forecasts:
  - Branch probabilities come from observed outcomes (Laplace-smoothed).
  - Parallel branches take the slowest one.
  - Loops converge by fixed-point iteration (expected number of passes).
  - Uncertainty propagates as variance.
- **D-049 · P9.8** Accuracy tracking works as follows:
  - A forecast snapshot is taken when a run starts and settled when it completes, which gives MAPE and P90
    coverage per workflow.
  - The snapshot is taken by the journal exporter a moment after start, so it uses history as of that moment.
- **D-050 · P9.4/P15.2** Where DuckDB is used was **decided by a benchmark** (`tools/bench_inmemory.py`,
  5,000 executions, median):

  | operation | old in-memory | DuckDB in-memory | **indexed in-memory (shipped)** |
  |---|---|---|---|
  | commit (every step) | 0.04 ms | 9.3 ms | **0.04 ms** |
  | get by id | 0.08 ms | 1.2 ms | **0.01 ms** |
  | timer scan | 130 ms | 2.8 ms | **0.30 ms** |
  | message routing | 122 ms | 3.3 ms | **0.24 ms** |

  Execution state is write-heavy, so DuckDB loses there. The in-memory store got proper indexes instead.
  DuckDB **is** used for forecasting history, which is append-only and aggregation-heavy (quantiles,
  regression, filtered re-aggregation). It can be a file, so history survives restarts.
- **D-051 · P9** The forecasting tests uncovered **engine timing bugs**: successor work, parking start, retry
  delays and `completed_at` were stamped with the step's *start* time. All are now stamped with the
  completion time.
- **D-052 · P10** Keycloak security:
  - DnA Workflows is an OIDC resource server. It validates JWTs via discovery and JWKS, with refresh on key
    rotation, audience and issuer checks, and leeway.
  - Realm and client roles map to permissions. There are 8 default roles, overridable in configuration.
  - With a token, the **actor is the token identity** (`X-Actor` is ignored, so it can't be spoofed).
  - API keys remain for automation.
  - If the security feature is not composed, the API runs in development mode (anonymous, full access).
    The production example always composes security.
  - `jwks_url` can differ from the issuer, for container networking.
- **D-053 · P11** Collaborative authoring:
  - There is **one shared draft per workflow**. Anyone with the design role can continue it; optimistic
    revisions prevent lost updates.
  - Saving never blocks on validation: problems are shown.
  - A draft moves from submit to review (optional four-eyes) to publish. Publishing assigns the next version
    and records `published_by` and `reviewed_by`.
  - Direct `POST /workflows` remains for automation/GitOps and needs `workflows.publish`.
- **D-054 · P11.3** Test runs use an isolated in-memory runtime:
  - Steps with external dependencies are **simulated** unless the analyst gives sample results or allows
    live calls.
  - Messages that would be sent are listed, not sent.
- **D-055 · P11.4** Activities now describe themselves for analysts: display name, help text and **output
  schema**. The Studio's data picker is built on that; the stored format is still the engine's binding
  JSON.
- **D-056 · P12** The Studio stack:
  - React 19, Vite 8, TypeScript 5.9 (the new TS 7 native compiler was skipped for stability), React
    Flow 12, and keycloak-js (authorization code + PKCE).
  - Runtime configuration comes from `/studio/config.json`, so the same build serves every environment.
  - It is served by the API under `/studio`. The designer is lazy-loaded, which cuts the bundle from 517 kB
    to 315 kB.
  - In development mode there is a persona switcher.
- **D-057 · P12** UX choices for the domain analyst:
  - Every input has three modes: *fixed value*, *from data* (a picker of earlier steps' fields) or *text
    with data*.
  - Estimates are entered in human units.
  - The timeline uses plain-language labels.
  - **"My work"** shows what any holder of your roles can pick up (G8).
  - Test runs and forecast previews live inside the designer.
- **D-058 · P13** MCP uses SDK 2.x (`MCPServer`):
  - **stdio**: a local subprocess with configured permissions (default: the agent role).
  - **Streamable HTTP** (stateless, JSON) at `/mcp` on the API. It validates Keycloak bearer tokens
    (client-credentials service accounts), publishes RFC 9728 metadata, filters tools by permission and
    re-checks permissions on every call.
  - Agents can read, start workflows and *propose*; they can never approve.
  - A shutdown hang was found and fixed: the session manager must live in the lifecycle task.
- **D-059 · P14.1** Sub-workflows:
  - Children are started **through the outbox** with deterministic IDs, so starts are idempotent.
  - Completion reaches the parent as a durable intent plus a bookmark.
  - A child's fault reaches the parent step with its classification, so parent retry, park and compensation
    apply.
  - Cancellation cascades to children.
- **D-060 · P14.2** Compensation (saga):
  - Undo actions are ordinary work items, so retries, parking and holds apply to them. They run in reverse
    order and can use the step's original output (`$payload original.output…`).
  - Triggers:
    - a `Compensate` step, after which the flow continues;
    - an operator action or approved proposal, ending in status `compensated`;
    - `compensate_on_fault`, ending in `compensated` with the fault kept.
  - A failing undo stops for an operator. An operator retry then re-runs only the undo steps; a test caught
    the case where the original failed step was re-run too.
- **D-061 · P15.1** PostgreSQL provider:
  - psycopg 3 async pool, `FOR UPDATE` plus revision fencing, lazy pool opening, one **schema per
    installation**.
  - Verified against a **real PostgreSQL 16** (pip `pgserver`): the full conformance, engine, resilience,
    messaging and observability suites run on it.
  - API modules driven through `TestClient` skip the PostgreSQL variant, because the async pool is bound to
    one event loop.
- **D-062 · P16** The Azure path goes through the **OpenTelemetry Collector** (`azuremonitor` exporter). The
  in-process exporter stays optional.
- **D-063 · P17** Quality status:
  - 426 backend tests on 3 storage backends; 97.3 % coverage; new critical modules added to the gate.
  - Studio: 30 tests, 80 % line coverage, logic modules ≥ 85 %.
  - `tools/check_all.sh` runs every gate.
  - The Studio was also verified in a **real headless Chromium** against a live host (screenshots).
- **D-064** New extension point: `ApiExtension` (Extend axis), through which features add routes or sub-apps
  (the MCP server uses it).
- **D-065 · Q-009** Tenancy is single-tenant per installation: isolation comes from the deployment (own
  database/schema, broker vhost, Keycloak realm or client), not from code.
- **D-066 · Q-002** There is no expression language. If one is added later, it plugs in as a new binding form
  (`{"$expr": …}`) without breaking definitions.

---

## Open questions / blockers for you

- **Q-003 (still open) — real infrastructure.** When you run `docker compose -f deploy/docker-compose.yml up
  --build`, please also try the Studio login via Keycloak (users anna/ben/dora/olga/alice/admin, password =
  username) and `pytest -m rabbitmq` with `DnA Workflows_TEST_RABBITMQ_URL`.
- **Q-010 — Cluster ownership.** With PostgreSQL several nodes can run safely: writes are fenced. Under heavy
  contention, two nodes may still *attempt* the same step and one commit is rejected, so work is done twice.
  Should the next iteration add **execution ownership leases** to remove that waste?
- **Q-011 — Roles and UX with a real analyst.** Please confirm the 8 default roles (viewer, analyst,
  reviewer, publisher, operator, approver, agent, admin). A short session with a domain analyst on the
  Studio would tell us whether to add **templates or wizards**, e.g. "new integration from a message to
  HTTP".
- **Q-012 — Keycloak realm management.** The shipped realm export is for development (passwords = user
  names, placeholder client secret). For production: **keycloak-config-cli**, Terraform, or manual?
- **Q-013 — Business calendars in forecasts.** Latency is wall-clock time. For human steps (approvals),
  working hours and weekends matter: "approved in 2 business days". Add calendar-aware forecasting?
- **Q-014 — Studio language.** English only, or also Polish (i18n) for your analysts?

---

## Progress log

- **P0** Kernel done: container, features, shells, config, hosting, clock, IDs. Tests in `tests/kernel`.
- **P1** Runtime core done: model, bindings, activities, registry, engine, router, outbox, timers,
  built-ins, builder, in-memory + SQLite stores. Tests in `tests/runtime` and `tests/persistence`.
  Every engine test runs on both backends. P1.12 (HTTP plugin) added.
- **P2** Messaging done: envelope/endpoints/transport contracts, bus, transactional send, consumer host
  with ingress holds, activities, providers (in-memory, RabbitMQ, Service Bus), conformance kit. Tests in
  `tests/messaging`, including two-node end-to-end scenarios.
- **P3** Operational lifecycle done: classifiers, retry budget, holds (6 scopes), auto-trip, park/resume,
  release strategies + coordinator, proposals. Tests in `tests/resilience`.
- **P4** Observability done: journal exporter and sinks, log correlation, Prometheus, OpenTelemetry (spans
  and logs), Azure Monitor. Tests in `tests/observability`, including one trace across two nodes.
- **P5** AI done (except P5.5 MCP, deferred: Q-004): MAF client registry, three activities, operations
  tools, triage agent, `ai-recommend` strategy, scripted test client. Tests in `tests/ai`.
- **P6** Hosting done: host and CLI, management API, order-integration example (demo + three shell
  flavours + fake ERP), deploy assets (Docker, compose Grafana stack, OTel collector, dashboards, Azure
  runbook).
- **P7** Gates: 233 passed / 2 skipped (real brokers, opt-in), total coverage 97.7 %, critical ≥ 87 %.

### Iteration 2
- **P8** Rename → DnA Workflows.
- **P9** Forecasting: timing capture, estimates, features, DuckDB history, estimator, graph forecast, live
  ETA, accuracy, API/MCP. Tests in `tests/forecasting`.
- **P10** Keycloak security (roles → permissions, JWKS rotation, service accounts). Realm export in
  `deploy/keycloak`.
- **P11** Collaborative authoring: drafts, review, four-eyes, publish, test runs, activity descriptors.
- **P12** DnA Workflows Studio (`studio/`): designer, data picker, estimates, undo editor, test runs, forecast
  preview, review/history, "My work", executions with ETA, operations, forecasts.
- **P13** MCP server: stdio + HTTP/OAuth2, role-filtered tools.
- **P14** Sub-workflows and saga compensation (engine, API, proposals, Studio).
- **P15** PostgreSQL provider (real PG 16 in tests); DuckDB benchmark; indexed in-memory store.
- **P16** Azure Monitor via the OpenTelemetry Collector.
- **P17** Gates: 426 backend tests (3 backends), 97.3 % coverage; Studio 30 tests / 80 % lines; all in
  `tools/check_all.sh`.
