"""Management API (plan items P6.2, P9.9, P10.2, P11, P12.6).

* Routes exist only for composed features (no holds without ``resilience``, no forecasts without
  ``forecasting``, ...). ``GET /capabilities`` and ``GET /me`` tell clients – the Studio, agents –
  what this shell offers and what the caller may do.
* Every route declares the **permission** it needs. Callers authenticate with a Keycloak bearer
  token (``security`` feature), an API key, or – in development without the security feature –
  anonymously with full access. With tokens the actor is the token's identity; ``X-Actor`` is only
  honoured for API keys and development mode.
* ``traceparent`` on starts and messages joins the caller's trace.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from DnA Workflows import __version__
from DnA Workflows.authoring import DraftConflict, DraftService, DraftStateError, TestRunner, TestRunRequest
from DnA Workflows.kernel.container import ServiceProvider
from DnA Workflows.kernel.features import FeatureCatalog
from DnA Workflows.kernel.shell import ShellInfo
from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore, OutboxStore
from DnA Workflows.resilience.holds import HoldService
from DnA Workflows.resilience.proposals import ProposalService, ProposalStateError, ProposalStatus
from DnA Workflows.runtime.activity import ActivityRegistry
from DnA Workflows.runtime.engine import WorkflowRuntime
from DnA Workflows.runtime.lifecycle import InvalidTransition
from DnA Workflows.runtime.models import ExecutionStatus, IntentStatus, WorkflowDefinition
from DnA Workflows.runtime.outbox import OutboxProcessor
from DnA Workflows.runtime.registry import DefinitionValidationError, WorkflowRegistry
from DnA Workflows.runtime.router import StimulusRouter
from DnA Workflows.security import AuthError, Authenticator, Principal, development_authenticator


class StartRequest(BaseModel):
    input: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    version: int | None = None
    wait: bool = True


class EventRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    message_id: str | None = None


class MessageRequest(BaseModel):
    type: str
    data: Any = None
    subject: str | None = None
    correlation_id: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)


class ForecastRequest(BaseModel):
    input: dict[str, Any] = Field(default_factory=dict)
    conditions: dict[str, Any] = Field(default_factory=dict)
    version: int | None = None


class PreviewRequest(BaseModel):
    definition: WorkflowDefinition
    input: dict[str, Any] = Field(default_factory=dict)


class HoldRequest(BaseModel):
    scope: str
    target: str = "*"
    reason: str
    release_strategy: str | None = None
    release_options: dict[str, Any] | None = None


class ReasonRequest(BaseModel):
    reason: str = "via api"


class DecisionRequest(BaseModel):
    note: str | None = None


class ProposalRequest(BaseModel):
    kind: str
    target: dict[str, Any]
    reason: str
    confidence: float | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)


class DraftSave(BaseModel):
    definition: WorkflowDefinition
    revision: int


class DraftStart(BaseModel):
    definition: WorkflowDefinition | None = None


class ReviewRequest(BaseModel):
    decision: Literal["approve", "request_changes"]
    note: str | None = None


class CommentRequest(BaseModel):
    text: str
    node: str | None = None


class ApiExtension:
    """Contributor (Extend axis): lets any feature add routes or mount sub-apps (e.g. MCP at /mcp)."""

    def __init__(self, apply: Any) -> None:
        self.apply = apply


def _dump(model: Any) -> Any:
    if isinstance(model, list):
        return [_dump(m) for m in model]
    return model.model_dump(mode="json") if hasattr(model, "model_dump") else model


class Guard:
    """Authentication + permission dependencies for routes."""

    def __init__(self, authenticator: Authenticator) -> None:
        self.authenticator = authenticator

    async def principal(self, authorization: str | None = Header(default=None),
                        x_api_key: str | None = Header(default=None),
                        x_actor: str | None = Header(default=None)) -> Principal:
        try:
            return await self.authenticator.authenticate(authorization, x_api_key, x_actor)
        except AuthError as exc:
            raise HTTPException(exc.status, str(exc), headers={"WWW-Authenticate": "Bearer"}) from exc

    def require(self, permission: str) -> Any:
        async def check(principal: Principal = Depends(self.principal)) -> Principal:
            if not principal.can(permission):
                raise HTTPException(403, f"missing permission '{permission}'")
            return principal
        return check


def create_app(provider: ServiceProvider, *, api_key: str | None = None, title: str = "DnA Workflows",
               studio_dir: str | None = None, studio_config: dict[str, Any] | None = None) -> FastAPI:
    app = FastAPI(title=title, version=__version__)
    guard = Guard(provider.get_optional(Authenticator) or development_authenticator(api_key))
    need = guard.require

    @app.exception_handler(LookupError)
    async def not_found(request: Request, exc: LookupError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=404)

    @app.exception_handler(InvalidTransition)
    async def conflict(request: Request, exc: InvalidTransition) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        info = provider.get(ShellInfo)
        return {"status": "ok", "shell": info.name, "version": __version__}

    api = APIRouter()
    runtime = provider.get(WorkflowRuntime)
    registry = provider.get(WorkflowRegistry)
    executions = provider.get(ExecutionStore)
    journal = provider.get(JournalStore)

    @api.get("/me")
    async def me(who: Principal = Depends(guard.principal)) -> dict[str, Any]:
        return _dump(who)

    @api.get("/capabilities")
    async def capabilities(_: Principal = Depends(guard.principal)) -> dict[str, Any]:
        info = provider.get(ShellInfo)
        catalog = FeatureCatalog.default()
        return {"shell": info.name, "version": __version__, "features_enabled": info.features,
                "features": [catalog.get(f).describe() for f in info.features if f in catalog],
                "activities": provider.get(ActivityRegistry).describe(),
                "workflow_schema": WorkflowDefinition.model_json_schema()}

    # -- workflows --------------------------------------------------------------------------------------
    @api.get("/workflows")
    async def list_workflows(_: Principal = Depends(need("workflows.read"))) -> list[dict[str, Any]]:
        return [d.to_json_dict() for d in registry.list()]

    @api.get("/workflows/{workflow_id}")
    async def get_workflow(workflow_id: str, version: int | None = None,
                           _: Principal = Depends(need("workflows.read"))) -> dict[str, Any]:
        return (await registry.get(workflow_id, version)).to_json_dict()

    @api.post("/workflows", status_code=201)
    async def publish(definition: WorkflowDefinition,
                      _: Principal = Depends(need("workflows.publish"))) -> dict[str, Any]:
        """Direct publish (automation/GitOps). People use drafts + review instead."""
        try:
            return (await registry.publish(definition)).to_json_dict()
        except DefinitionValidationError as exc:
            raise HTTPException(422, {"message": "invalid workflow", "problems": exc.problems}) from exc

    @api.post("/workflows/{workflow_id}/executions", status_code=201)
    async def start(workflow_id: str, body: StartRequest, traceparent: str | None = Header(default=None),
                    tracestate: str | None = Header(default=None),
                    _: Principal = Depends(need("executions.start"))) -> dict[str, Any]:
        ex = await runtime.start(workflow_id, input=body.input, version=body.version,
                                 correlation_id=body.correlation_id, traceparent=traceparent,
                                 tracestate=tracestate, run=body.wait)
        return _dump(ex)

    # -- executions -------------------------------------------------------------------------------------
    @api.get("/executions")
    async def list_executions(status: ExecutionStatus | None = None, workflow: str | None = None,
                              correlation_id: str | None = None, limit: int = Query(100, le=1000),
                              offset: int = 0, _: Principal = Depends(need("executions.read"))) -> list[dict[str, Any]]:
        return _dump(await executions.list(status=status, definition_id=workflow, correlation_id=correlation_id,
                                           limit=limit, offset=offset))

    @api.get("/executions/{execution_id}")
    async def get_execution(execution_id: str, _: Principal = Depends(need("executions.read"))) -> dict[str, Any]:
        return _dump(await runtime.get(execution_id))

    @api.get("/executions/{execution_id}/journal")
    async def execution_journal(execution_id: str, limit: int = Query(1000, le=10_000),
                                _: Principal = Depends(need("journal.read"))) -> list[dict[str, Any]]:
        await runtime.get(execution_id)
        return _dump(await journal.query(execution_id=execution_id, limit=limit))

    @api.post("/executions/{execution_id}/cancel")
    async def cancel(execution_id: str, body: ReasonRequest,
                     who: Principal = Depends(need("executions.operate"))) -> dict[str, Any]:
        return _dump(await runtime.cancel(execution_id, reason=body.reason, actor=who.name))

    @api.post("/executions/{execution_id}/compensate")
    async def compensate(execution_id: str, body: ReasonRequest,
                         who: Principal = Depends(need("executions.operate"))) -> dict[str, Any]:
        """Stop the run and undo its completed steps (saga compensation)."""
        return _dump(await runtime.compensate(execution_id, reason=body.reason, actor=who.name))

    @api.post("/executions/{execution_id}/retry")
    async def retry(execution_id: str, body: ReasonRequest,
                    who: Principal = Depends(need("executions.operate"))) -> dict[str, Any]:
        retried = await runtime.retry_faulted(execution_id, reason=body.reason, actor=who.name)
        return {"retried": retried, "execution": _dump(await runtime.get(execution_id))}

    @api.post("/executions/{execution_id}/work/{work_item_id}/resume")
    async def resume_work(execution_id: str, work_item_id: str, body: ReasonRequest,
                          who: Principal = Depends(need("executions.operate"))) -> dict[str, Any]:
        resumed = await runtime.resume_parked(execution_id, [work_item_id], reason=body.reason, actor=who.name)
        if not resumed:
            raise HTTPException(409, f"work item {work_item_id} is not parked")
        return {"resumed": resumed, "execution": _dump(await runtime.get(execution_id))}

    @api.post("/executions/{execution_id}/work/{work_item_id}/abandon")
    async def abandon_work(execution_id: str, work_item_id: str, body: ReasonRequest,
                           who: Principal = Depends(need("executions.operate"))) -> dict[str, Any]:
        return _dump(await runtime.abandon(execution_id, work_item_id, reason=body.reason, actor=who.name))

    @api.get("/parked")
    async def parked(dependency: str | None = None, hold_id: str | None = None,
                     _: Principal = Depends(need("holds.read"))) -> list[dict[str, str]]:
        return [{"execution_id": e, "work_item_id": w}
                for e, w in await executions.find_parked(dependency=dependency, hold_id=hold_id)]

    # -- stimuli / journal --------------------------------------------------------------------------------
    @api.post("/events/{name}")
    async def publish_event(name: str, body: EventRequest, traceparent: str | None = Header(default=None),
                            _: Principal = Depends(need("executions.start"))) -> dict[str, Any]:
        result = await provider.get(StimulusRouter).publish_event(
            name, body.payload, correlation_id=body.correlation_id, message_id=body.message_id,
            traceparent=traceparent)
        return _dump(result)

    @api.get("/journal")
    async def query_journal(execution_id: str | None = None, correlation_id: str | None = None,
                            trace_id: str | None = None, kind: list[str] | None = Query(default=None),
                            limit: int = Query(1000, le=10_000),
                            _: Principal = Depends(need("journal.read"))) -> list[dict[str, Any]]:
        return _dump(await journal.query(execution_id=execution_id, correlation_id=correlation_id,
                                         trace_id=trace_id, kinds=kind, limit=limit))

    # -- outbox -----------------------------------------------------------------------------------------
    @api.get("/outbox")
    async def outbox(status: IntentStatus | None = None, dependency: str | None = None,
                     limit: int = Query(100, le=1000),
                     _: Principal = Depends(need("executions.read"))) -> list[dict[str, Any]]:
        return _dump(await provider.get(OutboxStore).list(status=status, dependency=dependency, limit=limit))

    @api.post("/outbox/{intent_id}/redeliver")
    async def redeliver(intent_id: str, who: Principal = Depends(need("executions.operate"))) -> dict[str, Any]:
        if not await provider.get(OutboxProcessor).redeliver_failed(intent_id, actor=who.name):
            raise HTTPException(409, f"intent {intent_id} is not in failed state")
        return {"redelivery_requested": intent_id}

    _messaging_routes(api, provider, need)
    _forecast_routes(api, provider, need)
    _resilience_routes(api, provider, need)
    _authoring_routes(api, provider, need)
    _ai_routes(api, provider, need)
    app.include_router(api)
    _metrics_route(app, provider)
    for extension in provider.get_all(ApiExtension):
        extension.apply(app, provider)
    if studio_dir:
        _studio(app, Path(studio_dir), {"apiBase": "", "keycloak": None, **(studio_config or {})})
    return app


def _messaging_routes(api: APIRouter, provider: ServiceProvider, need: Any) -> None:
    from DnA Workflows.messaging.bus import MessageBus, UnknownEndpoint
    bus = provider.get_optional(MessageBus)
    if bus is None:
        return

    @api.post("/messages/{endpoint}", status_code=202)
    async def send_message(endpoint: str, body: MessageRequest, traceparent: str | None = Header(default=None),
                           _: Principal = Depends(need("messages.send"))) -> dict[str, Any]:
        """HTTP → broker bridge: publish to a logical endpoint (joins the caller's trace)."""
        from DnA Workflows.tracing import TraceContext
        try:
            envelope = bus.envelope(body.type, body.data, subject=body.subject, correlation_id=body.correlation_id,
                                    headers=body.headers, trace=TraceContext.parse(traceparent),
                                    source="DnA Workflows://api")
            await bus.send(endpoint, envelope)
        except UnknownEndpoint as exc:
            raise HTTPException(404, str(exc)) from exc
        return {"message_id": envelope.id, "endpoint": endpoint}

    @api.get("/messaging")
    async def messaging_topology(_: Principal = Depends(need("workflows.read"))) -> dict[str, Any]:
        from DnA Workflows.messaging.bus import describe_bus
        return describe_bus(bus)


def _forecast_routes(api: APIRouter, provider: ServiceProvider, need: Any) -> None:
    try:
        from DnA Workflows.forecasting.service import ForecastService
    except ImportError:  # pragma: no cover - forecasting extra not installed
        return
    forecasts = provider.get_optional(ForecastService)
    if forecasts is None:
        return
    read = need("forecasts.read")

    @api.post("/forecasts/workflows/{workflow_id}")
    async def forecast_workflow(workflow_id: str, body: ForecastRequest, _: Principal = Depends(read)) -> dict[str, Any]:
        """How long will a run take for this input? (estimate + history, with basis and P50/P90)"""
        return await forecasts.forecast_workflow(workflow_id, input=body.input, conditions=body.conditions or None,
                                                 version=body.version)

    @api.post("/forecasts/preview")
    async def forecast_preview(body: PreviewRequest, _: Principal = Depends(read)) -> dict[str, Any]:
        """Forecast an unpublished (draft) definition – used by the designer while editing."""
        return await forecasts.forecast_workflow(body.definition.id, input=body.input, definition=body.definition)

    @api.get("/forecasts/executions/{execution_id}")
    async def forecast_execution(execution_id: str, _: Principal = Depends(read)) -> dict[str, Any]:
        """Live ETA of a running execution, including parked work and waits."""
        return await forecasts.forecast_execution(execution_id)

    @api.get("/forecasts/workflows/{workflow_id}/stats")
    async def forecast_stats(workflow_id: str, _: Principal = Depends(read)) -> list[dict[str, Any]]:
        return await forecasts.node_stats(workflow_id)

    @api.get("/forecasts/accuracy")
    async def forecast_accuracy(workflow: str | None = None, _: Principal = Depends(read)) -> list[dict[str, Any]]:
        return await forecasts.accuracy(workflow)


def _resilience_routes(api: APIRouter, provider: ServiceProvider, need: Any) -> None:
    holds = provider.get_optional(HoldService)
    proposals = provider.get_optional(ProposalService)
    if holds is None or proposals is None:
        return

    @api.get("/holds")
    async def list_holds(active_only: bool = False, _: Principal = Depends(need("holds.read"))) -> list[dict[str, Any]]:
        return _dump(await holds.list(active_only=active_only))

    @api.get("/holds/{hold_id}")
    async def get_hold(hold_id: str, _: Principal = Depends(need("holds.read"))) -> dict[str, Any]:
        return _dump(await holds.get(hold_id))

    @api.post("/holds", status_code=201)
    async def place_hold(body: HoldRequest, who: Principal = Depends(need("holds.manage"))) -> dict[str, Any]:
        try:
            hold = await holds.place(body.scope, body.target, body.reason, actor=who.name, source="api",
                                     release_strategy=body.release_strategy, release_options=body.release_options)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        return _dump(hold)

    @api.post("/holds/{hold_id}/release")
    async def release_hold(hold_id: str, body: ReasonRequest,
                           who: Principal = Depends(need("holds.manage"))) -> dict[str, Any]:
        return _dump(await holds.release(hold_id, actor=who.name, reason=body.reason))

    @api.get("/proposals")
    async def list_proposals(status: ProposalStatus | None = None,
                             _: Principal = Depends(need("proposals.read"))) -> list[dict[str, Any]]:
        return _dump(await proposals.list(status=status))

    @api.post("/proposals", status_code=201)
    async def file_proposal(body: ProposalRequest, who: Principal = Depends(need("proposals.propose"))) -> dict[str, Any]:
        """External systems and agents may *propose*; only an approval executes anything."""
        try:
            return _dump(await proposals.propose(body.kind, body.target, body.reason, proposed_by=who.name,
                                                 confidence=body.confidence, evidence=body.evidence))
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @api.post("/proposals/{proposal_id}/approve")
    async def approve(proposal_id: str, body: DecisionRequest,
                      who: Principal = Depends(need("proposals.approve"))) -> dict[str, Any]:
        try:
            return _dump(await proposals.approve(proposal_id, actor=who.name, note=body.note))
        except ProposalStateError as exc:
            raise HTTPException(409, str(exc)) from exc

    @api.post("/proposals/{proposal_id}/reject")
    async def reject(proposal_id: str, body: DecisionRequest,
                     who: Principal = Depends(need("proposals.approve"))) -> dict[str, Any]:
        try:
            return _dump(await proposals.reject(proposal_id, actor=who.name, note=body.note))
        except ProposalStateError as exc:
            raise HTTPException(409, str(exc)) from exc


def _authoring_routes(api: APIRouter, provider: ServiceProvider, need: Any) -> None:
    drafts = provider.get_optional(DraftService)
    runner = provider.get_optional(TestRunner)
    if drafts is None or runner is None:
        return

    async def guarded(call: Any) -> dict[str, Any]:
        try:
            return _dump(await call)
        except DraftConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except DraftStateError as exc:
            raise HTTPException(422, str(exc)) from exc

    @api.get("/drafts")
    async def list_drafts(_: Principal = Depends(need("workflows.read"))) -> list[dict[str, Any]]:
        return _dump(await drafts.list())

    @api.get("/drafts/{workflow_id}")
    async def get_draft(workflow_id: str, _: Principal = Depends(need("workflows.read"))) -> dict[str, Any]:
        return _dump(await drafts.get(workflow_id))

    @api.post("/drafts/{workflow_id}", status_code=201)
    async def start_draft(workflow_id: str, body: DraftStart,
                          who: Principal = Depends(need("workflows.design"))) -> dict[str, Any]:
        return await guarded(drafts.start(workflow_id, who.name, body.definition))

    @api.put("/drafts/{workflow_id}")
    async def save_draft(workflow_id: str, body: DraftSave,
                         who: Principal = Depends(need("workflows.design"))) -> dict[str, Any]:
        return await guarded(drafts.save(workflow_id, body.definition, who.name, body.revision))

    @api.post("/drafts/{workflow_id}/submit")
    async def submit_draft(workflow_id: str, who: Principal = Depends(need("workflows.design"))) -> dict[str, Any]:
        return await guarded(drafts.submit(workflow_id, who.name))

    @api.post("/drafts/{workflow_id}/review")
    async def review_draft(workflow_id: str, body: ReviewRequest,
                           who: Principal = Depends(need("workflows.review"))) -> dict[str, Any]:
        return await guarded(drafts.review(workflow_id, who.name, body.decision, body.note))

    @api.post("/drafts/{workflow_id}/publish")
    async def publish_draft(workflow_id: str, who: Principal = Depends(need("workflows.publish"))) -> dict[str, Any]:
        return await guarded(drafts.publish(workflow_id, who.name))

    @api.post("/drafts/{workflow_id}/comments")
    async def comment_draft(workflow_id: str, body: CommentRequest,
                            who: Principal = Depends(need("workflows.read"))) -> dict[str, Any]:
        return await guarded(drafts.comment(workflow_id, who.name, body.text, body.node))

    @api.delete("/drafts/{workflow_id}", status_code=204)
    async def discard_draft(workflow_id: str, who: Principal = Depends(need("workflows.design"))) -> None:
        await drafts.discard(workflow_id, who.name)

    @api.post("/drafts/{workflow_id}/test-run")
    async def test_run(workflow_id: str, body: TestRunRequest,
                       _: Principal = Depends(need("workflows.design"))) -> dict[str, Any]:
        draft = await drafts.get(workflow_id)
        return await runner.run(draft.definition, body)

    @api.get("/workflows/{workflow_id}/versions")
    async def versions(workflow_id: str, _: Principal = Depends(need("workflows.read"))) -> list[dict[str, Any]]:
        return await drafts.versions(workflow_id)


def _ai_routes(api: APIRouter, provider: ServiceProvider, need: Any) -> None:
    try:
        from DnA Workflows.ai.operations import TriageError, TriageService
    except ImportError:  # pragma: no cover - ai extra not installed
        return
    triage = provider.get_optional(TriageService)
    if triage is None:
        return

    @api.post("/ai/triage/{hold_id}")
    async def run_triage(hold_id: str, propose: bool = True,
                         _: Principal = Depends(need("ai.triage"))) -> dict[str, Any]:
        try:
            return _dump(await triage.analyze(hold_id, propose=propose))
        except TriageError as exc:
            raise HTTPException(502, str(exc)) from exc

    @api.get("/ai/triage")
    async def triage_reports(hold_id: str | None = None,
                             _: Principal = Depends(need("holds.read"))) -> list[dict[str, Any]]:
        return _dump(await triage.reports(hold_id))


def _metrics_route(app: FastAPI, provider: ServiceProvider) -> None:
    try:
        from DnA Workflows.observability.prometheus import DnA WorkflowsMetrics
    except ImportError:  # pragma: no cover - prometheus extra not installed
        return
    metrics = provider.get_optional(DnA WorkflowsMetrics)
    if metrics is None:
        return

    @app.get("/metrics", response_class=PlainTextResponse)
    async def render_metrics() -> PlainTextResponse:
        return PlainTextResponse(metrics.render().decode(), media_type="text/plain; version=0.0.4")


def _studio(app: FastAPI, root: Path, config: dict[str, Any]) -> None:
    """Serve the built Studio (single-page app) under /studio, with runtime configuration."""
    index = root / "index.html"

    @app.get("/studio/config.json", include_in_schema=False)
    async def studio_config() -> dict[str, Any]:
        return config

    @app.get("/studio/{path:path}", include_in_schema=False)
    async def studio(path: str) -> FileResponse:
        candidate = (root / path).resolve()
        if path and candidate.is_file() and root.resolve() in candidate.parents:
            return FileResponse(candidate)
        if not index.is_file():
            raise HTTPException(404, "Studio is not built (npm run build in studio/)")
        return FileResponse(index)
