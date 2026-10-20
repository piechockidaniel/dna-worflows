"""MCP server (plan phase P13, Q-004).

Exposes DnA Workflows to AI agents through the Model Context Protocol, on two transports:

* **stdio** – a local agent starts ``python -m DnA Workflows mcp --config shell.yaml`` as a subprocess.
  The caller is trusted as the local user; its permissions come from ``mcp.stdio_permissions``.
* **streamable HTTP** – mounted at ``/mcp`` on the API. Protected by **OAuth2 bearer tokens from
  Keycloak**: machine clients use the *client credentials* grant (service accounts), the server
  validates the JWT exactly like the REST API and publishes RFC 9728 protected-resource metadata
  pointing to the realm, so MCP clients can discover where to get tokens.

Every tool declares a DnA Workflows permission. ``tools/list`` only shows the tools the caller may use and
calls are re-checked – an agent with the ``DnA Workflows-agent`` role can read, start workflows and
*propose*, but never approve.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from DnA Workflows.kernel.container import ServiceCollection, ServiceProvider
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import HostedService
from DnA Workflows.operations import OperationsTools
from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore
from DnA Workflows.resilience.holds import HoldService
from DnA Workflows.resilience.proposals import ProposalService
from DnA Workflows.resilience.release import ProbeRegistry
from DnA Workflows.runtime.activity import ActivityRegistry
from DnA Workflows.runtime.engine import WorkflowRuntime
from DnA Workflows.runtime.models import ExecutionStatus
from DnA Workflows.runtime.registry import WorkflowRegistry
from DnA Workflows.security import DEFAULT_ROLES, AuthError, Authenticator, Principal

log = logging.getLogger(__name__)

INSTRUCTIONS = """DnA Workflows runs integration workflows. Work that cannot proceed is never dropped: it is
parked behind a *hold* (a frozen dependency) until released. You can inspect workflows, executions,
their history (journal), holds and parked work, get duration forecasts, start workflows, and *propose*
releases or resumptions – a human approves proposals. Correlation ids are business keys (e.g. order-42)."""


class McpPermissionError(ToolError):
    """Reported to the agent as a tool error with a clear message."""


class KeycloakTokenVerifier:
    """Adapter: DnA Workflows's JWT validation → MCP ``TokenVerifier``. Permissions become scopes."""

    def __init__(self, authenticator: Authenticator) -> None:
        self.authenticator = authenticator

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            principal = await self.authenticator.authenticate(f"Bearer {token}")
        except AuthError as exc:
            log.info("MCP token rejected: %s", exc)
            return None
        return AccessToken(token=token, client_id=principal.client_id or principal.name,
                           scopes=list(principal.permissions), subject=principal.subject,
                           claims={"name": principal.name, "kind": principal.kind, "roles": principal.roles})


class DnA WorkflowsMcp:
    def __init__(self, provider: ServiceProvider, *, stdio_permissions: list[str],
                 authenticator: Authenticator | None = None, issuer: str | None = None,
                 resource_url: str | None = None) -> None:
        self.provider = provider
        self.stdio_permissions = stdio_permissions
        self.authenticator = authenticator
        self.issuer = issuer
        self.resource_url = resource_url
        self.permissions: dict[str, str] = {}
        self._server: MCPServer | None = None
        self._http_app: Any = None

    # -- identity -------------------------------------------------------------------------------
    def principal(self) -> Principal:
        token = get_access_token()
        if token is not None:
            claims = token.claims or {}
            return Principal(subject=token.subject or token.client_id, name=str(claims.get("name", token.client_id)),
                             kind=claims.get("kind", "service"), roles=list(claims.get("roles", [])),
                             permissions=list(token.scopes), client_id=token.client_id)
        return Principal(subject="mcp-stdio", name="mcp-stdio", kind="service", permissions=self.stdio_permissions)

    def require(self, permission: str) -> Principal:
        principal = self.principal()
        if not principal.can(permission):
            raise McpPermissionError(f"'{principal.name}' lacks permission '{permission}'")
        return principal

    # -- server ---------------------------------------------------------------------------------
    @property
    def server(self) -> MCPServer:
        if self._server is None:
            self._server = self._build()
        return self._server

    def _build(self) -> MCPServer:
        kwargs: dict[str, Any] = {}
        if self.authenticator is not None and self.issuer:
            kwargs["token_verifier"] = KeycloakTokenVerifier(self.authenticator)
            # the audience is validated by DnA Workflows's JWT validation (security.audience), not by the SDK
            kwargs["auth"] = AuthSettings(issuer_url=self.issuer, resource_server_url=self.resource_url,
                                          validate_token_resource=False)
        server = MCPServer(name="DnA Workflows", instructions=INSTRUCTIONS, middleware=[self._filter_tools], **kwargs)
        for name, permission, fn in self._tools():
            self.permissions[name] = permission
            server.add_tool(fn, name=name, description=f"{(fn.__doc__ or '').strip()} [permission: {permission}]")
        return server

    async def _filter_tools(self, ctx: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
        """``tools/list`` shows only the tools the caller may use (calls are re-checked anyway)."""
        result = await call_next(ctx)
        if ctx.method != "tools/list" or result is None:
            return result
        principal = self.principal()

        def allowed(name: str) -> bool:
            return principal.can(self.permissions.get(name, "admin"))
        if isinstance(result, dict):
            return {**result, "tools": [t for t in result.get("tools", []) if allowed(t.get("name", ""))]}
        tools = [t for t in result.tools if allowed(t.name)]
        return replace(result, tools=tools) if hasattr(result, "__dataclass_fields__") else \
            result.model_copy(update={"tools": tools})

    # -- tools ----------------------------------------------------------------------------------
    def _ops(self) -> OperationsTools:
        p = self.provider
        return OperationsTools(p.get(HoldService), p.get(ExecutionStore), p.get(JournalStore), p.get(ProposalService),
                               p.get(ProbeRegistry), p.get(WorkflowRegistry))

    def _tools(self) -> list[tuple[str, str, Callable[..., Any]]]:
        p = self.provider
        ops = self._ops()
        runtime = p.get(WorkflowRuntime)
        registry = p.get(WorkflowRegistry)
        executions = p.get(ExecutionStore)

        async def list_workflows() -> list[dict[str, Any]]:
            """List published workflows with their steps."""
            self.require("workflows.read")
            return await ops.list_workflows()

        async def describe_workflow(workflow_id: str) -> dict[str, Any]:
            """Full definition of a published workflow (steps, connections, inputs, estimates)."""
            self.require("workflows.read")
            return (await registry.get(workflow_id)).to_json_dict()

        async def describe_activities() -> list[dict[str, Any]]:
            """Available activity types with input JSON Schemas and outcomes."""
            self.require("workflows.read")
            return p.get(ActivityRegistry).describe()

        async def start_workflow(workflow_id: str, input: dict[str, Any] | None = None,
                                 correlation_id: str | None = None) -> dict[str, Any]:
            """Start a workflow run with the given input and business correlation id."""
            self.require("executions.start")
            ex = await runtime.start(workflow_id, input=input or {}, correlation_id=correlation_id)
            return {"execution_id": ex.id, "status": ex.status.value, "correlation_id": ex.correlation_id}

        async def get_execution(execution_id: str) -> dict[str, Any]:
            """Status, open work and waits of one workflow run."""
            self.require("executions.read")
            return await ops.get_execution(execution_id)

        async def find_executions(correlation_id: str | None = None, status: str | None = None,
                                  workflow_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
            """Find workflow runs by business key (correlation id), status or workflow."""
            self.require("executions.read")
            found = await executions.list(status=ExecutionStatus(status) if status else None,
                                          definition_id=workflow_id, correlation_id=correlation_id,
                                          limit=min(limit, 100))
            return [{"execution_id": e.id, "workflow": e.definition_id, "status": e.status.value,
                     "correlation_id": e.correlation_id} for e in found]

        async def get_journal(execution_id: str | None = None, correlation_id: str | None = None,
                              limit: int = 50) -> list[dict[str, Any]]:
            """History of a run or of a whole business transaction (correlation id)."""
            self.require("journal.read")
            return await ops.get_journal(execution_id, correlation_id, limit)

        async def list_holds(active_only: bool = True) -> list[dict[str, Any]]:
            """Frozen areas of the system (holds) and how they will be released."""
            self.require("holds.read")
            return await ops.list_holds(active_only)

        async def list_parked_work(dependency: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
            """Work that is parked behind holds or after exhausting retries."""
            self.require("holds.read")
            return await ops.list_parked_work(dependency=dependency, limit=limit)

        async def check_dependency(dependency: str) -> dict[str, Any]:
            """Run the health probe of a dependency such as 'http:erp' or 'rabbitmq:orders'."""
            self.require("holds.read")
            return await ops.check_dependency(dependency)

        async def propose_release(hold_id: str, reason: str, confidence: float) -> dict[str, Any]:
            """Propose releasing a hold. A human approves; nothing is released by this call."""
            who = self.require("proposals.propose")
            proposal = await p.get(ProposalService).propose("hold.release", {"hold_id": hold_id}, reason,
                                                            proposed_by=f"mcp:{who.name}", confidence=confidence)
            return {"proposal_id": proposal.id, "status": proposal.status.value}

        async def propose_resume(execution_id: str, work_item_id: str, reason: str,
                                 confidence: float) -> dict[str, Any]:
            """Propose resuming one parked work item. A human approves."""
            who = self.require("proposals.propose")
            proposal = await p.get(ProposalService).propose(
                "work.resume", {"execution_id": execution_id, "work_item_id": work_item_id}, reason,
                proposed_by=f"mcp:{who.name}", confidence=confidence)
            return {"proposal_id": proposal.id, "status": proposal.status.value}

        tools: list[tuple[str, str, Callable[..., Any]]] = [
            ("list_workflows", "workflows.read", list_workflows),
            ("describe_workflow", "workflows.read", describe_workflow),
            ("describe_activities", "workflows.read", describe_activities),
            ("start_workflow", "executions.start", start_workflow),
            ("get_execution", "executions.read", get_execution),
            ("find_executions", "executions.read", find_executions),
            ("get_journal", "journal.read", get_journal),
            ("list_holds", "holds.read", list_holds),
            ("list_parked_work", "holds.read", list_parked_work),
            ("check_dependency", "holds.read", check_dependency),
            ("propose_release", "proposals.propose", propose_release),
            ("propose_resume", "proposals.propose", propose_resume),
        ]
        tools.extend(self._forecast_tools())
        return tools

    def _forecast_tools(self) -> list[tuple[str, str, Callable[..., Any]]]:
        try:
            from DnA Workflows.forecasting.service import ForecastService
        except ImportError:  # pragma: no cover - forecasting extra not installed
            return []
        forecasts = self.provider.get_optional(ForecastService)
        if forecasts is None:
            return []

        async def forecast_workflow(workflow_id: str, input: dict[str, Any] | None = None) -> dict[str, Any]:
            """Expected duration of a new run for this input (P50/P90, basis, per step)."""
            self.require("forecasts.read")
            return await forecasts.forecast_workflow(workflow_id, input=input or {})

        async def forecast_execution(execution_id: str) -> dict[str, Any]:
            """Live ETA of a running workflow, including waits and parked work."""
            self.require("forecasts.read")
            return await forecasts.forecast_execution(execution_id)

        return [("forecast_workflow", "forecasts.read", forecast_workflow),
                ("forecast_execution", "forecasts.read", forecast_execution)]

    # -- transports -----------------------------------------------------------------------------
    def http_app(self) -> Any:
        """The streamable-HTTP ASGI app (built once: it also creates the session manager)."""
        if self._http_app is None:
            self._http_app = self.server.streamable_http_app(streamable_http_path="/", stateless_http=True,
                                                             json_response=True, host="0.0.0.0")
        return self._http_app

    async def run_stdio(self) -> None:
        await self.server.run_stdio_async()


class McpSessionService:
    """Keeps the streamable-HTTP session manager running while the shell runs.

    The manager's context is entered and exited by the shell's lifecycle task itself: anyio cancel
    scopes must be left from the task that entered them (a helper task here hangs shutdown).
    """

    def __init__(self, mcp: DnA WorkflowsMcp) -> None:
        self.mcp = mcp
        self._stack: contextlib.AsyncExitStack | None = None

    async def start(self) -> None:
        if self._stack is None:
            self.mcp.http_app()
            stack = contextlib.AsyncExitStack()
            await stack.enter_async_context(self.mcp.server.session_manager.run())
            self._stack = stack

    async def stop(self) -> None:
        stack, self._stack = self._stack, None
        if stack is not None:
            await stack.aclose()


class McpOptions(FeatureOptions):
    #: Mount the streamable-HTTP transport on the API feature at this path (None = stdio only).
    http_path: str | None = "/mcp"
    #: Public URL of the MCP endpoint (RFC 9728 resource metadata), e.g. https://DnA Workflows.example/mcp
    resource_url: str | None = None
    #: Permissions of the local stdio caller (default: the ``DnA Workflows-agent`` role).
    stdio_permissions: list[str] = Field(default_factory=lambda: sorted(DEFAULT_ROLES["DnA Workflows-agent"]))


class McpFeature(Feature):
    """MCP server for AI agents (stdio + streamable HTTP with Keycloak OAuth2)."""

    name = "mcp"
    depends_on = ("runtime", "resilience")
    Options = McpOptions

    def configure(self, services: ServiceCollection) -> None:
        o: McpOptions = self.options  # type: ignore[assignment]

        def build(p: ServiceProvider) -> DnA WorkflowsMcp:
            authenticator = p.get_optional(Authenticator)
            issuer = None
            if authenticator is not None and authenticator.validator is not None:
                issuer = authenticator.validator.jwks.issuer
            return DnA WorkflowsMcp(p, stdio_permissions=list(o.stdio_permissions), authenticator=authenticator,
                               issuer=issuer, resource_url=o.resource_url)
        services.add(DnA WorkflowsMcp, build)
        if o.http_path:
            from DnA Workflows.api.app import ApiExtension
            path = o.http_path

            def mount(app: Any, p: ServiceProvider) -> None:
                app.mount(path, p.get(DnA WorkflowsMcp).http_app())
            services.add_contributor(ApiExtension, ApiExtension(mount), key="mcp")
            services.add_contributor(HostedService, lambda p: McpSessionService(p.get(DnA WorkflowsMcp)), key="mcp.http")
