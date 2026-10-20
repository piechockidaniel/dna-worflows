"""HTTP integration plugin (plan item P1.12 – added during implementation, see CHANGELOG D-036).

* ``HttpRequest`` activity. Dependency key ``http:<name>`` for named dependencies, otherwise
  ``http:<host>``. HTTP errors raise :class:`HttpStatusError` which the status-code classifier
  maps (429 → throttled honouring Retry-After, 502/503/504 → dependency unavailable, 4xx → permanent).
* Named dependencies centralise base URLs, headers and health probes::

      http:
        dependencies:
          erp: {base_url: "https://erp.internal/api", headers: {Authorization: "Bearer ${ERP_TOKEN}"},
                health_url: "https://erp.internal/health"}

  A workflow then calls ``{"dependency": "erp", "path": "/orders"}``; the ``http:erp`` hold can be
  auto-released by the built-in probe (``probe-auto``/``probe-recommend`` strategies).
"""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel, ConfigDict, Field

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.resilience.feature import add_health_probe, add_release_rule
from DnA Workflows.resilience.release import ProbeResult, ReleaseRule
from DnA Workflows.runtime.activity import Activity, ActivityContext, ActivityFault, ActivityInputs, ActivityResult, Done
from DnA Workflows.runtime.feature import add_activity
from DnA Workflows.runtime.models import FaultKind, NodeSpec
from DnA Workflows.tracing import current_correlation_id, current_trace


class HttpDependency(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_url: str
    headers: dict[str, str] = Field(default_factory=dict)
    timeout: float = 30.0
    health_url: str | None = None
    #: Status codes the probe accepts as healthy.
    healthy_statuses: list[int] = Field(default_factory=lambda: [200, 204])


class HttpStatusError(Exception):
    """Non-success HTTP response. Carries ``status_code``/``headers`` for the classifier."""

    def __init__(self, status_code: int, reason: str, url: str, headers: dict[str, str], body: str) -> None:
        super().__init__(f"HTTP {status_code} {reason} from {url}: {body[:300]}")
        self.status_code = status_code
        self.headers = headers
        self.url = url


class HttpClientProvider:
    """Override axis: supplies the ``httpx.AsyncClient`` (tests inject a ``MockTransport``)."""

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(transport=self._transport, follow_redirects=True)
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


class HttpDependencies:
    def __init__(self, dependencies: dict[str, HttpDependency]) -> None:
        self.items = dependencies


class HttpRequest(Activity):
    """Call an HTTP endpoint. Output: ``{"status", "headers", "body"}`` (JSON bodies are parsed)."""

    type_name = "HttpRequest"
    output_schema = {"type": "object", "properties": {"status": {"type": "integer"}, "headers": {"type": "object"}, "body": {"type": "object"}}}
    category = "Integration"

    class Inputs(ActivityInputs):
        method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] = "GET"
        url: str | None = None
        dependency: str | None = None        # named dependency (base URL, headers, probe)
        path: str = ""
        json_body: Any = None
        params: dict[str, Any] = {}
        headers: dict[str, str] = {}
        timeout: float | None = None
        #: Statuses treated as success in addition to 2xx (e.g. [404] for "not found is fine").
        accept_statuses: list[int] = []

    def __init__(self, http: HttpClientProvider, dependencies: HttpDependencies) -> None:
        self.http = http
        self.dependencies = dependencies

    def _target(self, inputs: Any) -> tuple[str, dict[str, str], float]:
        if inputs.dependency:
            dep = self.dependencies.items.get(inputs.dependency)
            if dep is None:
                raise ActivityFault(f"Unknown HTTP dependency '{inputs.dependency}'", FaultKind.PERMANENT)
            return dep.base_url.rstrip("/") + inputs.path, dict(dep.headers), dep.timeout
        if not inputs.url:
            raise ActivityFault("HttpRequest needs 'url' or 'dependency'", FaultKind.PERMANENT)
        return inputs.url + inputs.path, {}, 30.0

    def dependency(self, inputs: Any, node: NodeSpec) -> str | None:
        if inputs.dependency:
            return f"http:{inputs.dependency}"
        return f"http:{urlparse(inputs.url or '').netloc or 'unknown'}"

    async def execute(self, context: ActivityContext) -> ActivityResult:
        i = context.inputs
        url, headers, timeout = self._target(i)
        headers.update(i.headers)
        trace = current_trace() or context.trace
        headers.setdefault("traceparent", trace.traceparent)   # downstream services join the trace
        correlation = current_correlation_id() or context.correlation_id
        if correlation:
            headers.setdefault("x-correlation-id", correlation)
        response = await self.http.client().request(
            i.method, url, json=i.json_body, params=i.params or None, headers=headers, timeout=i.timeout or timeout)
        if response.is_success or response.status_code in i.accept_statuses:
            body: Any
            try:
                body = response.json() if response.content else None
            except ValueError:
                body = response.text
            return Done(output={"status": response.status_code, "headers": dict(response.headers), "body": body})
        raise HttpStatusError(response.status_code, response.reason_phrase, url, dict(response.headers),
                              response.text)


class HttpHealthProbe:
    """Probes ``http:<name>`` dependencies that declare a ``health_url``."""

    def __init__(self, http: HttpClientProvider, dependencies: HttpDependencies) -> None:
        self.http = http
        self.dependencies = dependencies

    def supports(self, dependency: str) -> bool:
        kind, _, name = dependency.partition(":")
        dep = self.dependencies.items.get(name)
        return kind == "http" and dep is not None and dep.health_url is not None

    async def check(self, dependency: str) -> ProbeResult:
        dep = self.dependencies.items[dependency.partition(":")[2]]
        try:
            response = await self.http.client().get(dep.health_url, headers=dep.headers, timeout=min(dep.timeout, 10))
        except httpx.HTTPError as exc:
            return ProbeResult(False, f"{type(exc).__name__}: {exc}")
        return ProbeResult(response.status_code in dep.healthy_statuses, f"HTTP {response.status_code}")


class HttpErrorClassifier:
    """httpx transport errors: connect/timeouts mean the dependency is unavailable."""

    def classify(self, error: BaseException, dependency: str | None) -> Any:
        from DnA Workflows.resilience.classification import fault
        if isinstance(error, (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout,
                              httpx.PoolTimeout, httpx.RemoteProtocolError, httpx.NetworkError)):
            return fault(FaultKind.DEPENDENCY_UNAVAILABLE, error, dependency)
        if isinstance(error, (httpx.InvalidURL, httpx.UnsupportedProtocol)):
            return fault(FaultKind.PERMANENT, error, dependency)
        return None


class HttpOptions(FeatureOptions):
    dependencies: dict[str, HttpDependency] = Field(default_factory=dict)
    #: Release strategy contributed for ``http:<name>`` holds of dependencies with a health_url.
    release_strategy: str | None = "probe-auto"
    release_options: dict[str, Any] = Field(default_factory=lambda: {"successes_required": 2, "interval": 30})


class HttpFeature(Feature):
    """HTTP activity, named HTTP dependencies, health probes and error classification."""

    name = "http"
    depends_on = ("runtime",)
    Options = HttpOptions

    def configure(self, services: ServiceCollection) -> None:
        o: HttpOptions = self.options  # type: ignore[assignment]
        services.try_add(HttpClientProvider, lambda p: HttpClientProvider())
        services.add(HttpDependencies, lambda p: HttpDependencies(dict(o.dependencies)))
        add_activity(services, HttpRequest)
        from DnA Workflows.runtime.contracts import FaultClassifier
        services.add_contributor(FaultClassifier, HttpErrorClassifier, key="http")
        add_health_probe(services, lambda p: HttpHealthProbe(p.get(HttpClientProvider), p.get(HttpDependencies)),
                         key="http")
        if o.release_strategy:
            for name, dep in o.dependencies.items():
                if dep.health_url:
                    add_release_rule(services, ReleaseRule(f"http:{name}", o.release_strategy, dict(o.release_options)))
