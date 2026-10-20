import httpx
import pytest

from DnA Workflows.activities.http import HttpClientProvider, HttpErrorClassifier, HttpHealthProbe
from DnA Workflows.kernel import ManualClock
from DnA Workflows.resilience import HoldService, ReleaseCoordinator
from DnA Workflows.resilience.release import ProbeRegistry
from DnA Workflows.runtime import ExecutionStatus, TimerService, WorkflowBuilder, WorkflowRegistry, WorkflowRuntime
from DnA Workflows.runtime.models import FaultKind


class Api:
    def __init__(self):
        self.status = 200
        self.requests = []
        self.raise_error = None
        self.headers = {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.raise_error is not None:
            raise self.raise_error
        if request.url.path == "/health":
            return httpx.Response(self.status)
        if request.url.path == "/text":
            return httpx.Response(200, text="plain")
        if request.url.path == "/empty":
            return httpx.Response(204)
        return httpx.Response(self.status, json={"echo": request.url.path}, headers=self.headers)


@pytest.fixture
def env(make_shell):
    def build(**http):
        api = Api()
        clock = ManualClock()
        cfg = {"dependencies": {"erp": {"base_url": "http://erp.local/api", "headers": {"Authorization": "Bearer t"},
                                        "health_url": "http://erp.local/health"}}}
        cfg.update(http)
        shell = make_shell({"http": cfg, "resilience": {
            "retry": {"dependency_unavailable": {"max_attempts": 2, "jitter": 0}}}}, clock=clock,
            configure=lambda s: s.add_instance(HttpClientProvider, HttpClientProvider(httpx.MockTransport(api.handler))))
        return shell, api, clock
    return build


async def run(shell, **inputs):
    await shell.get(WorkflowRegistry).publish(WorkflowBuilder("call").add("req", "HttpRequest", **inputs).build())
    return await shell.get(WorkflowRuntime).start("call", correlation_id="corr-1")


async def test_named_dependency_request_propagates_trace_and_correlation(env):
    shell, api, _ = env()
    ex = await run(shell, method="POST", dependency="erp", path="/orders", json_body={"id": 1}, params={"x": 1},
                   headers={"X-Extra": "1"})
    assert ex.status is ExecutionStatus.COMPLETED
    out = ex.outputs["req"]
    assert out["status"] == 200 and out["body"] == {"echo": "/api/orders"}
    req = api.requests[0]
    assert str(req.url) == "http://erp.local/api/orders?x=1"
    assert req.headers["authorization"] == "Bearer t" and req.headers["x-extra"] == "1"
    assert req.headers["x-correlation-id"] == "corr-1"
    assert req.headers["traceparent"].split("-")[1] == ex.trace_id
    assert ex.work == [] and (await shell.get(WorkflowRuntime).get(ex.id)).status is ExecutionStatus.COMPLETED


async def test_plain_url_text_and_empty_bodies_and_accepted_status(env):
    shell, api, _ = env()
    assert (await run(shell, url="http://other.local/text")).outputs["req"]["body"] == "plain"
    assert (await run(shell, url="http://other.local/empty")).outputs["req"]["body"] is None
    api.status = 404
    ex = await run(shell, url="http://other.local/missing", accept_statuses=[404])
    assert ex.outputs["req"]["status"] == 404


async def test_http_errors_are_classified(env):
    shell, api, clock = env()
    api.status = 400
    ex = await run(shell, dependency="erp", path="/bad")
    assert ex.status is ExecutionStatus.FAULTED and ex.fault.kind is FaultKind.PERMANENT
    assert "HTTP 400" in ex.fault.message

    api.status = 429
    api.headers = {"Retry-After": "120"}
    ex = await run(shell, dependency="erp")
    item = ex.work[0]
    assert item.last_fault.kind is FaultKind.THROTTLED and (item.not_before - clock.now()).total_seconds() >= 120

    api.raise_error = httpx.ConnectError("refused")
    ex = await run(shell, dependency="erp")
    assert ex.work[0].last_fault.kind is FaultKind.DEPENDENCY_UNAVAILABLE
    assert ex.work[0].dependency == "http:erp"


async def test_unknown_dependency_and_missing_url_are_permanent(env):
    shell, _, _ = env()
    assert "Unknown HTTP dependency" in (await run(shell, dependency="nope")).fault.message
    assert "needs 'url'" in (await run(shell)).fault.message


async def test_outage_trip_probe_release_cycle(env):
    shell, api, clock = env()
    api.status = 503
    ex = await run(shell, dependency="erp", path="/orders")
    clock.advance(60)
    await shell.get(TimerService).run_due()
    assert (await shell.get(WorkflowRuntime).get(ex.id)).status is ExecutionStatus.PARKED
    [hold] = await shell.get(HoldService).list(active_only=True)
    assert hold.target == "http:erp" and hold.release_strategy == "probe-auto"   # contributed by the http module
    coordinator = shell.get(ReleaseCoordinator)
    await coordinator.evaluate_all()
    api.status = 200
    await coordinator.evaluate(await shell.get(HoldService).get(hold.id))
    clock.advance(31)
    await coordinator.evaluate(await shell.get(HoldService).get(hold.id))
    clock.advance(31)
    await coordinator.evaluate(await shell.get(HoldService).get(hold.id))
    assert (await shell.get(WorkflowRuntime).get(ex.id)).status is ExecutionStatus.COMPLETED


async def test_probe_and_classifier_units(env):
    shell, api, _ = env()
    probes = shell.get(ProbeRegistry)
    assert (await probes.check("http:erp")).healthy
    api.status = 503
    assert (await probes.check("http:erp")).detail == "HTTP 503"
    api.raise_error = httpx.ConnectTimeout("slow")
    result = await probes.check("http:erp")
    assert not result.healthy and "ConnectTimeout" in result.detail
    probe = next(p for p in probes.probes if isinstance(p, HttpHealthProbe))
    assert not probe.supports("http:unknown") and not probe.supports("rabbitmq:erp")
    c = HttpErrorClassifier()
    assert c.classify(httpx.ReadTimeout("t"), "d").kind is FaultKind.DEPENDENCY_UNAVAILABLE
    assert c.classify(httpx.InvalidURL("bad"), "d").kind is FaultKind.PERMANENT
    assert c.classify(ValueError(), "d") is None


async def test_client_provider_lifecycle():
    provider = HttpClientProvider()
    client = provider.client()
    assert provider.client() is client
    await provider.close()
    await provider.close()
