import pytest
from fastapi.testclient import TestClient

from DnA Workflows.ai import add_chat_client
from DnA Workflows.ai.testing import ScriptedChatClient
from DnA Workflows.api.app import create_app
from DnA Workflows.api.feature import ApiApp, ApiServer
from DnA Workflows.kernel.hosting import HostedService
from DnA Workflows.runtime import Activity, ActivityInputs, Done, add_activity

POSTGRES = False  # TestClient/ASGI app: see tests/conftest.py


class Flaky(Activity):
    type_name = "ApiFlaky"
    fail = True

    class Inputs(ActivityInputs):
        pass

    def dependency(self, inputs, node):
        return "http:svc"

    async def execute(self, context):
        if Flaky.fail:
            raise ValueError("bad data")
        return Done(output="ok")


@pytest.fixture
def client(make_shell):
    triage = ScriptedChatClient({"root_cause": "x", "recommendation": "keep", "confidence": 0.5}, "garbage")
    shell = make_shell({"api": {"serve": False}, "resilience": {}, "ai": {}, "observability": {"sinks": []},
                        "observability.prometheus": {}},
                       configure=lambda s: (add_activity(s, Flaky), add_chat_client(s, "default", triage)))
    Flaky.fail = True
    with TestClient(shell.get(ApiApp).app) as c:
        yield c


WF = {"id": "hello", "nodes": {"s": {"id": "s", "type": "SetOutput", "inputs": {"value": {"$input": "name"}}}}}


def test_health_capabilities_and_workflow_lifecycle(client):
    assert client.get("/health").json()["status"] == "ok"
    caps = client.get("/capabilities").json()
    assert {f["name"] for f in caps["features"]} >= {"runtime", "resilience", "api"}
    assert any(a["type"] == "SendMessage" or a["type"] == "SetOutput" for a in caps["activities"])
    assert "nodes" in caps["workflow_schema"]["properties"]

    assert client.post("/workflows", json=WF).status_code == 201
    bad = client.post("/workflows", json={"id": "bad", "nodes": {"x": {"id": "x", "type": "Nope"}}})
    assert bad.status_code == 422 and "unknown activity" in bad.json()["detail"]["problems"][0]
    assert [w["id"] for w in client.get("/workflows").json()] == ["hello"]
    assert client.get("/workflows/hello").json()["id"] == "hello"
    assert client.get("/workflows/missing").status_code == 404

    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    started = client.post("/workflows/hello/executions", json={"input": {"name": "Ada"}, "correlation_id": "c1"},
                          headers={"traceparent": tp})
    assert started.status_code == 201
    ex = started.json()
    assert ex["output"] == "Ada" and ex["trace_id"] == "0af7651916cd43dd8448eb211c80319c"
    assert client.get(f"/executions/{ex['id']}").json()["status"] == "completed"
    assert client.get("/executions", params={"correlation_id": "c1"}).json()[0]["id"] == ex["id"]
    assert client.get(f"/executions/{ex['id']}/journal").json()[0]["kind"] == "execution.started"
    assert client.get("/journal", params={"correlation_id": "c1", "kind": ["execution.completed"]}).json()[0][
        "kind"] == "execution.completed"
    assert client.get("/executions/missing").status_code == 404


def test_events_cancel_and_retry(client):
    wf = {"id": "wait", "nodes": {"e": {"id": "e", "type": "Event", "inputs": {"name": "go"}},
                                  "x": {"id": "x", "type": "ApiFlaky"}},
          "edges": [{"source": "e", "target": "x"}]}
    client.post("/workflows", json=wf)
    ex = client.post("/workflows/wait/executions", json={}).json()
    assert ex["status"] == "waiting"
    # triggers also start new executions of 'wait' (its root is a trigger)
    result = client.post("/events/go", json={"payload": {"v": 1}}).json()
    assert ex["id"] in result["resumed"]
    faulted = client.get(f"/executions/{ex['id']}").json()
    assert faulted["status"] == "faulted" and faulted["fault"]["kind"] == "permanent"
    Flaky.fail = False
    retried = client.post(f"/executions/{ex['id']}/retry", json={"reason": "data fixed"},
                          headers={"X-Actor": "alice"}).json()
    assert retried["execution"]["status"] == "completed" and retried["retried"]
    other = client.post("/workflows/wait/executions", json={}).json()
    cancelled = client.post(f"/executions/{other['id']}/cancel", json={"reason": "dup"}).json()
    assert cancelled["status"] == "cancelled"
    events = client.get("/journal", params={"execution_id": ex["id"], "kind": ["work.retried"]}).json()
    assert events[0]["data"]["actor"] == "alice"


def test_holds_parked_work_proposals_outbox_metrics_and_triage(client):
    client.post("/workflows", json={"id": "f", "nodes": {"x": {"id": "x", "type": "ApiFlaky"}}})
    hold = client.post("/holds", json={"scope": "workflow", "target": "f", "reason": "investigating"},
                       headers={"X-Actor": "bob"})
    assert hold.status_code == 201 and hold.json()["created_by"] == "bob"
    hid = hold.json()["id"]
    assert client.post("/holds", json={"scope": "nope", "reason": "x"}).status_code == 422
    ex = client.post("/workflows/f/executions", json={}).json()
    assert ex["status"] == "parked"
    parked = client.get("/parked", params={"hold_id": hid}).json()
    assert parked == [{"execution_id": ex["id"], "work_item_id": ex["work"][0]["id"]}]
    assert client.get(f"/holds/{hid}").json()["status"] == "active"
    assert len(client.get("/holds", params={"active_only": True}).json()) == 1

    # triage over the API (scripted model answers "keep")
    report = client.post(f"/ai/triage/{hid}").json()
    assert report["recommendation"] == "keep"
    assert client.get("/ai/triage", params={"hold_id": hid}).json()[0]["hold_id"] == hid
    assert client.post(f"/ai/triage/{hid}").status_code == 502   # scripted garbage answer

    Flaky.fail = False
    released = client.post(f"/holds/{hid}/release", json={"reason": "ok"}).json()
    assert released["status"] == "released" and released["resumed_work"] == 1
    assert client.get(f"/executions/{ex['id']}").json()["status"] == "completed"

    # manual resume / abandon per work item
    Flaky.fail = True
    client.post("/holds", json={"scope": "activity_type", "target": "ApiFlaky", "reason": "x"})
    ex2 = client.post("/workflows/f/executions", json={}).json()
    wi = ex2["work"][0]["id"]
    assert client.post(f"/executions/{ex2['id']}/work/missing/resume", json={}).status_code == 409
    resumed = client.post(f"/executions/{ex2['id']}/work/{wi}/resume", json={}).json()
    assert resumed["execution"]["status"] == "parked"          # still held by the activity_type hold
    abandoned = client.post(f"/executions/{ex2['id']}/work/{wi}/abandon", json={"reason": "dup"}).json()
    assert abandoned["status"] == "cancelled"
    assert client.post(f"/executions/{ex2['id']}/work/{wi}/abandon", json={}).status_code == 404

    assert len(client.get("/holds", params={"active_only": True}).json()) == 1
    assert client.post("/proposals/missing/approve", json={}).status_code == 404

    # outbox & metrics
    assert client.get("/outbox", params={"status": "failed"}).json() == []
    assert client.post("/outbox/missing/redeliver").status_code == 409
    metrics = client.get("/metrics")
    assert metrics.status_code == 200 and "DnA Workflows_activity_duration_seconds" in metrics.text


def test_proposal_routes(client):
    hold = client.post("/holds", json={"scope": "dependency", "target": "http:x", "reason": "r"}).json()
    assert client.post("/proposals", json={"kind": "nope", "target": {}, "reason": "x"}).status_code == 422
    filed = client.post("/proposals", json={"kind": "hold.release", "target": {"hold_id": hold["id"]},
                                            "reason": "probe green", "confidence": 0.9},
                        headers={"X-Actor": "agent:copilot"})
    assert filed.status_code == 201 and filed.json()["proposed_by"] == "agent:copilot"
    pid = filed.json()["id"]
    assert client.get(f"/holds/{hold['id']}").json()["status"] == "active"      # proposing does nothing
    assert [p["id"] for p in client.get("/proposals", params={"status": "pending"}).json()] == [pid]
    approved = client.post(f"/proposals/{pid}/approve", json={"note": "checked"}, headers={"X-Actor": "alice"})
    assert approved.json()["status"] == "executed" and approved.json()["decided_by"] == "alice"
    assert client.get(f"/holds/{hold['id']}").json()["status"] == "released"
    assert client.post(f"/proposals/{pid}/reject", json={}).status_code == 409
    second = client.post("/proposals", json={"kind": "hold.release", "target": {"hold_id": hold["id"]},
                                             "reason": "again"}).json()
    assert client.post(f"/proposals/{second['id']}/reject", json={"note": "no"}).json()["status"] == "rejected"
    assert client.post(f"/proposals/{second['id']}/approve", json={}).status_code == 409


def test_api_key_and_server_lifecycle(make_shell):
    shell = make_shell({"api": {"serve": False, "api_key": "s3cret"}})
    with TestClient(shell.get(ApiApp).app) as c:
        assert c.get("/health").status_code == 200               # health is public
        assert c.get("/workflows").status_code == 401
        assert c.get("/workflows", headers={"X-API-Key": "s3cret"}).status_code == 200
    served = make_shell({"api": {"port": 0, "host": "127.0.0.1"}})
    servers = [h for h in served.provider.get_all(HostedService) if isinstance(h, ApiServer)]
    assert len(servers) == 1


async def test_api_server_starts_and_stops(make_shell):
    import asyncio

    import httpx
    shell = make_shell({"api": {"port": 18765, "host": "127.0.0.1"}})
    server = next(h for h in shell.provider.get_all(HostedService) if isinstance(h, ApiServer))
    await server.start()
    for _ in range(100):
        if server.server.started:
            break
        await asyncio.sleep(0.02)
    async with httpx.AsyncClient() as http:
        assert (await http.get("http://127.0.0.1:18765/health")).json()["status"] == "ok"
    await server.stop()
    await server.stop()


def test_create_app_without_optional_features(make_shell):
    shell = make_shell({})
    with TestClient(create_app(shell.provider)) as c:
        assert c.get("/holds").status_code == 404      # resilience not composed: no routes
        assert c.get("/metrics").status_code == 404
        assert c.get("/ai/triage").status_code == 404


def test_message_bridge(make_shell):
    from DnA Workflows.messaging.providers.inmemory import InMemoryBroker
    InMemoryBroker.reset()
    shell = make_shell({"api": {"serve": False}, "messaging": {"endpoints": {
        "out": {"transport": "inmemory", "address": "q.api"}}}, "messaging.inmemory": {}})
    with TestClient(shell.get(ApiApp).app) as c:
        tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        sent = c.post("/messages/out", json={"type": "ping", "data": {"x": 1}, "correlation_id": "c"},
                      headers={"traceparent": tp})
        assert sent.status_code == 202
        [msg] = InMemoryBroker.get().history["q.api"]
        assert msg.id == sent.json()["message_id"] and msg.trace.trace_id == "0af7651916cd43dd8448eb211c80319c"
        assert c.post("/messages/nope", json={"type": "x"}).status_code == 404
        assert c.get("/messaging").json()["endpoints"][0]["name"] == "out"
    InMemoryBroker.reset()


def test_studio_is_served_with_runtime_config(make_shell, tmp_path):
    (tmp_path / "index.html").write_text("<html>studio</html>")
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "app.js").write_text("console.log(1)")
    kc = {"url": "https://sso.example.com", "realm": "DnA Workflows", "clientId": "DnA Workflows-studio"}
    shell = make_shell({"api": {"serve": False, "studio_dir": str(tmp_path), "studio_config": {"keycloak": kc},
                                "cors_origins": ["http://localhost:5173"]}})
    with TestClient(shell.get(ApiApp).app) as c:
        assert c.get("/studio/config.json").json() == {"apiBase": "", "keycloak": kc}
        assert c.get("/studio/assets/app.js").text == "console.log(1)"
        assert "studio" in c.get("/studio/designer/orders").text          # SPA fallback to index.html
        for probe in ("/studio/..%2F..%2F..%2Fetc%2Fpasswd", "/studio/../../etc/passwd"):
            assert "root:" not in c.get(probe).text                        # no path traversal
        cors = c.options("/workflows", headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "GET"})
        assert cors.headers["access-control-allow-origin"] == "http://localhost:5173"
    empty = make_shell({"api": {"serve": False, "studio_dir": str(tmp_path / "missing")}})
    with TestClient(empty.get(ApiApp).app) as c:
        assert c.get("/studio/").status_code == 404
