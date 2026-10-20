"""MCP server (P13): tools, role filtering, stdio subprocess and HTTP with Keycloak tokens."""

import json
import sys
from pathlib import Path

import httpx
import pytest
from mcp.client.client import Client, StdioServerParameters

from DnA Workflows.api.feature import ApiApp
from DnA Workflows.mcpserver import McpSessionService, DnA WorkflowsMcp
from DnA Workflows.resilience import HoldService, ProposalService
from DnA Workflows.runtime import WorkflowBuilder, WorkflowRegistry
from DnA Workflows.security import SecurityFeature

from ..security.test_security import ISSUER, FakeKeycloak

FLOW = WorkflowBuilder("hello").add("s", "SetOutput", value={"$input": "name"}).build()

POSTGRES = False  # TestClient/ASGI app: see tests/conftest.py


def result_of(call) -> object:
    assert not call.is_error, call.content
    data = call.structured_content
    return data.get("result", data) if isinstance(data, dict) else data


@pytest.fixture
async def shell(make_shell):
    s = make_shell({"resilience": {}, "mcp": {"http_path": None}, "observability": {"sinks": []}, "forecasting": {}})
    await s.get(WorkflowRegistry).publish(FLOW)
    return s


async def test_agent_tools_in_memory(shell):
    mcp = shell.get(DnA WorkflowsMcp)
    async with Client(mcp.server) as client:
        names = {t.name for t in (await client.list_tools()).tools}
        assert {"start_workflow", "propose_release", "forecast_workflow", "get_journal"} <= names
        assert result_of(await client.call_tool("list_workflows", {}))[0]["id"] == "hello"
        started = result_of(await client.call_tool("start_workflow", {"workflow_id": "hello",
                                                                      "input": {"name": "Ada"},
                                                                      "correlation_id": "c-1"}))
        assert started["status"] == "completed"
        found = result_of(await client.call_tool("find_executions", {"correlation_id": "c-1"}))
        assert found[0]["execution_id"] == started["execution_id"]
        detail = result_of(await client.call_tool("get_execution", {"execution_id": started["execution_id"]}))
        assert detail["status"] == "completed"
        journal = result_of(await client.call_tool("get_journal", {"correlation_id": "c-1"}))
        assert journal[0]["kind"] == "execution.started"
        assert result_of(await client.call_tool("describe_workflow", {"workflow_id": "hello"}))["id"] == "hello"
        assert any(a["type"] == "SetOutput" for a in result_of(await client.call_tool("describe_activities", {})))
        forecast = result_of(await client.call_tool("forecast_workflow", {"workflow_id": "hello"}))
        assert forecast["workflow"] == "hello"
        eta = result_of(await client.call_tool("forecast_execution", {"execution_id": started["execution_id"]}))
        assert eta["basis"] == "actual"

        hold = await shell.get(HoldService).place("dependency", "http:erp", "down")
        assert result_of(await client.call_tool("list_holds", {}))[0]["target"] == "http:erp"
        assert result_of(await client.call_tool("list_parked_work", {})) == []
        assert result_of(await client.call_tool("check_dependency", {"dependency": "http:erp"}))["probe"] is None
        proposal = result_of(await client.call_tool("propose_release", {"hold_id": hold.id, "reason": "looks ok",
                                                                        "confidence": 0.8}))
        assert proposal["status"] == "pending"
        resume = result_of(await client.call_tool("propose_resume", {"execution_id": "e", "work_item_id": "w",
                                                                     "reason": "r", "confidence": 0.1}))
        assert resume["status"] == "pending"
        [p] = [x for x in await shell.get(ProposalService).list() if x.kind == "hold.release"]
        assert p.proposed_by == "mcp:mcp-stdio"          # proposals are attributed to the agent


async def test_tools_are_filtered_and_enforced_by_permission(make_shell):
    s = make_shell({"resilience": {}, "mcp": {"http_path": None, "stdio_permissions": ["workflows.read"]}})
    async with Client(s.get(DnA WorkflowsMcp).server) as client:
        names = {t.name for t in (await client.list_tools()).tools}
        assert names == {"list_workflows", "describe_workflow", "describe_activities"}
        denied = await client.call_tool("start_workflow", {"workflow_id": "x"})
        assert denied.is_error and "lacks permission 'executions.start'" in denied.content[0].text


async def test_stdio_transport_in_a_real_subprocess(tmp_path):
    (tmp_path / "wf").mkdir()
    (tmp_path / "wf" / "hello.json").write_text(json.dumps(FLOW.to_json_dict()))
    cfg = tmp_path / "shell.yaml"
    cfg.write_text(f"""features:
  persistence.sqlite: {{path: "{tmp_path / 'db.sqlite'}"}}
  runtime: {{definitions: ["{tmp_path / 'wf'}"], background_services: false}}
  resilience: {{background_services: false}}
  mcp: {{}}
""")
    params = StdioServerParameters(command=sys.executable, args=["-m", "DnA Workflows", "mcp", "--config", str(cfg)],
                                   cwd=str(Path(__file__).parents[2]))
    async with Client(params) as client:
        started = result_of(await client.call_tool("start_workflow", {"workflow_id": "hello",
                                                                      "input": {"name": "stdio"}}))
        assert started["status"] == "completed"


async def test_http_transport_requires_keycloak_tokens_and_applies_roles(make_shell, monkeypatch):
    kc = FakeKeycloak()
    monkeypatch.setattr(SecurityFeature, "transport", httpx.MockTransport(kc.handler))
    s = make_shell({"api": {"serve": False}, "resilience": {}, "security": {"issuer": ISSUER},
                    "mcp": {"resource_url": "https://DnA Workflows.test/mcp"}})
    await s.get(WorkflowRegistry).publish(FLOW)
    session = McpSessionService(s.get(DnA WorkflowsMcp))
    await session.start()
    headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json",
               "MCP-Protocol-Version": "2025-06-18"}

    def rpc(method, params=None, i=1):
        return {"jsonrpc": "2.0", "id": i, "method": method, "params": params or {}}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(s.get(ApiApp).app), base_url="http://t") as c:
            unauthenticated = await c.post("/mcp/", headers=headers, json=rpc("tools/list"))
            assert unauthenticated.status_code == 401
            assert "resource_metadata" in unauthenticated.headers.get("www-authenticate", "")
            bad = await c.post("/mcp/", headers={**headers, "Authorization": "Bearer nope"}, json=rpc("tools/list"))
            assert bad.status_code == 401

            # a machine client (client-credentials service account) with the agent role
            agent = kc.token(username="ops-bot", realm_roles=["DnA Workflows-agent"], service=True)
            auth = {**headers, "Authorization": f"Bearer {agent}"}
            tools = (await c.post("/mcp/", headers=auth, json=rpc("tools/list"))).json()["result"]["tools"]
            assert {"start_workflow", "propose_release"} <= {t["name"] for t in tools}
            call = (await c.post("/mcp/", headers=auth, json=rpc("tools/call", {
                "name": "start_workflow", "arguments": {"workflow_id": "hello", "input": {"name": "http"}}}))).json()
            assert call["result"]["structuredContent"]["status"] == "completed"

            # a viewer sees only read tools and cannot start anything
            viewer = {**headers, "Authorization": f"Bearer {kc.token(realm_roles=['DnA Workflows-viewer'])}"}
            names = {t["name"] for t in (await c.post("/mcp/", headers=viewer, json=rpc("tools/list"))).json()
                     ["result"]["tools"]}
            assert "start_workflow" not in names and "list_workflows" in names
            denied = (await c.post("/mcp/", headers=viewer, json=rpc("tools/call", {
                "name": "start_workflow", "arguments": {"workflow_id": "hello"}}))).json()
            assert denied["result"]["isError"] is True
    finally:
        await session.stop()
