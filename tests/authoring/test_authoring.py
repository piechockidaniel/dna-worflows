import pytest
from fastapi.testclient import TestClient

from DnA Workflows.api.feature import ApiApp
from DnA Workflows.authoring import DraftConflict, DraftService, DraftStateError, DraftStatus, TestRunner, TestRunRequest
from DnA Workflows.runtime import WorkflowRegistry, WorkflowRuntime
from DnA Workflows.runtime.models import WorkflowDefinition

ORDER_FLOW = {
    "id": "orders", "name": "Orders",
    "nodes": {
        "receive": {"id": "receive", "type": "Event", "inputs": {"name": "order"}},
        "book": {"id": "book", "type": "HttpRequest",
                 "inputs": {"method": "POST", "url": "https://erp.example/orders", "json_body": {"$output": "receive"}}},
        "big": {"id": "big", "type": "If", "inputs": {"condition": {"$output": "book.body.priority"}}},
        "notify": {"id": "notify", "type": "Log", "inputs": {"message": {"$tmpl": "booked {output.book.body.id}"}}},
    },
    "edges": [{"source": "receive", "target": "book"}, {"source": "book", "target": "big"},
              {"source": "big", "target": "notify", "outcome": "True"}],
}

POSTGRES = False  # TestClient/ASGI app: see tests/conftest.py


@pytest.fixture
def drafts(make_shell):
    shell = make_shell({"authoring": {}, "http": {}})
    return shell, shell.get(DraftService)


async def test_full_review_flow_with_many_hands(drafts):
    shell, svc = drafts
    d = await svc.start("orders", "anna", WorkflowDefinition.model_validate({"id": "orders", "nodes": {}}))
    assert d.status is DraftStatus.DRAFT and d.problems == ["workflow has no nodes"]
    assert (await svc.start("orders", "ben")).created_by == "anna"         # the draft is shared, not owned
    d = await svc.save("orders", WorkflowDefinition.model_validate(ORDER_FLOW), "ben", revision=d.revision)
    assert d.problems == [] and d.revision == 2 and d.updated_by == "ben"
    with pytest.raises(DraftConflict):
        await svc.save("orders", WorkflowDefinition.model_validate(ORDER_FLOW), "anna", revision=1)
    with pytest.raises(DraftStateError, match="expected one of"):
        await svc.publish("orders", "carl")                              # not reviewed yet
    d = await svc.submit("orders", "ben")
    assert d.status is DraftStatus.IN_REVIEW
    with pytest.raises(DraftStateError, match="Four-eyes"):
        await svc.review("orders", "ben", "approve")
    d = await svc.review("orders", "dora", "request_changes", "add a failure branch")
    assert d.status is DraftStatus.CHANGES_REQUESTED
    d = await svc.comment("orders", "anna", "I'll add it", node="big")
    assert d.comments[0].node == "big"
    d = await svc.save("orders", WorkflowDefinition.model_validate(ORDER_FLOW), "anna", revision=d.revision)
    await svc.submit("orders", "anna")
    d = await svc.review("orders", "dora", "approve", "looks good")
    assert d.status is DraftStatus.APPROVED
    d = await svc.publish("orders", "erik")
    assert d.status is DraftStatus.PUBLISHED and d.published_version == 1
    published = await shell.get(WorkflowRegistry).get("orders")
    assert published.metadata["published_by"] == "erik" and published.metadata["reviewed_by"] == ["dora"]
    assert [(h.actor, h.action) for h in d.history] == [
        ("anna", "created"), ("ben", "saved"), ("ben", "submitted"), ("dora", "review:request_changes"),
        ("anna", "saved"), ("anna", "submitted"), ("dora", "review:approve"), ("erik", "published")]

    # next change starts from the published version and publishes version 2
    d2 = await svc.start("orders", "ben")
    assert d2.base_version == 1 and d2.status is DraftStatus.DRAFT
    defn = d2.definition.model_copy(update={"name": "Orders v2"})
    d2 = await svc.save("orders", defn, "ben", d2.revision)
    await svc.submit("orders", "ben")
    await svc.review("orders", "anna", "approve")
    assert (await svc.publish("orders", "anna")).published_version == 2
    assert [v["version"] for v in await svc.versions("orders")] == [2, 1]
    await svc.discard("orders", "ben")
    with pytest.raises(LookupError):
        await svc.get("orders")


async def test_submit_requires_valid_definition_and_saving_resets_review(drafts):
    _, svc = drafts
    d = await svc.start("x", "a", WorkflowDefinition.model_validate({"id": "x", "nodes": {}}))
    with pytest.raises(DraftStateError, match="validation problems"):
        await svc.submit("x", "a")
    d = await svc.save("x", WorkflowDefinition.model_validate({**ORDER_FLOW, "id": "x"}), "a", d.revision)
    await svc.submit("x", "a")
    d = await svc.save("x", d.definition, "b", (await svc.get("x")).revision)
    assert d.status is DraftStatus.DRAFT and d.reviews == []
    with pytest.raises(DraftStateError, match="must match"):
        await svc.save("x", WorkflowDefinition.model_validate({**ORDER_FLOW, "id": "y"}), "a", d.revision)


async def test_without_review_requirement(make_shell):
    shell = make_shell({"authoring": {"review_required": False, "four_eyes": False}, "http": {}})
    svc = shell.get(DraftService)
    await svc.start("solo", "a", WorkflowDefinition.model_validate({**ORDER_FLOW, "id": "solo"}))
    assert (await svc.publish("solo", "a")).published_version == 1
    await svc.start("solo", "a")
    assert (await svc.submit("solo", "a")).status is DraftStatus.APPROVED


async def test_test_run_simulates_external_calls_and_reports_would_send(make_shell):
    shell = make_shell({"authoring": {}, "http": {}, "messaging": {"endpoints": {
        "out": {"transport": "inmemory", "address": "q.tr"}}}, "messaging.inmemory": {}})
    runner = shell.get(TestRunner)
    flow = WorkflowDefinition.model_validate({**ORDER_FLOW, "nodes": {
        **ORDER_FLOW["nodes"],
        "send": {"id": "send", "type": "SendMessage", "inputs": {"endpoint": "out", "type": "booked"}}},
        "edges": [*ORDER_FLOW["edges"], {"source": "notify", "target": "send"}]})
    # the trigger payload comes from the test input; HTTP is mocked with a sample answer
    result = await runner.run(flow, TestRunRequest(input={"id": 1}, mocks={
        "receive": {"output": {"id": 1}},
        "book": {"output": {"status": 201, "body": {"id": "ERP-9", "priority": True}}}}))
    assert result["status"] == "completed" and result["path"] == ["receive", "book", "big", "notify", "send"]
    assert result["simulated"] == ["receive", "book"]
    assert result["outputs"]["notify"] == "booked ERP-9"
    assert result["would_send"][0]["payload"]["endpoint"] == "out"
    # without a mock the HTTP call is simulated (never leaves the host)
    result = await runner.run(flow, TestRunRequest(mocks={"receive": {"output": {}}}))
    assert result["outputs"]["book"] == {"simulated": True, "dependency": "http:erp.example"}
    assert result["path"][-1] == "big"                          # simulated output has no priority → False
    waiting = await runner.run(flow, TestRunRequest())
    assert waiting["status"] == "waiting" and waiting["waiting_for"][0]["stimulus"] == "event:order"
    invalid = await runner.run(WorkflowDefinition(id="bad"), TestRunRequest())
    assert invalid["status"] == "invalid"
    assert await shell.get(WorkflowRuntime).store.list() == []   # nothing leaked into the real store


def test_authoring_api(make_shell):
    shell = make_shell({"api": {"serve": False}, "authoring": {"four_eyes": True}, "http": {}})
    with TestClient(shell.get(ApiApp).app) as c:
        anna, dora = {"X-Actor": "anna"}, {"X-Actor": "dora"}
        d = c.post("/drafts/orders", json={"definition": ORDER_FLOW}, headers=anna).json()
        assert d["status"] == "draft" and d["revision"] == 1
        assert c.put("/drafts/orders", json={"definition": ORDER_FLOW, "revision": 9}, headers=anna).status_code == 409
        d = c.put("/drafts/orders", json={"definition": ORDER_FLOW, "revision": 1}, headers=anna).json()
        assert c.post("/drafts/orders/comments", json={"text": "ok?"}, headers=dora).json()["comments"][0]["author"] == "dora"
        c.post("/drafts/orders/submit", headers=anna)
        assert c.post("/drafts/orders/review", json={"decision": "approve"}, headers=anna).status_code == 422
        assert c.post("/drafts/orders/review", json={"decision": "approve"}, headers=dora).json()["status"] == "approved"
        tr = c.post("/drafts/orders/test-run", json={"mocks": {"receive": {"output": {}}}})
        assert tr.status_code == 200, tr.text
        assert tr.json()["status"] == "completed"
        assert c.post("/drafts/orders/publish", headers=dora).json()["published_version"] == 1
        assert c.get("/workflows/orders/versions").json()[0]["version"] == 1
        assert c.get("/drafts").json()[0]["workflow_id"] == "orders"
        assert c.get("/drafts/orders").json()["status"] == "published"
        assert c.delete("/drafts/orders").status_code == 204
        assert c.get("/drafts/orders").status_code == 404
        caps = c.get("/capabilities").json()
        http = next(a for a in caps["activities"] if a["type"] == "HttpRequest")
        assert http["display_name"] == "Http Request" and http["output_schema"]["properties"]["body"]
        assert "authoring" in caps["features_enabled"]
