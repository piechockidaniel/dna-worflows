import pytest
from agent_framework import exceptions as maf

from DnA Workflows.ai import ChatClientRegistry, OperationsTools, TriageService, add_chat_client
from DnA Workflows.ai.clients import ChatClientConfig, UnknownChatClient
from DnA Workflows.ai.errors import AiErrorClassifier, AiOutputError
from DnA Workflows.ai.jsonout import extract_json
from DnA Workflows.ai.operations import TriageError
from DnA Workflows.ai.testing import ScriptedChatClient, ToolCall
from DnA Workflows.kernel import ManualClock
from DnA Workflows.persistence.contracts import JournalStore
from DnA Workflows.resilience import (HoldService, HoldStatus, ProposalService, ProposalStatus, ReleaseAction,
                                 ReleaseCoordinator)
from DnA Workflows.runtime import (ExecutionStatus, TimerService, WorkflowBuilder, WorkflowRegistry, WorkflowRuntime,
                              WorkState)
from DnA Workflows.runtime.models import FaultKind


@pytest.fixture
def ai_shell(make_shell):
    def build(*script, clock=None, ai=None, resilience=None, name="default"):
        client = ScriptedChatClient(*script, usage={"input_token_count": 10, "output_token_count": 5})
        shell = make_shell({"ai": ai or {}, "resilience": resilience or {
            "retry": {"transient": {"max_attempts": 2, "jitter": 0},
                      "dependency_unavailable": {"max_attempts": 2, "jitter": 0}}}},
            clock=clock, configure=lambda s: add_chat_client(s, name, client))
        return shell, client
    return build


async def run(shell, builder, **kwargs):
    await shell.get(WorkflowRegistry).publish(builder.build())
    return await shell.get(WorkflowRuntime).start(builder.build().id, **kwargs)


# ------------------------------------------------------------------------------------------------------
# activities
# ------------------------------------------------------------------------------------------------------

async def test_agent_prompt_with_template_and_usage(ai_shell):
    shell, client = ai_shell("Dear Ada, your order 7 has shipped.")
    ex = await run(shell, WorkflowBuilder("mail").add(
        "write", "AgentPrompt", prompt={"$tmpl": "Write a shipping note for {input.name}, order {input.order}"},
        instructions="Be brief", temperature=0.2, max_tokens=100), input={"name": "Ada", "order": 7})
    assert ex.status is ExecutionStatus.COMPLETED
    assert ex.outputs["write"]["text"].startswith("Dear Ada")
    assert ex.outputs["write"]["usage"]["input_token_count"] == 10
    assert client.conversations[0] == [("system", "Be brief"), ("user", "Write a shipping note for Ada, order 7")]
    logs = await shell.get(JournalStore).query(execution_id=ex.id, kinds=["activity.log"])
    assert logs[0].data["usage"]["output_token_count"] == 5 and logs[0].data["client"] == "default"
    assert (await shell.get(WorkflowRuntime).get(ex.id)).work == []


SCHEMA = {"type": "object", "required": ["invoice_no", "total"],
          "properties": {"invoice_no": {"type": "string"}, "total": {"type": "number"}}}


async def test_extract_structured_repairs_invalid_output(ai_shell):
    shell, client = ai_shell('Sure! ```json\n{"invoice_no": "F-1"}\n```', '{"invoice_no": "F-1", "total": 99.5}')
    ex = await run(shell, WorkflowBuilder("x").add("extract", "ExtractStructured", input="Invoice F-1 total 99.50 EUR",
                                                   json_schema=SCHEMA))
    assert ex.outputs["extract"] == {"invoice_no": "F-1", "total": 99.5}
    repair_prompt = client.conversations[1][-1][1]
    assert "invalid" in repair_prompt and "total" in repair_prompt


async def test_extract_structured_gives_up_as_transient_then_retries(ai_shell):
    clock = ManualClock()
    shell, _ = ai_shell("no json here", "still no json", '{"invoice_no": "A", "total": 1}', clock=clock)
    ex = await run(shell, WorkflowBuilder("x").add("extract", "ExtractStructured", input="...", json_schema=SCHEMA))
    item = ex.work[0]
    assert item.state is WorkState.RETRYING and item.last_fault.kind is FaultKind.TRANSIENT
    assert item.last_fault.error_type == "AiOutputError" and item.dependency == "ai:default"
    clock.advance(60)
    await shell.get(TimerService).run_due()
    assert (await shell.get(WorkflowRuntime).get(ex.id)).outputs["extract"] == {"invoice_no": "A", "total": 1}


async def test_classify_routes_by_label(ai_shell):
    shell, client = ai_shell({"label": "invoice", "confidence": 0.93, "reason": "has totals"},
                             {"label": "complaint", "confidence": 0.4},
                             {"label": "spam", "confidence": 0.99},
                             {"confidence": "high"},
                             "[1, 2]")
    wf = (WorkflowBuilder("route")
          .add("classify", "Classify", input={"$input": "text"}, labels=["invoice", "complaint"], min_confidence=0.5,
               instructions="Inbox triage")
          .then("erp", "SetOutput", outcome="invoice", value="ERP")
          .then("crm", "SetOutput", after="classify", outcome="complaint", value="CRM")
          .then("human", "SetOutput", after="classify", outcome="Unknown", value="HUMAN")
          .build())
    await shell.get(WorkflowRegistry).publish(wf)
    rt = shell.get(WorkflowRuntime)
    first = await rt.start("route", input={"text": "Invoice 123"})
    assert first.output == "ERP" and first.outputs["classify"]["confidence"] == 0.93
    assert (await rt.start("route", input={"text": "meh"})).output == "HUMAN"      # low confidence
    assert (await rt.start("route", input={"text": "buy now"})).output == "HUMAN"  # label not allowed
    assert (await rt.start("route", input={"text": "?"})).output == "HUMAN"        # no label / bad confidence
    assert (await rt.start("route", input={"text": "?"})).output == "HUMAN"        # JSON but not an object
    assert "Inbox triage" in client.conversations[0][0][1]


async def test_classify_non_json_answer_is_transient(ai_shell):
    shell, _ = ai_shell("I think it is an invoice")
    ex = await run(shell, WorkflowBuilder("c").add("c", "Classify", input="x", labels=["a"]))
    assert ex.work[0].last_fault.kind is FaultKind.TRANSIENT


async def test_model_outage_freezes_ai_work_behind_a_hold(ai_shell):
    clock = ManualClock()
    outage = maf.ChatClientException("service call failed")
    outage.__cause__ = ConnectionError("connection refused")
    shell, client = ai_shell(outage, outage, "recovered answer", clock=clock)
    ex = await run(shell, WorkflowBuilder("p").add("ask", "AgentPrompt", prompt="hi"))
    clock.advance(60)
    await shell.get(TimerService).run_due()
    ex = await shell.get(WorkflowRuntime).get(ex.id)
    assert ex.status is ExecutionStatus.PARKED
    [hold] = await shell.get(HoldService).list(active_only=True)
    assert hold.target == "ai:default" and hold.fault.kind is FaultKind.DEPENDENCY_UNAVAILABLE
    await shell.get(HoldService).release(hold.id, actor="ops", reason="model endpoint back")
    assert (await shell.get(WorkflowRuntime).get(ex.id)).outputs["ask"]["text"] == "recovered answer"


def test_ai_error_classifier():
    c = AiErrorClassifier()

    def chained(exc, cause):
        exc.__cause__ = cause
        return exc

    class RateLimited(Exception):
        status_code = 429

    class APITimeoutError(Exception):
        pass

    assert c.classify(maf.ChatClientInvalidAuthException("no"), "ai:x").kind is FaultKind.PERMANENT
    assert c.classify(maf.ChatClientContentFilterException("filtered"), None).kind is FaultKind.PERMANENT
    assert c.classify(chained(maf.ChatClientException("x"), RateLimited()), None).kind is FaultKind.THROTTLED
    assert c.classify(chained(maf.ChatClientException("x"), APITimeoutError()), None).kind is \
        FaultKind.DEPENDENCY_UNAVAILABLE
    assert c.classify(maf.ChatClientInvalidResponseException("bad"), None).kind is FaultKind.TRANSIENT
    assert c.classify(maf.ChatClientException("?"), None).kind is FaultKind.UNKNOWN
    assert c.classify(AiOutputError("shape"), None).details == {"ai": "invalid_output"}
    assert c.classify(ValueError("not ai"), None) is None


def test_extract_json_variants():
    assert extract_json('{"a": 1}') == {"a": 1}
    assert extract_json('Here:\n```json\n{"a": [1, 2]}\n```\nbye') == {"a": [1, 2]}
    assert extract_json('The answer is {"s": "brace } and \\" quote", "n": {"x": 1}} ok') == \
        {"s": 'brace } and " quote', "n": {"x": 1}}
    assert extract_json("list: [1, 2, 3] end") == [1, 2, 3]
    assert extract_json('{broken {"ok": true}') == {"ok": True}
    with pytest.raises(ValueError):
        extract_json("no json at all")
    with pytest.raises(ValueError):
        extract_json("{unclosed")


# ------------------------------------------------------------------------------------------------------
# client registry and providers
# ------------------------------------------------------------------------------------------------------

def test_registry_builds_configured_providers_lazily():
    from DnA Workflows.ai.clients import BUILTIN_PROVIDERS
    providers = [p() for p in BUILTIN_PROVIDERS]
    configs = {
        "gpt": ChatClientConfig(provider="openai", model="gpt-4o-mini", api_key="sk-test"),
        "azure": ChatClientConfig(provider="azure_openai", model="dep", endpoint="https://x.openai.azure.com",
                                  api_version="2024-10-21", api_key="k"),
        "vllm": ChatClientConfig(provider="openai_compatible", model="qwen", endpoint="http://vllm:8000/v1"),
        "local": ChatClientConfig(provider="ollama", model="llama3.1", endpoint="http://localhost:11434"),
    }
    registry = ChatClientRegistry(configs, providers, [], default="local")
    assert registry.names() == ["azure", "gpt", "local", "vllm"]
    assert registry._clients == {}                       # nothing built yet
    built = {n: type(registry.get(n)).__name__ for n in registry.names()}
    assert built == {"azure": "OpenAIChatClient", "gpt": "OpenAIChatClient", "local": "OllamaChatClient",
                     "vllm": "OpenAIChatCompletionClient"}
    assert registry.get() is registry.get("local")
    assert {d["name"]: d["default"] for d in registry.describe()}["local"] is True
    with pytest.raises(UnknownChatClient):
        registry.get("nope")
    with pytest.raises(ValueError, match="unknown provider"):
        ChatClientRegistry({"x": ChatClientConfig(provider="nope")}, providers, [])
    empty = ChatClientRegistry({}, providers, [])
    with pytest.raises(UnknownChatClient):
        empty.get()


def test_azure_openai_without_key_uses_azure_ad(monkeypatch):
    import sys
    import types
    created = []

    class FakeCredential:
        def __init__(self):
            created.append(self)
    fake_module = types.ModuleType("azure.identity.aio")
    fake_module.DefaultAzureCredential = FakeCredential
    monkeypatch.setitem(sys.modules, "azure.identity.aio", fake_module)
    monkeypatch.setitem(sys.modules, "azure.identity", types.ModuleType("azure.identity"))
    import agent_framework.openai as oa
    seen = {}

    class FakeClient:
        def __init__(self, model, **kwargs):
            seen.update(kwargs, model=model)
    monkeypatch.setattr(oa, "OpenAIChatClient", FakeClient, raising=False)
    from DnA Workflows.ai.clients import AzureOpenAIProvider
    AzureOpenAIProvider().create(ChatClientConfig(provider="azure_openai", model="dep", endpoint="https://e"))
    assert isinstance(seen["credential"], FakeCredential) and seen["azure_endpoint"] == "https://e"


async def test_ai_feature_from_config_and_descriptors(make_shell):
    shell = make_shell({"ai": {"clients": {"gpt": {"provider": "openai", "model": "m", "api_key": "sk"}},
                               "default_client": "gpt"}})
    registry = shell.get(ChatClientRegistry)
    assert registry.default == "gpt"
    from DnA Workflows.runtime import ActivityRegistry
    described = {d["type"]: d for d in shell.get(ActivityRegistry).describe()}
    assert described["ExtractStructured"]["category"] == "AI"
    assert "labels" in described["Classify"]["inputs_schema"]["properties"]


# ------------------------------------------------------------------------------------------------------
# operations tools, triage and the ai-recommend strategy
# ------------------------------------------------------------------------------------------------------

async def freeze_one(shell, clock):
    """Create a real parked execution behind an auto-tripped dependency hold."""
    from DnA Workflows.runtime import Activity, ActivityInputs, Done

    rt = shell.get(WorkflowRuntime)

    class Erp(Activity):
        type_name = "Erp"
        down = True

        class Inputs(ActivityInputs):
            pass

        def dependency(self, inputs, node):
            return "http:erp"

        async def execute(self, context):
            if Erp.down:
                raise ConnectionError("erp refused")
            return Done()
    if "Erp" not in rt.activities:
        rt.activities.register(Erp)
    ex = await run(shell, WorkflowBuilder("sync").add("call", "Erp"), correlation_id="order-5")
    clock.advance(60)
    await shell.get(TimerService).run_due()
    [hold] = await shell.get(HoldService).list(active_only=True)
    return ex, hold, Erp


async def test_operations_tools(ai_shell):
    clock = ManualClock()
    shell, _ = ai_shell(clock=clock)
    ex, hold, _ = await freeze_one(shell, clock)
    tools = shell.get(OperationsTools)
    assert (await tools.list_holds())[0]["target"] == "http:erp"
    [parked] = await tools.list_parked_work(dependency="http:erp")
    assert parked["execution_id"] == ex.id and parked["attempts"] == 2 and "ConnectionError" in parked["last_fault"]
    summary = await tools.get_execution(ex.id)
    assert summary["status"] == "parked" and summary["work"][0]["state"] == "parked"
    assert (await tools.get_execution("nope"))["error"]
    history = await tools.get_journal(correlation_id="order-5")
    assert history[0]["kind"] == "execution.started" and history[-1]["kind"] == "execution.parked"
    assert "error" in (await tools.get_journal())[0]
    assert (await tools.check_dependency("http:erp"))["probe"] is None
    assert (await tools.list_workflows())[0]["nodes"] == {"call": "Erp"}
    proposal = await tools.propose_release(hold.id, "probe is green", 0.9)
    assert proposal["status"] == "pending"
    assert (await tools.propose_resume(ex.id, parked["work_item_id"], "retry", 0.5))["status"] == "pending"
    names = [t.name for t in tools.as_maf_tools()]
    assert "propose_release" in names and "get_journal" in names
    tools.allow_proposals = False
    assert "error" in await tools.propose_release(hold.id, "x", 1)
    assert "error" in await tools.propose_resume(ex.id, "w", "x", 1)
    assert "propose_release" not in [t.name for t in tools.as_maf_tools()]


async def test_triage_uses_tools_and_files_a_proposal(ai_shell):
    clock = ManualClock()
    verdict = {"root_cause": "ERP maintenance window ended", "recommendation": "release", "confidence": 0.86,
               "reasoning": "probe ok", "suggested_checks": ["check ERP queue depth"]}
    shell, client = ai_shell(ToolCall("list_parked_work", {"dependency": "http:erp"}), verdict, clock=clock)
    ex, hold, erp = await freeze_one(shell, clock)
    triage = shell.get(TriageService)
    report = await triage.analyze(hold.id)
    assert report.recommendation == "release" and report.proposal_id is not None
    # the agent really called the tool: its result was fed back into the conversation
    assert client.conversations[1][-1][0] == "tool"
    assert "Investigate hold" in client.conversations[0][-1][1]
    proposals = shell.get(ProposalService)
    proposal = await proposals.get(report.proposal_id)
    assert proposal.proposed_by == "ai:triage:default" and proposal.evidence["confidence"] == 0.86
    assert (await shell.get(HoldService).get(hold.id)).status is HoldStatus.ACTIVE   # AI did not act
    erp.down = False
    await proposals.approve(proposal.id, actor="alice")
    assert (await shell.get(WorkflowRuntime).get(ex.id)).status is ExecutionStatus.COMPLETED
    assert [r.hold_id for r in await triage.reports(hold.id)] == [hold.id]


async def test_triage_low_confidence_and_invalid_answer(ai_shell):
    clock = ManualClock()
    shell, _ = ai_shell({"root_cause": "unclear", "recommendation": "release", "confidence": 0.3},
                        "I am not sure", clock=clock)
    _, hold, _ = await freeze_one(shell, clock)
    triage = shell.get(TriageService)
    report = await triage.analyze(hold.id)
    assert report.proposal_id is None
    with pytest.raises(TriageError):
        await triage.analyze(hold.id)


async def test_ai_recommend_release_strategy(ai_shell):
    clock = ManualClock()
    shell, _ = ai_shell(
        {"root_cause": "still down", "recommendation": "keep", "confidence": 0.9},
        {"root_cause": "healthy again", "recommendation": "release", "confidence": 0.95},
        clock=clock,
        resilience={"retry": {"dependency_unavailable": {"max_attempts": 2, "jitter": 0}},
                    "release": {"rules": [{"pattern": "http:*", "strategy": "ai-recommend",
                                           "options": {"min_confidence": 0.8}}]}})
    _, hold, _ = await freeze_one(shell, clock)
    assert hold.release_strategy == "ai-recommend"
    coordinator = shell.get(ReleaseCoordinator)
    [(_, keep)] = await coordinator.evaluate_all()
    assert keep.action is ReleaseAction.KEEP and "recommends keep" in keep.reason
    [(_, rec)] = await coordinator.evaluate_all()
    assert rec.action is ReleaseAction.RECOMMEND
    [proposal] = await shell.get(ProposalService).list(status=ProposalStatus.PENDING)
    assert proposal.proposed_by == "strategy:ai-recommend"
    [(_, failed)] = await coordinator.evaluate_all()          # script exhausted -> triage fails -> keep
    assert failed.action is ReleaseAction.KEEP and "triage failed" in failed.reason
