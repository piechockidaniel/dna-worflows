"""AI in operations (plan items P5.3, P5.4): tools, triage, and an AI release strategy.

Principle: **AI proposes, humans (or explicit policy) dispose.** The tools an agent receives are
read-only, plus *propose_* tools that file reviewable proposals (P3.9). Nothing here can release a
hold, resume or abandon work on its own.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from DnA Workflows.ai.clients import ChatClientRegistry
from DnA Workflows.ai.jsonout import extract_json
from DnA Workflows.kernel.clock import Clock
from DnA Workflows.persistence.contracts import KeyValueStore
from DnA Workflows.resilience.holds import Hold, HoldScope, HoldService
from DnA Workflows.resilience.proposals import ProposalService
from DnA Workflows.resilience.release import ReleaseAction, ReleaseContext, ReleaseDecision
from DnA Workflows.operations import OperationsTools, _brief  # noqa: F401 - re-exported for compatibility

log = logging.getLogger(__name__)


# -- triage ------------------------------------------------------------------------------------------

TRIAGE_INSTRUCTIONS = """You are an operations engineer for an integration platform.
A part of the system is FROZEN behind a hold because work kept failing. Work is parked, nothing is lost.
Use the tools to investigate (parked work, journal history, dependency health).
Then answer with ONLY a JSON object:
{"root_cause": "<one sentence>",
 "recommendation": "release" | "keep" | "abandon",
 "confidence": <0..1>,
 "reasoning": "<short explanation referencing evidence>",
 "suggested_checks": ["<what a human should verify>", ...]}
Recommend "release" only if evidence shows the dependency is healthy again and the parked work is valid.
Recommend "abandon" only for work that can never succeed (e.g. invalid data). You cannot act yourself."""


class TriageReport(BaseModel):
    hold_id: str
    root_cause: str
    recommendation: Literal["release", "keep", "abandon"]
    confidence: float = Field(ge=0, le=1)
    reasoning: str = ""
    suggested_checks: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    client: str | None = None
    proposal_id: str | None = None


class TriageError(RuntimeError):
    pass


class TriageService:
    COLLECTION = "triage-reports"

    def __init__(self, holds: HoldService, tools: OperationsTools, proposals: ProposalService,
                 clients: ChatClientRegistry, store: KeyValueStore, clock: Clock, client: str | None = None,
                 min_confidence: float = 0.7) -> None:
        self.holds = holds
        self.tools = tools
        self.proposals = proposals
        self.clients = clients
        self.store = store
        self.clock = clock
        self.client = client
        self.min_confidence = min_confidence

    async def context_for(self, hold: Hold) -> dict[str, Any]:
        parked = await self.tools.list_parked_work(hold_id=hold.id, limit=5)
        ctx: dict[str, Any] = {
            "hold": {"id": hold.id, "scope": hold.scope.value, "target": hold.target, "reason": hold.reason,
                     "source": hold.source, "created_at": hold.created_at.isoformat(),
                     "release_strategy": hold.release_strategy},
            "fault": hold.fault.model_dump(mode="json") if hold.fault else None,
            "parked_sample": parked,
            "parked_total": len(await self.holds.executions.find_parked(hold_id=hold.id))}
        if hold.scope is HoldScope.DEPENDENCY:
            ctx["dependency_health"] = await self.tools.check_dependency(hold.target)
        if parked:
            ctx["recent_history"] = await self.tools.get_journal(execution_id=parked[0]["execution_id"], limit=15)
        return ctx

    async def analyze(self, hold_id: str, *, propose: bool = True) -> TriageReport:
        from agent_framework import Agent
        hold = await self.holds.get(hold_id)
        context = await self.context_for(hold)
        client_name = self.clients.resolve_name(self.client)
        agent = Agent(client=self.clients.get(client_name), instructions=TRIAGE_INSTRUCTIONS, name="DnA Workflows-triage",
                      tools=self.tools.as_maf_tools(include_proposals=False))
        response = await agent.run(f"Investigate hold {hold.id}. Initial context:\n"
                                   f"{json.dumps(context, default=str, indent=1)}")
        try:
            report = TriageReport.model_validate({**extract_json(response.text), "hold_id": hold.id,
                                                  "created_at": self.clock.now(), "client": client_name})
        except (ValueError, ValidationError) as exc:
            raise TriageError(f"triage answer was not a valid report: {exc}") from exc
        if propose and report.recommendation == "release" and report.confidence >= self.min_confidence:
            proposal = await self.proposals.propose(
                "hold.release", {"hold_id": hold.id}, report.root_cause, proposed_by=f"ai:triage:{client_name}",
                confidence=report.confidence, evidence=report.model_dump(mode="json"))
            report.proposal_id = proposal.id
        await self.store.put(self.COLLECTION, f"{hold.id}:{report.created_at.isoformat()}",
                             report.model_dump(mode="json"))
        return report

    async def reports(self, hold_id: str | None = None) -> list[TriageReport]:
        items = [TriageReport.model_validate(d) for d in await self.store.list(self.COLLECTION)]
        return [r for r in items if hold_id is None or r.hold_id == hold_id]


class AiRecommendRelease:
    """Release strategy ``ai-recommend``: the triage agent evaluates the hold; a confident
    "release" becomes a proposal a human approves. AI is just one more pluggable strategy (P3.7)."""

    name = "ai-recommend"

    def __init__(self, services: Any) -> None:
        self.services = services

    async def evaluate(self, hold: Hold, context: ReleaseContext) -> ReleaseDecision:
        triage: TriageService = self.services.get(TriageService)
        try:
            report = await triage.analyze(hold.id, propose=False)
        except Exception as exc:
            return ReleaseDecision.keep(f"triage failed: {type(exc).__name__}: {exc}")
        context.state["triage"] = {"recommendation": report.recommendation, "confidence": report.confidence,
                                   "root_cause": report.root_cause}
        threshold = float(hold.release_options.get("min_confidence", triage.min_confidence))
        if report.recommendation == "release" and report.confidence >= threshold:
            return ReleaseDecision(ReleaseAction.RECOMMEND,
                                   f"AI triage ({report.confidence:.0%}): {report.root_cause}")
        return ReleaseDecision.keep(f"AI triage recommends {report.recommendation} "
                                    f"({report.confidence:.0%}): {report.root_cause}")
