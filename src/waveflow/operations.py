"""Operations tools (plan items P5.3, P13.1) – shared by AI agents (MAF tools) and the MCP server.

Principle: **agents propose, humans (or explicit policy) dispose.** Read-only functions plus
*propose_* functions that file reviewable proposals (P3.9); nothing here acts on its own.
"""

from __future__ import annotations

import json
from typing import Any

from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore
from DnA Workflows.resilience.holds import HoldService
from DnA Workflows.resilience.proposals import ProposalService
from DnA Workflows.resilience.release import ProbeRegistry
from DnA Workflows.runtime.registry import WorkflowRegistry


class OperationsTools:
    """Functions exposed to agents (as MAF tools) and, later, over MCP (Q-004)."""

    def __init__(self, holds: HoldService, executions: ExecutionStore, journal: JournalStore,
                 proposals: ProposalService, probes: ProbeRegistry, workflows: WorkflowRegistry,
                 allow_proposals: bool = True, actor: str = "ai:agent") -> None:
        self.holds = holds
        self.executions = executions
        self.journal = journal
        self.proposals = proposals
        self.probes = probes
        self.workflows = workflows
        self.allow_proposals = allow_proposals
        self.actor = actor

    # -- read-only ------------------------------------------------------------------------------
    async def list_holds(self, active_only: bool = True) -> list[dict[str, Any]]:
        """List holds (frozen areas of the system) with scope, target, reason and release strategy."""
        return [{"id": h.id, "scope": h.scope.value, "target": h.target, "reason": h.reason, "source": h.source,
                 "status": h.status.value, "created_at": h.created_at.isoformat(),
                 "release_strategy": h.release_strategy, "last_decision": h.strategy_state.get("last_decision")}
                for h in await self.holds.list(active_only=active_only)]

    async def list_parked_work(self, dependency: str | None = None, hold_id: str | None = None,
                               limit: int = 20) -> list[dict[str, Any]]:
        """List parked work items (waiting behind a hold or after exhausting retries)."""
        result = []
        for execution_id, item_id in (await self.executions.find_parked(dependency=dependency,
                                                                         hold_id=hold_id))[:limit]:
            ex = await self.executions.get(execution_id)
            item = ex.item(item_id) if ex else None
            if item is None:
                continue
            result.append({"execution_id": ex.id, "workflow": ex.definition_id, "work_item_id": item.id,
                           "node": item.node_id, "activity_type": item.activity_type, "attempts": item.attempt,
                           "dependency": item.dependency, "hold_id": item.hold_id, "reason": item.parked_reason,
                           "last_fault": item.last_fault.summary() if item.last_fault else None,
                           "correlation_id": ex.correlation_id})
        return result

    async def get_execution(self, execution_id: str) -> dict[str, Any]:
        """Summarise one workflow execution: status, fault, work items and what it waits for."""
        ex = await self.executions.get(execution_id)
        if ex is None:
            return {"error": f"execution {execution_id} not found"}
        return {"id": ex.id, "workflow": ex.definition_id, "version": ex.definition_version,
                "status": ex.status.value, "correlation_id": ex.correlation_id, "trace_id": ex.trace_id,
                "fault": ex.fault.summary() if ex.fault else None,
                "work": [{"id": w.id, "node": w.node_id, "state": w.state.value, "attempt": w.attempt,
                          "dependency": w.dependency, "last_fault": w.last_fault.summary() if w.last_fault else None}
                         for w in ex.work],
                "waiting_for": [{"type": b.stimulus_type, "key": b.key} for b in ex.bookmarks]}

    async def get_journal(self, execution_id: str | None = None, correlation_id: str | None = None,
                          limit: int = 50) -> list[dict[str, Any]]:
        """Read the execution journal (history) for an execution or a whole business transaction."""
        if not execution_id and not correlation_id:
            return [{"error": "provide execution_id or correlation_id"}]
        events = await self.journal.query(execution_id=execution_id, correlation_id=correlation_id, limit=10_000)
        return [{"ts": e.timestamp.isoformat(), "kind": e.kind, "execution_id": e.execution_id, "node": e.node_id,
                 "data": _brief(e.data)} for e in events[-limit:]]

    async def check_dependency(self, dependency: str) -> dict[str, Any]:
        """Run the health probe for a dependency (e.g. 'rabbitmq:orders', 'http:sap-prod')."""
        result = await self.probes.check(dependency)
        if result is None:
            return {"dependency": dependency, "probe": None, "note": "no health probe registered"}
        return {"dependency": dependency, "healthy": result.healthy, "detail": result.detail}

    async def list_workflows(self) -> list[dict[str, Any]]:
        """List published workflows with their nodes (machine-readable structure)."""
        return [{"id": d.id, "version": d.version, "name": d.name,
                 "nodes": {n: node.type for n, node in d.nodes.items()}} for d in self.workflows.list()]

    # -- propose-only ---------------------------------------------------------------------------
    async def propose_release(self, hold_id: str, reason: str, confidence: float) -> dict[str, Any]:
        """Propose releasing a hold. A human must approve; nothing is released by this call."""
        if not self.allow_proposals:
            return {"error": "proposals are disabled for this agent"}
        p = await self.proposals.propose("hold.release", {"hold_id": hold_id}, reason, proposed_by=self.actor,
                                         confidence=confidence)
        return {"proposal_id": p.id, "status": p.status.value}

    async def propose_resume(self, execution_id: str, work_item_id: str, reason: str,
                             confidence: float) -> dict[str, Any]:
        """Propose resuming one parked work item. A human must approve."""
        if not self.allow_proposals:
            return {"error": "proposals are disabled for this agent"}
        p = await self.proposals.propose("work.resume", {"execution_id": execution_id, "work_item_id": work_item_id},
                                         reason, proposed_by=self.actor, confidence=confidence)
        return {"proposal_id": p.id, "status": p.status.value}

    # -- adapters -------------------------------------------------------------------------------
    READ_ONLY = ("list_holds", "list_parked_work", "get_execution", "get_journal", "check_dependency",
                 "list_workflows")
    PROPOSE = ("propose_release", "propose_resume")

    def functions(self, include_proposals: bool = True) -> list[Any]:
        names = self.READ_ONLY + (self.PROPOSE if include_proposals and self.allow_proposals else ())
        return [getattr(self, n) for n in names]

    def as_maf_tools(self, include_proposals: bool = True) -> list[Any]:
        from agent_framework import tool
        return [tool(fn, name=fn.__name__, description=(fn.__doc__ or "").strip())
                for fn in self.functions(include_proposals)]


def _brief(data: dict[str, Any], limit: int = 400) -> Any:
    text = json.dumps(data, default=str)
    return data if len(text) <= limit else text[:limit] + "…"
