"""Proposals: reviewable, approval-gated operator actions (plan item P3.9).

Anything that is not allowed to act on its own – a probe that only *recommends* a release, an AI
triage agent – files a proposal. A human approves or rejects it; approval executes the action through
a handler registered for the proposal kind (Extend axis). Every step is journaled.
"""

from __future__ import annotations

import logging
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.persistence.contracts import JournalStore, KeyValueStore
from DnA Workflows.runtime.models import JournalEvent

log = logging.getLogger(__name__)

COLLECTION = "proposals"


class ProposalStatus(StrEnum):
    PENDING = "pending"
    EXECUTED = "executed"
    REJECTED = "rejected"
    FAILED = "failed"


class Proposal(BaseModel):
    id: str
    kind: str                                   # e.g. "hold.release", "work.resume", "work.abandon"
    target: dict[str, Any]
    reason: str
    proposed_by: str
    created_at: datetime
    status: ProposalStatus = ProposalStatus.PENDING
    confidence: float | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)
    decided_by: str | None = None
    decided_at: datetime | None = None
    decision_note: str | None = None
    result: dict[str, Any] = Field(default_factory=dict)


@runtime_checkable
class ProposalHandler(Protocol):
    """Contributor: executes approved proposals of one kind."""

    kind: str

    async def execute(self, proposal: Proposal, actor: str) -> dict[str, Any]: ...


class ProposalNotFound(LookupError):
    pass


class ProposalStateError(RuntimeError):
    pass


class ProposalService:
    def __init__(self, store: KeyValueStore, journal: JournalStore, clock: Clock, ids: IdGenerator,
                 handlers: list[ProposalHandler]) -> None:
        self.store = store
        self.journal = journal
        self.clock = clock
        self.ids = ids
        self.handlers = {h.kind: h for h in handlers}

    async def propose(self, kind: str, target: dict[str, Any], reason: str, *, proposed_by: str,
                      confidence: float | None = None, evidence: dict[str, Any] | None = None) -> Proposal:
        if kind not in self.handlers:
            raise ValueError(f"No handler for proposal kind '{kind}'. Known: {sorted(self.handlers)}")
        proposal = Proposal(id=self.ids.new_id("prop"), kind=kind, target=target, reason=reason,
                            proposed_by=proposed_by, created_at=self.clock.now(), confidence=confidence,
                            evidence=evidence or {})
        await self._save(proposal, "proposal.created", proposed_by)
        return proposal

    async def get(self, proposal_id: str) -> Proposal:
        data = await self.store.get(COLLECTION, proposal_id)
        if data is None:
            raise ProposalNotFound(f"Proposal '{proposal_id}' not found")
        return Proposal.model_validate(data)

    async def list(self, *, status: ProposalStatus | None = None) -> list[Proposal]:
        items = [Proposal.model_validate(d) for d in await self.store.list(COLLECTION)]
        items.sort(key=lambda p: (p.created_at, p.id))
        return [p for p in items if status is None or p.status is status]

    async def approve(self, proposal_id: str, *, actor: str, note: str | None = None) -> Proposal:
        proposal = await self._pending(proposal_id)
        proposal.decided_by = actor
        proposal.decided_at = self.clock.now()
        proposal.decision_note = note
        try:
            proposal.result = await self.handlers[proposal.kind].execute(proposal, actor)
            proposal.status = ProposalStatus.EXECUTED
        except Exception as exc:
            log.exception("Executing proposal %s failed", proposal.id)
            proposal.status = ProposalStatus.FAILED
            proposal.result = {"error": f"{type(exc).__name__}: {exc}"}
        await self._save(proposal, f"proposal.{proposal.status.value}", actor)
        return proposal

    async def reject(self, proposal_id: str, *, actor: str, note: str | None = None) -> Proposal:
        proposal = await self._pending(proposal_id)
        proposal.status = ProposalStatus.REJECTED
        proposal.decided_by = actor
        proposal.decided_at = self.clock.now()
        proposal.decision_note = note
        await self._save(proposal, "proposal.rejected", actor)
        return proposal

    async def _pending(self, proposal_id: str) -> Proposal:
        proposal = await self.get(proposal_id)
        if proposal.status is not ProposalStatus.PENDING:
            raise ProposalStateError(f"Proposal {proposal_id} is already {proposal.status.value}")
        return proposal

    async def _save(self, proposal: Proposal, kind: str, actor: str) -> None:
        await self.store.put(COLLECTION, proposal.id, proposal.model_dump(mode="json"))
        await self.journal.append([JournalEvent(
            id=self.ids.new_id("ev"), kind=kind, timestamp=self.clock.now(),
            data={"proposal": proposal.model_dump(mode="json"), "actor": actor})])


# -- built-in proposal handlers ----------------------------------------------------------------------

class ReleaseHoldHandler:
    kind = "hold.release"

    def __init__(self, holds: Any) -> None:
        self.holds = holds

    async def execute(self, proposal: Proposal, actor: str) -> dict[str, Any]:
        hold = await self.holds.release(proposal.target["hold_id"], actor=actor,
                                        reason=f"approved proposal {proposal.id}: {proposal.reason}")
        return {"hold_id": hold.id, "resumed_work": hold.resumed_work,
                "released_intents": hold.released_intents}


class ResumeWorkHandler:
    kind = "work.resume"

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    async def execute(self, proposal: Proposal, actor: str) -> dict[str, Any]:
        t = proposal.target
        ids = [t["work_item_id"]] if t.get("work_item_id") else None
        resumed = await self.runtime.resume_parked(t["execution_id"], ids, actor=actor,
                                                   reason=f"approved proposal {proposal.id}")
        return {"resumed": resumed}


class AbandonWorkHandler:
    kind = "work.abandon"

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    async def execute(self, proposal: Proposal, actor: str) -> dict[str, Any]:
        t = proposal.target
        ex = await self.runtime.abandon(t["execution_id"], t["work_item_id"], actor=actor,
                                        reason=f"approved proposal {proposal.id}: {proposal.reason}")
        return {"execution_status": ex.status.value}


class CompensateExecutionHandler:
    kind = "execution.compensate"

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    async def execute(self, proposal: Proposal, actor: str) -> dict[str, Any]:
        ex = await self.runtime.compensate(proposal.target["execution_id"], actor=actor,
                                           reason=f"approved proposal {proposal.id}: {proposal.reason}")
        return {"execution_status": ex.status.value}
