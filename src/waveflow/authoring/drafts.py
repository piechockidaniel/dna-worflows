"""Collaborative authoring: drafts, review, publish (plan items P11.1, P11.2).

No single-person bottleneck (G8): every step is a *role*, not a person –

    draft ──submit──▶ in_review ──approve──▶ approved ──publish──▶ published
      ▲                   │
      └── changes_requested (any designer can pick the draft up again)

Anyone with ``workflows.design`` may continue anybody's draft (optimistic ``revision`` prevents lost
updates), anyone with ``workflows.review`` may review (optionally not the last editor – "four eyes"),
anyone with ``workflows.publish`` may publish. Comments and the full history stay with the draft.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.persistence.contracts import DefinitionStore, JournalStore, KeyValueStore
from DnA Workflows.runtime.models import JournalEvent, WorkflowDefinition
from DnA Workflows.runtime.registry import WorkflowRegistry

COLLECTION = "drafts"


class DraftStatus(StrEnum):
    DRAFT = "draft"
    IN_REVIEW = "in_review"
    CHANGES_REQUESTED = "changes_requested"
    APPROVED = "approved"
    PUBLISHED = "published"


class Comment(BaseModel):
    id: str
    author: str
    at: datetime
    text: str
    node: str | None = None


class Review(BaseModel):
    reviewer: str
    at: datetime
    decision: Literal["approve", "request_changes"]
    note: str | None = None


class HistoryEntry(BaseModel):
    at: datetime
    actor: str
    action: str
    detail: str | None = None


class Draft(BaseModel):
    workflow_id: str
    definition: WorkflowDefinition
    status: DraftStatus = DraftStatus.DRAFT
    revision: int = 1
    base_version: int | None = None
    created_by: str
    created_at: datetime
    updated_by: str
    updated_at: datetime
    problems: list[str] = Field(default_factory=list)
    comments: list[Comment] = Field(default_factory=list)
    reviews: list[Review] = Field(default_factory=list)
    history: list[HistoryEntry] = Field(default_factory=list)
    published_version: int | None = None


class DraftConflict(RuntimeError):
    """Someone else saved the draft in the meantime (reload and re-apply)."""


class DraftStateError(RuntimeError):
    pass


class DraftNotFound(LookupError):
    pass


class DraftService:
    def __init__(self, store: KeyValueStore, registry: WorkflowRegistry, definitions: DefinitionStore,
                 journal: JournalStore, clock: Clock, ids: IdGenerator, *, review_required: bool = True,
                 four_eyes: bool = True) -> None:
        self.store = store
        self.registry = registry
        self.definitions = definitions
        self.journal = journal
        self.clock = clock
        self.ids = ids
        self.review_required = review_required
        self.four_eyes = four_eyes

    # -- queries ----------------------------------------------------------------------------------
    async def get(self, workflow_id: str) -> Draft:
        data = await self.store.get(COLLECTION, workflow_id)
        if data is None:
            raise DraftNotFound(f"No draft for workflow '{workflow_id}'")
        return Draft.model_validate(data)

    async def list(self, status: DraftStatus | None = None) -> list[Draft]:
        drafts = [Draft.model_validate(d) for d in await self.store.list(COLLECTION)]
        return [d for d in drafts if status is None or d.status is status]

    async def versions(self, workflow_id: str) -> list[dict[str, Any]]:
        """Published versions (newest first) for history and diff in the Studio."""
        result = []
        for d in await self.definitions.list():
            if d.id == workflow_id:
                result.append({"version": d.version, "name": d.name, "nodes": len(d.nodes),
                               "published_by": d.metadata.get("published_by"),
                               "published_at": d.metadata.get("published_at")})
        return sorted(result, key=lambda v: -v["version"])

    # -- commands ---------------------------------------------------------------------------------
    async def start(self, workflow_id: str, actor: str, definition: WorkflowDefinition | None = None) -> Draft:
        """Open a draft – a copy of the latest published version, or a new workflow."""
        if await self.store.get(COLLECTION, workflow_id) is not None:
            draft = await self.get(workflow_id)
            if draft.status is not DraftStatus.PUBLISHED:
                return draft                       # continue the existing draft (shared, G8)
        base = None
        if definition is None:
            try:
                published = await self.registry.get(workflow_id)
                definition, base = published.model_copy(deep=True), published.version
            except LookupError:
                definition = WorkflowDefinition(id=workflow_id, name=workflow_id)
        now = self.clock.now()
        draft = Draft(workflow_id=workflow_id, definition=definition, base_version=base, created_by=actor,
                      created_at=now, updated_by=actor, updated_at=now,
                      problems=self.registry.validate(definition))
        draft.history.append(HistoryEntry(at=now, actor=actor, action="created",
                                          detail=f"from version {base}" if base else "new workflow"))
        await self._save(draft, actor, "draft.created")
        return draft

    async def save(self, workflow_id: str, definition: WorkflowDefinition, actor: str, revision: int) -> Draft:
        draft = await self.get(workflow_id)
        if draft.revision != revision:
            raise DraftConflict(f"Draft was changed by {draft.updated_by} (revision {draft.revision}, yours {revision})")
        if definition.id != workflow_id:
            raise DraftStateError("Definition id must match the draft's workflow id")
        now = self.clock.now()
        if draft.status in (DraftStatus.IN_REVIEW, DraftStatus.APPROVED):
            draft.reviews.clear()                 # changes invalidate earlier approvals
        draft.definition = definition
        draft.status = DraftStatus.DRAFT
        draft.revision += 1
        draft.updated_by, draft.updated_at = actor, now
        draft.problems = self.registry.validate(definition)
        draft.history.append(HistoryEntry(at=now, actor=actor, action="saved",
                                          detail=f"{len(draft.problems)} problem(s)"))
        await self._save(draft, actor, "draft.saved")
        return draft

    async def submit(self, workflow_id: str, actor: str) -> Draft:
        draft = await self._in(workflow_id, {DraftStatus.DRAFT, DraftStatus.CHANGES_REQUESTED})
        if draft.problems:
            raise DraftStateError("Fix the validation problems before submitting: " + "; ".join(draft.problems))
        draft.status = DraftStatus.APPROVED if not self.review_required else DraftStatus.IN_REVIEW
        return await self._record(draft, actor, "submitted")

    async def review(self, workflow_id: str, actor: str, decision: Literal["approve", "request_changes"],
                     note: str | None = None) -> Draft:
        draft = await self._in(workflow_id, {DraftStatus.IN_REVIEW})
        if self.four_eyes and decision == "approve" and actor == draft.updated_by:
            raise DraftStateError("Four-eyes rule: the last editor cannot approve their own changes")
        draft.reviews.append(Review(reviewer=actor, at=self.clock.now(), decision=decision, note=note))
        draft.status = DraftStatus.APPROVED if decision == "approve" else DraftStatus.CHANGES_REQUESTED
        return await self._record(draft, actor, f"review:{decision}", note)

    async def publish(self, workflow_id: str, actor: str) -> Draft:
        allowed = {DraftStatus.APPROVED} if self.review_required else {DraftStatus.APPROVED, DraftStatus.DRAFT}
        draft = await self._in(workflow_id, allowed)
        if draft.problems:
            raise DraftStateError("Draft has validation problems")
        latest = [v["version"] for v in await self.versions(workflow_id)]
        try:
            latest.append((await self.registry.get(workflow_id)).version)
        except LookupError:
            pass
        version = max(latest, default=0) + 1
        now = self.clock.now()
        definition = draft.definition.model_copy(update={
            "version": version,
            "metadata": {**draft.definition.metadata, "published_by": actor, "published_at": now.isoformat(),
                         "reviewed_by": [r.reviewer for r in draft.reviews if r.decision == "approve"]}})
        await self.registry.publish(definition)
        draft.status = DraftStatus.PUBLISHED
        draft.published_version = version
        draft.definition = definition
        return await self._record(draft, actor, "published", f"version {version}")

    async def comment(self, workflow_id: str, actor: str, text: str, node: str | None = None) -> Draft:
        draft = await self.get(workflow_id)
        draft.comments.append(Comment(id=self.ids.new_id("c"), author=actor, at=self.clock.now(), text=text,
                                      node=node))
        await self._save(draft, actor, "draft.commented")
        return draft

    async def discard(self, workflow_id: str, actor: str) -> None:
        draft = await self.get(workflow_id)
        await self.store.delete(COLLECTION, workflow_id)
        await self._journal("draft.discarded", draft, actor)

    # -- internals --------------------------------------------------------------------------------
    async def _in(self, workflow_id: str, allowed: set[DraftStatus]) -> Draft:
        draft = await self.get(workflow_id)
        if draft.status not in allowed:
            raise DraftStateError(f"Draft is {draft.status.value}; expected one of {sorted(s.value for s in allowed)}")
        return draft

    async def _record(self, draft: Draft, actor: str, action: str, detail: str | None = None) -> Draft:
        draft.history.append(HistoryEntry(at=self.clock.now(), actor=actor, action=action, detail=detail))
        await self._save(draft, actor, f"draft.{action.split(':')[0]}")
        return draft

    async def _save(self, draft: Draft, actor: str, kind: str) -> None:
        await self.store.put(COLLECTION, draft.workflow_id, draft.model_dump(mode="json"))
        await self._journal(kind, draft, actor)

    async def _journal(self, kind: str, draft: Draft, actor: str) -> None:
        await self.journal.append([JournalEvent(
            id=self.ids.new_id("ev"), kind=kind, timestamp=self.clock.now(), definition_id=draft.workflow_id,
            data={"actor": actor, "status": draft.status.value, "revision": draft.revision})])
