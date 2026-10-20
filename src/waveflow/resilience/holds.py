"""Holds: freeze work at a boundary until released (plan items P3.4, P3.6, P3.8).

A hold is a control-plane record, not workflow state. Work that meets an active hold is **parked**
(it keeps its place and its history, consumes no resources) and is resumed when the hold is
released. Releasing is done by a human, by a release strategy (P3.7) or by an approved proposal (P3.9).
"""

from __future__ import annotations

import fnmatch
import logging
import time
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.container import ServiceProvider
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore, KeyValueStore
from DnA Workflows.runtime.contracts import GateDecision, GateRequest, RuntimeSignals
from DnA Workflows.runtime.models import FaultInfo, JournalEvent

log = logging.getLogger(__name__)

COLLECTION = "holds"


class HoldScope(StrEnum):
    GLOBAL = "global"              # everything, including ingress
    WORKFLOW = "workflow"          # definition id pattern
    EXECUTION = "execution"        # one execution id
    ACTIVITY_TYPE = "activity_type"
    DEPENDENCY = "dependency"      # e.g. "http:sap-*", "rabbitmq:orders"
    INGRESS = "ingress"            # logical endpoint pattern: stop consuming


class HoldStatus(StrEnum):
    ACTIVE = "active"
    RELEASED = "released"


class Hold(BaseModel):
    id: str
    scope: HoldScope
    target: str = "*"
    reason: str
    status: HoldStatus = HoldStatus.ACTIVE
    source: str = "manual"                      # manual | auto-trip | strategy | ai | api
    created_by: str | None = None
    created_at: datetime
    release_strategy: str = "manual"
    release_options: dict[str, Any] = Field(default_factory=dict)
    strategy_state: dict[str, Any] = Field(default_factory=dict)
    fault: FaultInfo | None = None
    released_at: datetime | None = None
    released_by: str | None = None
    release_reason: str | None = None
    resumed_work: int = 0
    released_intents: int = 0

    def matches(self, request: GateRequest) -> bool:
        if self.status is not HoldStatus.ACTIVE:
            return False
        s = self.scope
        if s is HoldScope.GLOBAL:
            return True
        if s is HoldScope.INGRESS:
            return request.subject == "ingress" and _glob(request.endpoint, self.target)
        if request.subject == "ingress":
            return False
        if s is HoldScope.DEPENDENCY:
            return _glob(request.dependency, self.target)
        if s is HoldScope.EXECUTION:
            return request.execution_id == self.target
        if request.subject == "intent":
            return False  # intents carry no definition/activity information
        if s is HoldScope.WORKFLOW:
            return _glob(request.definition_id, self.target)
        return _glob(request.activity_type, self.target)  # ACTIVITY_TYPE


def _glob(value: str | None, pattern: str) -> bool:
    return value is not None and fnmatch.fnmatchcase(value, pattern)


class HoldNotFound(LookupError):
    pass


class HoldService:
    def __init__(self, store: KeyValueStore, journal: JournalStore, executions: ExecutionStore, clock: Clock,
                 ids: IdGenerator, signals: RuntimeSignals, services: ServiceProvider,
                 cache_seconds: float = 1.0) -> None:
        self.store = store
        self.journal = journal
        self.executions = executions
        self.clock = clock
        self.ids = ids
        self.signals = signals
        self.services = services     # runtime/outbox/release policy are resolved lazily (no cycles)
        self.cache_seconds = cache_seconds
        self._cache: list[Hold] | None = None
        self._cache_at = 0.0

    # -- queries ---------------------------------------------------------------------------------
    async def list(self, *, active_only: bool = False) -> list[Hold]:
        holds = [Hold.model_validate(d) for d in await self.store.list(COLLECTION)]
        holds.sort(key=lambda h: (h.created_at, h.id))
        return [h for h in holds if h.status is HoldStatus.ACTIVE] if active_only else holds

    async def get(self, hold_id: str) -> Hold:
        data = await self.store.get(COLLECTION, hold_id)
        if data is None:
            raise HoldNotFound(f"Hold '{hold_id}' not found")
        return Hold.model_validate(data)

    async def active(self) -> list[Hold]:
        """Active holds, cached briefly: the gate is evaluated on every step. Other nodes' changes
        become visible within ``cache_seconds``; local changes invalidate immediately."""
        now = time.monotonic()
        if self._cache is None or now - self._cache_at > self.cache_seconds:
            self._cache = await self.list(active_only=True)
            self._cache_at = now
        return self._cache

    async def match(self, request: GateRequest) -> Hold | None:
        return next((h for h in await self.active() if h.matches(request)), None)

    async def active_dependency_hold(self, dependency: str) -> Hold | None:
        return await self.match(GateRequest(subject="work", dependency=dependency))

    # -- commands --------------------------------------------------------------------------------
    async def place(self, scope: HoldScope | str, target: str = "*", reason: str = "", *,
                    actor: str | None = None, source: str = "manual", release_strategy: str | None = None,
                    release_options: dict[str, Any] | None = None, fault: FaultInfo | None = None) -> Hold:
        """Place a hold. Idempotent: an active hold with the same scope and target is returned."""
        scope = HoldScope(scope)
        for existing in await self.list(active_only=True):
            if existing.scope is scope and existing.target == target:
                return existing
        if release_strategy is None:
            release_strategy, defaults = self._release_policy().resolve(scope, target)
            release_options = {**defaults, **(release_options or {})}
        hold = Hold(id=self.ids.new_id("hold"), scope=scope, target=target, reason=reason or "manual hold",
                    source=source, created_by=actor, created_at=self.clock.now(),
                    release_strategy=release_strategy, release_options=release_options or {}, fault=fault)
        await self._save(hold)
        await self._journal("hold.placed", hold, actor)
        log.warning("Hold %s placed on %s '%s': %s", hold.id, scope.value, target, hold.reason)
        return hold

    async def trip(self, dependency: str, fault: FaultInfo) -> Hold:
        """Automatic dependency hold after the retry budget is exhausted (P3.6)."""
        return await self.place(HoldScope.DEPENDENCY, dependency,
                                f"auto-trip: {fault.summary()}", source="auto-trip", actor="policy", fault=fault)

    async def update_strategy_state(self, hold: Hold) -> None:
        await self._save(hold)

    async def release(self, hold_id: str, *, actor: str | None = None, reason: str = "released") -> Hold:
        """Release a hold and resume everything it parked: workflow work and outbox intents."""
        hold = await self.get(hold_id)
        if hold.status is HoldStatus.RELEASED:
            return hold
        hold.status = HoldStatus.RELEASED
        hold.released_at = self.clock.now()
        hold.released_by = actor
        hold.release_reason = reason
        await self._save(hold)

        from DnA Workflows.runtime.engine import WorkflowRuntime   # lazy: avoid construction cycles
        from DnA Workflows.runtime.outbox import OutboxProcessor
        runtime = self.services.get(WorkflowRuntime)
        resumed = 0
        for execution_id in await self._in_parking_order(hold.id):
            try:
                resumed += len(await runtime.resume_parked(execution_id, hold_id=hold.id,
                                                           reason=f"hold {hold.id} released: {reason}",
                                                           actor=actor))
            except Exception:  # one broken execution must not block the others
                log.exception("Failed resuming execution %s after releasing %s", execution_id, hold.id)
        outbox = self.services.get_optional(OutboxProcessor)
        released = await outbox.release(hold_id=hold.id, reason=reason, actor=actor) if outbox else []
        hold.resumed_work = resumed
        hold.released_intents = len(released)
        await self._save(hold)
        await self._journal("hold.released", hold, actor)
        self.signals.notify("holds")
        return hold

    async def resume_orphans(self) -> int:
        """Resume work parked behind holds that are released or gone.

        ``release`` marks the hold released *before* resuming its work; if the process dies in
        between, this sweep (run by the release coordinator) finishes the job – crash-safe release.
        """
        from DnA Workflows.runtime.engine import WorkflowRuntime
        active = {h.id for h in await self.list(active_only=True)}
        orphaned: dict[str, str] = {}
        for execution_id, item_id in await self.executions.find_parked():
            ex = await self.executions.get(execution_id)
            item = ex.item(item_id) if ex else None
            if item is not None and item.hold_id is not None and item.hold_id not in active:
                orphaned.setdefault(execution_id, item.hold_id)
        runtime = self.services.get(WorkflowRuntime)
        resumed = 0
        for execution_id, hold_id in orphaned.items():
            try:
                resumed += len(await runtime.resume_parked(execution_id, hold_id=hold_id,
                                                           reason=f"hold {hold_id} no longer active",
                                                           actor="release-coordinator"))
            except Exception:
                log.exception("Failed resuming orphaned work in %s", execution_id)
        return resumed

    # -- internals -------------------------------------------------------------------------------
    async def _in_parking_order(self, hold_id: str) -> list[str]:
        """Executions parked behind ``hold_id``, first parked first (FIFO fairness on release)."""
        first_parked: dict[str, Any] = {}
        for execution_id, item_id in await self.executions.find_parked(hold_id=hold_id):
            ex = await self.executions.get(execution_id)
            item = ex.item(item_id) if ex else None
            when = (item.parked_at if item and item.parked_at else None) or self.clock.now()
            if execution_id not in first_parked or when < first_parked[execution_id]:
                first_parked[execution_id] = when
        return [e for e, _ in sorted(first_parked.items(), key=lambda kv: (kv[1], kv[0]))]

    def _release_policy(self) -> Any:
        from DnA Workflows.resilience.release import ReleasePolicyResolver
        return self.services.get(ReleasePolicyResolver)

    async def _save(self, hold: Hold) -> None:
        await self.store.put(COLLECTION, hold.id, hold.model_dump(mode="json"))
        self._cache = None
        self.signals.notify("holds")

    async def _journal(self, kind: str, hold: Hold, actor: str | None) -> None:
        await self.journal.append([JournalEvent(
            id=self.ids.new_id("ev"), kind=kind, timestamp=self.clock.now(),
            data={"hold": hold.model_dump(mode="json"), "actor": actor})])


class HoldGate:
    """The :class:`WorkGate` implementation backed by holds (Override of the runtime default)."""

    def __init__(self, holds: HoldService) -> None:
        self.holds = holds

    async def evaluate(self, request: GateRequest) -> GateDecision | None:
        hold = await self.holds.match(request)
        if hold is None:
            return None
        return GateDecision(hold_id=hold.id, reason=f"{hold.scope.value} hold '{hold.target}': {hold.reason}")
