"""Pluggable release strategies (plan item P3.7).

How a hold gets released is a *strategy*, chosen per hold when it is placed:

* rules from configuration win (``release.rules``),
* then rules **contributed by modules** (the RabbitMQ plugin may say "my holds auto-release when the
  broker answers again"),
* then the configured default (``manual``).

Strategies are contributors (Extend axis) – adding a new one (say, "release only inside a
maintenance window", or "ask the AI triage agent") is writing one class.
"""

from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.hosting import PeriodicService
from DnA Workflows.resilience.holds import Hold, HoldScope, HoldService

log = logging.getLogger(__name__)


# -- health probes ------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ProbeResult:
    healthy: bool
    detail: str = ""


@runtime_checkable
class HealthProbe(Protocol):
    """Contributor: checks whether a dependency is reachable again."""

    def supports(self, dependency: str) -> bool: ...

    async def check(self, dependency: str) -> ProbeResult: ...


class ProbeRegistry:
    def __init__(self, probes: list[HealthProbe]) -> None:
        self.probes = probes

    def find(self, dependency: str) -> HealthProbe | None:
        return next((p for p in self.probes if p.supports(dependency)), None)

    async def check(self, dependency: str) -> ProbeResult | None:
        probe = self.find(dependency)
        if probe is None:
            return None
        try:
            return await probe.check(dependency)
        except Exception as exc:  # a crashing probe means "not healthy"
            return ProbeResult(False, f"probe error: {type(exc).__name__}: {exc}")


# -- strategies -------------------------------------------------------------------------------------

class ReleaseAction(StrEnum):
    KEEP = "keep"
    RELEASE = "release"
    RECOMMEND = "recommend"


@dataclass(frozen=True, slots=True)
class ReleaseDecision:
    action: ReleaseAction
    reason: str = ""

    @classmethod
    def keep(cls, reason: str = "") -> "ReleaseDecision":
        return cls(ReleaseAction.KEEP, reason)


@dataclass
class ReleaseContext:
    now: datetime
    probes: ProbeRegistry
    state: dict[str, Any] = field(default_factory=dict)   # persisted on the hold between evaluations


@runtime_checkable
class ReleaseStrategy(Protocol):
    name: str

    async def evaluate(self, hold: Hold, context: ReleaseContext) -> ReleaseDecision: ...


class ManualRelease:
    """Only a human (or an approved proposal) releases the hold."""

    name = "manual"

    async def evaluate(self, hold: Hold, context: ReleaseContext) -> ReleaseDecision:
        return ReleaseDecision.keep("manual release required")


class TtlRelease:
    """Release after ``seconds`` – suits throttling ("back off for 5 minutes")."""

    name = "ttl"

    async def evaluate(self, hold: Hold, context: ReleaseContext) -> ReleaseDecision:
        seconds = float(hold.release_options.get("seconds", 300))
        if context.now >= hold.created_at + timedelta(seconds=seconds):
            return ReleaseDecision(ReleaseAction.RELEASE, f"ttl of {seconds:g}s elapsed")
        return ReleaseDecision.keep("ttl not elapsed")


class _ProbeStrategy:
    action = ReleaseAction.RELEASE
    name = ""

    async def evaluate(self, hold: Hold, context: ReleaseContext) -> ReleaseDecision:
        if hold.scope is not HoldScope.DEPENDENCY:
            return ReleaseDecision.keep("probe strategies only apply to dependency holds")
        required = int(hold.release_options.get("successes_required", 2))
        result = await context.probes.check(hold.target)
        if result is None:
            return ReleaseDecision.keep(f"no health probe for '{hold.target}'")
        streak = context.state.get("healthy_streak", 0) + 1 if result.healthy else 0
        context.state["healthy_streak"] = streak
        context.state["last_probe"] = {"healthy": result.healthy, "detail": result.detail,
                                       "at": context.now.isoformat()}
        if streak >= required:
            return ReleaseDecision(self.action, f"'{hold.target}' healthy {streak}x in a row: {result.detail}")
        return ReleaseDecision.keep(f"healthy streak {streak}/{required}: {result.detail}")


class ProbeAutoRelease(_ProbeStrategy):
    """Release automatically after N consecutive healthy probes."""

    name = "probe-auto"
    action = ReleaseAction.RELEASE


class ProbeRecommendRelease(_ProbeStrategy):
    """Probe, but only *recommend* the release – a human approves the proposal."""

    name = "probe-recommend"
    action = ReleaseAction.RECOMMEND


BUILTIN_STRATEGIES = [ManualRelease, TtlRelease, ProbeAutoRelease, ProbeRecommendRelease]


# -- choosing a strategy per hold ------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ReleaseRule:
    """Contributor (Extend axis) or configuration entry: which strategy applies to which holds."""

    pattern: str
    strategy: str
    options: dict[str, Any] = field(default_factory=dict)
    scope: HoldScope = HoldScope.DEPENDENCY


class ReleasePolicyResolver:
    def __init__(self, configured: list[ReleaseRule], contributed: list[ReleaseRule],
                 default_strategy: str = "manual", default_options: dict[str, Any] | None = None,
                 known: set[str] | None = None) -> None:
        self.rules = [*configured, *contributed]
        self.default_strategy = default_strategy
        self.default_options = default_options or {}
        if known is not None:
            for name in [default_strategy, *(r.strategy for r in self.rules)]:
                if name not in known:
                    raise ValueError(f"Unknown release strategy '{name}'. Known: {sorted(known)}")

    def resolve(self, scope: HoldScope, target: str) -> tuple[str, dict[str, Any]]:
        for rule in self.rules:
            if rule.scope is scope and fnmatch.fnmatchcase(target, rule.pattern):
                return rule.strategy, dict(rule.options)
        return self.default_strategy, dict(self.default_options)


# -- the background coordinator ---------------------------------------------------------------------------

class ReleaseCoordinator(PeriodicService):
    name = "release-coordinator"

    def __init__(self, holds: HoldService, strategies: list[ReleaseStrategy], probes: ProbeRegistry,
                 clock: Clock, proposals: Any, interval: float = 5.0) -> None:
        super().__init__()
        self.holds = holds
        self.strategies = {s.name: s for s in strategies}
        self.probes = probes
        self.clock = clock
        self.proposals = proposals
        self.interval = interval
        self.orphans_resumed = 0

    async def tick(self) -> None:
        await self.evaluate_all()

    async def evaluate_all(self) -> list[tuple[str, ReleaseDecision]]:
        results = []
        for hold in await self.holds.list(active_only=True):
            results.append((hold.id, await self.evaluate(hold)))
        self.orphans_resumed = await self.holds.resume_orphans()
        return results

    async def evaluate(self, hold: Hold) -> ReleaseDecision:
        strategy = self.strategies.get(hold.release_strategy)
        if strategy is None:
            return ReleaseDecision.keep(f"unknown strategy '{hold.release_strategy}'")
        now = self.clock.now()
        interval = float(hold.release_options.get("interval", 0))
        last = hold.strategy_state.get("evaluated_at")
        if interval and last and now < datetime.fromisoformat(last) + timedelta(seconds=interval):
            return ReleaseDecision.keep("not due")
        context = ReleaseContext(now=now, probes=self.probes, state=dict(hold.strategy_state))
        decision = await strategy.evaluate(hold, context)
        context.state["evaluated_at"] = now.isoformat()
        context.state["last_decision"] = {"action": decision.action.value, "reason": decision.reason}
        hold.strategy_state = context.state
        if decision.action is ReleaseAction.RELEASE:
            await self.holds.update_strategy_state(hold)
            await self.holds.release(hold.id, actor=f"strategy:{strategy.name}", reason=decision.reason)
        elif decision.action is ReleaseAction.RECOMMEND:
            if not hold.strategy_state.get("proposal_id"):
                proposal = await self.proposals.propose(
                    "hold.release", {"hold_id": hold.id}, decision.reason,
                    proposed_by=f"strategy:{strategy.name}")
                hold.strategy_state["proposal_id"] = proposal.id
            await self.holds.update_strategy_state(hold)
        else:
            await self.holds.update_strategy_state(hold)
        return decision
