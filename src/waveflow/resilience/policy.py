"""Retry budget + park + auto-trip failure policy (plan items P3.3, P3.6).

Decision flow for one failure::

    dependency already held?  ──yes──▶ Park behind that hold (do not even retry)
            │no
    attempts < budget?        ──yes──▶ RetryAfter(backoff)   (short, bounded – Polly-like)
            │no (budget exhausted)
    on_exhausted == fail      ──yes──▶ Fail (faulted, still retryable by an operator)
            │no
    kind in trip_on & dependency? ─yes─▶ place a Dependency Hold, Park behind it
            │no
    Park (no hold; resumed per item)
"""

from __future__ import annotations

import fnmatch
import random
from datetime import timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from DnA Workflows.runtime.contracts import Fail, FailureContext, FailureDecision, Park, RetryAfter
from DnA Workflows.runtime.models import FaultKind


class RetryRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = 3              # total attempts, including the first one
    base_delay: float = 1.0            # seconds
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: float = 0.1                # ± fraction of the delay
    on_exhausted: Literal["park", "fail"] = "park"

    def merged(self, override: dict[str, Any] | None) -> "RetryRule":
        return self.model_copy(update=override) if override else self

    def delay_for(self, attempt: int, retry_after: float | None, rng: random.Random) -> timedelta:
        delay = min(self.max_delay, self.base_delay * (self.multiplier ** max(0, attempt - 1)))
        if self.jitter:
            delay *= 1 + rng.uniform(-self.jitter, self.jitter)
        if retry_after is not None:
            delay = max(delay, retry_after)   # never retry earlier than the dependency asked
        return timedelta(seconds=max(0.0, delay))


DEFAULT_RULES: dict[FaultKind, RetryRule] = {
    FaultKind.TRANSIENT: RetryRule(max_attempts=3, base_delay=1, max_delay=30),
    FaultKind.DEPENDENCY_UNAVAILABLE: RetryRule(max_attempts=3, base_delay=2, max_delay=30),
    FaultKind.THROTTLED: RetryRule(max_attempts=5, base_delay=5, max_delay=300),
    FaultKind.PERMANENT: RetryRule(max_attempts=1, on_exhausted="fail"),
    FaultKind.UNKNOWN: RetryRule(max_attempts=2, base_delay=2, max_delay=30),
}


class RetryBudgetPolicy:
    def __init__(self, hold_service: Any, *, rules: dict[FaultKind, RetryRule] | None = None,
                 dependency_rules: dict[str, dict[str, Any]] | None = None,
                 trip_on: set[FaultKind] | None = None, rng: random.Random | None = None) -> None:
        self.holds = hold_service
        self.rules = {**DEFAULT_RULES, **(rules or {})}
        self.dependency_rules = dependency_rules or {}
        self.trip_on = trip_on if trip_on is not None else {FaultKind.DEPENDENCY_UNAVAILABLE}
        self.rng = rng or random.Random()

    def rule_for(self, context: FailureContext) -> RetryRule:
        rule = self.rules.get(context.fault.kind, self.rules[FaultKind.UNKNOWN])
        if context.dependency:
            for pattern, override in self.dependency_rules.items():
                if fnmatch.fnmatchcase(context.dependency, pattern):
                    rule = rule.merged(override)
                    break
        return rule.merged(context.retry_override)

    async def decide(self, context: FailureContext) -> FailureDecision:
        fault = context.fault
        dependency = context.dependency or fault.dependency
        if dependency and fault.kind is not FaultKind.PERMANENT:
            existing = await self.holds.active_dependency_hold(dependency)
            if existing is not None:
                return Park(f"dependency '{dependency}' is held: {existing.reason}", existing.id)

        rule = self.rule_for(context)
        if context.attempt < rule.max_attempts:
            return RetryAfter(rule.delay_for(context.attempt, fault.retry_after, self.rng),
                              f"{fault.kind.value} (attempt {context.attempt}/{rule.max_attempts})")
        if rule.on_exhausted == "fail":
            return Fail(f"{fault.summary()} (after {context.attempt} attempt(s))")
        if dependency and fault.kind in self.trip_on:
            hold = await self.holds.trip(dependency, fault)
            return Park(f"dependency '{dependency}' unavailable after {context.attempt} attempts; "
                        f"hold {hold.id} placed", hold.id)
        return Park(f"retry budget exhausted after {context.attempt} attempts: {fault.summary()}")
