"""Resilience feature: freeze / investigate / unpause (plan phase P3).

Example configuration::

    resilience:
      retry:
        dependency_unavailable: {max_attempts: 3, base_delay: 2}
      dependencies:
        "http:sap-*": {max_attempts: 5, max_delay: 120}
      trip_on: [dependency_unavailable]
      classification:
        mycompany.errors.LockedRecord: transient
      release:
        default: manual
        rules:
          - {pattern: "http:*", strategy: probe-auto, options: {successes_required: 2, interval: 30}}
          - {pattern: "ai:*", strategy: ttl, options: {seconds: 600}}
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import HostedService
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore, KeyValueStore
from DnA Workflows.resilience.classification import (ConfiguredClassifier, DataErrorClassifier,
                                                NetworkErrorClassifier, StatusCodeClassifier)
from DnA Workflows.resilience.holds import HoldGate, HoldScope, HoldService
from DnA Workflows.resilience.policy import RetryBudgetPolicy, RetryRule
from DnA Workflows.resilience.proposals import (AbandonWorkHandler, CompensateExecutionHandler, ProposalHandler,
                                           ProposalService, ReleaseHoldHandler, ResumeWorkHandler)
from DnA Workflows.resilience.release import (BUILTIN_STRATEGIES, HealthProbe, ProbeRegistry, ReleaseCoordinator,
                                         ReleasePolicyResolver, ReleaseRule, ReleaseStrategy)
from DnA Workflows.runtime.contracts import FailurePolicy, FaultClassifier, RuntimeSignals, WorkGate
from DnA Workflows.runtime.models import FaultKind


class ReleaseRuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pattern: str
    strategy: str
    options: dict[str, Any] = Field(default_factory=dict)
    scope: HoldScope = HoldScope.DEPENDENCY


class ReleaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str = "manual"
    default_options: dict[str, Any] = Field(default_factory=dict)
    rules: list[ReleaseRuleConfig] = Field(default_factory=list)


class ResilienceOptions(FeatureOptions):
    retry: dict[FaultKind, RetryRule] = Field(default_factory=dict)
    dependencies: dict[str, dict[str, Any]] = Field(default_factory=dict)
    trip_on: list[FaultKind] = Field(default_factory=lambda: [FaultKind.DEPENDENCY_UNAVAILABLE])
    classification: dict[str, FaultKind] = Field(default_factory=dict)
    #: Treat ValueError/KeyError/TypeError/validation errors as permanent data errors.
    classify_data_errors: bool = True
    release: ReleaseConfig = Field(default_factory=ReleaseConfig)
    coordinator_interval: float = 5.0
    gate_cache_seconds: float = 1.0
    background_services: bool = True


class ResilienceFeature(Feature):
    """Fault classification, retry budgets, holds, parking and pluggable release strategies."""

    name = "resilience"
    depends_on = ("runtime",)
    Options = ResilienceOptions

    def configure(self, services: ServiceCollection) -> None:
        o: ResilienceOptions = self.options  # type: ignore[assignment]

        # classifiers – configured mappings first so operators can override built-in opinions
        if o.classification:
            services.add_contributor(FaultClassifier, ConfiguredClassifier(dict(o.classification)),
                                     key="configured")
        services.add_contributor(FaultClassifier, StatusCodeClassifier, key="status-code")
        services.add_contributor(FaultClassifier, NetworkErrorClassifier, key="network")
        if o.classify_data_errors:
            services.add_contributor(FaultClassifier, DataErrorClassifier, key="data")

        services.add(HoldService, lambda p: HoldService(
            p.get(KeyValueStore), p.get(JournalStore), p.get(ExecutionStore), p.get(Clock), p.get(IdGenerator),
            p.get(RuntimeSignals), p, cache_seconds=o.gate_cache_seconds))
        services.replace(WorkGate, HoldGate)
        services.replace(FailurePolicy, lambda p: RetryBudgetPolicy(
            p.get(HoldService), rules=dict(o.retry), dependency_rules=dict(o.dependencies),
            trip_on=set(o.trip_on)))

        for strategy in BUILTIN_STRATEGIES:
            services.add_contributor(ReleaseStrategy, strategy, key=strategy.name)
        services.add(ProbeRegistry, lambda p: ProbeRegistry(p.get_all(HealthProbe)))
        configured = [ReleaseRule(r.pattern, r.strategy, r.options, r.scope) for r in o.release.rules]
        services.add(ReleasePolicyResolver, lambda p: ReleasePolicyResolver(
            configured, p.get_all(ReleaseRule), o.release.default, o.release.default_options,
            known={s.name for s in p.get_all(ReleaseStrategy)}))

        services.add_contributor(ProposalHandler, lambda p: ReleaseHoldHandler(p.get(HoldService)),
                                 key="hold.release")
        services.add_contributor(ProposalHandler, lambda p: ResumeWorkHandler(_runtime(p)), key="work.resume")
        services.add_contributor(ProposalHandler, lambda p: AbandonWorkHandler(_runtime(p)), key="work.abandon")
        services.add_contributor(ProposalHandler, lambda p: CompensateExecutionHandler(_runtime(p)),
                                 key="execution.compensate")
        services.add(ProposalService, lambda p: ProposalService(
            p.get(KeyValueStore), p.get(JournalStore), p.get(Clock), p.get(IdGenerator),
            p.get_all(ProposalHandler)))
        services.add(ReleaseCoordinator, lambda p: ReleaseCoordinator(
            p.get(HoldService), p.get_all(ReleaseStrategy), p.get(ProbeRegistry), p.get(Clock),
            p.get(ProposalService), interval=o.coordinator_interval))
        if o.background_services:
            services.add_contributor(HostedService, lambda p: p.get(ReleaseCoordinator),
                                     key="resilience.coordinator")


def _runtime(p: Any) -> Any:
    from DnA Workflows.runtime.engine import WorkflowRuntime
    return p.get(WorkflowRuntime)


def add_health_probe(services: ServiceCollection, probe: Any, *, key: str | None = None) -> None:
    """Helper for modules contributing a health probe (P3.7)."""
    services.add_contributor(HealthProbe, probe, key=key)


def add_release_rule(services: ServiceCollection, rule: ReleaseRule, *, key: str | None = None) -> None:
    """Helper for modules contributing a default release rule for their dependencies (P3.7)."""
    services.add_contributor(ReleaseRule, rule, key=key or f"{rule.scope}:{rule.pattern}")
