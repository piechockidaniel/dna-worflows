"""Runtime feature: composes the engine and registers safe defaults (plan phase P1)."""

from __future__ import annotations

from DnA Workflows.kernel.clock import Clock, SystemClock
from DnA Workflows.kernel.container import ServiceCollection, ServiceProvider
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import CallbackService, HostedService
from DnA Workflows.kernel.ids import IdGenerator, UuidIdGenerator
from DnA Workflows.persistence.contracts import (DefinitionStore, ExecutionStore, JournalStore, KeyValueStore,
                                            OutboxStore)
from DnA Workflows.persistence.memory import (InMemoryDefinitionStore, InMemoryExecutionStore,
                                         InMemoryJournalStore, InMemoryKeyValueStore,
                                         InMemoryOutboxStore, InMemoryState)
from DnA Workflows.runtime.activity import ActivityFault, ActivityRegistry, ActivityType
from DnA Workflows.runtime.builtins import BUILTIN_ACTIVITIES
from DnA Workflows.runtime.contracts import (ActivityMiddleware, DeliveryMiddleware, FailurePolicy, FaultClassifier,
                                        FaultImmediatelyPolicy, IdOnlySpanFactory, OpenGate, RuntimeSignals,
                                        SpanFactory, WorkGate)
from DnA Workflows.runtime.engine import EngineOptions, WorkflowRuntime
from DnA Workflows.runtime.models import FaultInfo, WorkflowDefinition
from DnA Workflows.runtime.outbox import IntentHandler, OutboxProcessor
from DnA Workflows.runtime.registry import WorkflowRegistry
from DnA Workflows.runtime.router import StimulusRouter
from DnA Workflows.runtime.timers import TimerService


class RuntimeOptions(FeatureOptions):
    host_id: str | None = None
    timer_interval: float = 0.5
    outbox_interval: float = 0.5
    max_steps_per_drain: int = 1000
    #: Start timer/outbox loops with the shell. Tests switch this off and tick manually.
    background_services: bool = True
    #: Directories/files with workflow definitions (*.json, *.yaml, *.yml) published at startup.
    definitions: list[str] = []


class ActivityFaultClassifier:
    """Recognises :class:`ActivityFault` raised explicitly by activities."""

    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo | None:
        if isinstance(error, ActivityFault):
            return FaultInfo(kind=error.kind, error_type=type(error).__name__, message=str(error),
                             dependency=error.dependency or dependency, retry_after=error.retry_after)
        return None


def add_activity(services: ServiceCollection, *activities: type) -> None:
    """Contribute activity types (Extend axis). Used by every feature that ships activities."""
    for activity in activities:
        services.add_contributor(ActivityType, ActivityType(activity), key=activity.type_name)


class RuntimeFeature(Feature):
    """Workflow engine, stimulus router, outbox processor and timers."""

    name = "runtime"
    Options = RuntimeOptions

    def configure(self, services: ServiceCollection) -> None:
        o: RuntimeOptions = self.options  # type: ignore[assignment]
        engine_options = EngineOptions(max_steps_per_drain=o.max_steps_per_drain)
        if o.host_id:
            engine_options.host_id = o.host_id

        services.try_add(Clock, SystemClock)
        services.try_add(IdGenerator, UuidIdGenerator)
        services.add_instance(EngineOptions, engine_options)
        services.try_add_instance(RuntimeSignals, RuntimeSignals())

        # persistence defaults (a persistence.* feature overrides them with `replace`)
        state = InMemoryState()
        services.try_add(InMemoryState, lambda p: state)
        services.try_add(ExecutionStore, lambda p: InMemoryExecutionStore(p.get(InMemoryState)))
        services.try_add(OutboxStore, lambda p: InMemoryOutboxStore(p.get(InMemoryState)))
        services.try_add(JournalStore, lambda p: InMemoryJournalStore(p.get(InMemoryState)))
        services.try_add(DefinitionStore, lambda p: InMemoryDefinitionStore(p.get(InMemoryState)))
        services.try_add(KeyValueStore, lambda p: InMemoryKeyValueStore(p.get(InMemoryState)))

        # resilience seams – safe defaults, overridden by the resilience feature
        services.try_add(FailurePolicy, FaultImmediatelyPolicy)
        services.try_add(WorkGate, OpenGate)
        services.try_add(SpanFactory, IdOnlySpanFactory)
        services.add_contributor(FaultClassifier, ActivityFaultClassifier, key="activity-fault")

        add_activity(services, *BUILTIN_ACTIVITIES)
        from DnA Workflows.runtime.subflows import (CancelChildHandler, ChildFinishedHandler, CompensateStep,
                                               RunWorkflow, StartChildHandler)
        add_activity(services, RunWorkflow, CompensateStep)
        services.add_contributor(IntentHandler, lambda p: StartChildHandler(p.get(WorkflowRuntime),
                                                                           p.get(StimulusRouter)),
                                 key="workflow.start_child")
        services.add_contributor(IntentHandler, lambda p: ChildFinishedHandler(p.get(StimulusRouter)),
                                 key="workflow.child_finished")
        services.add_contributor(IntentHandler, lambda p: CancelChildHandler(p.get(WorkflowRuntime)),
                                 key="workflow.cancel_child")
        services.add(ActivityRegistry, _activity_registry)
        services.add(WorkflowRegistry)
        services.add(WorkflowRuntime, lambda p: WorkflowRuntime(
            store=p.get(ExecutionStore), registry=p.get(WorkflowRegistry),
            activities=p.get(ActivityRegistry), services=p, clock=p.get(Clock),
            ids=p.get(IdGenerator), policy=p.get(FailurePolicy), gate=p.get(WorkGate),
            classifiers=p.get_all(FaultClassifier), middleware=p.get_all(ActivityMiddleware),
            signals=p.get(RuntimeSignals), options=p.get(EngineOptions), spans=p.get(SpanFactory)))
        services.add(StimulusRouter)
        services.add(OutboxProcessor, lambda p: OutboxProcessor(
            store=p.get(OutboxStore), journal=p.get(JournalStore), handlers=p.get_all(IntentHandler),
            policy=p.get(FailurePolicy), gate=p.get(WorkGate), classifiers=p.get_all(FaultClassifier),
            clock=p.get(Clock), ids=p.get(IdGenerator), signals=p.get(RuntimeSignals),
            options=p.get(EngineOptions), interval=o.outbox_interval,
            middleware=p.get_all(DeliveryMiddleware)))
        services.add(TimerService, lambda p: TimerService(
            p.get(ExecutionStore), p.get(WorkflowRuntime), p.get(Clock), interval=o.timer_interval))

        paths = list(o.definitions)
        services.add_contributor(HostedService, lambda p: CallbackService(
            lambda: _load_definitions(p, paths)), key="runtime.load-definitions")
        if o.background_services:
            services.add_contributor(HostedService, lambda p: p.get(TimerService), key="runtime.timers")
            services.add_contributor(HostedService, lambda p: p.get(OutboxProcessor), key="runtime.outbox")


def _activity_registry(p: ServiceProvider) -> ActivityRegistry:
    return ActivityRegistry([a.activity for a in p.get_all(ActivityType)])


async def _load_definitions(p: ServiceProvider, paths: list[str]) -> None:
    registry = p.get(WorkflowRegistry)
    await registry.load_persisted()
    for definition in load_definition_files(paths):
        await registry.publish(definition)


def load_definition_files(paths: list[str]) -> list[WorkflowDefinition]:
    """Read workflow definitions from files and directories (sorted, deterministic)."""
    import json
    from pathlib import Path

    import yaml
    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            files.extend(sorted(f for f in path.rglob("*") if f.suffix.lower() in {".json", ".yaml", ".yml"}))
        elif path.exists():
            files.append(path)
        else:
            raise FileNotFoundError(f"Workflow definition path not found: {raw}")
    definitions = []
    for f in files:
        text = f.read_text(encoding="utf-8")
        data = json.loads(text) if f.suffix.lower() == ".json" else yaml.safe_load(text)
        items = data if isinstance(data, list) else [data]
        definitions.extend(WorkflowDefinition.model_validate(item) for item in items)
    return definitions
