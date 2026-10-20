"""The workflow engine (plan items P1.6, P3.5, P3.8, P4.2, P4.3).

Execution model
---------------
* An execution is drained by **one writer at a time** (an in-process lock per execution plus
  optimistic concurrency on ``revision`` in the store, which fences writers in other processes).
* Each step picks the next runnable work item, passes the **gate** (holds), runs the activity
  through the middleware pipeline, and commits **one checkpoint**: execution state + outbox intents
  + journal events (+ inbox keys) atomically.
* Failures are **classified** and handed to the **failure policy**, which decides between retrying
  later (durable timer), parking the work, or faulting the execution. Nothing is dropped and nothing
  hammers a dependency in a tight loop.
* Delivery semantics are at-least-once: an activity may run again if the process crashes between
  running it and committing. Side effects that must not duplicate go through the outbox.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import socket
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.container import ServiceProvider
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.persistence.contracts import ConcurrencyConflict, ExecutionStore
from DnA Workflows.runtime.activity import (Activity, ActivityContext, ActivityFault, ActivityRegistry,
                                       ActivityResult, Compensate, Done, Sleep, Suspend, validate_inputs)
from DnA Workflows.runtime.bindings import BindingScope, resolve
from DnA Workflows.runtime.contracts import (ActivityMiddleware, DefaultFaultClassifier, Fail,
                                        FailureContext, FailurePolicy, FaultClassifier, GateRequest,
                                        IdOnlySpanFactory, Park, RetryAfter, RuntimeSignals, SpanFactory,
                                        WorkGate)
from DnA Workflows.runtime.lifecycle import ACTIVE, RUNNABLE, transition
from DnA Workflows.runtime.models import (Bookmark, Checkpoint, CompensationRecord, ExecutionStatus, FaultInfo,
                                     FaultKind, JournalEvent, NodeSpec, OutboxIntent, WorkflowDefinition,
                                     WorkflowExecution, WorkItem, WorkState)
from DnA Workflows.runtime.registry import WorkflowRegistry
from DnA Workflows.tracing import (TraceContext, reset_correlation, reset_trace, use_correlation,
                              use_trace)

log = logging.getLogger(__name__)


@dataclass
class EngineOptions:
    host_id: str = field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    max_steps_per_drain: int = 1000


@dataclass
class _Step:
    intents: list[OutboxIntent] = field(default_factory=list)
    journal: list[JournalEvent] = field(default_factory=list)
    inbox_keys: list[str] = field(default_factory=list)


class ExecutionNotFound(LookupError):
    pass


class WorkflowRuntime:
    def __init__(self, store: ExecutionStore, registry: WorkflowRegistry, activities: ActivityRegistry,
                 services: ServiceProvider, clock: Clock, ids: IdGenerator, policy: FailurePolicy,
                 gate: WorkGate, classifiers: list[FaultClassifier],
                 middleware: list[ActivityMiddleware], signals: RuntimeSignals,
                 options: EngineOptions | None = None, spans: SpanFactory | None = None) -> None:
        self.store = store
        self.registry = registry
        self.activities = activities
        self.services = services
        self.clock = clock
        self.ids = ids
        self.policy = policy
        self.gate = gate
        self.classifiers = classifiers
        self.middleware = sorted(middleware, key=lambda m: getattr(m, "order", 0))
        self.signals = signals
        self.options = options or EngineOptions()
        self.spans = spans or IdOnlySpanFactory()
        self._locks: dict[str, asyncio.Lock] = {}
        self._lock_users: dict[str, int] = {}

    # ================================================================================================
    # Public API
    # ================================================================================================
    async def start(self, definition_id: str, *, input: dict[str, Any] | None = None,
                    version: int | None = None, correlation_id: str | None = None,
                    traceparent: str | None = None, tracestate: str | None = None,
                    start_node: str | None = None, stimulus: dict[str, Any] | None = None,
                    inbox_key: str | None = None, causation_id: str | None = None,
                    run: bool = True, execution_id: str | None = None, parent_execution_id: str | None = None,
                    parent_node_id: str | None = None, notify_parent: bool = False) -> WorkflowExecution:
        definition = await self.registry.get(definition_id, version)
        now = self.clock.now()
        incoming = TraceContext.parse(traceparent, tracestate)
        ex_id = execution_id or self.ids.new_id("wf")
        root = self.spans.instant(f"workflow.start {definition.id}", incoming, {
            "DnA Workflows.execution_id": ex_id, "DnA Workflows.workflow": definition.id,
            "DnA Workflows.correlation_id": correlation_id or ex_id})
        ex = WorkflowExecution(
            id=ex_id, definition_id=definition.id, definition_version=definition.version,
            input=dict(input or {}), variables=_clone(definition.variables),
            correlation_id=correlation_id or ex_id, trace_id=root.trace_id,
            traceparent=root.traceparent, created_at=now, updated_at=now,
            parent_execution_id=parent_execution_id, parent_node_id=parent_node_id, notify_parent=notify_parent)
        step = _Step(inbox_keys=[inbox_key] if inbox_key else [])
        nodes = [start_node] if start_node else definition.roots()
        for node_id in nodes:
            if node_id not in definition.nodes:
                raise LookupError(f"Node '{node_id}' not found in workflow '{definition.id}'")
            payload = {"stimulus": stimulus} if stimulus is not None and node_id == start_node else {}
            self._enqueue(ex, definition, node_id, payload, root, causation_id, now)
        step.journal.append(self._event("execution.started", ex, trace=root, data={
            "definition_version": definition.version, "start_nodes": nodes,
            "parent_traceparent": traceparent, "causation_id": causation_id,
            "parent_execution_id": parent_execution_id}))
        await self._commit(ex, step, expected_revision=0)
        if run:
            return await self.run(ex.id)
        return ex

    async def resume(self, execution_id: str, bookmark_id: str, payload: dict[str, Any] | None = None,
                     *, inbox_key: str | None = None, traceparent: str | None = None,
                     causation_id: str | None = None, run: bool = True) -> WorkflowExecution | None:
        """Resume a bookmark. Returns ``None`` when the bookmark no longer exists."""
        async with self._lock(execution_id):
            ex = await self._load(execution_id)
            bookmark = next((b for b in ex.bookmarks if b.id == bookmark_id), None)
            if bookmark is None or ex.status.is_terminal:
                return None
            now = self.clock.now()
            definition = await self.registry.get(ex.definition_id, ex.definition_version)
            ex.bookmarks.remove(bookmark)
            parent = self.spans.instant(f"workflow.resume {ex.definition_id}", _resume_parent(ex, traceparent), {
                "DnA Workflows.execution_id": ex.id, "DnA Workflows.workflow": ex.definition_id,
                "DnA Workflows.correlation_id": ex.correlation_id or "", "DnA Workflows.bookmark": bookmark.key,
                "DnA Workflows.link_traceparent": traceparent or ""})
            self._enqueue(ex, definition, bookmark.node_id,
                          {"stimulus": payload or {}, "bookmark": bookmark.payload,
                           "waiting_since": (bookmark.created_at or now).isoformat()},
                          parent, causation_id, now, kind="resume")
            step = _Step(inbox_keys=[inbox_key] if inbox_key else [])
            step.journal.append(self._event("bookmark.resumed", ex, node_id=bookmark.node_id, trace=parent,
                                            data={"bookmark_id": bookmark.id, "key": bookmark.key,
                                                  "link_traceparent": traceparent,
                                                  "waited_ms": _ms(now - (bookmark.created_at or now))}))
            self._settle(ex, now, step)
            await self._commit(ex, step)
        if run:
            return await self.run(execution_id)
        return ex

    async def run(self, execution_id: str) -> WorkflowExecution:
        """Drain runnable work until the execution waits, parks, completes or faults."""
        async with self._lock(execution_id):
            ex = await self._load(execution_id)
            for _ in range(self.options.max_steps_per_drain):
                if ex.status.is_terminal:
                    return ex
                now = self.clock.now()
                item = _next_runnable(ex, now)
                step = _Step()
                if item is None:
                    if self._settle(ex, now, step):
                        await self._commit(ex, step)
                    return ex
                definition = await self.registry.get(ex.definition_id, ex.definition_version)
                revision = ex.revision
                await self._process(ex, definition, item, now, step)
                self._settle(ex, self.clock.now(), step)
                try:
                    await self._commit(ex, step, expected_revision=revision)
                except ConcurrencyConflict:
                    log.warning("Execution %s changed concurrently; reloading", execution_id)
                    ex = await self._load(execution_id)
            log.warning("Execution %s reached max steps per drain; the timer will continue it",
                        execution_id)
            return ex

    async def get(self, execution_id: str) -> WorkflowExecution:
        return await self._load(execution_id)

    # -- operator actions (P3.8) --------------------------------------------------------------------
    async def resume_parked(self, execution_id: str, work_item_ids: Sequence[str] | None = None, *,
                            hold_id: str | None = None, reason: str = "resumed",
                            actor: str | None = None, run: bool = True) -> list[str]:
        """Move parked work back to runnable. Filters: explicit ids and/or the hold that parked it."""
        async with self._lock(execution_id):
            ex = await self._load(execution_id)
            now = self.clock.now()
            step = _Step()
            resumed = []
            for item in ex.parked():
                if work_item_ids is not None and item.id not in work_item_ids:
                    continue
                if hold_id is not None and item.hold_id != hold_id:
                    continue
                self._reactivate(ex, item, now, step, "work.resumed", reason, actor)
                resumed.append(item.id)
            if resumed:
                self._settle(ex, now, step)
                await self._commit(ex, step)
        if resumed and run:
            await self.run(execution_id)
        return resumed

    async def retry_faulted(self, execution_id: str, *, reason: str = "operator retry",
                            actor: str | None = None, run: bool = True) -> list[str]:
        """Give faulted work another chance after the root cause was fixed."""
        async with self._lock(execution_id):
            ex = await self._load(execution_id)
            # while compensating, only the undo steps are retried (the step that triggered the saga stays failed)
            faulted = [w for w in ex.work if w.state is WorkState.FAULTED
                       and (not ex.compensation_mode or w.kind == "compensate")]
            if ex.status is not ExecutionStatus.FAULTED or not faulted:
                return []
            now = self.clock.now()
            step = _Step()
            ex.status = ExecutionStatus.RUNNING
            ex.fault = None
            ex.completed_at = None
            for item in faulted:
                self._reactivate(ex, item, now, step, "work.retried", reason, actor)
            self._settle(ex, now, step)
            await self._commit(ex, step)
        if run:
            await self.run(execution_id)
        return [w.id for w in faulted]

    async def abandon(self, execution_id: str, work_item_id: str, *, reason: str = "abandoned",
                      actor: str | None = None) -> WorkflowExecution:
        async with self._lock(execution_id):
            ex = await self._load(execution_id)
            item = ex.item(work_item_id)
            if item is None:
                raise LookupError(f"Work item {work_item_id} not found in {execution_id}")
            now = self.clock.now()
            step = _Step()
            if ex.status is ExecutionStatus.FAULTED:
                ex.status = ExecutionStatus.RUNNING
            transition(item, WorkState.ABANDONED)
            ex.work.remove(item)
            step.journal.append(self._event("work.abandoned", ex, item=item,
                                            data={"reason": reason, "actor": actor}))
            if not any(w.state in ACTIVE or w.state is WorkState.PARKED for w in ex.work) \
                    and not ex.bookmarks:
                self._finish(ex, ExecutionStatus.CANCELLED, now, step,
                             fault=FaultInfo(kind=FaultKind.PERMANENT, error_type="Abandoned",
                                             message=reason, at=now))
            else:
                self._settle(ex, now, step)
            await self._commit(ex, step)
            return ex

    async def cancel(self, execution_id: str, *, reason: str = "cancelled",
                     actor: str | None = None) -> WorkflowExecution:
        async with self._lock(execution_id):
            ex = await self._load(execution_id)
            if ex.status.is_terminal:
                return ex
            now = self.clock.now()
            step = _Step()
            ex.work.clear()
            ex.bookmarks.clear()
            self._cancel_children(ex, reason, now, step)
            self._finish(ex, ExecutionStatus.CANCELLED, now, step, data={"reason": reason, "actor": actor})
            await self._commit(ex, step)
            return ex

    async def compensate(self, execution_id: str, *, reason: str = "compensate", actor: str | None = None,
                         run: bool = True) -> WorkflowExecution:
        """Operator action: stop the execution and undo its completed steps (reverse order)."""
        async with self._lock(execution_id):
            ex = await self._load(execution_id)
            if ex.status in (ExecutionStatus.CANCELLED, ExecutionStatus.COMPENSATED) or ex.compensation_mode:
                return ex
            definition = await self.registry.get(ex.definition_id, ex.definition_version)
            now = self.clock.now()
            step = _Step()
            for item in list(ex.work):
                if item.state in (WorkState.PENDING, WorkState.RETRYING, WorkState.RESUMING, WorkState.PARKED,
                                  WorkState.FAULTED):
                    transition(item, WorkState.ABANDONED)
                    ex.work.remove(item)
            ex.bookmarks.clear()
            self._cancel_children(ex, reason, now, step)
            ex.status = ExecutionStatus.RUNNING
            ex.completed_at = None
            self._start_compensation(ex, definition, now, step, "operator", reason=reason, actor=actor)
            await self._commit(ex, step)
        if run:
            return await self.run(execution_id)
        return ex

    # ================================================================================================
    # Step processing
    # ================================================================================================
    async def _process(self, ex: WorkflowExecution, definition: WorkflowDefinition, item: WorkItem,
                       now: Any, step: _Step) -> None:
        node = definition.nodes[item.node_id]
        if item.kind == "compensate":
            spec = node.compensate
            assert spec is not None
            node = NodeSpec(id=f"{node.id}.undo", type=spec.type, name=spec.name or f"undo {node.name or node.id}",
                            inputs=spec.inputs, retry=node.retry)
        activity_type = self.activities.get(node.type)
        parent = TraceContext.parse(item.traceparent) or TraceContext.parse(ex.traceparent) \
            or TraceContext.new_root()
        trace = parent.child()

        try:
            inputs = validate_inputs(activity_type, self._bind(ex, definition, node, item))
            activity: Activity = self.services.create(activity_type)
            dependency = node.dependency or activity.dependency(inputs, node)
        except Exception as exc:  # binding/validation/construction errors are permanent
            await self._fail(ex, definition, node, item, exc, now, step, trace, node.dependency)
            return
        item.dependency = dependency

        held = await self.gate.evaluate(GateRequest(
            subject="work", definition_id=definition.id, execution_id=ex.id, activity_type=node.type,
            node_id=node.id, dependency=dependency))
        if held is not None:
            self._park(ex, item, now, step, held.reason, held.hold_id, trace)
            return

        transition(item, WorkState.RUNNING)
        started = self.clock.now()
        context = ActivityContext(
            execution=ex, definition=definition, node=node, work_item=item, inputs=inputs,
            services=self.services, now=now, trace=trace, activity_type=node.type,
            dependency=dependency, new_id=self.ids.new_id)
        # a failed attempt must not leak partial variable/output changes into the checkpoint
        snapshot = (_clone(ex.variables), _clone(ex.output))
        token = use_trace(trace)
        corr_token = use_correlation(ex.correlation_id)
        try:
            result = await self._invoke(activity, context)
            if isinstance(result, Done):
                _ensure_json(result.output, f"output of node '{node.id}'")
            _ensure_json(ex.variables, "workflow variables")
            _ensure_json(ex.output, "workflow output")
        except Exception as exc:
            ex.variables, ex.output = snapshot
            await self._fail(ex, definition, node, item, exc, now, step, context.trace, dependency)
            return
        finally:
            reset_correlation(corr_token)
            reset_trace(token)
        trace = context.trace  # middleware (OpenTelemetry) may have replaced the span ids
        now = self.clock.now()   # successors, timers and events are stamped with the completion time
        item.payload = {**item.payload, "_timing": _timing(item, started, now)}

        for level, message, data in context.logs:
            step.journal.append(self._event("activity.log", ex, item=item, trace=trace,
                                            data={"level": level, "message": message, **data}))
        step.intents.extend(context.intents)
        self._apply(ex, definition, node, item, result, now, step, trace)

    async def _invoke(self, activity: Activity, context: ActivityContext) -> ActivityResult:
        async def terminal(ctx: ActivityContext) -> ActivityResult:
            if ctx.work_item.kind == "resume":
                return await activity.resume(ctx)
            return await activity.execute(ctx)

        pipeline = terminal
        for mw in reversed(self.middleware):
            pipeline = _wrap(mw, pipeline)
        result = await pipeline(context)
        if not isinstance(result, (Done, Suspend, Sleep, Compensate)):
            raise ActivityFault(f"Activity {context.activity_type} returned {type(result).__name__}; "
                                "expected Done, Suspend, Sleep or Compensate")
        return result

    def _apply(self, ex: WorkflowExecution, definition: WorkflowDefinition, node: NodeSpec,
               item: WorkItem, result: ActivityResult, now: Any, step: _Step,
               trace: TraceContext) -> None:
        if item.kind == "compensate":
            transition(item, WorkState.COMPLETED)
            ex.work.remove(item)
            self._continue_compensation(ex, definition, item, now, step, trace,
                                        result.output if isinstance(result, Done) else None)
            return
        if isinstance(result, Compensate):
            transition(item, WorkState.COMPLETED)
            ex.work.remove(item)
            step.journal.append(self._event("activity.completed", ex, item=item, trace=trace, data={
                "outcomes": ["Compensate"], "attempt": item.attempt, **item.payload.get("_timing", {})}))
            self._start_compensation(ex, definition, now, step, "activity", continue_node=node.id, trace=trace)
            return
        if isinstance(result, Done):
            ex.outputs[node.id] = result.output
            transition(item, WorkState.COMPLETED)
            ex.work.remove(item)
            if node.compensate is not None:
                ex.compensations.append(CompensationRecord(node_id=node.id, output=result.output, completed_at=now))
            step.journal.append(self._event("activity.completed", ex, item=item, trace=trace, data={
                "outcomes": list(result.outcomes), "attempt": item.attempt, "dependency": item.dependency,
                **item.payload.get("_timing", {})}))
            for outcome in result.outcomes:
                for edge in definition.outgoing(node.id, outcome):
                    self._enqueue(ex, definition, edge.target, {"from": node.id}, trace, item.id, now)
        elif isinstance(result, Suspend):
            transition(item, WorkState.COMPLETED)
            ex.work.remove(item)
            bookmark = Bookmark(id=self.ids.new_id("bm"), node_id=node.id,
                                stimulus_type=result.stimulus_type, key=result.key,
                                correlation_id=result.correlation_id, payload=result.payload,
                                created_at=now)
            ex.bookmarks.append(bookmark)
            step.journal.append(self._event("activity.suspended", ex, item=item, trace=trace, data={
                "bookmark_id": bookmark.id, "stimulus_type": bookmark.stimulus_type,
                "key": bookmark.key, "correlation_id": bookmark.correlation_id, **item.payload.get("_timing", {})}))
        else:  # Sleep
            transition(item, WorkState.PENDING)
            item.payload = {**item.payload, "woke": True}
            item.not_before = now + result.delay
            step.journal.append(self._event("activity.sleeping", ex, item=item, trace=trace, data={
                "until": item.not_before.isoformat()}))

    async def _fail(self, ex: WorkflowExecution, definition: WorkflowDefinition, node: NodeSpec,
                    item: WorkItem, error: BaseException, now: Any, step: _Step,
                    trace: TraceContext, dependency: str | None) -> None:
        fault = self.classify(error, dependency)
        fault.at = now
        attempt_ms = _ms(self.clock.now() - now)
        now = self.clock.now()   # retry delays and parking start when the attempt ended
        if item.state is not WorkState.RUNNING:
            transition(item, WorkState.RUNNING)  # failures before execution still count as an attempt
        item.attempt += 1
        item.last_fault = fault
        decision = await self.policy.decide(FailureContext(
            fault=fault, attempt=item.attempt, subject="work", dependency=dependency,
            execution_id=ex.id, definition_id=definition.id, node_id=node.id,
            activity_type=node.type, retry_override=node.retry))
        fault_data = fault.model_dump(mode="json")
        if isinstance(decision, RetryAfter):
            transition(item, WorkState.RETRYING)
            item.not_before = now + decision.delay
            step.journal.append(self._event("work.retry_scheduled", ex, item=item, trace=trace, data={
                "attempt": item.attempt, "duration_ms": attempt_ms, "delay_seconds": decision.delay.total_seconds(),
                "reason": decision.reason, "fault": fault_data}))
        elif isinstance(decision, Park):
            self._park(ex, item, now, step, decision.reason, decision.hold_id, trace, fault_data)
        else:
            assert isinstance(decision, Fail)
            transition(item, WorkState.FAULTED)
            step.journal.append(self._event("work.faulted", ex, item=item, trace=trace, data={
                "attempt": item.attempt, "reason": decision.reason, "fault": fault_data}))
            if item.kind == "compensate":   # an undo step failed: stop, an operator decides (retry/abandon)
                ex.fault = fault
                self._finish(ex, ExecutionStatus.FAULTED, now, step, fault=fault,
                             data={"reason": "compensation failed"})
            elif definition.compensate_on_fault and ex.compensations and not ex.compensation_mode:
                ex.fault = fault
                for other in list(ex.work):
                    if other is not item and other.state in (WorkState.PENDING, WorkState.RETRYING,
                                                             WorkState.RESUMING, WorkState.PARKED):
                        transition(other, WorkState.ABANDONED)
                        ex.work.remove(other)
                ex.bookmarks.clear()
                self._start_compensation(ex, definition, now, step, "fault", reason=decision.reason, trace=trace)
            else:
                self._finish(ex, ExecutionStatus.FAULTED, now, step, fault=fault)

    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo:
        if isinstance(error, ActivityFault):
            return FaultInfo(kind=error.kind, error_type=type(error).__name__, message=str(error),
                             dependency=error.dependency or dependency, retry_after=error.retry_after)
        for classifier in self.classifiers:
            fault = classifier.classify(error, dependency)
            if fault is not None:
                return fault
        return DefaultFaultClassifier().classify(error, dependency)

    # ================================================================================================
    # State helpers
    # ================================================================================================
    def _bind(self, ex: WorkflowExecution, definition: WorkflowDefinition, node: NodeSpec,
              item: WorkItem) -> dict[str, Any]:
        scope = BindingScope(
            variables=ex.variables, input=ex.input, outputs=ex.outputs, payload=item.payload,
            context={"execution_id": ex.id, "correlation_id": ex.correlation_id,
                     "definition_id": definition.id, "node_id": node.id, "trace_id": ex.trace_id,
                     "attempt": item.attempt})
        return {name: resolve(value, scope) for name, value in node.inputs.items()}

    def _enqueue(self, ex: WorkflowExecution, definition: WorkflowDefinition, node_id: str,
                 payload: dict[str, Any], parent: TraceContext, causation_id: str | None, now: Any,
                 kind: str = "execute") -> WorkItem:
        """Schedule a node (``kind``: execute | resume | compensate)."""
        ex.next_seq += 1
        item = WorkItem(id=self.ids.new_id("wi"), node_id=node_id, activity_type=definition.nodes[node_id].type,
                        kind=kind, payload=payload,
                        seq=ex.next_seq, traceparent=parent.traceparent, causation_id=causation_id,
                        created_at=now)
        ex.work.append(item)
        return item

    def _park(self, ex: WorkflowExecution, item: WorkItem, now: Any, step: _Step, reason: str,
              hold_id: str | None, trace: TraceContext, fault: dict[str, Any] | None = None) -> None:
        transition(item, WorkState.PARKED)
        item.parked_reason = reason
        item.hold_id = hold_id
        item.parked_at = now
        item.not_before = None
        step.journal.append(self._event("work.parked", ex, item=item, trace=trace, data={
            "reason": reason, "hold_id": hold_id, "dependency": item.dependency,
            "attempt": item.attempt, "fault": fault}))

    def _reactivate(self, ex: WorkflowExecution, item: WorkItem, now: Any, step: _Step, kind: str,
                    reason: str, actor: str | None) -> None:
        transition(item, WorkState.RESUMING)
        step.journal.append(self._event(kind, ex, item=item, data={
            "reason": reason, "actor": actor, "hold_id": item.hold_id, "dependency": item.dependency,
            "parked_reason": item.parked_reason,
            "parked_ms": _ms(now - item.parked_at) if item.parked_at else None}))
        item.attempt = 0
        item.not_before = None
        item.hold_id = None
        item.parked_reason = None
        item.parked_at = None

    def _settle(self, ex: WorkflowExecution, now: Any, step: _Step) -> bool:
        """Derive the execution status from its work and bookmarks. Returns True if it changed."""
        if ex.status.is_terminal:
            return False
        if any(w.state in ACTIVE for w in ex.work):
            status = ExecutionStatus.RUNNING
        elif any(w.state is WorkState.PARKED for w in ex.work):
            status = ExecutionStatus.PARKED
        elif ex.bookmarks:
            status = ExecutionStatus.WAITING
        else:
            self._finish(ex, ExecutionStatus.COMPLETED, now, step)
            return True
        if status is ex.status:
            return False
        ex.status = status
        step.journal.append(self._event(f"execution.{status.value}", ex))
        return True

    # -- compensation (P14.2) ----------------------------------------------------------------------------
    def _start_compensation(self, ex: WorkflowExecution, definition: WorkflowDefinition, now: Any, step: _Step,
                            mode: str, *, continue_node: str | None = None, reason: str | None = None,
                            actor: str | None = None, trace: TraceContext | None = None) -> None:
        ex.compensation_mode = mode if continue_node is None else f"{mode}:{continue_node}"
        undoable = [r for r in ex.compensations if definition.nodes.get(r.node_id) and
                    definition.nodes[r.node_id].compensate is not None]
        step.journal.append(self._event("compensation.started", ex, trace=trace, data={
            "mode": mode, "steps": [r.node_id for r in reversed(undoable)], "reason": reason, "actor": actor}))
        self._next_compensation(ex, definition, now, step, trace or TraceContext.parse(ex.traceparent)
                                or TraceContext.new_root())

    def _next_compensation(self, ex: WorkflowExecution, definition: WorkflowDefinition, now: Any, step: _Step,
                           trace: TraceContext) -> None:
        while ex.compensations:
            record = ex.compensations[-1]
            if definition.nodes.get(record.node_id) and definition.nodes[record.node_id].compensate is not None:
                self._enqueue(ex, definition, record.node_id, {"original": record.model_dump(mode="json")},
                              trace, None, now, kind="compensate")
                return
            ex.compensations.pop()   # node changed/removed: nothing to undo
        mode = ex.compensation_mode or ""
        ex.compensation_mode = None
        step.journal.append(self._event("compensation.completed", ex, trace=trace, data={"mode": mode}))
        if mode.startswith("activity:"):
            node_id = mode.split(":", 1)[1]
            for edge in definition.outgoing(node_id, "Done"):
                self._enqueue(ex, definition, edge.target, {"from": node_id}, trace, None, now)
        else:
            self._finish(ex, ExecutionStatus.COMPENSATED, now, step, data={"mode": mode})

    def _continue_compensation(self, ex: WorkflowExecution, definition: WorkflowDefinition, item: WorkItem,
                               now: Any, step: _Step, trace: TraceContext, output: Any) -> None:
        if ex.compensations and ex.compensations[-1].node_id == item.node_id:
            ex.compensations.pop()
        step.journal.append(self._event("compensation.step_completed", ex, item=item, trace=trace, data={
            "output": output, **item.payload.get("_timing", {})}))
        self._next_compensation(ex, definition, now, step, trace)

    # -- sub-workflows (P14.1) -----------------------------------------------------------------------------
    def _cancel_children(self, ex: WorkflowExecution, reason: str, now: Any, step: _Step) -> None:
        for child in ex.children:
            step.intents.append(OutboxIntent(
                id=self.ids.new_id("intent"), kind="workflow.cancel_child", execution_id=ex.id,
                payload={"child_execution_id": child, "reason": f"parent {ex.id}: {reason}"},
                traceparent=ex.traceparent, correlation_id=ex.correlation_id, created_at=now, next_attempt_at=now))

    def _finish(self, ex: WorkflowExecution, status: ExecutionStatus, now: Any, step: _Step, *,
                fault: FaultInfo | None = None, data: dict[str, Any] | None = None) -> None:
        ex.status = status
        ex.completed_at = now
        if ex.notify_parent and ex.parent_execution_id:
            effective = fault or ex.fault
            step.intents.append(OutboxIntent(
                id=self.ids.new_id("intent"), kind="workflow.child_finished", execution_id=ex.id,
                payload={"parent_execution_id": ex.parent_execution_id, "child_execution_id": ex.id,
                         "status": status.value, "output": ex.output,
                         "fault": effective.model_dump(mode="json") if effective else None},
                traceparent=ex.traceparent, correlation_id=ex.correlation_id, created_at=now, next_attempt_at=now))
        if fault is not None:
            ex.fault = fault
        payload = dict(data or {})
        if fault is not None:
            payload["fault"] = fault.model_dump(mode="json")
        if status is ExecutionStatus.COMPLETED:
            payload["output"] = ex.output
        step.journal.append(self._event(f"execution.{status.value}", ex, data=payload))

    def _event(self, kind: str, ex: WorkflowExecution, *, item: WorkItem | None = None,
               node_id: str | None = None, trace: TraceContext | None = None,
               data: dict[str, Any] | None = None) -> JournalEvent:
        trace = trace or TraceContext.parse(ex.traceparent)
        return JournalEvent(
            id=self.ids.new_id("ev"), kind=kind, timestamp=self.clock.now(), execution_id=ex.id,
            definition_id=ex.definition_id, node_id=item.node_id if item else node_id,
            work_item_id=item.id if item else None,
            activity_type=item.activity_type if item else None, trace_id=trace.trace_id if trace else ex.trace_id,
            span_id=trace.span_id if trace else None, correlation_id=ex.correlation_id,
            host=self.options.host_id, data=data or {})

    async def _commit(self, ex: WorkflowExecution, step: _Step, expected_revision: int | None = None) -> None:
        ex.updated_at = self.clock.now()
        checkpoint = Checkpoint(
            execution=ex, expected_revision=ex.revision if expected_revision is None else expected_revision,
            intents=step.intents, journal=step.journal, inbox_keys=step.inbox_keys)
        await self.store.commit(checkpoint)
        if step.intents:
            self.signals.notify("outbox")
        if step.journal:
            self.signals.notify("journal")

    async def _load(self, execution_id: str) -> WorkflowExecution:
        ex = await self.store.get(execution_id)
        if ex is None:
            raise ExecutionNotFound(f"Execution '{execution_id}' not found")
        return ex

    # -- per-execution single writer ------------------------------------------------------------------
    def _lock(self, execution_id: str) -> "_LockHandle":
        return _LockHandle(self, execution_id)


class _LockHandle:
    def __init__(self, runtime: WorkflowRuntime, key: str) -> None:
        self._rt = runtime
        self._key = key

    async def __aenter__(self) -> None:
        rt = self._rt
        lock = rt._locks.setdefault(self._key, asyncio.Lock())
        rt._lock_users[self._key] = rt._lock_users.get(self._key, 0) + 1
        await lock.acquire()

    async def __aexit__(self, *exc: object) -> None:
        rt = self._rt
        rt._locks[self._key].release()
        rt._lock_users[self._key] -= 1
        if rt._lock_users[self._key] == 0:
            del rt._lock_users[self._key]
            del rt._locks[self._key]


def _wrap(mw: ActivityMiddleware, next_: Any) -> Any:
    async def call(ctx: ActivityContext) -> ActivityResult:
        return await mw.invoke(ctx, next_)
    return call


def _ms(delta: Any) -> float:
    return round(delta.total_seconds() * 1000, 3)


def _timing(item: WorkItem, started: Any, finished: Any) -> dict[str, float]:
    """Duration of this attempt and latency of the node (enqueue → now, including queueing, retry
    delays and parking; for resumed bookmarks including the time spent waiting for the stimulus)."""
    since = item.payload.get("waiting_since")
    origin = item.created_at or started
    if since:
        from datetime import datetime
        origin = min(origin, datetime.fromisoformat(since))
    return {"duration_ms": _ms(finished - started), "latency_ms": _ms(finished - origin)}


def _next_runnable(ex: WorkflowExecution, now: Any) -> WorkItem | None:
    candidates = [w for w in ex.work if w.state in RUNNABLE and (w.not_before is None or w.not_before <= now)]
    return min(candidates, key=lambda w: w.seq) if candidates else None


def _resume_parent(ex: WorkflowExecution, traceparent: str | None) -> TraceContext:
    """Continue the execution's own trace; adopt the stimulus span only if it is the same trace."""
    own = TraceContext.parse(ex.traceparent) or TraceContext.new_root()
    incoming = TraceContext.parse(traceparent)
    if incoming is not None and incoming.trace_id == own.trace_id:
        return incoming
    return own


def _clone(value: Any) -> Any:
    return copy.deepcopy(value)


def _ensure_json(value: Any, what: str) -> None:
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise ActivityFault(f"{what} is not JSON-serialisable: {exc}") from exc
