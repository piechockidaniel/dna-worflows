"""Stimulus routing: start triggered workflows and resume matching bookmarks (plan item P1.7)."""

from __future__ import annotations

import logging
from typing import Any

from DnA Workflows.persistence.contracts import DuplicateDelivery, ExecutionStore
from DnA Workflows.runtime.engine import WorkflowRuntime
from DnA Workflows.runtime.models import DispatchResult, Stimulus
from DnA Workflows.runtime.registry import WorkflowRegistry

log = logging.getLogger(__name__)


class StimulusRouter:
    def __init__(self, runtime: WorkflowRuntime, registry: WorkflowRegistry, store: ExecutionStore) -> None:
        self._runtime = runtime
        self._registry = registry
        self._store = store

    async def dispatch(self, stimulus: Stimulus) -> DispatchResult:
        """Route one stimulus. Idempotent per ``message_id`` (inbox keys are committed atomically
        with the start/resume checkpoint, so a redelivered message is recognised as a duplicate)."""
        result = DispatchResult()
        for entry in self._registry.triggers_for(stimulus.type, stimulus.keys):
            inbox = (f"start:{entry.definition_id}:{entry.node_id}:{stimulus.message_id}"
                     if stimulus.message_id else None)
            try:
                ex = await self._runtime.start(
                    entry.definition_id, input=stimulus.payload, start_node=entry.node_id,
                    stimulus=stimulus.payload, correlation_id=stimulus.correlation_id,
                    traceparent=stimulus.traceparent, tracestate=stimulus.tracestate,
                    inbox_key=inbox, causation_id=stimulus.causation_id or stimulus.message_id)
                result.started.append(ex.id)
            except DuplicateDelivery:
                result.duplicates += 1
        for execution_id, bookmark in await self._store.find_bookmarks(
                stimulus.type, stimulus.keys, stimulus.correlation_id):
            inbox = f"resume:{bookmark.id}:{stimulus.message_id}" if stimulus.message_id else None
            try:
                ex = await self._runtime.resume(
                    execution_id, bookmark.id, stimulus.payload, inbox_key=inbox,
                    traceparent=stimulus.traceparent,
                    causation_id=stimulus.causation_id or stimulus.message_id)
            except DuplicateDelivery:
                result.duplicates += 1
                continue
            if ex is not None:
                result.resumed.append(execution_id)
        if not result.matched:
            log.debug("Stimulus %s %s matched nothing", stimulus.type, stimulus.keys)
        return result

    async def publish_event(self, name: str, payload: dict[str, Any] | None = None, *,
                            correlation_id: str | None = None, message_id: str | None = None,
                            traceparent: str | None = None) -> DispatchResult:
        """Convenience for the built-in ``Event`` trigger."""
        return await self.dispatch(Stimulus(type="event", keys=[name], payload=payload or {},
                                            correlation_id=correlation_id, message_id=message_id,
                                            traceparent=traceparent))
