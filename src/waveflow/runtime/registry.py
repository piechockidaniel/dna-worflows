"""Published workflows and the publish-time trigger index (plan items P1.1, P1.7).

Trigger identities are computed **at publish time** from the definition only;
routing an inbound stimulus is a lookup, never a scan of running workflows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from DnA Workflows.persistence.contracts import DefinitionStore
from DnA Workflows.runtime.activity import ActivityRegistry, validate_inputs
from DnA Workflows.runtime.bindings import is_serializable, is_static
from DnA Workflows.runtime.models import WorkflowDefinition

log = logging.getLogger(__name__)


class DefinitionValidationError(ValueError):
    def __init__(self, definition_id: str, problems: list[str]) -> None:
        super().__init__(f"Workflow '{definition_id}' is invalid: " + "; ".join(problems))
        self.problems = problems


@dataclass(frozen=True, slots=True)
class TriggerEntry:
    definition_id: str
    node_id: str
    stimulus_type: str
    key: str


class WorkflowRegistry:
    def __init__(self, activities: ActivityRegistry, store: DefinitionStore) -> None:
        self._activities = activities
        self._store = store
        self._definitions: dict[tuple[str, int], WorkflowDefinition] = {}
        self._latest: dict[str, int] = {}
        self._triggers: dict[tuple[str, str], list[TriggerEntry]] = {}

    # -- validation -----------------------------------------------------------------------------
    def validate(self, definition: WorkflowDefinition) -> list[str]:
        problems: list[str] = []
        if not definition.nodes:
            problems.append("workflow has no nodes")
        for node_id, node in definition.nodes.items():
            if node_id != node.id:
                problems.append(f"node key '{node_id}' does not match node id '{node.id}'")
            if node.type not in self._activities:
                problems.append(f"node '{node_id}': unknown activity type '{node.type}'")
                continue
            activity = self._activities.get(node.type)
            # literal inputs can be validated now; dynamic ones are validated at run time
            if all(is_static(v) for v in node.inputs.values()):
                try:
                    validate_inputs(activity, node.inputs)
                except Exception as exc:
                    problems.append(f"node '{node_id}': {exc}")
        for edge in definition.edges:
            for end in (edge.source, edge.target):
                if end not in definition.nodes:
                    problems.append(f"edge {edge.source}->{edge.target}: unknown node '{end}'")
            src = definition.nodes.get(edge.source)
            if src is not None and src.type in self._activities:
                outcomes = self._activities.get(src.type).outcomes
                if "*" not in outcomes and edge.outcome not in outcomes:
                    problems.append(
                        f"edge {edge.source}->{edge.target}: outcome '{edge.outcome}' not in {list(outcomes)}")
        if definition.nodes and not definition.roots():
            problems.append("workflow has no root node (every node has an incoming edge)")
        return problems

    # -- publishing -----------------------------------------------------------------------------
    async def publish(self, definition: WorkflowDefinition, *, persist: bool = True) -> WorkflowDefinition:
        problems = self.validate(definition)
        if problems:
            raise DefinitionValidationError(definition.id, problems)
        key = (definition.id, definition.version)
        self._definitions[key] = definition
        if definition.version >= self._latest.get(definition.id, 0):
            self._latest[definition.id] = definition.version
            self._reindex(definition)
        serializable = all(is_serializable(n.inputs) for n in definition.nodes.values())
        if persist and serializable:
            await self._store.save(definition)
        elif persist:
            log.info("Workflow '%s' uses code bindings; kept in memory only", definition.id)
        return definition

    async def load_persisted(self) -> int:
        """Load JSON definitions from the store (startup)."""
        count = 0
        for definition in await self._store.list():
            if (definition.id, definition.version) not in self._definitions:
                await self.publish(definition, persist=False)
                count += 1
        return count

    def _reindex(self, definition: WorkflowDefinition) -> None:
        for k in list(self._triggers):
            self._triggers[k] = [t for t in self._triggers[k] if t.definition_id != definition.id]
            if not self._triggers[k]:
                del self._triggers[k]
        roots = set(definition.roots())
        for node_id in roots:
            node = definition.nodes[node_id]
            activity = self._activities.get(node.type)
            if not activity.is_trigger:
                continue
            for stimulus_type, key in activity.trigger_keys(node):
                self._triggers.setdefault((stimulus_type, key), []).append(
                    TriggerEntry(definition.id, node_id, stimulus_type, key))

    # -- lookup ---------------------------------------------------------------------------------
    async def get(self, definition_id: str, version: int | None = None) -> WorkflowDefinition:
        v = version if version is not None else self._latest.get(definition_id)
        if v is not None and (definition_id, v) in self._definitions:
            return self._definitions[(definition_id, v)]
        stored = await self._store.get(definition_id, version)
        if stored is None:
            raise LookupError(f"Workflow '{definition_id}' (version {version or 'latest'}) not found")
        await self.publish(stored, persist=False)
        return stored

    def list(self) -> list[WorkflowDefinition]:
        return [self._definitions[(i, v)] for i, v in sorted(self._latest.items())]

    def triggers_for(self, stimulus_type: str, keys: list[str]) -> list[TriggerEntry]:
        seen: list[TriggerEntry] = []
        for key in keys:
            for entry in self._triggers.get((stimulus_type, key), []):
                if entry not in seen:
                    seen.append(entry)
        return seen

    def trigger_keys(self) -> list[tuple[str, str]]:
        return sorted(self._triggers)
