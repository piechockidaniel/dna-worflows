"""Fluent builder for code-first workflows (plan item P1.1)."""

from __future__ import annotations

from typing import Any

from DnA Workflows.runtime.models import EdgeSpec, NodeSpec, WorkflowDefinition


class WorkflowBuilder:
    """Example::

        wf = (WorkflowBuilder("greet")
              .add("start", "Start")
              .then("check", "If", condition={"$input": "vip"})
              .then("vip", "Log", outcome="True", message="VIP!")
              .then("std", "Log", after="check", outcome="False", message="Hello")
              .build())
    """

    def __init__(self, workflow_id: str, *, version: int = 1, name: str | None = None,
                 description: str | None = None) -> None:
        self._def = WorkflowDefinition(id=workflow_id, version=version, name=name, description=description)
        self._cursor: str | None = None

    def add(self, node_id: str, activity_type: str, /, *, inputs: dict[str, Any] | None = None,
            dependency_override: str | None = None, retry_override: dict[str, Any] | None = None,
            display_name: str | None = None, **kw_inputs: Any) -> "WorkflowBuilder":
        """Add a node. Keyword arguments are activity inputs. Node-level options have deliberately
        unusual names so they never collide with inputs; ``inputs=`` is available as well."""
        if node_id in self._def.nodes:
            raise ValueError(f"Duplicate node id '{node_id}'")
        self._def.nodes[node_id] = NodeSpec(id=node_id, type=activity_type, name=display_name,
                                            inputs={**(inputs or {}), **kw_inputs},
                                            dependency=dependency_override, retry=retry_override)
        self._cursor = node_id
        return self

    def then(self, node_id: str, activity_type: str, /, *, outcome: str = "Done",
             after: str | None = None, **kwargs: Any) -> "WorkflowBuilder":
        source = after or self._cursor
        if source is None:
            raise ValueError("then() needs a previous node; use add() first")
        self.add(node_id, activity_type, **kwargs)
        return self.connect(source, node_id, outcome)

    def connect(self, source: str, target: str, outcome: str = "Done") -> "WorkflowBuilder":
        self._def.edges.append(EdgeSpec(source=source, target=target, outcome=outcome))
        return self

    def at(self, node_id: str) -> "WorkflowBuilder":
        """Move the cursor (so the next ``then`` continues from ``node_id``)."""
        if node_id not in self._def.nodes:
            raise ValueError(f"Unknown node '{node_id}'")
        self._cursor = node_id
        return self

    def variable(self, name: str, value: Any) -> "WorkflowBuilder":
        self._def.variables[name] = value
        return self

    def build(self) -> WorkflowDefinition:
        return self._def.model_copy(deep=True)
