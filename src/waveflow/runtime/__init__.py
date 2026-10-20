"""Workflow runtime (plan phase P1).

Public names are loaded lazily (PEP 562) so that light modules such as ``runtime.models`` can be
imported by persistence providers without pulling in the engine (avoids import cycles).
"""

from __future__ import annotations

import importlib
from typing import Any

_EXPORTS = {
    "Activity": "activity", "ActivityContext": "activity", "ActivityFault": "activity",
    "ActivityInputs": "activity", "ActivityRegistry": "activity", "ActivityResult": "activity",
    "Done": "activity", "Sleep": "activity", "Suspend": "activity",
    "WorkflowBuilder": "builder", "code": "builtins",
    "ExecutionNotFound": "engine", "WorkflowRuntime": "engine",
    "RuntimeFeature": "feature", "add_activity": "feature",
    "ExecutionStatus": "models", "FaultInfo": "models", "FaultKind": "models", "Stimulus": "models",
    "WorkflowDefinition": "models", "WorkflowExecution": "models", "WorkState": "models",
    "IntentHandler": "outbox", "OutboxProcessor": "outbox",
    "WorkflowRegistry": "registry", "StimulusRouter": "router", "TimerService": "timers",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module 'DnA Workflows.runtime' has no attribute '{name}'")
    return getattr(importlib.import_module(f"DnA Workflows.runtime.{module}"), name)
