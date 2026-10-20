"""Operational lifecycle of a unit of work (plan item P3.5).

::

             transient fault                budget exhausted / dependency unavailable / hold
  Pending ──▶ Running ──────────▶ Retrying ─────────────────────────────────────────▶ Parked
     │          │  ▲                 │                                                  │
     │          │  └─────────────────┘ (retry due)                        release/resume │
     │          │                                                                        ▼
     │          ├──▶ Completed                               Running ◀──────────── Resuming
     │          └──▶ Faulted ──(operator retry)──▶ Resuming
     └──▶ Parked (held before it ever ran)          Parked/Pending/Retrying/Resuming ──▶ Abandoned

Every transition is validated here and journaled by the engine, which gives a complete, auditable
history of why a piece of work did (or did not) run.
"""

from __future__ import annotations

from DnA Workflows.runtime.models import WorkItem, WorkState

S = WorkState

ALLOWED: dict[WorkState, frozenset[WorkState]] = {
    S.PENDING: frozenset({S.RUNNING, S.PARKED, S.ABANDONED}),
    S.RUNNING: frozenset({S.COMPLETED, S.RETRYING, S.PARKED, S.FAULTED, S.PENDING}),
    S.RETRYING: frozenset({S.RUNNING, S.PARKED, S.ABANDONED}),
    S.PARKED: frozenset({S.RESUMING, S.ABANDONED}),
    S.RESUMING: frozenset({S.RUNNING, S.PARKED, S.ABANDONED}),
    S.FAULTED: frozenset({S.RESUMING, S.ABANDONED}),
    S.COMPLETED: frozenset(),
    S.ABANDONED: frozenset(),
}

RUNNABLE = frozenset({S.PENDING, S.RETRYING, S.RESUMING})
ACTIVE = frozenset({S.PENDING, S.RUNNING, S.RETRYING, S.RESUMING})


class InvalidTransition(RuntimeError):
    def __init__(self, item: WorkItem, target: WorkState) -> None:
        super().__init__(f"Work item {item.id} cannot move from {item.state.value} to {target.value}")
        self.item_id = item.id
        self.source = item.state
        self.target = target


def can_transition(source: WorkState, target: WorkState) -> bool:
    return target in ALLOWED[source]


def transition(item: WorkItem, target: WorkState) -> WorkState:
    """Move ``item`` to ``target`` or raise :class:`InvalidTransition`. Returns the previous state."""
    if not can_transition(item.state, target):
        raise InvalidTransition(item, target)
    previous = item.state
    item.state = target
    return previous
