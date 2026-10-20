"""Time abstraction so every time-dependent behaviour is deterministic in tests (P0.5)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class ManualClock:
    """A clock that only moves when told to. Used by tests and simulations."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2030, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta | float) -> datetime:
        if not isinstance(delta, timedelta):
            delta = timedelta(seconds=delta)
        self._now += delta
        return self._now

    def set(self, value: datetime) -> None:
        self._now = value
