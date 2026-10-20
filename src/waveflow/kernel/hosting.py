"""Background services started/stopped by a shell (P0.4)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

log = logging.getLogger(__name__)


@runtime_checkable
class HostedService(Protocol):
    """Contributor contract (Extend axis): anything that must run while the shell is alive."""

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


class PeriodicService:
    """Base class for services that run ``tick`` every ``interval`` seconds.

    ``tick`` is also callable directly, which is how tests drive background behaviour
    deterministically without sleeping.
    """

    interval: float = 1.0
    name: str = "periodic"

    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._wake: asyncio.Event | None = None

    def wake(self) -> None:
        """Run the next tick now instead of waiting for the interval (e.g. new work committed)."""
        if self._wake is not None:
            self._wake.set()

    async def tick(self) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopping = False
        self._wake = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name=self.name)

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stopping = True
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self) -> None:
        assert self._wake is not None
        while not self._stopping:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # keep the loop alive, failures are logged
                log.exception("Periodic service %s failed", self.name)
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.interval)
            except TimeoutError:
                pass
            self._wake.clear()


class CallbackService:
    """Adapts a pair of coroutine functions to :class:`HostedService`."""

    def __init__(self, on_start: Callable[[], Awaitable[None]],
                 on_stop: Callable[[], Awaitable[None]] | None = None) -> None:
        self._on_start = on_start
        self._on_stop = on_stop

    async def start(self) -> None:
        await self._on_start()

    async def stop(self) -> None:
        if self._on_stop is not None:
            await self._on_stop()
