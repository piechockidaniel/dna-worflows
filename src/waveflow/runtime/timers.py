"""Timer / recovery service (plan item P1.9).

Drives every execution whose work became due: delayed retries, ``Delay`` activities and – after a
crash – work that was pending when the process died. Because it is driven by the store, timers are
durable and any node sharing the store can pick them up.
"""

from __future__ import annotations

import logging

from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.hosting import PeriodicService
from DnA Workflows.persistence.contracts import ExecutionStore
from DnA Workflows.runtime.engine import WorkflowRuntime

log = logging.getLogger(__name__)


class TimerService(PeriodicService):
    name = "timer-service"

    def __init__(self, store: ExecutionStore, runtime: WorkflowRuntime, clock: Clock,
                 interval: float = 0.5, batch_size: int = 100) -> None:
        super().__init__()
        self.store = store
        self.runtime = runtime
        self.clock = clock
        self.interval = interval
        self.batch_size = batch_size

    async def tick(self) -> None:
        await self.run_due()

    async def run_due(self) -> list[str]:
        ran = []
        for execution_id in await self.store.due(self.clock.now(), self.batch_size):
            try:
                await self.runtime.run(execution_id)
                ran.append(execution_id)
            except Exception:
                log.exception("Timer failed to run execution %s", execution_id)
        return ran
