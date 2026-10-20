"""Freeze / investigate / unpause – an end-to-end narrated demo (plan item P6.3).

Runs fully in-process (in-memory broker, SQLite in a temp dir, simulated ERP), with the real
background services, so you can watch the operational lifecycle happen:

    python examples/order_integration/demo.py

1. ERP up: order 1 is booked and confirmed.
2. ERP goes down: order 2 retries within its budget, then a dependency hold is placed and it parks.
3. Order 3 arrives while the hold is active: parked immediately, the ERP is not even called.
4. Investigation: holds, parked work and the journal of one business transaction.
5. ERP comes back: the probe-auto release strategy sees it healthy twice, releases the hold,
   orders 2 and 3 complete, confirmations are sent. Nothing was dropped, nothing hammered.
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import httpx

HERE = Path(__file__).parent


class SimulatedErp:
    """An httpx MockTransport standing in for the ERP (switchable)."""

    def __init__(self) -> None:
        self.up = True
        self.calls = 0
        self.booked: list[dict[str, Any]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/health"):
            return httpx.Response(200 if self.up else 503)
        self.calls += 1
        if not self.up:
            return httpx.Response(503, json={"error": "maintenance"})
        order = json.loads(request.content)
        erp_id = f"ERP-{1000 + len(self.booked)}"
        self.booked.append(order)
        return httpx.Response(201, json={"erp_id": erp_id, "order_id": order["order_id"]})


def config(db_path: str) -> dict[str, Any]:
    return {"name": "order-demo", "features": {
        "persistence.sqlite": {"path": db_path},
        "runtime": {"definitions": [str(HERE / "workflows")], "timer_interval": 0.05, "outbox_interval": 0.05,
                    "host_id": "demo-node"},
        "resilience": {
            "retry": {"dependency_unavailable": {"max_attempts": 3, "base_delay": 0.1, "max_delay": 0.3,
                                                 "jitter": 0}},
            "release": {"rules": [{"pattern": "http:erp", "strategy": "probe-auto",
                                   "options": {"successes_required": 2}}]},
            "coordinator_interval": 0.2},
        "messaging": {"endpoints": {
            "orders": {"transport": "inmemory", "address": "orders", "consume": True},
            "confirmations": {"transport": "inmemory", "address": "confirmations"}}},
        "messaging.inmemory": {"broker": "order-demo", "redelivery_delay": 0.05},
        "http": {"dependencies": {"erp": {"base_url": "http://erp.local", "health_url": "http://erp.local/health"}}},
        "observability": {"sinks": []},
        "observability.prometheus": {},
    }}


async def wait_for(predicate: Any, timeout: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await predicate():
        if loop.time() > deadline:
            raise TimeoutError("demo condition not reached")
        await asyncio.sleep(0.05)


async def run_demo(say: Any = print) -> dict[str, Any]:
    from DnA Workflows.activities.http import HttpClientProvider
    from DnA Workflows.kernel import ServiceCollection, Shell
    from DnA Workflows.messaging import MessageBus
    from DnA Workflows.messaging.providers.inmemory import InMemoryBroker
    from DnA Workflows.observability import JournalExporter
    from DnA Workflows.observability.prometheus import DnA WorkflowsMetrics
    from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore
    from DnA Workflows.resilience import HoldService
    from DnA Workflows.runtime import ExecutionStatus

    erp = SimulatedErp()
    services = ServiceCollection()
    services.add_instance(HttpClientProvider, HttpClientProvider(httpx.MockTransport(erp.handler)))
    broker = InMemoryBroker.get("order-demo")

    with tempfile.TemporaryDirectory() as tmp:
        shell = Shell.build(config(str(Path(tmp) / "demo.db")), services=services)
        async with shell:
            bus = shell.get(MessageBus)
            store = shell.get(ExecutionStore)
            holds = shell.get(HoldService)

            async def order(n: int) -> None:
                await bus.publish("orders", "order.placed", {"order_id": n, "sku": "A-1", "qty": n},
                                  correlation_id=f"order-{n}")

            async def status_of(n: int) -> ExecutionStatus | None:
                found = await store.list(correlation_id=f"order-{n}")
                return found[0].status if found else None

            say("1) ERP is up – order 1")
            await order(1)
            await wait_for(lambda: _is(status_of(1), ExecutionStatus.COMPLETED))
            say(f"   order 1 completed, ERP calls so far: {erp.calls}")

            say("2) ERP goes down – order 2")
            erp.up = False
            await order(2)
            await wait_for(lambda: _is(status_of(2), ExecutionStatus.PARKED))
            [hold] = await holds.list(active_only=True)
            say(f"   order 2 PARKED after {erp.calls - 1} ERP attempts; hold {hold.id} on '{hold.target}' "
                f"({hold.source}, release strategy: {hold.release_strategy})")

            say("3) Order 3 arrives while frozen")
            calls_before = erp.calls
            await order(3)
            await wait_for(lambda: _is(status_of(3), ExecutionStatus.PARKED))
            say(f"   order 3 PARKED immediately – ERP calls unchanged: {erp.calls == calls_before}")

            say("4) Investigate")
            parked = await store.find_parked(hold_id=hold.id)
            say(f"   parked work behind {hold.id}: {len(parked)} item(s)")
            history = await shell.get(JournalStore).query(correlation_id="order-2")
            say("   journal for correlation 'order-2': " + " → ".join(e.kind for e in history))

            say("5) ERP comes back – the probe-auto strategy releases the hold")
            erp.up = True
            await wait_for(lambda: _is(status_of(2), ExecutionStatus.COMPLETED))
            await wait_for(lambda: _is(status_of(3), ExecutionStatus.COMPLETED))

            async def release_finished() -> bool:
                return (await holds.get(hold.id)).resumed_work > 0
            await wait_for(release_finished)
            hold = await holds.get(hold.id)
            say(f"   hold released by {hold.released_by}: {hold.release_reason}")
            await wait_for(lambda: _len_at_least(broker.history, "confirmations", 3))
            confirmations = [m.data for m in broker.history["confirmations"]]
            say(f"   confirmations sent: {confirmations}")

            await shell.get(JournalExporter).export()
            metrics = shell.get(DnA WorkflowsMetrics).render().decode()
            say("   metrics: " + ", ".join(line for line in metrics.splitlines()
                                            if line.startswith(("DnA Workflows_work_parked_total",
                                                                "DnA Workflows_holds_total"))))
            summary = {"erp_calls": erp.calls, "booked": [o["order_id"] for o in erp.booked],
                       "confirmations": confirmations, "hold_released_by": hold.released_by,
                       "resumed_work": hold.resumed_work}
        InMemoryBroker.reset()
        return summary


async def _is(awaitable: Any, expected: Any) -> bool:
    return await awaitable is expected


async def _len_at_least(history: dict[str, list[Any]], key: str, n: int) -> bool:
    return len(history.get(key, [])) >= n


if __name__ == "__main__":
    sys.path.insert(0, str(HERE.parents[1] / "src"))
    result = asyncio.run(run_demo())
    print(json.dumps(result, indent=2))
