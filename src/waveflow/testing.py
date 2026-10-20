"""Test helpers shipped with the package so plugin authors can test the same way we do."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from DnA Workflows.kernel.clock import Clock, ManualClock
from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import FeatureCatalog
from DnA Workflows.kernel.ids import IdGenerator, SequentialIdGenerator
from DnA Workflows.kernel.shell import Shell


def build_test_shell(features: dict[str, dict[str, Any]] | None = None, *,
                     clock: ManualClock | None = None,
                     configure: Callable[[ServiceCollection], None] | None = None,
                     catalog: FeatureCatalog | None = None,
                     background_services: bool = False) -> Shell:
    """Build a shell with a manual clock, sequential ids and (by default) no background loops,
    so tests drive timers and the outbox explicitly and deterministically."""
    catalog = catalog or FeatureCatalog.default()
    features = {name: dict(opts or {}) for name, opts in (features or {}).items()}
    features.setdefault("runtime", {})
    features["runtime"].setdefault("host_id", "test-host")
    for name, opts in features.items():
        # every feature that runs background loops gets the same switch
        if name in catalog and "background_services" in catalog.get(name).Options.model_fields:
            opts.setdefault("background_services", background_services)
    services = ServiceCollection()
    services.add_instance(Clock, clock or ManualClock())
    services.add_instance(IdGenerator, SequentialIdGenerator())
    if configure is not None:
        configure(services)
    return Shell.build({"name": "test", "features": features}, catalog, services=services)
