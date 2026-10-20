"""Shell: a composed, runnable set of features (plan item P0.4)."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from DnA Workflows.kernel.container import ServiceCollection, ServiceProvider
from DnA Workflows.kernel.features import Feature, FeatureCatalog
from DnA Workflows.kernel.hosting import HostedService

log = logging.getLogger(__name__)


class ShellConfig(BaseModel):
    name: str = "default"
    features: dict[str, dict[str, Any]] = Field(default_factory=dict)
    #: When true, a missing dependency is added with default options; when false it is an error.
    auto_include_dependencies: bool = True


class CompositionError(ValueError):
    pass


class ShellInfo:
    """Registered in every shell; lets services know what they are composed with."""

    def __init__(self, name: str, features: list[str]) -> None:
        self.name = name
        self.features = features

    def has(self, feature: str) -> bool:
        return feature in self.features


class Shell:
    def __init__(self, name: str, features: list[Feature], provider: ServiceProvider) -> None:
        self.name = name
        self.features = features
        self.provider = provider
        self._started: list[HostedService] = []

    # -- construction ---------------------------------------------------------------------------
    @classmethod
    def build(cls, config: ShellConfig | dict[str, Any], catalog: FeatureCatalog | None = None,
              *, services: ServiceCollection | None = None) -> "Shell":
        cfg = config if isinstance(config, ShellConfig) else ShellConfig.model_validate(config)
        catalog = catalog or FeatureCatalog.default()
        ordered = _resolve(cfg, catalog)
        services = services or ServiceCollection()
        features: list[Feature] = []
        for name in ordered:
            feature = catalog.get(name)(cfg.features.get(name))
            features.append(feature)
        services.add_instance(ShellInfo, ShellInfo(cfg.name, ordered))
        for feature in features:
            feature.configure(services)
        return cls(cfg.name, features, services.build())

    # -- lifecycle ------------------------------------------------------------------------------
    async def start(self) -> None:
        for service in self.provider.get_all(HostedService):
            await service.start()
            self._started.append(service)
        log.info("Shell '%s' started with %s", self.name, [f.name for f in self.features])

    async def stop(self) -> None:
        while self._started:
            service = self._started.pop()
            try:
                await service.stop()
            except Exception:  # pragma: no cover - best effort shutdown
                log.exception("Failed stopping %r", service)

    async def __aenter__(self) -> "Shell":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    def get(self, contract: Any) -> Any:
        return self.provider.get(contract)


def _resolve(cfg: ShellConfig, catalog: FeatureCatalog) -> list[str]:
    """Depth-first topological order; dependencies come before dependants."""
    ordered: list[str] = []
    visiting: set[str] = set()

    def visit(name: str, requested_by: str | None) -> None:
        if name in ordered:
            return
        if name in visiting:
            raise CompositionError(f"Circular feature dependency involving '{name}'")
        if name not in cfg.features:
            if requested_by is not None and not cfg.auto_include_dependencies:
                raise CompositionError(
                    f"Feature '{requested_by}' requires '{name}' which is not enabled")
        feature = catalog.get(name)
        visiting.add(name)
        for dep in feature.depends_on:
            visit(dep, name)
        visiting.discard(name)
        ordered.append(name)

    for name in cfg.features:
        visit(name, None)
    return ordered
