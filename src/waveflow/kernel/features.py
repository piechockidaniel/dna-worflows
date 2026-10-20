"""Features: the unit of composition (plan item P0.3).

A feature owns one capability. It declares its dependencies by name, validates its options with a
pydantic model, and contributes registrations to the :class:`ServiceCollection`. Features never reach
into each other's internals – only into contracts.
"""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict

from DnA Workflows.kernel.container import ServiceCollection

log = logging.getLogger(__name__)

ENTRY_POINT_GROUP = "DnA Workflows.features"


class FeatureOptions(BaseModel):
    """Base class for feature options. Unknown keys are rejected to catch config typos early."""

    model_config = ConfigDict(extra="forbid")


class Feature:
    name: ClassVar[str] = ""
    description: ClassVar[str] = ""
    depends_on: ClassVar[tuple[str, ...]] = ()
    Options: ClassVar[type[FeatureOptions]] = FeatureOptions

    def __init__(self, options: FeatureOptions | dict[str, Any] | None = None) -> None:
        if isinstance(options, FeatureOptions):
            self.options = options
        else:
            self.options = self.Options.model_validate(options or {})

    def configure(self, services: ServiceCollection) -> None:  # pragma: no cover - default no-op
        """Register services. Use ``try_add`` for defaults, ``add_contributor`` for extensions."""

    @classmethod
    def describe(cls) -> dict[str, Any]:
        """Machine-readable descriptor (AI readiness: agents can reason about composition)."""
        return {
            "name": cls.name,
            "description": cls.description or (cls.__doc__ or "").strip().split("\n")[0],
            "depends_on": list(cls.depends_on),
            "options_schema": cls.Options.model_json_schema(),
        }


_DISCOVERY_CACHE: dict[str, list[tuple[str, type[Feature]]]] = {}


def _discovered(group: str) -> list[tuple[str, type[Feature]]]:
    """Entry-point scan, cached per process (scanning all distributions is slow)."""
    if group not in _DISCOVERY_CACHE:
        found = []
        for ep in entry_points(group=group):
            try:
                found.append((ep.name, ep.load()))
            except Exception as exc:  # pragma: no cover - depends on installed extras
                log.warning("Feature '%s' unavailable: %s", ep.name, exc)
        _DISCOVERY_CACHE[group] = found
    return _DISCOVERY_CACHE[group]


def clear_discovery_cache() -> None:
    _DISCOVERY_CACHE.clear()


class FeatureNotFoundError(LookupError):
    pass


class FeatureCatalog:
    """Known feature types by name. Populated explicitly and/or from Python entry points."""

    def __init__(self) -> None:
        self._features: dict[str, type[Feature]] = {}

    def register(self, feature: type[Feature]) -> type[Feature]:
        if not feature.name:
            raise ValueError(f"Feature {feature.__qualname__} has no name")
        existing = self._features.get(feature.name)
        if existing is not None and existing is not feature:
            raise ValueError(
                f"Feature name '{feature.name}' already registered by {existing.__qualname__}")
        self._features[feature.name] = feature
        return feature

    def get(self, name: str) -> type[Feature]:
        try:
            return self._features[name]
        except KeyError:
            raise FeatureNotFoundError(
                f"Unknown feature '{name}'. Known: {sorted(self._features)}") from None

    def __contains__(self, name: str) -> bool:
        return name in self._features

    def names(self) -> list[str]:
        return sorted(self._features)

    def discover(self, group: str = ENTRY_POINT_GROUP) -> "FeatureCatalog":
        """Load every feature advertised through the ``DnA Workflows.features`` entry-point group.

        A plugin that fails to import (typically a missing optional dependency) is skipped with a
        warning instead of breaking the host: it simply cannot be selected.
        """
        for name, feature in _discovered(group):
            if name not in self._features:
                self.register(feature)
        return self

    @classmethod
    def default(cls) -> "FeatureCatalog":
        return cls().discover()
