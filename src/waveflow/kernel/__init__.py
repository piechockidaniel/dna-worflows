"""Kernel: container, features, shells, hosting primitives (plan phase P0)."""

from DnA Workflows.kernel.clock import Clock, ManualClock, SystemClock
from DnA Workflows.kernel.container import Lifetime, ServiceCollection, ServiceProvider, ServiceResolutionError
from DnA Workflows.kernel.features import Feature, FeatureCatalog, FeatureNotFoundError, FeatureOptions
from DnA Workflows.kernel.hosting import CallbackService, HostedService, PeriodicService
from DnA Workflows.kernel.ids import IdGenerator, SequentialIdGenerator, UuidIdGenerator
from DnA Workflows.kernel.shell import CompositionError, Shell, ShellConfig, ShellInfo

__all__ = [
    "CallbackService", "Clock", "CompositionError", "Feature", "FeatureCatalog", "FeatureNotFoundError",
    "FeatureOptions", "HostedService", "IdGenerator", "Lifetime", "ManualClock", "PeriodicService",
    "SequentialIdGenerator", "ServiceCollection", "ServiceProvider", "ServiceResolutionError", "Shell",
    "ShellConfig", "ShellInfo", "SystemClock", "UuidIdGenerator",
]
