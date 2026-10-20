"""Minimal, explicit IoC container (plan item P0.2).

* **Override** – a contract has exactly one implementation. ``replace`` swaps it, ``try_add`` only
  registers a default when nothing is registered yet (features use ``try_add`` so that a later or
  more specific feature can override them).
* **Extend** – a contract is a fan-in list of contributors. ``add_contributor`` appends one more; no
  contributor ever removes another. Consumers resolve them with ``get_all``.

Registrations can be an instance, a class (autowired by constructor type hints) or a factory
``Callable[[ServiceProvider], T]``.
"""

from __future__ import annotations

import inspect
import threading
import typing
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeVar, cast

T = TypeVar("T")


class Lifetime(str, Enum):
    SINGLETON = "singleton"
    TRANSIENT = "transient"


class ServiceResolutionError(LookupError):
    """Raised when a contract cannot be resolved."""


@dataclass(slots=True)
class Registration:
    contract: Any
    implementation: Any
    lifetime: Lifetime = Lifetime.SINGLETON
    key: str | None = None
    _instance: Any = field(default=None, repr=False)
    _built: bool = field(default=False, repr=False)


class _InstanceMarker:
    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value


def _wrap(impl: Any) -> Any:
    """Classes and plain functions/lambdas are factories; anything else is a ready instance."""
    if isinstance(impl, _InstanceMarker):
        return impl
    if inspect.isclass(impl) or inspect.isfunction(impl) or inspect.ismethod(impl):
        return impl
    return _InstanceMarker(impl)


class ServiceCollection:
    """Mutable registration list; build it into an immutable :class:`ServiceProvider`."""

    def __init__(self) -> None:
        self._single: dict[Any, Registration] = {}
        self._multi: dict[Any, list[Registration]] = {}

    # -- override axis -------------------------------------------------------------------------
    def replace(self, contract: Any, implementation: Any = None, *,
                lifetime: Lifetime = Lifetime.SINGLETON) -> "ServiceCollection":
        """Register (or replace) the single implementation of ``contract``."""
        impl = _wrap(contract if implementation is None else implementation)
        self._single[contract] = Registration(contract, impl, lifetime)
        return self

    add = replace  # alias – reads better for first registrations

    def try_add(self, contract: Any, implementation: Any = None, *,
                lifetime: Lifetime = Lifetime.SINGLETON) -> bool:
        """Register a default only when ``contract`` is not registered yet."""
        if contract in self._single:
            return False
        self.replace(contract, implementation, lifetime=lifetime)
        return True

    def add_instance(self, contract: Any, instance: Any) -> "ServiceCollection":
        """Register an already-built object (even if it is callable)."""
        self._single[contract] = Registration(contract, _InstanceMarker(instance))
        return self

    def try_add_instance(self, contract: Any, instance: Any) -> bool:
        if contract in self._single:
            return False
        self.add_instance(contract, instance)
        return True

    # -- extend axis ---------------------------------------------------------------------------
    def add_contributor(self, contract: Any, implementation: Any, *, key: str | None = None,
                        lifetime: Lifetime = Lifetime.SINGLETON) -> "ServiceCollection":
        """Append a contributor. Same ``key`` registered twice is idempotent (first wins)."""
        bucket = self._multi.setdefault(contract, [])
        if key is not None and any(r.key == key for r in bucket):
            return self
        bucket.append(Registration(contract, _wrap(implementation), lifetime, key=key))
        return self

    # -- introspection -------------------------------------------------------------------------
    def is_registered(self, contract: Any) -> bool:
        return contract in self._single

    def contributor_count(self, contract: Any) -> int:
        return len(self._multi.get(contract, []))

    def build(self) -> "ServiceProvider":
        return ServiceProvider(dict(self._single), {k: list(v) for k, v in self._multi.items()})


class ServiceProvider:
    """Resolves contracts. Thread-safe singleton construction."""

    def __init__(self, single: dict[Any, Registration], multi: dict[Any, list[Registration]]) -> None:
        self._single = single
        self._multi = multi
        self._lock = threading.RLock()
        self._resolving: list[Any] = []
        # the provider can be injected into factories/classes that need late resolution
        self._single.setdefault(ServiceProvider, Registration(ServiceProvider, _InstanceMarker(self)))

    def get(self, contract: type[T] | Any) -> T:
        reg = self._single.get(contract)
        if reg is None:
            raise ServiceResolutionError(f"No registration for {_name(contract)}")
        return cast(T, self._materialize(reg))

    def get_optional(self, contract: type[T] | Any) -> T | None:
        reg = self._single.get(contract)
        return None if reg is None else cast(T, self._materialize(reg))

    def get_all(self, contract: type[T] | Any) -> list[T]:
        return [cast(T, self._materialize(r)) for r in self._multi.get(contract, [])]

    def is_registered(self, contract: Any) -> bool:
        return contract in self._single

    def create(self, cls: type[T], **overrides: Any) -> T:
        """Construct ``cls`` with autowiring, without registering it."""
        return cast(T, self._construct(cls, overrides))

    # -- internals -----------------------------------------------------------------------------
    def _materialize(self, reg: Registration) -> Any:
        impl = reg.implementation
        if isinstance(impl, _InstanceMarker):
            return impl.value
        if reg.lifetime is Lifetime.TRANSIENT:
            return self._build(reg)
        if reg._built:
            return reg._instance
        with self._lock:
            if not reg._built:
                reg._instance = self._build(reg)
                reg._built = True
        return reg._instance

    def _build(self, reg: Registration) -> Any:
        if reg.contract in self._resolving:
            chain = " -> ".join(_name(c) for c in [*self._resolving, reg.contract])
            raise ServiceResolutionError(f"Circular dependency: {chain}")
        self._resolving.append(reg.contract)
        try:
            impl = reg.implementation
            if inspect.isclass(impl):
                return self._construct(impl, {})
            return impl(self)
        finally:
            self._resolving.pop()

    def _construct(self, cls: type, overrides: dict[str, Any]) -> Any:
        try:
            hints = typing.get_type_hints(cls.__init__)
        except Exception:  # pragma: no cover - unresolvable forward refs
            hints = {}
        sig = inspect.signature(cls.__init__)
        kwargs: dict[str, Any] = {}
        for name, param in list(sig.parameters.items())[1:]:
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            if name in overrides:
                kwargs[name] = overrides[name]
                continue
            hint = hints.get(name)
            resolved, ok = self._resolve_hint(hint)
            if ok:
                kwargs[name] = resolved
            elif param.default is inspect.Parameter.empty:
                raise ServiceResolutionError(
                    f"Cannot autowire parameter '{name}: {_name(hint)}' of {cls.__qualname__}")
        return cls(**kwargs)

    def _resolve_hint(self, hint: Any) -> tuple[Any, bool]:
        if hint is None:
            return None, False
        origin = typing.get_origin(hint)
        args = typing.get_args(hint)
        if origin in (list, Sequence, Iterable, typing.Sequence, typing.Iterable) or (
                origin is not None and origin.__name__ in {"Sequence", "Iterable", "list"}):
            return self.get_all(args[0]) if args else [], True
        if origin in (typing.Union, getattr(__import__("types"), "UnionType", None)):
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return self.get_optional(non_none[0]), True
            return None, False
        if hint in self._single:
            return self._materialize(self._single[hint]), True
        return None, False


def _name(contract: Any) -> str:
    return getattr(contract, "__qualname__", None) or repr(contract)


Factory = Callable[[ServiceProvider], Any]
