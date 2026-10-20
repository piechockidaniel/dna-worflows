"""Fault classifiers (plan item P3.1).

Classifiers are contributors (Extend axis): each recognises the errors it knows and returns
``None`` otherwise. Providers ship their own (the RabbitMQ plugin knows AMQP errors, the HTTP plugin
knows status codes), and operators can map any exception type by name from configuration.
"""

from __future__ import annotations

import asyncio
import json
import socket
from typing import Any

from pydantic import ValidationError

from DnA Workflows.runtime.models import FaultInfo, FaultKind


def fault(kind: FaultKind, error: BaseException, dependency: str | None, *,
          retry_after: float | None = None, **details: Any) -> FaultInfo:
    return FaultInfo(kind=kind, error_type=type(error).__name__, message=str(error) or type(error).__name__,
                     dependency=dependency, retry_after=retry_after, details=details)


def kind_for_status(status: int) -> FaultKind:
    """HTTP-style status → fault kind. Shared by every protocol that speaks in status codes."""
    if status == 429:
        return FaultKind.THROTTLED
    if status in (502, 503, 504):
        return FaultKind.DEPENDENCY_UNAVAILABLE
    if status in (408, 500) or status >= 500:
        return FaultKind.TRANSIENT
    if 400 <= status < 500:
        return FaultKind.PERMANENT
    return FaultKind.UNKNOWN


def parse_retry_after(value: Any) -> float | None:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None


class NetworkErrorClassifier:
    """Python-level connectivity errors mean the dependency is unavailable (or slow)."""

    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo | None:
        if isinstance(error, (ConnectionError, socket.gaierror)):
            return fault(FaultKind.DEPENDENCY_UNAVAILABLE, error, dependency)
        if isinstance(error, (TimeoutError, asyncio.TimeoutError)):
            return fault(FaultKind.DEPENDENCY_UNAVAILABLE, error, dependency, timeout=True)
        return None


class StatusCodeClassifier:
    """Duck-typed: any error exposing ``status_code``/``status`` (or ``response.status_code``)."""

    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo | None:
        status = getattr(error, "status_code", None) or getattr(error, "status", None)
        response = getattr(error, "response", None)
        if status is None and response is not None:
            status = getattr(response, "status_code", None)
        if not isinstance(status, int):
            return None
        retry_after = None
        headers = getattr(response, "headers", None) or getattr(error, "headers", None)
        if headers is not None:
            try:
                retry_after = parse_retry_after(headers.get("retry-after") or headers.get("Retry-After"))
            except AttributeError:
                retry_after = None
        return fault(kind_for_status(status), error, dependency, retry_after=retry_after, status=status)


class DataErrorClassifier:
    """Errors caused by the data itself – retrying the same input will not help."""

    TYPES: tuple[type[BaseException], ...] = (ValidationError, json.JSONDecodeError, KeyError, TypeError,
                                              ValueError, UnicodeError)

    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo | None:
        if isinstance(error, self.TYPES):
            return fault(FaultKind.PERMANENT, error, dependency)
        return None


class ConfiguredClassifier:
    """Maps exception type names (``module.Class`` or ``Class``, incl. base classes) to kinds."""

    def __init__(self, mapping: dict[str, FaultKind]) -> None:
        self.mapping = mapping

    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo | None:
        for cls in type(error).__mro__:
            for name in (f"{cls.__module__}.{cls.__qualname__}", cls.__qualname__, cls.__name__):
                if name in self.mapping:
                    return fault(self.mapping[name], error, dependency)
        return None
