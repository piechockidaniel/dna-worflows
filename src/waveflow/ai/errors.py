"""Classification of AI failures (plan items P5.2, P3.1).

AI providers are dependencies like any other: an unreachable model endpoint or a 503 becomes
``dependency_unavailable`` (retry, then park behind an ``ai:<client>`` hold), 429 becomes
``throttled``, auth/invalid-request/content-filter errors are ``permanent``.
"""

from __future__ import annotations

from DnA Workflows.resilience.classification import fault, kind_for_status
from DnA Workflows.runtime.models import FaultInfo, FaultKind


class AiOutputError(Exception):
    """The model answered, but not in the required shape (retrying may produce a valid answer)."""


def _chain(error: BaseException, depth: int = 6) -> list[BaseException]:
    chain, current = [], error
    while current is not None and len(chain) < depth:
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


class AiErrorClassifier:
    def classify(self, error: BaseException, dependency: str | None) -> FaultInfo | None:
        if isinstance(error, AiOutputError):
            return fault(FaultKind.TRANSIENT, error, dependency, ai="invalid_output")
        try:
            from agent_framework import exceptions as ex
        except ImportError:  # pragma: no cover - ai extra not installed
            return None
        if not isinstance(error, ex.AgentFrameworkException):
            return None
        permanent = (ex.ChatClientInvalidAuthException, ex.ChatClientInvalidRequestException,
                     ex.ChatClientContentFilterException, ex.AgentInvalidAuthException,
                     ex.AgentInvalidRequestException, ex.AgentContentFilterException)
        if isinstance(error, permanent):
            return fault(FaultKind.PERMANENT, error, dependency)
        for cause in _chain(error)[1:]:
            status = getattr(cause, "status_code", None)
            if isinstance(status, int):
                return fault(kind_for_status(status), error, dependency, status=status)
            name = type(cause).__name__
            if isinstance(cause, (ConnectionError, TimeoutError)) or "Connection" in name or "Timeout" in name:
                return fault(FaultKind.DEPENDENCY_UNAVAILABLE, error, dependency)
        if isinstance(error, (ex.ChatClientInvalidResponseException, ex.AgentInvalidResponseException)):
            return fault(FaultKind.TRANSIENT, error, dependency)
        return fault(FaultKind.UNKNOWN, error, dependency)
