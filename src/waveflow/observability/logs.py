"""Log correlation (plan item P4.2): every Python log record carries trace and correlation ids."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from DnA Workflows.tracing import current_correlation_id, current_trace


class TraceContextFilter(logging.Filter):
    """Adds ``trace_id``, ``span_id`` and ``correlation_id`` to records (unless already set)."""

    def filter(self, record: logging.LogRecord) -> bool:
        trace = current_trace()
        if not getattr(record, "trace_id", None):
            record.trace_id = trace.trace_id if trace else None
        if not getattr(record, "span_id", None):
            record.span_id = trace.span_id if trace else None
        if not getattr(record, "correlation_id", None):
            record.correlation_id = current_correlation_id()
        return True


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line – the format Loki, Azure Monitor and most shippers parse natively."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(), "level": record.levelname,
            "logger": record.name, "message": record.getMessage(),
            "trace_id": getattr(record, "trace_id", None), "span_id": getattr(record, "span_id", None),
            "correlation_id": getattr(record, "correlation_id", None),
        }
        journal = getattr(record, "journal", None)
        if isinstance(journal, dict):   # journal events are emitted as structured fields, not a string
            payload["journal"] = journal
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", json_format: bool = True) -> logging.Handler:
    """Opt-in root logging setup for hosts (the library itself never configures logging).

    Idempotent: a second call reuses the handler installed by the first one."""
    root = logging.getLogger()
    for existing in root.handlers:
        if getattr(existing, "_DnA Workflows", False):
            root.setLevel(level.upper())
            return existing
    handler = logging.StreamHandler()
    handler._DnA Workflows = True  # type: ignore[attr-defined]
    handler.addFilter(TraceContextFilter())
    handler.setFormatter(JsonLogFormatter() if json_format else logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s [trace=%(trace_id)s corr=%(correlation_id)s] %(message)s"))
    root.addHandler(handler)
    root.setLevel(level.upper())
    return handler
