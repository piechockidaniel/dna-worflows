"""Identifier generation (P0.5). Overridable so tests can use predictable ids."""

from __future__ import annotations

import itertools
import uuid
from typing import Protocol, runtime_checkable


@runtime_checkable
class IdGenerator(Protocol):
    def new_id(self, prefix: str = "") -> str: ...


class UuidIdGenerator:
    def new_id(self, prefix: str = "") -> str:
        value = uuid.uuid4().hex
        return f"{prefix}-{value}" if prefix else value


class SequentialIdGenerator:
    def __init__(self) -> None:
        self._counter = itertools.count(1)

    def new_id(self, prefix: str = "") -> str:
        n = next(self._counter)
        return f"{prefix}-{n}" if prefix else str(n)
