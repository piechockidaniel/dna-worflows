"""Input bindings (plan item P1.2).

JSON-serialisable binding forms (usable from definition files, APIs and AI-generated workflows):

* any plain JSON value                   → literal
* ``{"$var": "name.path"}``              → workflow variable
* ``{"$input": "path"}``                 → workflow input
* ``{"$output": "node.path"}``           → output of a completed node
* ``{"$payload": "path"}``               → payload of the current work item (e.g. trigger message)
* ``{"$ctx": "correlation_id"}``         → execution attribute (correlation_id, execution_id, ...)
* ``{"$tmpl": "Hi {var.name}"}``         → string template; a template that is exactly one
  placeholder returns the raw value (keeps types)

Code-first workflows may also bind a Python callable ``lambda scope: ...``; it receives the same
:class:`BindingScope`. There is intentionally no ``eval`` of arbitrary code strings.
"""

from __future__ import annotations

import inspect
import json
import re
from dataclasses import dataclass
from typing import Any

_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z0-9_\-]+)*)\}")
_MISSING = object()


class BindingError(ValueError):
    pass


@dataclass(slots=True)
class BindingScope:
    variables: dict[str, Any]
    input: dict[str, Any]
    outputs: dict[str, Any]
    payload: dict[str, Any]
    context: dict[str, Any]

    def root(self, name: str) -> Any:
        return {
            "var": self.variables, "input": self.input, "output": self.outputs,
            "payload": self.payload, "ctx": self.context,
        }.get(name, _MISSING)


def lookup(data: Any, path: str | None) -> Any:
    if not path:
        return data
    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            current = getattr(current, part, None)
        if current is None:
            return None
    return current


def render_template(template: str, scope: BindingScope) -> Any:
    def value_of(expr: str) -> Any:
        root, _, rest = expr.partition(".")
        base = scope.root(root)
        if base is _MISSING:
            raise BindingError(f"Unknown template root '{root}' in '{{{expr}}}'")
        return lookup(base, rest)

    whole = _PLACEHOLDER.fullmatch(template)
    if whole:
        return value_of(whole.group(1))

    def repl(m: re.Match[str]) -> str:
        v = value_of(m.group(1))
        if v is None:
            return ""
        if isinstance(v, (dict, list)):
            return json.dumps(v, default=str)
        return str(v)

    return _PLACEHOLDER.sub(repl, template)


_REF_KEYS = {"$var": "var", "$input": "input", "$output": "output", "$payload": "payload", "$ctx": "ctx"}


def is_reference(value: Any) -> bool:
    return isinstance(value, dict) and len(value) == 1 and next(iter(value)) in (*_REF_KEYS, "$tmpl")


def resolve(value: Any, scope: BindingScope) -> Any:
    """Resolve a binding (recursively for containers)."""
    if callable(value) and not isinstance(value, type):
        result = value(scope)
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if close is not None:
                close()  # do not leak an un-awaited coroutine
            raise BindingError("Binding callables must be synchronous")
        return result
    if isinstance(value, dict):
        if is_reference(value):
            key, arg = next(iter(value.items()))
            if key == "$tmpl":
                if not isinstance(arg, str):
                    raise BindingError("$tmpl expects a string")
                return render_template(arg, scope)
            return lookup(scope.root(_REF_KEYS[key]), arg)
        return {k: resolve(v, scope) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, scope) for v in value]
    return value


def is_static(value: Any) -> bool:
    """True when a binding contains no references/callables (safe to evaluate at publish time)."""
    if callable(value):
        return False
    if isinstance(value, dict):
        return not is_reference(value) and all(is_static(v) for v in value.values())
    if isinstance(value, list):
        return all(is_static(v) for v in value)
    return True


def is_serializable(value: Any) -> bool:
    """Strict JSON check (used to decide whether a definition can be persisted)."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, dict):
        return all(isinstance(k, str) and is_serializable(v) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return all(is_serializable(v) for v in value)
    return False
