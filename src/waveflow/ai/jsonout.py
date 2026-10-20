"""Getting JSON out of model answers robustly (fences, prose around the object, ...)."""

from __future__ import annotations

import json
import re
from typing import Any

_FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def extract_json(text: str) -> Any:
    """Return the first JSON object/array in ``text``; raise ``ValueError`` if there is none."""
    candidates = [m.group(1) for m in _FENCE.finditer(text)] + [text]
    for candidate in candidates:
        candidate = candidate.strip()
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
        for opener, closer in (("{", "}"), ("[", "]")):
            start = candidate.find(opener)
            while start != -1:
                depth, in_str, escaped = 0, False, False
                for i in range(start, len(candidate)):
                    ch = candidate[i]
                    if in_str:
                        if escaped:
                            escaped = False
                        elif ch == "\\":
                            escaped = True
                        elif ch == '"':
                            in_str = False
                        continue
                    if ch == '"':
                        in_str = True
                    elif ch == opener:
                        depth += 1
                    elif ch == closer:
                        depth -= 1
                        if depth == 0:
                            try:
                                return json.loads(candidate[start:i + 1])
                            except json.JSONDecodeError:
                                break
                start = candidate.find(opener, start + 1)
    raise ValueError("no JSON object found in model output")
