"""Shell configuration loading with ``${ENV}`` / ``${ENV:-default}`` substitution (P0.4)."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class ConfigError(ValueError):
    pass


def substitute_env(value: Any, env: dict[str, str] | None = None) -> Any:
    env = dict(os.environ) if env is None else env
    if isinstance(value, str):
        def repl(m: re.Match[str]) -> str:
            name, default = m.group(1), m.group(2)
            if name in env:
                return env[name]
            if default is not None:
                return default
            raise ConfigError(f"Environment variable '{name}' is not set and has no default")
        return _ENV.sub(repl, value)
    if isinstance(value, dict):
        return {k: substitute_env(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute_env(v, env) for v in value]
    return value


def load_config(path: str | Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if p.suffix.lower() in {".yaml", ".yml"}:
        data = yaml.safe_load(text) or {}
    elif p.suffix.lower() == ".json":
        data = json.loads(text)
    else:
        raise ConfigError(f"Unsupported config format: {p.suffix}")
    if not isinstance(data, dict):
        raise ConfigError("Shell configuration root must be a mapping")
    return substitute_env(data, env)
