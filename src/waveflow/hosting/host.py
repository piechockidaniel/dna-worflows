"""Process host and CLI (plan item P6.1).

    python -m DnA Workflows run      --config shell.yaml     # run until SIGINT/SIGTERM
    python -m DnA Workflows validate --config shell.yaml     # build the composition, validate workflows
    python -m DnA Workflows mcp      --config shell.yaml     # MCP server on stdio for local AI agents
    python -m DnA Workflows describe [--config shell.yaml]   # machine-readable capabilities (features, activities)
    python -m DnA Workflows schema                           # JSON Schema of workflow definitions
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from typing import Any

from DnA Workflows.kernel.config import load_config
from DnA Workflows.kernel.features import FeatureCatalog
from DnA Workflows.kernel.shell import Shell, ShellConfig

log = logging.getLogger("DnA Workflows.host")


class Host:
    def __init__(self, shell: Shell) -> None:
        self.shell = shell
        self._stop = asyncio.Event()

    @classmethod
    def from_file(cls, path: str, env: dict[str, str] | None = None) -> "Host":
        return cls(Shell.build(ShellConfig.model_validate(load_config(path, env))))

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self, install_signal_handlers: bool = True) -> None:
        if install_signal_handlers:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self.request_stop)
                except (NotImplementedError, RuntimeError):  # pragma: no cover - e.g. Windows
                    pass
        await self.shell.start()
        log.info("DnA Workflows shell '%s' running", self.shell.name)
        try:
            await self._stop.wait()
        finally:
            log.info("Stopping DnA Workflows shell '%s'", self.shell.name)
            await self.shell.stop()


async def validate(path: str, env: dict[str, str] | None = None) -> dict[str, Any]:
    from DnA Workflows.runtime.feature import load_definition_files
    from DnA Workflows.runtime.registry import WorkflowRegistry
    config = ShellConfig.model_validate(load_config(path, env))
    shell = Shell.build(config)
    registry = shell.get(WorkflowRegistry)
    paths = config.features.get("runtime", {}).get("definitions", [])
    report: dict[str, Any] = {"shell": shell.name, "features": [f.name for f in shell.features], "workflows": {}}
    for definition in load_definition_files(paths):
        report["workflows"][f"{definition.id}@{definition.version}"] = registry.validate(definition) or "ok"
    return report


async def run_mcp_stdio(path: str) -> None:
    """Run the shell and speak MCP on stdin/stdout. Logs go to stderr (stdout is the protocol)."""
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    from DnA Workflows.mcpserver import DnA WorkflowsMcp
    config = ShellConfig.model_validate(load_config(path))
    config.features.setdefault("mcp", {})["http_path"] = None
    shell = Shell.build(config)
    async with shell:
        await shell.get(DnA WorkflowsMcp).run_stdio()


def describe(path: str | None = None) -> dict[str, Any]:
    catalog = FeatureCatalog.default()
    result: dict[str, Any] = {"features": [catalog.get(n).describe() for n in catalog.names()]}
    if path:
        from DnA Workflows.runtime.activity import ActivityRegistry
        shell = Shell.build(ShellConfig.model_validate(load_config(path)))
        result["activities"] = shell.get(ActivityRegistry).describe()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="DnA Workflows")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "validate"):
        p = sub.add_parser(name)
        p.add_argument("--config", required=True)
    m = sub.add_parser("mcp", help="serve the MCP tools over stdio (for local AI agents)")
    m.add_argument("--config", required=True)
    d = sub.add_parser("describe")
    d.add_argument("--config")
    sub.add_parser("schema")
    args = parser.parse_args(argv)

    if args.command == "run":
        if not logging.getLogger().handlers:
            from DnA Workflows.observability.logs import configure_logging
            configure_logging("INFO")
        asyncio.run(Host.from_file(args.config).run())
        return 0
    if args.command == "mcp":
        asyncio.run(run_mcp_stdio(args.config))
        return 0
    if args.command == "validate":
        report = asyncio.run(validate(args.config))
        print(json.dumps(report, indent=2))
        return 0 if all(v == "ok" for v in report["workflows"].values()) else 1
    if args.command == "describe":
        print(json.dumps(describe(args.config), indent=2, default=str))
        return 0
    from DnA Workflows.runtime.models import WorkflowDefinition
    print(json.dumps(WorkflowDefinition.model_json_schema(), indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
