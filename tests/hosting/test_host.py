import asyncio
import json
import sys
from pathlib import Path

import pytest

from DnA Workflows.hosting.host import Host, describe, main, validate
from DnA Workflows.runtime import ExecutionStatus, WorkflowRegistry, WorkflowRuntime
from DnA Workflows.runtime.feature import load_definition_files

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "examples" / "order_integration"

POSTGRES = False  # TestClient/ASGI app: see tests/conftest.py


def write_config(tmp_path, workflows_dir, extra=""):
    cfg = tmp_path / "shell.yaml"
    cfg.write_text(f"""
name: test-host
features:
  persistence.sqlite: {{path: "{tmp_path / 'db.sqlite'}"}}
  runtime: {{definitions: ["{workflows_dir}"], timer_interval: 0.05, outbox_interval: 0.05}}
  resilience: {{}}
{extra}""")
    return cfg


def test_definition_files_json_yaml_lists_and_errors(tmp_path):
    d = tmp_path / "wf"
    d.mkdir()
    (d / "a.json").write_text(json.dumps({"id": "a", "nodes": {"s": {"id": "s", "type": "Start"}}}))
    (d / "b.yaml").write_text("- {id: b, nodes: {s: {id: s, type: Start}}}\n- {id: c, nodes: {s: {id: s, type: Start}}}")
    (d / "ignored.txt").write_text("nope")
    single = tmp_path / "single.yml"
    single.write_text("{id: d, nodes: {s: {id: s, type: Start}}}")
    assert [w.id for w in load_definition_files([str(d), str(single)])] == ["a", "b", "c", "d"]
    with pytest.raises(FileNotFoundError):
        load_definition_files([str(tmp_path / "missing")])


async def test_host_runs_shell_with_file_definitions_until_stopped(tmp_path):
    cfg = write_config(tmp_path, EXAMPLE / "workflows", """  messaging:
    endpoints:
      orders: {transport: inmemory, address: h.orders, consume: true}
      confirmations: {transport: inmemory, address: h.confirmations}
  messaging.inmemory: {broker: host-test}
  http: {dependencies: {erp: {base_url: "http://127.0.0.1:9"}}}
""")
    host = Host.from_file(str(cfg))
    task = asyncio.create_task(host.run(install_signal_handlers=False))
    registry = host.shell.get(WorkflowRegistry)
    for _ in range(100):
        if registry.list():
            break
        await asyncio.sleep(0.02)
    assert [w.id for w in registry.list()] == ["order-to-erp"]
    ex = await host.shell.get(WorkflowRuntime).start("order-to-erp")
    assert ex.status is ExecutionStatus.WAITING    # root trigger waits for an order message
    host.request_stop()
    await asyncio.wait_for(task, 5)


async def test_validate_and_describe(tmp_path):
    good = tmp_path / "good"
    good.mkdir()
    (good / "ok.yaml").write_text("{id: ok, nodes: {s: {id: s, type: Start}}}")
    (good / "bad.yaml").write_text("{id: bad, nodes: {s: {id: s, type: Nope}}}")
    report = await validate(str(write_config(tmp_path, good)))
    assert report["workflows"]["ok@1"] == "ok"
    assert "unknown activity type" in report["workflows"]["bad@1"][0]
    assert "runtime" in report["features"]
    described = describe(str(write_config(tmp_path, good)))
    assert {f["name"] for f in described["features"]} >= {"runtime", "messaging.rabbitmq", "ai"}
    assert any(a["type"] == "Start" for a in described["activities"])


def test_cli_commands(tmp_path, capsys, monkeypatch):
    wf = tmp_path / "wf"
    wf.mkdir()
    (wf / "ok.yaml").write_text("{id: ok, nodes: {s: {id: s, type: Start}}}")
    cfg = write_config(tmp_path, wf)
    assert main(["validate", "--config", str(cfg)]) == 0
    assert json.loads(capsys.readouterr().out)["workflows"] == {"ok@1": "ok"}
    (wf / "bad.yaml").write_text("{id: bad, nodes: {s: {id: s, type: Nope}}}")
    assert main(["validate", "--config", str(cfg)]) == 1
    capsys.readouterr()
    assert main(["schema"]) == 0
    assert "nodes" in json.loads(capsys.readouterr().out)["properties"]
    assert main(["describe"]) == 0
    assert json.loads(capsys.readouterr().out)["features"]

    ran = []

    async def fake_run(self, install_signal_handlers=True):
        ran.append(self.shell.name)
    monkeypatch.setattr(Host, "run", fake_run)
    (wf / "bad.yaml").unlink()
    assert main(["run", "--config", str(cfg)]) == 0
    assert ran == ["test-host"]
    import logging
    for handler in list(logging.getLogger().handlers):
        if type(handler).__name__ == "StreamHandler" and handler.formatter.__class__.__name__ == "JsonLogFormatter":
            logging.getLogger().removeHandler(handler)


async def test_order_integration_demo_end_to_end():
    sys.path.insert(0, str(EXAMPLE))
    try:
        import demo
    finally:
        sys.path.remove(str(EXAMPLE))
    lines = []
    summary = await demo.run_demo(say=lines.append)
    assert summary["booked"] == [1, 2, 3]
    assert [c["order_id"] for c in summary["confirmations"]] == [1, 2, 3]
    assert summary["hold_released_by"] == "strategy:probe-auto" and summary["resumed_work"] == 2
    assert summary["erp_calls"] == 6          # 1 + 3 attempts while down + 2 after release: no hammering
    assert any("ERP calls unchanged: True" in line for line in lines)


def test_example_shell_configs_compose(monkeypatch):
    from DnA Workflows.kernel.config import load_config
    from DnA Workflows.kernel.shell import Shell
    monkeypatch.setenv("ERP_URL", "http://erp")
    monkeypatch.setenv("SERVICEBUS_CONNECTION", "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=a2V5")
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "InstrumentationKey=abc")
    monkeypatch.setenv("DnA Workflows_API_KEY", "k")
    monkeypatch.setenv("DnA Workflows_PG", "postgresql://u:p@db/DnA Workflows")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbit/")
    monkeypatch.setenv("KEYCLOAK_ISSUER", "https://sso.example.com/realms/DnA Workflows")
    for name in ("shell.inmemory.yaml", "shell.rabbitmq.yaml", "shell.servicebus.yaml", "shell.production.yaml",
                 "shell.studio.yaml"):
        shell = Shell.build(load_config(EXAMPLE / name))
        assert "runtime" in [f.name for f in shell.features], name
