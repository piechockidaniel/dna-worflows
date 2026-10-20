import asyncio
from datetime import timedelta

import pytest

from DnA Workflows.kernel import (CallbackService, CompositionError, Feature, FeatureCatalog,
                             FeatureNotFoundError, FeatureOptions, HostedService, ManualClock,
                             PeriodicService, SequentialIdGenerator, ServiceCollection, Shell,
                             ShellInfo, SystemClock, UuidIdGenerator)
from DnA Workflows.kernel.config import ConfigError, load_config, substitute_env

events: list[str] = []


class BaseOptions(FeatureOptions):
    greeting: str = "hi"


class BaseFeature(Feature):
    name = "base"
    Options = BaseOptions

    def configure(self, services: ServiceCollection) -> None:
        services.try_add("greeting", lambda p, g=self.options.greeting: g)
        services.add_contributor(HostedService, CallbackService(
            lambda: _record("base.start"), lambda: _record("base.stop")))


class TopFeature(Feature):
    name = "top"
    depends_on = ("base",)

    def configure(self, services: ServiceCollection) -> None:
        services.add_contributor(HostedService, CallbackService(lambda: _record("top.start"),
                                                                lambda: _record("top.stop")))


class CycleA(Feature):
    name = "cycle.a"
    depends_on = ("cycle.b",)


class CycleB(Feature):
    name = "cycle.b"
    depends_on = ("cycle.a",)


async def _record(what: str) -> None:
    events.append(what)


@pytest.fixture
def catalog() -> FeatureCatalog:
    c = FeatureCatalog()
    for f in (BaseFeature, TopFeature, CycleA, CycleB):
        c.register(f)
    return c


async def test_shell_orders_dependencies_and_runs_hosted_services(catalog):
    events.clear()
    shell = Shell.build({"name": "s1", "features": {"top": {}}}, catalog)
    assert [f.name for f in shell.features] == ["base", "top"]
    assert shell.get("greeting") == "hi"
    assert shell.get(ShellInfo).has("base")
    async with shell:
        pass
    assert events == ["base.start", "top.start", "top.stop", "base.stop"]


def test_feature_options_are_validated(catalog):
    shell = Shell.build({"features": {"base": {"greeting": "yo"}}}, catalog)
    assert shell.get("greeting") == "yo"
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        Shell.build({"features": {"base": {"unknown": 1}}}, catalog)


def test_composition_errors(catalog):
    with pytest.raises(CompositionError, match="requires"):
        Shell.build({"features": {"top": {}}, "auto_include_dependencies": False}, catalog)
    with pytest.raises(CompositionError, match="Circular"):
        Shell.build({"features": {"cycle.a": {}}}, catalog)
    with pytest.raises(FeatureNotFoundError):
        Shell.build({"features": {"nope": {}}}, catalog)


def test_catalog_rules_and_descriptor(catalog):
    class Nameless(Feature):
        pass

    class Clash(Feature):
        name = "base"

    with pytest.raises(ValueError):
        catalog.register(Nameless)
    with pytest.raises(ValueError):
        catalog.register(Clash)
    catalog.register(BaseFeature)  # same type twice is fine
    assert "base" in catalog and "top" in catalog.names()
    d = BaseFeature.describe()
    assert d["name"] == "base" and "greeting" in d["options_schema"]["properties"]


def test_entry_point_discovery_finds_builtin_features():
    catalog = FeatureCatalog.default()
    assert "runtime" in catalog
    assert "messaging.rabbitmq" in catalog


def test_env_substitution_and_loading(tmp_path):
    env = {"HOST": "rabbit"}
    assert substitute_env({"a": ["${HOST}", "${PORT:-5672}"], "b": 1}, env) == {
        "a": ["rabbit", "5672"], "b": 1}
    with pytest.raises(ConfigError):
        substitute_env("${MISSING}", {})
    y = tmp_path / "shell.yaml"
    y.write_text("name: x\nfeatures:\n  base: {greeting: '${G:-hey}'}\n")
    assert load_config(y, {})["features"]["base"]["greeting"] == "hey"
    j = tmp_path / "shell.json"
    j.write_text('{"name": "${N}"}')
    assert load_config(j, {"N": "z"}) == {"name": "z"}
    with pytest.raises(ConfigError):
        (tmp_path / "bad.toml").write_text("")
        load_config(tmp_path / "bad.toml")
    (tmp_path / "list.json").write_text("[]")
    with pytest.raises(ConfigError):
        load_config(tmp_path / "list.json")


async def test_periodic_service_ticks_and_survives_errors():
    calls: list[int] = []

    class Ticker(PeriodicService):
        interval = 0.01

        async def tick(self) -> None:
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("boom")

    t = Ticker()
    await t.start()
    await t.start()  # idempotent
    await asyncio.sleep(0.05)
    await t.stop()
    await t.stop()
    assert len(calls) >= 2


def test_clocks_and_ids():
    c = ManualClock()
    t0 = c.now()
    c.advance(5)
    c.advance(timedelta(seconds=5))
    assert (c.now() - t0).total_seconds() == 10
    c.set(t0)
    assert c.now() == t0
    assert SystemClock().now().tzinfo is not None
    seq = SequentialIdGenerator()
    assert seq.new_id("x") == "x-1" and seq.new_id() == "2"
    assert UuidIdGenerator().new_id("w").startswith("w-") and len(UuidIdGenerator().new_id()) == 32
