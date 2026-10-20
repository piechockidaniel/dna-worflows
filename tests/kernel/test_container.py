from typing import Protocol

import pytest

from DnA Workflows.kernel import Lifetime, ServiceCollection, ServiceProvider, ServiceResolutionError


class Greeter(Protocol):
    def greet(self) -> str: ...


class English:
    def greet(self) -> str:
        return "hello"


class Polish:
    def greet(self) -> str:
        return "cześć"


class Plugin(Protocol):
    name: str


class PluginA:
    name = "a"


class PluginB:
    name = "b"


class Consumer:
    def __init__(self, greeter: Greeter, plugins: list[Plugin], optional: English | None = None,
                 default: int = 5) -> None:
        self.greeter = greeter
        self.plugins = plugins
        self.optional = optional
        self.default = default


def test_override_axis_replace_and_try_add():
    services = ServiceCollection()
    assert services.try_add(Greeter, English) is True
    assert services.try_add(Greeter, Polish) is False  # default already present
    assert services.get if False else True
    provider = services.build()
    assert provider.get(Greeter).greet() == "hello"

    services.replace(Greeter, Polish)  # explicit override wins
    assert services.build().get(Greeter).greet() == "cześć"


def test_extend_axis_contributors_are_all_resolved_and_keys_are_idempotent():
    services = ServiceCollection()
    services.add_contributor(Plugin, PluginA, key="a")
    services.add_contributor(Plugin, PluginB, key="b")
    services.add_contributor(Plugin, PluginB, key="b")
    assert services.contributor_count(Plugin) == 2
    assert [p.name for p in services.build().get_all(Plugin)] == ["a", "b"]


def test_autowiring_resolves_single_lists_optionals_and_defaults():
    services = ServiceCollection()
    services.add(Greeter, English)
    services.add_contributor(Plugin, PluginA())
    services.add(Consumer)
    consumer = services.build().get(Consumer)
    assert consumer.greeter.greet() == "hello"
    assert [p.name for p in consumer.plugins] == ["a"]
    assert consumer.optional is None
    assert consumer.default == 5


def test_singleton_vs_transient_and_factories():
    services = ServiceCollection()
    services.add(English)
    services.add(Polish, Polish, lifetime=Lifetime.TRANSIENT)
    services.add("answer", lambda p: 42)
    provider = services.build()
    assert provider.get(English) is provider.get(English)
    assert provider.get(Polish) is not provider.get(Polish)
    assert provider.get("answer") == 42
    assert provider.get(ServiceProvider) is provider


def test_instances_even_callable_ones_are_not_invoked():
    class CallableThing:
        def __call__(self, *_):
            raise AssertionError("must not be called")

    thing = CallableThing()
    services = ServiceCollection()
    services.add("thing", thing)
    services.add_instance("other", thing)
    services.try_add_instance("other", object())
    provider = services.build()
    assert provider.get("thing") is thing
    assert provider.get("other") is thing


def test_missing_and_unwireable_and_circular():
    provider = ServiceCollection().build()
    with pytest.raises(ServiceResolutionError):
        provider.get(Greeter)
    assert provider.get_optional(Greeter) is None
    assert provider.get_all(Plugin) == []

    services = ServiceCollection()
    services.add(Consumer)
    with pytest.raises(ServiceResolutionError, match="autowire"):
        services.build().get(Consumer)

    class A:
        def __init__(self, b: "B") -> None: ...

    class B:
        def __init__(self, a: A) -> None: ...

    services = ServiceCollection()
    services.add(A, lambda p: A(p.get(B)))
    services.add(B, lambda p: B(p.get(A)))
    with pytest.raises(ServiceResolutionError, match="Circular"):
        services.build().get(A)


def test_create_with_overrides_and_is_registered():
    services = ServiceCollection()
    services.add(Greeter, English)
    provider = services.build()
    consumer = provider.create(Consumer, default=9)
    assert consumer.default == 9
    assert provider.is_registered(Greeter)
    assert not services.is_registered(Plugin)
