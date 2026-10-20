import pytest

from DnA Workflows.runtime.bindings import (BindingError, BindingScope, is_reference, is_serializable, is_static,
                                       lookup, render_template, resolve)


@pytest.fixture
def scope():
    return BindingScope(variables={"name": "Ada", "items": [{"sku": "A1"}], "n": 3},
                        input={"order": {"id": 7, "tags": ["x"]}}, outputs={"step": {"ok": True}},
                        payload={"stimulus": {"v": 1}}, context={"correlation_id": "c-1"})


def test_references(scope):
    assert resolve({"$var": "name"}, scope) == "Ada"
    assert resolve({"$var": "items.0.sku"}, scope) == "A1"
    assert resolve({"$var": "items.9.sku"}, scope) is None
    assert resolve({"$var": "items.x"}, scope) is None
    assert resolve({"$input": "order.id"}, scope) == 7
    assert resolve({"$input": "order.missing.deeper"}, scope) is None
    assert resolve({"$output": "step.ok"}, scope) is True
    assert resolve({"$payload": "stimulus.v"}, scope) == 1
    assert resolve({"$ctx": "correlation_id"}, scope) == "c-1"
    assert resolve({"$input": ""}, scope) == scope.input


def test_templates_keep_type_for_single_placeholder(scope):
    assert resolve({"$tmpl": "{var.n}"}, scope) == 3
    assert resolve({"$tmpl": "Hi {var.name} #{input.order.id} {input.order.tags} {var.none}!"}, scope) == \
        'Hi Ada #7 ["x"] !'
    with pytest.raises(BindingError):
        render_template("{nope.x}", scope)
    with pytest.raises(BindingError):
        resolve({"$tmpl": 5}, scope)


def test_containers_callables_and_literals(scope):
    assert resolve({"a": [{"$var": "name"}, 1], "b": {"c": {"$ctx": "correlation_id"}}}, scope) == \
        {"a": ["Ada", 1], "b": {"c": "c-1"}}
    assert resolve(lambda s: s.variables["n"] * 2, scope) == 6
    assert resolve(int, scope) is int  # types are literals, not callables

    async def bad(s):
        return 1
    with pytest.raises(BindingError):
        resolve(bad, scope)


def test_static_reference_and_serializable_checks():
    assert is_reference({"$var": "x"}) and not is_reference({"$var": "x", "y": 1})
    assert is_static({"a": [1, {"b": 2}]}) and not is_static({"a": [{"$var": "x"}]})
    assert not is_static(lambda s: 1)
    assert is_serializable({"a": [1, 2.5, None, True, ("t",)]})
    assert not is_serializable({"a": object()}) and not is_serializable({1: "x"})


def test_lookup_attribute_fallback():
    class Obj:
        attr = {"k": "v"}
    assert lookup(Obj(), "attr.k") == "v"
    assert lookup({"a": None}, "a.b") is None
