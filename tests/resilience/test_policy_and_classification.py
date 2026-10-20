import asyncio
import json
import random
import socket
from datetime import timedelta

from pydantic import BaseModel

from DnA Workflows.resilience.classification import (ConfiguredClassifier, DataErrorClassifier, NetworkErrorClassifier,
                                                StatusCodeClassifier, kind_for_status, parse_retry_after)
from DnA Workflows.resilience.policy import RetryBudgetPolicy, RetryRule
from DnA Workflows.runtime.contracts import Fail, FailureContext, Park, RetryAfter
from DnA Workflows.runtime.models import FaultInfo, FaultKind


class FakeHolds:
    def __init__(self, held=None):
        self.held = held
        self.tripped = []

    async def active_dependency_hold(self, dependency):
        return self.held

    async def trip(self, dependency, fault):
        self.tripped.append(dependency)

        class H:
            id = "hold-x"
        return H()


def ctx(kind, attempt, dependency="http:a", retry_after=None, override=None):
    return FailureContext(fault=FaultInfo(kind=kind, error_type="E", message="m", retry_after=retry_after),
                          attempt=attempt, subject="work", dependency=dependency, retry_override=override)


def policy(**kw):
    kw.setdefault("rules", {k: RetryRule(max_attempts=3, base_delay=1, jitter=0) for k in FaultKind
                            if k is not FaultKind.PERMANENT})
    return RetryBudgetPolicy(kw.pop("holds", FakeHolds()), rng=random.Random(1), **kw)


async def test_backoff_grows_and_is_capped():
    rule = RetryRule(base_delay=1, multiplier=2, max_delay=5, jitter=0)
    rng = random.Random(0)
    assert [rule.delay_for(a, None, rng).total_seconds() for a in (1, 2, 3, 4)] == [1, 2, 4, 5]
    assert rule.delay_for(1, 30, rng) == timedelta(seconds=30)  # Retry-After wins when larger
    jittered = RetryRule(base_delay=10, jitter=0.5).delay_for(1, None, rng).total_seconds()
    assert 5 <= jittered <= 15


async def test_retry_until_budget_then_trip_or_park_or_fail():
    p = policy()
    d = await p.decide(ctx(FaultKind.DEPENDENCY_UNAVAILABLE, 1))
    assert isinstance(d, RetryAfter) and d.delay == timedelta(seconds=1) and "attempt 1/3" in d.reason
    d = await p.decide(ctx(FaultKind.DEPENDENCY_UNAVAILABLE, 3))
    assert isinstance(d, Park) and d.hold_id == "hold-x" and p.holds.tripped == ["http:a"]
    d = await p.decide(ctx(FaultKind.TRANSIENT, 3))
    assert isinstance(d, Park) and d.hold_id is None and "budget exhausted" in d.reason
    d = await p.decide(ctx(FaultKind.DEPENDENCY_UNAVAILABLE, 3, dependency=None))
    assert isinstance(d, Park) and d.hold_id is None
    d = await p.decide(ctx(FaultKind.PERMANENT, 1))
    assert isinstance(d, Fail) and "after 1 attempt" in d.reason


async def test_existing_hold_parks_immediately_but_not_permanent_errors():
    class Held:
        id = "h-1"
        reason = "down"
    p = policy(holds=FakeHolds(held=Held()))
    d = await p.decide(ctx(FaultKind.TRANSIENT, 1))
    assert isinstance(d, Park) and d.hold_id == "h-1"
    assert isinstance(await p.decide(ctx(FaultKind.PERMANENT, 1)), Fail)


async def test_dependency_and_node_overrides():
    p = policy(dependency_rules={"http:sap-*": {"max_attempts": 5}, "http:*": {"max_attempts": 1}})
    assert isinstance(await p.decide(ctx(FaultKind.TRANSIENT, 4, "http:sap-prod")), RetryAfter)
    assert isinstance(await p.decide(ctx(FaultKind.TRANSIENT, 1, "http:other")), Park)
    d = await p.decide(ctx(FaultKind.TRANSIENT, 1, override={"max_attempts": 1, "on_exhausted": "fail"}))
    assert isinstance(d, Fail)
    assert p.rule_for(ctx(FaultKind.THROTTLED, 1, dependency=None)).max_attempts == 3
    default = RetryBudgetPolicy(FakeHolds())
    assert default.rule_for(ctx(FaultKind.THROTTLED, 1)).max_attempts == 5
    assert isinstance(await default.decide(ctx(FaultKind.THROTTLED, 1, retry_after=120)), RetryAfter)


async def test_trip_on_is_configurable():
    p = policy(trip_on={FaultKind.TRANSIENT})
    d = await p.decide(ctx(FaultKind.TRANSIENT, 3))
    assert d.hold_id == "hold-x"
    d = await p.decide(ctx(FaultKind.DEPENDENCY_UNAVAILABLE, 3))
    assert d.hold_id is None


def test_network_classifier():
    c = NetworkErrorClassifier()
    assert c.classify(ConnectionRefusedError("x"), "d").kind is FaultKind.DEPENDENCY_UNAVAILABLE
    assert c.classify(socket.gaierror("dns"), None).kind is FaultKind.DEPENDENCY_UNAVAILABLE
    f = c.classify(asyncio.TimeoutError(), "d")
    assert f.kind is FaultKind.DEPENDENCY_UNAVAILABLE and f.details == {"timeout": True}
    assert f.message == "TimeoutError"
    assert c.classify(ValueError(), None) is None


def test_status_code_classifier_duck_typing():
    class Resp:
        status_code = 429
        headers = {"retry-after": "12"}

    class HttpError(Exception):
        response = Resp()

    class WithStatus(Exception):
        status = 503

    class WithCode(Exception):
        status_code = 404
        headers = None

    class WeirdHeaders(Exception):
        status_code = 500
        headers = ["not", "a", "mapping"]

    c = StatusCodeClassifier()
    f = c.classify(HttpError("slow down"), "http:a")
    assert f.kind is FaultKind.THROTTLED and f.retry_after == 12 and f.details["status"] == 429
    assert c.classify(WithStatus(), None).kind is FaultKind.DEPENDENCY_UNAVAILABLE
    assert c.classify(WithCode(), None).kind is FaultKind.PERMANENT
    assert c.classify(WeirdHeaders(), None).kind is FaultKind.TRANSIENT
    assert c.classify(Exception("plain"), None) is None
    assert [kind_for_status(s) for s in (408, 500, 507, 302)] == [
        FaultKind.TRANSIENT, FaultKind.TRANSIENT, FaultKind.TRANSIENT, FaultKind.UNKNOWN]
    assert parse_retry_after("x") is None and parse_retry_after(-3) == 0


def test_data_and_configured_classifiers():
    class M(BaseModel):
        a: int
    try:
        M(a="nope")
    except Exception as exc:
        validation = exc
    d = DataErrorClassifier()
    for err in (validation, json.JSONDecodeError("x", "doc", 0), KeyError("k"), ValueError("v")):
        assert d.classify(err, None).kind is FaultKind.PERMANENT
    assert d.classify(RuntimeError(), None) is None

    class LockedRecord(RuntimeError):
        pass

    class SubLocked(LockedRecord):
        pass

    c = ConfiguredClassifier({"LockedRecord": FaultKind.TRANSIENT,
                              "builtins.PermissionError": FaultKind.PERMANENT})
    assert c.classify(SubLocked("busy"), "db:x").kind is FaultKind.TRANSIENT   # base classes match
    assert c.classify(PermissionError(), None).kind is FaultKind.PERMANENT
    assert c.classify(RuntimeError(), None) is None


async def test_configured_classification_via_feature(make_shell):
    from DnA Workflows.runtime import WorkflowRuntime
    shell = make_shell({"resilience": {"classification": {"RuntimeError": "permanent"},
                                       "classify_data_errors": False}})
    rt = shell.get(WorkflowRuntime)
    assert rt.classify(RuntimeError("x"), None).kind is FaultKind.PERMANENT
    assert rt.classify(ValueError("x"), None).kind is FaultKind.UNKNOWN
