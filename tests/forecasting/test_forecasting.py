"""Forecasting (P9): from designer estimates to history-driven, self-refining forecasts."""

import math
from datetime import UTC, datetime, timedelta

import pytest

from DnA Workflows.forecasting import ForecastService, Forecaster, ObservationStore, extract_features
from DnA Workflows.forecasting.model import lognormal_quantiles, weighted_stats
from DnA Workflows.forecasting.store import Sample
from DnA Workflows.kernel import ManualClock
from DnA Workflows.observability import JournalExporter
from DnA Workflows.resilience import HoldService
from DnA Workflows.runtime import (Activity, ActivityInputs, Done, StimulusRouter, TimerService, WorkflowRegistry,
                              WorkflowRuntime, add_activity)
from DnA Workflows.runtime.models import Driver, Estimate, FeatureSpec, WorkflowDefinition

T0 = datetime(2030, 1, 1, tzinfo=UTC)

POSTGRES = False  # TestClient/ASGI app: see tests/conftest.py


class Work(Activity):
    """Takes `seconds` of (manual) clock time; optionally fails while `Work.down`."""

    type_name = "Work"
    clock: ManualClock | None = None
    down = False

    class Inputs(ActivityInputs):
        seconds: float = 1
        fail_when_down: bool = False

    def dependency(self, inputs, node):
        return "http:erp" if inputs.fail_when_down else None

    async def execute(self, context):
        Work.clock.advance(context.inputs.seconds)
        if context.inputs.fail_when_down and Work.down:
            raise ConnectionError("down")
        return Done()


def definition(**overrides) -> WorkflowDefinition:
    data = {
        "id": "orders", "forecast": {"features": {"lines": {"path": "order.lines", "transform": "count"},
                                                  "region": {"path": "order.region"}}},
        "nodes": {
            "validate": {"id": "validate", "type": "Work", "inputs": {"seconds": {"$input": "t_validate"}},
                         "estimate": {"likely": 10, "optimistic": 5, "pessimistic": 30}},
            "pick": {"id": "pick", "type": "Work", "inputs": {"seconds": {"$input": "t_pick"}},
                     "estimate": {"likely": 60, "driver": {"feature": "lines", "per_unit": 20}}},
            "check": {"id": "check", "type": "If", "inputs": {"condition": {"$input": "express"}}},
            "express": {"id": "express", "type": "Work", "inputs": {"seconds": 5}},
            "standard": {"id": "standard", "type": "Delay", "inputs": {"seconds": 100}},
        },
        "edges": [{"source": "validate", "target": "pick"}, {"source": "pick", "target": "check"},
                  {"source": "check", "target": "express", "outcome": "True"},
                  {"source": "check", "target": "standard", "outcome": "False"}],
    }
    data.update(overrides)
    return WorkflowDefinition.model_validate(data)


@pytest.fixture
def env(make_shell):
    clock = ManualClock(T0)
    Work.clock = clock
    Work.down = False
    shell = make_shell({"observability": {"sinks": []}, "forecasting": {"prior_strength": 3, "half_life": 1000},
                        "resilience": {"retry": {"dependency_unavailable": {"max_attempts": 1}}}},
                       clock=clock, configure=lambda s: add_activity(s, Work))
    return shell, clock


async def run_order(shell, clock, *, lines, t_validate=12, t_pick=None, express=True, region="EU"):
    rt = shell.get(WorkflowRuntime)
    ex = await rt.start("orders", input={"order": {"lines": list(range(lines)), "region": region},
                                         "t_validate": t_validate, "t_pick": t_pick if t_pick is not None else 10 * lines,
                                         "express": express})
    if not express:
        clock.advance(100)
        await shell.get(TimerService).run_due()
    await shell.get(JournalExporter).export()
    return await rt.get(ex.id)


# ------------------------------------------------------------------------------------------------------
# pure functions
# ------------------------------------------------------------------------------------------------------

def test_feature_extraction():
    spec = {"lines": FeatureSpec(path="order.lines", transform="count"),
            "name_len": FeatureSpec(path="customer", transform="length"),
            "size": FeatureSpec(path="amount", transform="bucket", buckets=[100, 10]),
            "big": FeatureSpec(path="huge", transform="bucket", buckets=[10]),
            "any": FeatureSpec(path="x", transform="bucket"),
            "bad": FeatureSpec(path="customer", transform="bucket", buckets=[1]),
            "region": FeatureSpec(path="region"), "obj": FeatureSpec(path="obj"),
            "missing": FeatureSpec(path="nope")}
    data = {"order": {"lines": [1, 2, 3]}, "customer": "ACME", "amount": 50, "huge": 99, "x": 1,
            "region": "EU", "obj": {"a": 1}}
    assert extract_features(spec, data) == {"lines": 3, "name_len": 4, "size": "<=100", "big": ">10", "any": "all",
                                            "region": "EU"}
    assert extract_features({"one": FeatureSpec(path="region", transform="count")}, data) == {"one": 1}


def test_distribution_helpers():
    assert lognormal_quantiles(0, 1) == (0.0, 0.0)
    assert lognormal_quantiles(10, 0) == (10, 10)
    p50, p90 = lognormal_quantiles(10, 5)
    assert p50 < 10 < p90
    samples = [Sample(10.0, T0), Sample(20.0, T0)]
    mean, var, n = weighted_stats(samples, half_life=1)    # newest weight 1, older 0.5
    assert math.isclose(mean, (10 + 10) / 1.5) and n == 1.5 and var > 0


# ------------------------------------------------------------------------------------------------------
# cold start: only estimates
# ------------------------------------------------------------------------------------------------------

async def test_cold_start_uses_designer_estimates_drivers_and_builtins(env):
    shell, _ = env
    await shell.get(WorkflowRegistry).publish(definition())
    service = shell.get(ForecastService)
    f = await service.forecast_workflow("orders", input={"order": {"lines": [1, 2, 3], "region": "EU"}})
    nodes = f["nodes"]
    assert math.isclose(nodes["validate"]["mean"], (5 + 40 + 30) / 6) and nodes["validate"]["basis"] == "prior"
    assert nodes["pick"]["mean"] == pytest.approx(60 * 0 + (30 + 240 + 120) / 6 + 60)    # driver: 3 lines × 20 s
    assert nodes["pick"]["driver"] == {"feature": "lines", "units": 3, "per_unit": 20}
    assert nodes["standard"]["mean"] == 100 and nodes["standard"]["sd"] == 0      # Delay is exact
    assert nodes["check"]["basis"] == "default" and nodes["check"]["outcome_probabilities"] == {
        "True": 0.5, "False": 0.5}
    expected = nodes["validate"]["mean"] + nodes["pick"]["mean"] + 1.0 + 0.5 * 1.0 + 0.5 * 100
    assert f["mean"] == pytest.approx(expected)
    assert f["p50"] < f["p90"] and f["basis"] == "partial" and f["features"] == {"lines": 3, "region": "EU"}
    assert f["critical_path"][:3] == ["validate", "pick", "check"]
    assert nodes["express"]["expected_visits"] == pytest.approx(0.5)
    assert f["history_lead_time"] is None


# ------------------------------------------------------------------------------------------------------
# learning from history
# ------------------------------------------------------------------------------------------------------

async def test_history_refines_estimates_branches_and_regression(env):
    shell, clock = env
    await shell.get(WorkflowRegistry).publish(definition())
    service = shell.get(ForecastService)
    for i in range(8):
        await run_order(shell, clock, lines=1 + i % 4, t_validate=12, express=i % 4 != 0)

    f = await service.forecast_workflow("orders", input={"order": {"lines": [0] * 10, "region": "EU"}})
    v = f["nodes"]["validate"]
    assert v["basis"] == "mixed" and v["samples"] == 8 and v["history_mean"] == pytest.approx(12)
    # with 8 runs vs prior strength 3 the mean moved most of the way from the estimate (12.5) to reality (12)
    assert 12 < v["mean"] < 12.5 and v["level"] == "node"
    pick = f["nodes"]["pick"]
    assert pick["driver"]["fitted_per_unit"] == pytest.approx(10) and pick["driver"]["fit_samples"] == 8
    assert pick["history_mean"] == pytest.approx(100)                  # regression: 10 s/line × 10 lines
    probs = f["nodes"]["check"]["outcome_probabilities"]
    assert probs["True"] == pytest.approx(7 / 10) and probs["False"] == pytest.approx(3 / 10)
    assert f["history_lead_time"]["samples"] == 8

    stats = await service.node_stats("orders")
    assert {s["node"]: s["samples"] for s in stats}["validate"] == 8
    accuracy = await service.accuracy("orders")
    assert accuracy[0]["settled"] == 8 and 0 <= accuracy[0]["p90_coverage"] <= 1
    assert accuracy[0]["mape"] is not None


async def test_conditions_narrow_history_and_fall_back_when_scarce(env):
    shell, clock = env
    await shell.get(WorkflowRegistry).publish(definition())
    service = shell.get(ForecastService)
    for _ in range(3):
        await run_order(shell, clock, lines=1, t_validate=10, region="EU")
    for _ in range(3):
        await run_order(shell, clock, lines=1, t_validate=40, region="US")
    us = await service.forecast_workflow("orders", conditions={"region": "US"})
    assert us["nodes"]["validate"]["history_mean"] == pytest.approx(40) and us["nodes"]["validate"]["level"] == \
        "conditions"
    assert (await service.forecast_workflow("orders", conditions={"lines": 1}))["nodes"]["validate"]["level"] == \
        "conditions"
    rare = await service.forecast_workflow("orders", conditions={"region": "APAC"})
    assert rare["nodes"]["validate"]["level"] == "node"        # no APAC data: falls back to all history


async def test_activity_level_history_transfers_across_workflows(env):
    shell, clock = env
    registry = shell.get(WorkflowRegistry)
    await registry.publish(definition())
    for _ in range(3):
        await run_order(shell, clock, lines=1, t_validate=12)
    other = WorkflowDefinition.model_validate({"id": "new", "nodes": {"w": {"id": "w", "type": "Work"}}})
    f = await shell.get(ForecastService).forecast_workflow("new", definition=other)
    assert f["nodes"]["w"]["level"] == "activity" and f["nodes"]["w"]["basis"] == "history"


async def test_loops_converge_to_expected_passes(env):
    shell, _ = env
    loop = WorkflowDefinition.model_validate({
        "id": "loop", "nodes": {
            "do": {"id": "do", "type": "Work", "estimate": {"likely": 10, "optimistic": 10, "pessimistic": 10}},
            "again": {"id": "again", "type": "If", "inputs": {"condition": True},
                      "estimate": {"likely": 0, "optimistic": 0, "pessimistic": 0}},
            "start": {"id": "start", "type": "Start", "estimate": {"likely": 0, "optimistic": 0, "pessimistic": 0}}},
        "edges": [{"source": "start", "target": "do"}, {"source": "do", "target": "again"},
                  {"source": "again", "target": "do", "outcome": "True"}]})
    f = await shell.get(ForecastService).forecast_workflow("loop", definition=loop)
    assert f["mean"] == pytest.approx(20, rel=1e-3)          # P(loop)=0.5 → 2 expected passes of 10 s
    assert f["nodes"]["do"]["expected_visits"] == pytest.approx(2, rel=1e-3)


async def test_fork_join_takes_the_slowest_branch(env):
    shell, _ = env
    fork = WorkflowDefinition.model_validate({
        "id": "fork", "nodes": {
            "s": {"id": "s", "type": "Start", "estimate": {"likely": 0, "optimistic": 0, "pessimistic": 0}},
            "a": {"id": "a", "type": "Work", "estimate": {"likely": 10, "optimistic": 10, "pessimistic": 10}},
            "b": {"id": "b", "type": "Work", "estimate": {"likely": 50, "optimistic": 50, "pessimistic": 50}},
            "j": {"id": "j", "type": "Join", "estimate": {"likely": 0, "optimistic": 0, "pessimistic": 0}}},
        "edges": [{"source": "s", "target": "a"}, {"source": "s", "target": "b"}, {"source": "a", "target": "j"},
                  {"source": "b", "target": "j"}]})
    f = await shell.get(ForecastService).forecast_workflow("fork", definition=fork)
    assert f["mean"] == pytest.approx(50) and f["basis"] == "prior" and f["critical_path"] == ["s", "b", "j"]


# ------------------------------------------------------------------------------------------------------
# live ETA
# ------------------------------------------------------------------------------------------------------

async def test_live_eta_for_waiting_parked_and_finished_executions(env):
    shell, clock = env
    registry = shell.get(WorkflowRegistry)
    await registry.publish(WorkflowDefinition.model_validate({
        "id": "approval", "nodes": {
            "prep": {"id": "prep", "type": "Work", "inputs": {"seconds": 5}},
            "wait": {"id": "wait", "type": "Event", "inputs": {"name": "approved"},
                     "estimate": {"likely": 3600, "optimistic": 600, "pessimistic": 7200}},
            "book": {"id": "book", "type": "Work", "inputs": {"seconds": 5, "fail_when_down": True},
                     "estimate": {"likely": 5, "optimistic": 5, "pessimistic": 5}}},
        "edges": [{"source": "prep", "target": "wait"}, {"source": "wait", "target": "book"}]}))
    service = shell.get(ForecastService)
    rt = shell.get(WorkflowRuntime)
    ex = await rt.start("approval")
    clock.advance(600)
    eta = await service.forecast_execution(ex.id)
    [waiting] = eta["frontier"]
    assert waiting["state"] == "waiting" and waiting["waiting_for"] == "event:approved"
    assert waiting["in_node"] == pytest.approx(600)
    assert eta["remaining_mean"] == pytest.approx((600 + 4 * 3600 + 7200) / 6 - 600 + 5)
    assert datetime.fromisoformat(eta["eta"]) > clock.now()

    Work.down = True
    await shell.get(StimulusRouter).publish_event("approved", {})
    ex = await rt.get(ex.id)
    assert ex.status.value == "parked"
    eta = await service.forecast_execution(ex.id)
    [parked] = eta["frontier"]
    assert parked["park"]["basis"] == "default" and eta["blocked_by"] and parked["hold"]["target"] == "http:erp"
    assert eta["parked"] is True and eta["remaining_mean"] > 3000

    Work.down = False
    clock.advance(1800)
    hold = (await shell.get(HoldService).list(active_only=True))[0]
    await shell.get(HoldService).release(hold.id)
    await shell.get(JournalExporter).export()
    done = await service.forecast_execution(ex.id)
    assert done["basis"] == "actual" and done["remaining_mean"] == 0 and done["elapsed"] > 2400
    assert done["forecast_at_start"]["actual"] == pytest.approx(done["elapsed"])
    park = await shell.get(ObservationStore).park_durations("http:erp")
    assert park[0].value == pytest.approx(1800)
    with pytest.raises(LookupError):
        await service.forecast_execution("missing")


async def test_forecasting_api_routes(make_shell):
    from fastapi.testclient import TestClient

    from DnA Workflows.api.feature import ApiApp
    clock = ManualClock(T0)
    Work.clock = clock
    shell = make_shell({"api": {"serve": False}, "observability": {"sinks": []}, "forecasting": {}},
                       clock=clock, configure=lambda s: add_activity(s, Work))
    await shell.get(WorkflowRegistry).publish(definition())
    with TestClient(shell.get(ApiApp).app) as c:
        f = c.post("/forecasts/workflows/orders", json={"input": {"order": {"lines": [1]}}}).json()
        assert f["workflow"] == "orders" and f["features"] == {"lines": 1}
        draft = definition(id="draft").model_dump(mode="json")
        assert c.post("/forecasts/preview", json={"definition": draft}).json()["workflow"] == "draft"
        ex = c.post("/workflows/orders/executions", json={"input": {"t_validate": 1, "t_pick": 1, "express": True}})
        assert c.get(f"/forecasts/executions/{ex.json()['id']}").json()["basis"] == "actual"
        assert c.get("/forecasts/workflows/orders/stats").json() == []          # exporter not run yet
        assert c.get("/forecasts/accuracy").json() == []
        assert c.get("/forecasts/executions/nope").status_code == 404


async def test_observation_store_file_persistence(tmp_path):
    path = str(tmp_path / "history.duckdb")
    store = ObservationStore(path)
    await store.add_steps([(T0, "w", 1, "e1", "n", "Work", None, "Done", 1.0, 2.0, 1)])
    store.close()
    reopened = ObservationStore(path)
    assert (await reopened.latencies(workflow="w"))[0].value == 2.0
    assert await reopened.regression("w", "n", "lines") is None
    assert await reopened.execution_finished("unknown", "completed", T0) is None
    assert await reopened.forecast_of("unknown") is None
    await reopened.add_features("e1", "w", {})
    reopened.close()


def test_forecaster_prior_without_estimate_and_estimate_only_likely():
    from DnA Workflows.runtime.models import NodeSpec
    f = Forecaster(ObservationStore())
    assert f.prior(NodeSpec(id="x", type="Work"), {}) is None
    assert f.prior(NodeSpec(id="d", type="Delay", inputs={"seconds": {"$input": "s"}}), {}) is None
    mean, sd, driver = f.prior(NodeSpec(id="x", type="Work", estimate=Estimate(likely=10)), {})
    assert mean == pytest.approx((5 + 40 + 20) / 6) and sd == pytest.approx(15 / 6) and driver is None
    zero = f.prior(NodeSpec(id="z", type="Work", estimate=Estimate(likely=0, driver=Driver(feature="n", per_unit=2))),
                   {"n": 3})
    assert zero[0] == 6
    assert timedelta(seconds=1)
