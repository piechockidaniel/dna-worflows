"""Processing-time forecasting (plan items P9.2, P9.3, P9.5–P9.7).

How an estimate is formed for one node
--------------------------------------
1. **Prior** – the designer's three-point estimate (PERT: mean = (o + 4m + p) / 6, sd = (p - o) / 6),
   optionally scaled by a *driver* (seconds per unit of an input feature, e.g. per order line).
   Known activities have built-in priors (a ``Delay`` of 30 s takes 30 s).
2. **History** – recent node latencies from the observation store, recency-weighted (half-life in
   observations). The most specific level with enough data wins:
   node + conditions → node → activity type + dependency (other workflows).
   If the node has a driver and enough paired data, the history value comes from a regression
   ``latency = a + b · feature`` instead of a plain average.
3. **Blend** – the prior acts like ``prior_strength`` pseudo-observations, so with few runs the
   designer's estimate dominates and with many runs the data does:
   ``mean = (k · prior + n_eff · history) / (k + n_eff)``. Missing pieces are simply left out – a node
   without an estimate uses history only, a node without history uses the estimate only, a node with
   neither uses a flagged default.

Latency = enqueue → completion (queueing, retries, parking, waiting for messages/humans included), so
forecasts describe elapsed business time, not CPU time. Every result carries its *basis* and sample count.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any

from DnA Workflows.forecasting.store import ObservationStore, Sample
from DnA Workflows.runtime.bindings import is_static, lookup
from DnA Workflows.runtime.models import (ExecutionStatus, FeatureSpec, NodeSpec, WorkflowDefinition, WorkflowExecution,
                                     WorkState)

Z90 = 1.2815515655446004


# -- features -------------------------------------------------------------------------------------------

def extract_features(spec: dict[str, FeatureSpec], data: dict[str, Any]) -> dict[str, Any]:
    """Compute forecasting features from a workflow input (missing values are skipped)."""
    result: dict[str, Any] = {}
    for name, f in spec.items():
        value = lookup(data, f.path)
        if value is None:
            continue
        if f.transform == "count":
            result[name] = len(value) if isinstance(value, (list, dict)) else 1
        elif f.transform == "length":
            result[name] = len(str(value))
        elif f.transform == "bucket":
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            bounds = sorted(f.buckets)
            result[name] = next((f"<={b:g}" for b in bounds if number <= b), f">{bounds[-1]:g}" if bounds else "all")
        elif isinstance(value, (int, float, str, bool)):
            result[name] = value
    return result


# -- distributions --------------------------------------------------------------------------------------

def lognormal_quantiles(mean: float, sd: float) -> tuple[float, float]:
    """P50 and P90 of a log-normal with the given mean/sd (durations are positive and right-skewed)."""
    if mean <= 0:
        return 0.0, 0.0
    if sd <= 0:
        return mean, mean
    sigma2 = math.log(1 + (sd * sd) / (mean * mean))
    mu = math.log(mean) - sigma2 / 2
    sigma = math.sqrt(sigma2)
    return math.exp(mu), math.exp(mu + Z90 * sigma)


def weighted_stats(samples: list[Sample], half_life: float) -> tuple[float, float, float]:
    """Recency-weighted mean, variance and effective sample size (samples newest first)."""
    weights = [0.5 ** (i / half_life) for i in range(len(samples))]
    total = sum(weights)
    mean = sum(w * s.value for w, s in zip(weights, samples, strict=True)) / total
    var = sum(w * (s.value - mean) ** 2 for w, s in zip(weights, samples, strict=True)) / total
    return mean, var, total


@dataclass
class NodeEstimate:
    node: str
    mean: float
    sd: float
    p50: float
    p90: float
    basis: str                 # prior | history | mixed | default
    samples: int
    level: str                 # conditions | node | activity | prior | default
    prior_mean: float | None = None
    history_mean: float | None = None
    driver: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {k: (round(v, 6) if isinstance(v, float) else v) for k, v in asdict(self).items()}


@dataclass
class WorkflowForecast:
    workflow: str
    version: int
    mean: float
    sd: float
    p50: float
    p90: float
    basis: str
    features: dict[str, Any]
    conditions: dict[str, Any]
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    critical_path: list[str] = field(default_factory=list)
    history_lead_time: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        for k in ("mean", "sd", "p50", "p90"):
            d[k] = round(d[k], 6)
        return d


class Forecaster:
    def __init__(self, store: ObservationStore, *, prior_strength: float = 3.0, half_life: float = 50.0,
                 window: int = 500, min_level_samples: int = 3, default_seconds: float = 1.0,
                 park_default_seconds: float = 3600.0) -> None:
        self.store = store
        self.prior_strength = prior_strength
        self.half_life = half_life
        self.window = window
        self.min_level_samples = min_level_samples
        self.default_seconds = default_seconds
        self.park_default_seconds = park_default_seconds

    # -- one node ---------------------------------------------------------------------------------
    def prior(self, node: NodeSpec, features: dict[str, Any]) -> tuple[float, float, dict[str, Any] | None] | None:
        e = node.estimate
        if e is not None:
            likely = e.likely
            o = e.optimistic if e.optimistic is not None else likely * 0.5
            p = e.pessimistic if e.pessimistic is not None else likely * 2.0
            mean, sd = (o + 4 * likely + p) / 6, max(0.0, (p - o) / 6)
            driver = None
            if e.driver is not None:
                units = features.get(e.driver.feature)
                if isinstance(units, (int, float)) and not isinstance(units, bool):
                    extra = e.driver.per_unit * units
                    if mean > 0:
                        sd *= (mean + extra) / mean
                    mean += extra
                    driver = {"feature": e.driver.feature, "units": units, "per_unit": e.driver.per_unit}
            return mean, sd, driver
        if node.type == "Delay":
            seconds = node.inputs.get("seconds")
            if isinstance(seconds, (int, float)) and is_static(seconds):
                return float(seconds), 0.0, None
        return None

    async def node_estimate(self, definition: WorkflowDefinition, node_id: str, features: dict[str, Any],
                            conditions: dict[str, Any] | None = None, dependency: str | None = None) -> NodeEstimate:
        node = definition.nodes[node_id]
        prior = self.prior(node, features)
        samples: list[Sample] = []
        level = "prior"
        candidates: list[tuple[str, dict[str, Any]]] = []
        if conditions:
            candidates.append(("conditions", {"workflow": definition.id, "node": node_id, "conditions": conditions}))
        candidates.append(("node", {"workflow": definition.id, "node": node_id}))
        candidates.append(("activity", {"activity_type": node.type, "dependency": dependency or node.dependency}))
        fallback: tuple[str, list[Sample]] | None = None
        for name, query in candidates:
            found = await self.store.latencies(limit=self.window, **query)
            if len(found) >= self.min_level_samples:
                samples, level = found, name
                break
            if found and fallback is None and name != "activity":
                fallback = (name, found)
        if not samples and fallback is not None:
            level, samples = fallback

        history_mean = history_var = None
        n_eff = 0.0
        driver_info = prior[2] if prior else None
        if samples:
            history_mean, history_var, n_eff = weighted_stats(samples, self.half_life)
            e = node.estimate
            if e is not None and e.driver is not None and level != "activity":
                units = features.get(e.driver.feature)
                fit = await self.store.regression(definition.id, node_id, e.driver.feature)
                if fit is not None and fit[2] >= 5 and isinstance(units, (int, float)):
                    a, b, n = fit
                    history_mean = max(0.0, a + b * units)
                    driver_info = {**(driver_info or {}), "fitted_intercept": a, "fitted_per_unit": b, "fit_samples": n}

        if prior is None and history_mean is None:
            mean, sd = self.default_seconds, self.default_seconds
            basis, level = "default", "default"
        elif history_mean is None:
            mean, sd = prior[0], prior[1]
            basis = "prior"
        elif prior is None:
            mean, sd = history_mean, math.sqrt(history_var or 0.0)
            basis = "history"
        else:
            k = self.prior_strength
            pm, psd = prior[0], prior[1]
            mean = (k * pm + n_eff * history_mean) / (k + n_eff)
            var = (k * (psd ** 2 + (pm - mean) ** 2) + n_eff * ((history_var or 0.0) + (history_mean - mean) ** 2)) \
                / (k + n_eff)
            sd = math.sqrt(var)
            basis = "mixed"
        p50, p90 = lognormal_quantiles(mean, sd)
        return NodeEstimate(node_id, mean, sd, p50, p90, basis, len(samples), level,
                            prior_mean=prior[0] if prior else None, history_mean=history_mean, driver=driver_info)

    # -- branching ------------------------------------------------------------------------------------
    async def branch_probabilities(self, definition: WorkflowDefinition, node_id: str,
                                   declared: tuple[str, ...]) -> dict[str, float]:
        edge_outcomes = {e.outcome for e in definition.edges if e.source == node_id}
        if not edge_outcomes:
            return {}
        candidates = edge_outcomes | {o for o in declared if o != "*"}
        if "*" in declared:
            candidates.add("Default")
        if candidates == {"Done"}:
            return {"Done": 1.0}
        counts = await self.store.outcome_counts(definition.id, node_id)
        total = sum(counts.get(o, 0) for o in candidates) + len(candidates)   # Laplace smoothing
        return {o: (counts.get(o, 0) + 1) / total for o in candidates}

    # -- whole workflow -------------------------------------------------------------------------------
    async def forecast_workflow(self, definition: WorkflowDefinition, *, input: dict[str, Any] | None = None,
                                conditions: dict[str, Any] | None = None, outcomes: dict[str, tuple[str, ...]]
                                | None = None, start: list[str] | None = None) -> WorkflowForecast:
        features = extract_features(definition.forecast.features, input or {})
        estimates = {n: await self.node_estimate(definition, n, features, conditions) for n in definition.nodes}
        probs = {n: await self.branch_probabilities(definition, n, (outcomes or {}).get(n, ("Done",)))
                 for n in definition.nodes}
        targets = {n: {o: [e.target for e in definition.edges if e.source == n and e.outcome == o] for o in p}
                   for n, p in probs.items()}
        remaining, variance = self._solve(estimates, probs, targets)
        roots = start or definition.roots()
        root = max(roots, key=lambda r: remaining[r])
        mean, sd = remaining[root], math.sqrt(max(0.0, variance[root]))
        p50, p90 = lognormal_quantiles(mean, sd)
        visits = self._visits(roots, probs, targets)
        bases = {e.basis for e in estimates.values()}
        basis = bases.pop() if len(bases) == 1 else ("mixed" if "default" not in bases else "partial")
        nodes = {n: {**e.as_dict(), "expected_visits": round(visits.get(n, 0.0), 4),
                     "remaining_mean": round(remaining[n], 6), "outcome_probabilities": probs[n]}
                 for n, e in estimates.items()}
        history = await self.store.lead_times(definition.id)
        history_info = None
        if history:
            h_mean, h_var, _ = weighted_stats(history, self.half_life)
            history_info = {"samples": len(history), "mean": h_mean, "sd": math.sqrt(h_var)}
        return WorkflowForecast(definition.id, definition.version, mean, sd, p50, p90, basis, features,
                                conditions or {}, nodes, self._critical_path(root, probs, targets, remaining),
                                history_info)

    def _solve(self, estimates: dict[str, NodeEstimate], probs: dict[str, dict[str, float]],
               targets: dict[str, dict[str, list[str]]]) -> tuple[dict[str, float], dict[str, float]]:
        """Expected remaining time from each node (and its variance) – fixed-point iteration so cycles
        (retry loops, approval rounds) converge to their expected number of passes."""
        remaining = {n: 0.0 for n in estimates}
        variance = {n: 0.0 for n in estimates}
        for _ in range(200):
            changed = 0.0
            for n, est in estimates.items():
                mean_next = second = 0.0
                var_next = 0.0
                for outcome, p in probs[n].items():
                    nexts = targets[n].get(outcome) or []
                    if not nexts:
                        continue
                    t = max(nexts, key=lambda x: remaining[x])   # parallel branches: the slowest decides
                    mean_next += p * remaining[t]
                    second += p * (variance[t] + remaining[t] ** 2)
                var_next = max(0.0, second - mean_next ** 2)
                value = est.mean + mean_next
                changed = max(changed, abs(value - remaining[n]))
                remaining[n] = value
                variance[n] = est.sd ** 2 + var_next
            if changed < 1e-9:
                break
        return remaining, variance

    def _visits(self, roots: list[str], probs: dict[str, dict[str, float]],
                targets: dict[str, dict[str, list[str]]]) -> dict[str, float]:
        visits = {n: 0.0 for n in probs}
        for _ in range(200):
            new = {n: (1.0 if n in roots else 0.0) for n in probs}
            for n, p in probs.items():
                for outcome, prob in p.items():
                    for t in targets[n].get(outcome) or []:
                        new[t] += visits[n] * prob
            delta = max(abs(new[n] - visits[n]) for n in probs)
            visits = {n: min(v, 1000.0) for n, v in new.items()}
            if delta < 1e-9:
                break
        return visits

    def _critical_path(self, root: str, probs: dict[str, dict[str, float]], targets: dict[str, dict[str, list[str]]],
                       remaining: dict[str, float]) -> list[str]:
        path, node = [root], root
        while probs.get(node):
            outcome = max(probs[node], key=lambda o: probs[node][o])
            nexts = targets[node].get(outcome) or []
            if not nexts:
                break
            node = max(nexts, key=lambda x: remaining[x])
            if node in path:
                break
            path.append(node)
        return path

    # -- live ETA ---------------------------------------------------------------------------------------
    async def forecast_execution(self, execution: WorkflowExecution, definition: WorkflowDefinition,
                                 now: datetime, outcomes: dict[str, tuple[str, ...]] | None = None,
                                 holds: dict[str, Any] | None = None) -> dict[str, Any]:
        base = {"execution_id": execution.id, "workflow": execution.definition_id, "status": execution.status.value,
                "started_at": execution.created_at.isoformat() if execution.created_at else None}
        elapsed = (now - execution.created_at).total_seconds() if execution.created_at else 0.0
        if execution.status.is_terminal:
            end = execution.completed_at or now
            total = (end - execution.created_at).total_seconds() if execution.created_at else 0.0
            return {**base, "elapsed": total, "remaining_mean": 0.0, "eta": end.isoformat(), "eta_p90": end.isoformat(),
                    "frontier": [], "blocked_by": [], "basis": "actual"}
        wf = await self.forecast_workflow(definition, input=execution.input, outcomes=outcomes)
        frontier: list[dict[str, Any]] = []
        for item in execution.work:
            if item.state in (WorkState.COMPLETED, WorkState.ABANDONED, WorkState.FAULTED):
                continue
            node = wf.nodes[item.node_id]
            in_node = (now - item.created_at).total_seconds() if item.created_at else 0.0
            own = max(node["mean"] - in_node, 0.1 * node["mean"])
            entry: dict[str, Any] = {"node": item.node_id, "state": item.state.value, "in_node": in_node}
            if item.state is WorkState.PARKED:
                park = await self._expected_park(item.dependency, item.parked_at, now)
                own += park["remaining"]
                entry["park"] = park
                if item.hold_id:
                    entry["hold_id"] = item.hold_id
                    if holds and item.hold_id in holds:
                        entry["hold"] = holds[item.hold_id]
            downstream = node["remaining_mean"] - node["mean"]
            entry["remaining"] = own + downstream
            frontier.append(entry)
        for bookmark in execution.bookmarks:
            node = wf.nodes[bookmark.node_id]
            waited = (now - bookmark.created_at).total_seconds() if bookmark.created_at else 0.0
            own = max(node["mean"] - waited, 0.1 * node["mean"])
            frontier.append({"node": bookmark.node_id, "state": "waiting", "in_node": waited,
                             "waiting_for": f"{bookmark.stimulus_type}:{bookmark.key}",
                             "remaining": own + node["remaining_mean"] - node["mean"]})
        remaining = max((f["remaining"] for f in frontier), default=0.0)
        spread = wf.p90 / wf.mean if wf.mean > 0 else 1.0
        return {**base, "elapsed": elapsed, "remaining_mean": remaining,
                "eta": (now + timedelta(seconds=remaining)).isoformat(),
                "eta_p90": (now + timedelta(seconds=remaining * max(spread, 1.0))).isoformat(),
                "frontier": frontier, "blocked_by": sorted({f["hold_id"] for f in frontier if "hold_id" in f}),
                "basis": wf.basis, "parked": execution.status is ExecutionStatus.PARKED}

    async def _expected_park(self, dependency: str | None, parked_at: datetime | None, now: datetime) -> dict[str, Any]:
        history = await self.store.park_durations(dependency)
        if history:
            mean, _, _ = weighted_stats(history, self.half_life)
            basis = "history"
        else:
            mean, basis = self.park_default_seconds, "default"
        parked_for = (now - parked_at).total_seconds() if parked_at else 0.0
        return {"expected": mean, "parked_for": parked_for, "remaining": max(mean - parked_for, 0.1 * mean),
                "basis": basis, "samples": len(history)}
