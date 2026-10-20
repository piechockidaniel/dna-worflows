import { useState } from "react";
import { useSession } from "../auth/session";
import { Empty, ErrorBanner, Section, useAction, useAsync } from "../components/common";
import type { WorkflowForecast } from "../api/types";
import { basisLabel, formatDuration } from "../lib/format";

export function ForecastsPage() {
  const { api, capabilities } = useSession();
  const enabled = capabilities.features_enabled.includes("forecasting");
  const workflows = useAsync(() => api.workflows(), [api]);
  const accuracy = useAsync(() => (enabled ? api.forecastAccuracy() : Promise.resolve([])), [api]);
  const [selected, setSelected] = useState("");
  const [input, setInput] = useState("{}");
  const [forecast, setForecast] = useState<WorkflowForecast | null>(null);
  const stats = useAsync(() => (enabled && selected ? api.forecastStats(selected) : Promise.resolve([])), [api, selected]);
  const [run, busy, error] = useAction();

  if (!enabled) return <div className="page"><h1>Forecasts</h1><Empty>Forecasting is not enabled.</Empty></div>;
  return (
    <div className="page">
      <h1>Forecasts</h1>
      <ErrorBanner error={error || workflows.error} />
      <div className="inline-form">
        <select aria-label="workflow" value={selected} onChange={(e) => { setSelected(e.target.value); setForecast(null); }}>
          <option value="">choose a workflow…</option>
          {(workflows.data ?? []).map((w) => <option key={w.id} value={w.id}>{w.name || w.id}</option>)}
        </select>
        <input aria-label="sample input" value={input} onChange={(e) => setInput(e.target.value)} />
        <button disabled={!selected || busy}
          onClick={() => run(async () => setForecast(await api.forecastWorkflow(selected, JSON.parse(input || "{}"))))}>
          How long will it take?</button>
      </div>
      {forecast && <Section title="Estimate">
        <p className="big">Usually about <strong>{formatDuration(forecast.p50)}</strong>, rarely more than{" "}
          <strong>{formatDuration(forecast.p90)}</strong></p>
        <p className="hint">Based on {basisLabel(forecast.basis)} · critical path: {forecast.critical_path.join(" → ")}</p>
      </Section>}
      {selected && <Section title="What history says, step by step">
        {stats.data?.length ? <table><thead><tr><th>Step</th><th>Runs</th><th>Typical</th><th>Slow (90%)</th><th>Attempts</th></tr></thead>
          <tbody>{stats.data.map((s) => (
            <tr key={String(s.node)}><td>{String(s.node)}</td><td>{String(s.samples)}</td><td>{formatDuration(Number(s.p50))}</td>
              <td>{formatDuration(Number(s.p90))}</td><td>{Number(s.mean_attempts).toFixed(1)}</td></tr>))}</tbody></table>
          : <Empty>No runs recorded yet – forecasts use the designer’s estimates.</Empty>}
      </Section>}
      <Section title="How good were our forecasts?">
        {accuracy.data?.length ? <table><thead><tr><th>Workflow</th><th>Finished runs</th><th>Average error</th><th>Within “rarely more than”</th></tr></thead>
          <tbody>{accuracy.data.map((a) => (
            <tr key={String(a.workflow)}><td>{String(a.workflow)}</td><td>{String(a.settled)}</td>
              <td>{a.mape != null ? `${Math.round(Number(a.mape) * 100)}%` : "–"}</td>
              <td>{a.p90_coverage != null ? `${Math.round(Number(a.p90_coverage) * 100)}%` : "–"}</td></tr>))}</tbody></table>
          : <Empty>Accuracy appears after the first forecasted runs finish.</Empty>}
      </Section>
    </div>
  );
}
