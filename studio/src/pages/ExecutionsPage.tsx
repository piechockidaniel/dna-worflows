import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useSession } from "../auth/session";
import { Empty, ErrorBanner, Section, StatusChip, useAction, useAsync } from "../components/common";
import { basisLabel, formatDuration, formatTime } from "../lib/format";
import { eventLabel } from "../lib/journal";

export function ExecutionsPage() {
  const { api } = useSession();
  const [status, setStatus] = useState("");
  const [correlation, setCorrelation] = useState("");
  const list = useAsync(() => api.executions({ status: status || undefined, correlation_id: correlation || undefined,
    limit: "100" }), [api, status, correlation]);
  return (
    <div className="page">
      <h1>Executions</h1>
      <div className="inline-form">
        <select aria-label="status filter" value={status} onChange={(e) => setStatus(e.target.value)}>
          <option value="">all statuses</option>
          {["running", "waiting", "parked", "completed", "faulted", "cancelled"].map((s) => <option key={s}>{s}</option>)}
        </select>
        <input aria-label="correlation filter" placeholder="business key / correlation id, e.g. order-42"
          value={correlation} onChange={(e) => setCorrelation(e.target.value)} />
      </div>
      <ErrorBanner error={list.error} />
      {list.data?.length ? (
        <table><thead><tr><th>Workflow</th><th>Status</th><th>Correlation</th><th>Started</th></tr></thead>
          <tbody>{list.data.map((e) => (
            <tr key={e.id}><td><Link to={`/executions/${e.id}`}>{e.definition_id}</Link></td>
              <td><StatusChip status={e.status} /></td><td>{e.correlation_id}</td><td>{formatTime(e.created_at)}</td></tr>))}
          </tbody></table>) : <Empty>No executions match.</Empty>}
    </div>
  );
}

export function ExecutionDetailPage() {
  const { id = "" } = useParams();
  const { api, can, capabilities } = useSession();
  const execution = useAsync(() => api.execution(id), [api, id]);
  const journal = useAsync(() => api.executionJournal(id), [api, id]);
  const eta = useAsync(() => (capabilities.features_enabled.includes("forecasting") ? api.forecastExecution(id)
    : Promise.resolve(null)), [api, id]);
  const [run, busy, error] = useAction();
  const [reason, setReason] = useState("");
  const reload = () => { execution.reload(); journal.reload(); eta.reload(); };
  const act = (fn: () => Promise<unknown>) => run(async () => { await fn(); reload(); });
  const ex = execution.data;
  if (!ex) return <div className="page"><ErrorBanner error={execution.error} />Loading…</div>;
  const operate = can("executions.operate");

  return (
    <div className="page">
      <h1>{ex.definition_id} <StatusChip status={ex.status} /></h1>
      <p className="hint">correlation {ex.correlation_id} · trace {ex.trace_id} · started {formatTime(ex.created_at)}</p>
      <ErrorBanner error={error || execution.error} />
      {eta.data && ex.status !== "completed" && (
        <Section title="When will it be done?">
          <p className="big">Expected in <strong>{formatDuration(eta.data.remaining_mean)}</strong>
            {" "}(around {formatTime(eta.data.eta)}, latest ~{formatTime(eta.data.eta_p90)})</p>
          <p className="hint">Based on {basisLabel(eta.data.basis)}.{eta.data.blocked_by.length > 0 &&
            ` Blocked by hold ${eta.data.blocked_by.join(", ")} – the estimate includes typical waiting time.`}</p>
        </Section>)}
      {ex.fault && <div className="error">{ex.fault.kind}: {ex.fault.message}</div>}
      {ex.compensation_mode && <div className="problems">Undoing completed steps ({ex.compensation_mode.split(":")[0]})…</div>}
      {!!ex.children?.length && <p className="hint">Sub-runs: {ex.children.map((c) =>
        <Link key={c} to={`/executions/${c}`}>{c.split(".").slice(-2).join(".")} </Link>)}</p>}
      {ex.parent_execution_id && <p className="hint">Part of <Link to={`/executions/${ex.parent_execution_id}`}>
        {ex.parent_execution_id}</Link></p>}
      {operate && (
        <div className="inline-form">
          <input aria-label="reason" placeholder="reason (kept in the audit trail)" value={reason}
            onChange={(e) => setReason(e.target.value)} />
          {ex.status === "faulted" && <button disabled={busy} onClick={() => act(() => api.retry(id, reason || "retry"))}>Retry</button>}
          {!["cancelled", "compensated"].includes(ex.status) && !ex.compensation_mode && (
            <button disabled={busy} title="stop this run and undo its completed steps"
              onClick={() => act(() => api.compensate(id, reason || "compensated in Studio"))}>Undo completed steps</button>)}
          {!["completed", "cancelled", "faulted", "compensated"].includes(ex.status) && (
            <button className="danger" disabled={busy} onClick={() => act(() => api.cancel(id, reason || "cancelled"))}>Cancel</button>)}
        </div>)}
      <Section title="Open work">
        {ex.work.length || ex.bookmarks.length ? (
          <table><thead><tr><th>Step</th><th>State</th><th>Attempts</th><th>Why</th><th /></tr></thead>
            <tbody>
              {ex.work.map((w) => (
                <tr key={w.id}><td>{w.node_id}</td><td><StatusChip status={w.state} /></td><td>{w.attempt}</td>
                  <td>{w.parked_reason ?? w.last_fault?.message ?? ""}</td>
                  <td>{operate && w.state === "parked" && <>
                    <button disabled={busy} onClick={() => act(() => api.resumeWork(id, w.id, reason || "resumed"))}>Resume</button>{" "}
                    <button className="danger" disabled={busy}
                      onClick={() => act(() => api.abandonWork(id, w.id, reason || "abandoned"))}>Abandon</button></>}</td></tr>))}
              {ex.bookmarks.map((b) => <tr key={b.id}><td>{b.node_id}</td><td><StatusChip status="waiting" /></td><td />
                <td>waiting for {b.stimulus_type} {b.key}</td><td /></tr>)}
            </tbody></table>) : <Empty>Nothing open.</Empty>}
      </Section>
      <Section title="Timeline">
        <ol className="timeline">{(journal.data ?? []).map((e) => (
          <li key={e.seq}><span className="hint">{formatTime(e.timestamp)}</span> <strong title={e.kind}>{eventLabel(e.kind)}</strong>
            {e.node_id && <> · {e.node_id}</>}
            {typeof e.data.latency_ms === "number" && <span className="hint"> · took {formatDuration(e.data.latency_ms / 1000)}</span>}
            {typeof e.data.reason === "string" && <span className="hint"> · {e.data.reason}</span>}</li>))}</ol>
      </Section>
      <Section title="Data"><details><summary>Input and outputs</summary>
        <pre>{JSON.stringify({ input: ex.input, outputs: ex.outputs, output: ex.output }, null, 2)}</pre></details>
      </Section>
      <p><Link to="/executions">← all executions</Link></p>
    </div>
  );
}
