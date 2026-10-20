import { useState } from "react";
import { Link } from "react-router-dom";
import { useSession } from "../auth/session";
import { Empty, ErrorBanner, Section, StatusChip, useAction, useAsync } from "../components/common";
import { formatTime } from "../lib/format";
import { PROPOSAL_LABELS } from "../lib/journal";

export function OperationsPage() {
  const { api, can, capabilities } = useSession();
  const resilience = capabilities.features_enabled.includes("resilience");
  const ai = capabilities.features_enabled.includes("ai");
  const holds = useAsync(() => (resilience ? api.holds() : Promise.resolve([])), [api]);
  const proposals = useAsync(() => (resilience ? api.proposals() : Promise.resolve([])), [api]);
  const parked = useAsync(() => api.parked(), [api]);
  const [run, busy, error, clear] = useAction();
  const [note, setNote] = useState("");
  const [hold, setHold] = useState({ scope: "dependency", target: "", reason: "" });
  const [triage, setTriage] = useState<Record<string, unknown> | null>(null);
  const reload = () => { holds.reload(); proposals.reload(); parked.reload(); };
  const act = (fn: () => Promise<unknown>) => run(async () => { await fn(); reload(); });

  if (!resilience) return <div className="page"><h1>Operations</h1><Empty>Resilience is not enabled.</Empty></div>;
  const active = (holds.data ?? []).filter((h) => h.status === "active");
  const describeTarget = (target: Record<string, unknown>) => {
    const hold = (holds.data ?? []).find((h) => h.id === target.hold_id);
    if (hold) return `${hold.scope}: ${hold.target}`;
    return target.execution_id ? `run ${String(target.execution_id)}` : JSON.stringify(target);
  };
  const pending = (proposals.data ?? []).filter((p) => p.status === "pending");

  return (
    <div className="page">
      <h1>Operations</h1>
      <ErrorBanner error={error} onClose={clear} />
      <Section title={`Frozen areas (${active.length})`}>
        {can("holds.manage") && <div className="inline-form">
          <select aria-label="hold scope" value={hold.scope} onChange={(e) => setHold({ ...hold, scope: e.target.value })}>
            {["dependency", "workflow", "activity_type", "ingress", "execution", "global"].map((s) => <option key={s}>{s}</option>)}
          </select>
          <input aria-label="hold target" placeholder="e.g. http:erp" value={hold.target}
            onChange={(e) => setHold({ ...hold, target: e.target.value })} />
          <input aria-label="hold reason" placeholder="why" value={hold.reason}
            onChange={(e) => setHold({ ...hold, reason: e.target.value })} />
          <button disabled={busy || !hold.reason} onClick={() => act(() => api.placeHold(hold.scope, hold.target || "*", hold.reason))}>
            Freeze</button></div>}
        {active.length ? <table><thead><tr><th>What</th><th>Why</th><th>Since</th><th>Released by</th><th /></tr></thead>
          <tbody>{active.map((h) => (
            <tr key={h.id}><td>{h.scope}: <strong>{h.target}</strong></td><td>{h.reason}<div className="hint">{h.source}</div></td>
              <td>{formatTime(h.created_at)}</td><td>{h.release_strategy}</td>
              <td>{can("holds.manage") && <button disabled={busy}
                onClick={() => act(() => api.releaseHold(h.id, note || "released in Studio"))}>Release</button>}
                {ai && can("ai.triage") && <button disabled={busy} onClick={() => run(async () => setTriage(await api.triage(h.id)))}>
                  Ask AI</button>}</td></tr>))}</tbody></table> : <Empty>Everything is flowing.</Empty>}
        {triage && <div className="result"><strong>AI triage:</strong> {String(triage.recommendation)} ({String(triage.confidence)})
          – {String(triage.root_cause)}<div className="hint">{String(triage.reasoning ?? "")}</div></div>}
      </Section>
      <Section title={`Decisions waiting (${pending.length})`}>
        <input aria-label="decision note" placeholder="note for the audit trail" value={note} onChange={(e) => setNote(e.target.value)} />
        {pending.length ? <table><thead><tr><th>Proposal</th><th>Reason</th><th>From</th><th>Confidence</th><th /></tr></thead>
          <tbody>{pending.map((p) => (
            <tr key={p.id}><td>{PROPOSAL_LABELS[p.kind] ?? p.kind}<div className="hint">{describeTarget(p.target)}</div></td><td>{p.reason}</td>
              <td>{p.proposed_by}</td><td>{p.confidence != null ? `${Math.round(p.confidence * 100)}%` : "–"}</td>
              <td>{can("proposals.approve") && <>
                <button className="primary" disabled={busy} onClick={() => act(() => api.approve(p.id, note))}>Approve</button>{" "}
                <button disabled={busy} onClick={() => act(() => api.reject(p.id, note))}>Reject</button></>}</td></tr>))}
          </tbody></table> : <Empty>No pending decisions.</Empty>}
      </Section>
      <Section title={`Parked work (${parked.data?.length ?? 0})`}>
        {parked.data?.length ? <ul>{parked.data.map((p) => <li key={p.work_item_id}>
          <Link to={`/executions/${p.execution_id}`}>{p.execution_id}</Link> <StatusChip status="parked" /></li>)}</ul>
          : <Empty>No parked work.</Empty>}
      </Section>
      <Section title="Released recently">
        <ul>{(holds.data ?? []).filter((h) => h.status === "released").slice(-10).map((h) => (
          <li key={h.id}>{h.scope}: {h.target} <span className="hint">by {h.released_by} · resumed {h.resumed_work}</span></li>))}</ul>
      </Section>
    </div>
  );
}
