import { Link } from "react-router-dom";
import { useSession } from "../auth/session";
import { Empty, ErrorBanner, Section, StatusChip, useAsync } from "../components/common";
import { formatTime } from "../lib/format";

/** "What can I pick up?" – work waiting for *any* holder of my roles, so nobody is a bottleneck. */
export function HomePage() {
  const { api, can, principal, capabilities } = useSession();
  const has = (f: string) => capabilities.features_enabled.includes(f);
  const drafts = useAsync(() => (has("authoring") ? api.drafts() : Promise.resolve([])), [api]);
  const proposals = useAsync(() => (has("resilience") && can("proposals.read") ? api.proposals("pending")
    : Promise.resolve([])), [api]);
  const holds = useAsync(() => (has("resilience") && can("holds.read") ? api.holds(true) : Promise.resolve([])), [api]);
  const faulted = useAsync(() => (can("executions.read") ? api.executions({ status: "faulted", limit: "20" })
    : Promise.resolve([])), [api]);

  const all = drafts.data ?? [];
  const toReview = all.filter((d) => d.status === "in_review");
  const toPublish = all.filter((d) => d.status === "approved");
  const toRework = all.filter((d) => ["draft", "changes_requested"].includes(d.status));

  return (
    <div className="page">
      <h1>Hello {principal.name}</h1>
      <p className="hint">Anything below can be picked up by anyone with the same role – no single person holds the process.</p>
      <ErrorBanner error={drafts.error || proposals.error || holds.error || faulted.error} />
      <div className="grid">
        {can("workflows.review") && <Section title={`Waiting for review (${toReview.length})`}>
          {toReview.length ? <ul>{toReview.map((d) => <li key={d.workflow_id}>
            <Link to={`/designer/${d.workflow_id}`}>{d.definition.name || d.workflow_id}</Link>
            <span className="hint"> by {d.updated_by}</span></li>)}</ul> : <Empty>Nothing to review.</Empty>}
        </Section>}
        {can("workflows.publish") && <Section title={`Approved, ready to publish (${toPublish.length})`}>
          {toPublish.length ? <ul>{toPublish.map((d) => <li key={d.workflow_id}>
            <Link to={`/designer/${d.workflow_id}`}>{d.definition.name || d.workflow_id}</Link></li>)}</ul>
            : <Empty>Nothing to publish.</Empty>}
        </Section>}
        {can("workflows.design") && <Section title={`Drafts in progress (${toRework.length})`}>
          {toRework.length ? <ul>{toRework.map((d) => <li key={d.workflow_id}>
            <Link to={`/designer/${d.workflow_id}`}>{d.definition.name || d.workflow_id}</Link>{" "}
            <StatusChip status={d.status} /><span className="hint"> last: {d.updated_by}</span></li>)}</ul>
            : <Empty>No open drafts.</Empty>}
        </Section>}
        {can("proposals.approve") && <Section title={`Decisions waiting (${proposals.data?.length ?? 0})`}>
          {proposals.data?.length ? <ul>{proposals.data.map((p) => <li key={p.id}>
            <Link to="/operations">{p.kind}</Link> – {p.reason}<span className="hint"> ({p.proposed_by})</span></li>)}</ul>
            : <Empty>No pending proposals.</Empty>}
        </Section>}
        {can("holds.read") && has("resilience") && <Section title={`Frozen areas (${holds.data?.length ?? 0})`}>
          {holds.data?.length ? <ul>{holds.data.map((h) => <li key={h.id}>
            <Link to="/operations">{h.scope}: {h.target}</Link> <span className="hint">{h.reason} · since {formatTime(h.created_at)}</span></li>)}</ul>
            : <Empty>Everything is flowing.</Empty>}
        </Section>}
        {can("executions.read") && <Section title={`Failed runs (${faulted.data?.length ?? 0})`}>
          {faulted.data?.length ? <ul>{faulted.data.map((e) => <li key={e.id}>
            <Link to={`/executions/${e.id}`}>{e.definition_id}</Link> <span className="hint">{e.fault?.message}</span></li>)}</ul>
            : <Empty>No failed runs.</Empty>}
        </Section>}
      </div>
    </div>
  );
}
