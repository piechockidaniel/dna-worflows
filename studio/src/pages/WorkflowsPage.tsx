import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useSession } from "../auth/session";
import { Empty, ErrorBanner, Section, StatusChip, useAction, useAsync } from "../components/common";
import { emptyDefinition } from "../lib/graph";
import { slugify } from "../lib/format";

export function WorkflowsPage() {
  const { api, can, capabilities } = useSession();
  const navigate = useNavigate();
  const authoring = capabilities.features_enabled.includes("authoring");
  const workflows = useAsync(() => api.workflows(), [api]);
  const drafts = useAsync(() => (authoring ? api.drafts() : Promise.resolve([])), [api]);
  const [name, setName] = useState("");
  const [run, busy, error] = useAction();

  const create = () => run(async () => {
    const id = slugify(name);
    await api.startDraft(id, emptyDefinition(id, name));
    navigate(`/designer/${id}`);
  });
  const draftOf = (id: string) => drafts.data?.find((d) => d.workflow_id === id);
  const unpublished = (drafts.data ?? []).filter((d) => !workflows.data?.some((w) => w.id === d.workflow_id));

  return (
    <div className="page">
      <h1>Workflows</h1>
      <ErrorBanner error={error || workflows.error} />
      {authoring && can("workflows.design") && (
        <div className="inline-form">
          <input aria-label="new workflow name" placeholder="Name of a new workflow, e.g. Order to ERP" value={name}
            onChange={(e) => setName(e.target.value)} />
          <button className="primary" disabled={!slugify(name) || busy} onClick={create}>Create</button>
        </div>)}
      <Section title="Published">
        {workflows.data?.length ? (
          <table><thead><tr><th>Name</th><th>Version</th><th>Steps</th><th>Draft</th><th /></tr></thead>
            <tbody>{workflows.data.map((w) => (
              <tr key={w.id}><td>{w.name || w.id}<div className="hint">{w.description}</div></td><td>v{w.version}</td>
                <td>{Object.keys(w.nodes).length}</td>
                <td>{draftOf(w.id) && draftOf(w.id)!.status !== "published" ? <StatusChip status={draftOf(w.id)!.status} /> : "–"}</td>
                <td>{authoring && <Link to={`/designer/${w.id}`}>{can("workflows.design") ? "Open / edit" : "View"}</Link>}</td></tr>))}
            </tbody></table>) : <Empty>No published workflows yet.</Empty>}
      </Section>
      {unpublished.length > 0 && <Section title="New workflows in progress">
        <ul>{unpublished.map((d) => <li key={d.workflow_id}><Link to={`/designer/${d.workflow_id}`}>
          {d.definition.name || d.workflow_id}</Link> <StatusChip status={d.status} /></li>)}</ul>
      </Section>}
    </div>
  );
}
