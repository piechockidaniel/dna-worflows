import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import type { ActivityDescriptor, Draft, Json, TestRunResult, WorkflowDefinition, WorkflowForecast } from "../api/types";
import { useSession } from "../auth/session";
import { ErrorBanner, StatusChip, useAction } from "../components/common";
import { FlowCanvas, Palette, type Highlight, type Selection } from "../components/FlowCanvas";
import { NodeInspector } from "../components/NodeInspector";
import { basisLabel, formatDuration, formatTime } from "../lib/format";
import { addNode, disconnect, outcomeChoices, removeNode, setOutcome } from "../lib/graph";

type Panel = "step" | "test" | "forecast" | "review" | "settings";

export function DesignerPage() {
  const { id = "" } = useParams();
  const { api, can, descriptors } = useSession();
  const [draft, setDraft] = useState<Draft | null>(null);
  const [definition, setDefinition] = useState<WorkflowDefinition | null>(null);
  const [dirty, setDirty] = useState(false);
  const [selection, setSelection] = useState<Selection>(null);
  const [panel, setPanel] = useState<Panel>("step");
  const [highlights, setHighlights] = useState<Record<string, Highlight>>({});
  const [run, busy, error, clearError] = useAction();
  const editable = can("workflows.design") && draft?.status !== "in_review" && draft?.status !== "published";

  const load = (d: Draft) => { setDraft(d); setDefinition(d.definition); setDirty(false); };
  useEffect(() => {
    void run(async () => {
      let d = await api.draft(id).catch(() => api.startDraft(id));
      if (d.status === "published" && can("workflows.design")) d = await api.startDraft(id); // next version
      load(d);
    });
  }, [api, id, run, can]);

  const change = (next: WorkflowDefinition) => { setDefinition(next); setDirty(true); };
  const save = () => run(async () => load(await api.saveDraft(id, definition!, draft!.revision)));
  const act = (fn: () => Promise<Draft>) => run(async () => load(await fn()));

  const addStep = (descriptor: ActivityDescriptor) => {
    if (!definition) return;
    const from = selection?.kind === "node" ? selection.id : undefined;
    const fromPos = from ? definition.nodes[from].ui?.position : undefined;
    const position = fromPos ? { x: fromPos.x + 260, y: fromPos.y } : { x: 40, y: 40 + Object.keys(definition.nodes).length * 30 };
    const outcome = from ? outcomeChoices(descriptors[definition.nodes[from].type], definition, from)[0] ?? "Done" : "Done";
    const [next, newId] = addNode(definition, descriptor, position, from ? { source: from, outcome } : undefined);
    change(next);
    setSelection({ kind: "node", id: newId });
    setPanel("step");
  };

  if (!definition || !draft) return <div className="page"><ErrorBanner error={error} />Loading…</div>;

  return (
    <div className="designer">
      <header className="designer-bar">
        <Link to="/workflows">← Workflows</Link>
        <h1>{definition.name || definition.id}</h1>
        <StatusChip status={draft.status} />
        <span className="hint">rev {draft.revision} · last change by {draft.updated_by}</span>
        <div className="spacer" />
        {dirty && <span className="hint">unsaved changes</span>}
        {can("workflows.design") && <button disabled={!dirty || busy} onClick={save}>Save</button>}
        {can("workflows.design") && ["draft", "changes_requested"].includes(draft.status) && (
          <button disabled={dirty || busy || draft.problems.length > 0} onClick={() => act(() => api.submitDraft(id))}
            title={draft.problems.length ? "fix the problems first" : "ask a reviewer"}>Submit for review</button>)}
        {can("workflows.publish") && draft.status === "approved" && (
          <button className="primary" disabled={busy} onClick={() => act(() => api.publishDraft(id))}>Publish</button>)}
      </header>
      <ErrorBanner error={error} onClose={clearError} />
      {draft.problems.length > 0 && (
        <div className="problems"><strong>To fix before review:</strong>
          <ul>{draft.problems.map((p) => <li key={p}>{p}</li>)}</ul></div>)}
      <div className="designer-body">
        <Palette descriptors={descriptors} onAdd={addStep} disabled={!editable} />
        <FlowCanvas definition={definition} descriptors={descriptors} selection={selection} highlights={highlights}
          onSelect={(s) => { setSelection(s); if (s) setPanel("step"); }} onChange={change} readOnly={!editable} />
        <aside className="side">
          <nav className="tabs">
            {(["step", "test", "forecast", "review", "settings"] as Panel[]).map((p) => (
              <button key={p} className={p === panel ? "on" : ""} onClick={() => setPanel(p)}>{
                { step: "Step", test: "Test run", forecast: "Forecast", review: "Review & history",
                  settings: "Settings" }[p]}</button>))}
          </nav>
          {panel === "step" && <StepPanel definition={definition} selection={selection} descriptors={descriptors}
            editable={editable} onChange={change} onClear={() => setSelection(null)} onSelect={setSelection} />}
          {panel === "test" && <TestPanel draftId={id} definition={definition} dirty={dirty}
            onResult={setHighlights} />}
          {panel === "forecast" && <ForecastPanel definition={definition} onResult={setHighlights} />}
          {panel === "review" && <ReviewPanel draft={draft} onDraft={load} />}
          {panel === "settings" && <SettingsPanel definition={definition} editable={editable} onChange={change} />}
        </aside>
      </div>
    </div>
  );
}

function StepPanel({ definition, selection, descriptors, editable, onChange, onClear, onSelect }: {
  definition: WorkflowDefinition; selection: Selection; descriptors: Record<string, ActivityDescriptor>;
  editable: boolean; onChange: (d: WorkflowDefinition) => void; onClear: () => void; onSelect: (s: Selection) => void;
}) {
  if (!selection) return <div className="hint pad">Select a step or connection, or add a step from the left.
    A new step is connected after the selected one.</div>;
  if (selection.kind === "edge") {
    const e = selection.edge;
    const choices = outcomeChoices(descriptors[definition.nodes[e.source]?.type], definition, e.source);
    return (
      <div className="pad">
        <div className="field">
          <label htmlFor="edge-outcome">Follow this connection when “{definition.nodes[e.source]?.name ?? e.source}” ends with</label>
          <select id="edge-outcome" disabled={!editable} value={e.outcome}
            onChange={(ev) => {
              onChange(setOutcome(definition, e, ev.target.value));
              onSelect({ kind: "edge", edge: { ...e, outcome: ev.target.value } });   // keep the selection on the edited edge
            }}>
            {choices.map((c) => <option key={c}>{c}</option>)}
          </select>
        </div>
        {editable && <button className="danger" onClick={() => { onChange(disconnect(definition, e)); onClear(); }}>
          Remove connection</button>}
      </div>
    );
  }
  return (
    <div className="pad">
      <NodeInspector definition={definition} nodeId={selection.id} descriptors={descriptors} onChange={onChange}
        readOnly={!editable} />
      {editable && <button className="danger" onClick={() => { onChange(removeNode(definition, selection.id)); onClear(); }}>
        Remove step</button>}
    </div>
  );
}

function samplePlaceholder(schema: ActivityDescriptor["output_schema"]): string {
  const props = Object.keys(schema?.properties ?? {});
  return props.length ? `{${props.slice(0, 3).map((p) => `"${p}": …`).join(", ")}}` : "sample result (JSON)";
}

function TestPanel({ draftId, definition, dirty, onResult }: { draftId: string; definition: WorkflowDefinition;
  dirty: boolean; onResult: (h: Record<string, Highlight>) => void }) {
  const { api, descriptors } = useSession();
  const [input, setInput] = useState("{}");
  const [mocks, setMocks] = useState<Record<string, string>>({});
  const [result, setResult] = useState<TestRunResult | null>(null);
  const [run, busy, error] = useAction();

  const execute = () => run(async () => {
    const parsedMocks = Object.fromEntries(Object.entries(mocks).filter(([, v]) => v.trim())
      .map(([node, v]) => [node, { output: JSON.parse(v) as Json }]));
    const r = await api.testRun(draftId, JSON.parse(input || "{}"), parsedMocks);
    setResult(r);
    const h: Record<string, Highlight> = {};
    for (const n of r.path ?? []) h[n] = { tone: "ok", badge: r.simulated?.includes(n) ? "simulated" : "ran" };
    for (const w of r.waiting_for ?? []) h[w.node] = { tone: "info", badge: "waiting" };
    onResult(h);
  });

  return (
    <div className="pad">
      {dirty && <div className="hint">Save first – test runs use the saved draft.</div>}
      <div className="field"><label htmlFor="test-input">Sample input (JSON)</label>
        <textarea id="test-input" rows={4} value={input} onChange={(e) => setInput(e.target.value)} /></div>
      <details><summary>Sample results for steps (optional)</summary>
        <div className="hint">Steps that call other systems are simulated unless you give a sample result here.</div>
        {Object.values(definition.nodes).map((n) => (
          <div className="field" key={n.id}><label htmlFor={`mock-${n.id}`}>{n.name || n.id}</label>
            <textarea id={`mock-${n.id}`} rows={2} placeholder={samplePlaceholder(descriptors[n.type]?.output_schema)}
              value={mocks[n.id] ?? ""}
              onChange={(e) => setMocks({ ...mocks, [n.id]: e.target.value })} /></div>))}
      </details>
      <button className="primary" disabled={busy || dirty} onClick={execute}>Run test</button>
      <ErrorBanner error={error} />
      {result && (
        <div className="result">
          <p>Result: <StatusChip status={result.status} /></p>
          {result.problems?.map((p) => <div key={p} className="error">{p}</div>)}
          {result.fault && <div className="error">{result.fault.message}</div>}
          {!!result.path?.length && <p>Path: {result.path.map((n) => definition.nodes[n]?.name || n).join(" → ")}</p>}
          {!!result.waiting_for?.length && <p>Waiting for: {result.waiting_for.map((w) => w.stimulus).join(", ")}</p>}
          {!!result.would_send?.length && <p>Would send {result.would_send.length} message(s) – nothing was sent.</p>}
          <details><summary>Step outputs</summary><pre>{JSON.stringify(result.outputs, null, 2)}</pre></details>
        </div>)}
    </div>
  );
}

function ForecastPanel({ definition, onResult }: { definition: WorkflowDefinition;
  onResult: (h: Record<string, Highlight>) => void }) {
  const { api, capabilities } = useSession();
  const [input, setInput] = useState("{}");
  const [forecast, setForecast] = useState<WorkflowForecast | null>(null);
  const [run, busy, error] = useAction();
  const enabled = capabilities.features_enabled.includes("forecasting");
  const estimate = () => run(async () => {
    const f = await api.forecastPreview(definition, JSON.parse(input || "{}"));
    setForecast(f);
    onResult(Object.fromEntries(Object.values(f.nodes).map((n) => [n.node, {
      tone: n.basis === "default" ? "warn" : "info", badge: `~${formatDuration(n.mean)}` } as Highlight])));
  });
  if (!enabled) return <div className="hint pad">Forecasting is not enabled on this server.</div>;
  return (
    <div className="pad">
      <div className="field"><label htmlFor="fc-input">Sample input (JSON)</label>
        <textarea id="fc-input" rows={3} value={input} onChange={(e) => setInput(e.target.value)} /></div>
      <button disabled={busy} onClick={estimate}>Estimate duration</button>
      <ErrorBanner error={error} />
      {forecast && (
        <div className="result">
          <p className="big">Usually about <strong>{formatDuration(forecast.p50)}</strong>,
            rarely more than <strong>{formatDuration(forecast.p90)}</strong></p>
          <p className="hint">Based on {basisLabel(forecast.basis)}
            {forecast.history_lead_time ? ` · ${forecast.history_lead_time.samples} finished runs` : ""}</p>
          <table><thead><tr><th>Step</th><th>Typical</th><th>Basis</th><th>Runs through</th></tr></thead>
            <tbody>{Object.values(forecast.nodes).map((n) => (
              <tr key={n.node} className={forecast.critical_path.includes(n.node) ? "critical" : ""}>
                <td>{definition.nodes[n.node]?.name || n.node}</td><td>{formatDuration(n.mean)}</td>
                <td>{basisLabel(n.basis, n.samples)}</td><td>{Math.round(n.expected_visits * 100)}%</td></tr>))}
            </tbody></table>
          <p className="hint">Steps marked “no data yet” need an estimate (Step tab) until real runs exist.</p>
        </div>)}
    </div>
  );
}

function ReviewPanel({ draft, onDraft }: { draft: Draft; onDraft: (d: Draft) => void }) {
  const { api, can } = useSession();
  const [note, setNote] = useState("");
  const [comment, setComment] = useState("");
  const [run, busy, error] = useAction();
  const id = draft.workflow_id;
  return (
    <div className="pad">
      <ErrorBanner error={error} />
      {draft.status === "in_review" && can("workflows.review") && (
        <div className="review-box">
          <textarea aria-label="review note" rows={2} placeholder="Note for the team (optional)" value={note}
            onChange={(e) => setNote(e.target.value)} />
          <button className="primary" disabled={busy}
            onClick={() => run(async () => onDraft(await api.reviewDraft(id, "approve", note)))}>Approve</button>
          <button disabled={busy}
            onClick={() => run(async () => onDraft(await api.reviewDraft(id, "request_changes", note)))}>
            Request changes</button>
        </div>)}
      <h3>Comments</h3>
      {draft.comments.map((c) => <div key={c.id} className="comment"><strong>{c.author}</strong>
        <span className="hint"> {formatTime(c.at)}</span><p>{c.text}</p></div>)}
      <div className="comment-new">
        <textarea aria-label="new comment" rows={2} value={comment} onChange={(e) => setComment(e.target.value)} />
        <button disabled={busy || !comment.trim()}
          onClick={() => run(async () => { onDraft(await api.commentDraft(id, comment)); setComment(""); })}>Comment</button>
      </div>
      <h3>History</h3>
      <ul className="history">{[...draft.history].reverse().map((h, i) => (
        <li key={i}><span className="hint">{formatTime(h.at)}</span> <strong>{h.actor}</strong> {h.action.replace(":", " ")}
          {h.detail ? ` – ${h.detail}` : ""}</li>))}</ul>
    </div>
  );
}

function SettingsPanel({ definition, editable, onChange }: { definition: WorkflowDefinition; editable: boolean;
  onChange: (d: WorkflowDefinition) => void }) {
  const features = definition.forecast?.features ?? {};
  const [newFeature, setNewFeature] = useState({ name: "", path: "", transform: "value" });
  const setFeatures = (f: typeof features) => onChange({ ...definition, forecast: { ...definition.forecast, features: f } });
  const names = useMemo(() => Object.keys(features), [features]);
  return (
    <fieldset className="pad" disabled={!editable}>
      <div className="field"><label htmlFor="wf-name">Workflow name</label>
        <input id="wf-name" value={definition.name ?? ""} onChange={(e) => onChange({ ...definition, name: e.target.value })} /></div>
      <div className="field"><label htmlFor="wf-desc">Description</label>
        <textarea id="wf-desc" rows={3} value={definition.description ?? ""}
          onChange={(e) => onChange({ ...definition, description: e.target.value })} /></div>
      <div className="field">
        <label><input type="checkbox" checked={!!definition.compensate_on_fault}
          onChange={(e) => onChange({ ...definition, compensate_on_fault: e.target.checked })} />
          {" "}If the run fails, undo the completed steps automatically</label>
        <div className="hint">Uses each step’s “undo” action (Step tab), newest first.</div>
      </div>
      <h3>Inputs that influence duration</h3>
      <p className="hint">e.g. “lines” = number of order lines. Used for estimates per unit and to compare similar runs.</p>
      <table><tbody>{names.map((n) => (
        <tr key={n}><td>{n}</td><td>{features[n].transform} of {features[n].path}</td>
          <td><button className="link" onClick={() => { const f = { ...features }; delete f[n]; setFeatures(f); }}>remove</button></td></tr>))}
      </tbody></table>
      <div className="estimate-row">
        <input aria-label="feature name" placeholder="name" value={newFeature.name}
          onChange={(e) => setNewFeature({ ...newFeature, name: e.target.value })} />
        <input aria-label="feature path" placeholder="input path, e.g. data.lines" value={newFeature.path}
          onChange={(e) => setNewFeature({ ...newFeature, path: e.target.value })} />
        <select aria-label="feature transform" value={newFeature.transform}
          onChange={(e) => setNewFeature({ ...newFeature, transform: e.target.value })}>
          <option value="value">value</option><option value="count">count</option>
          <option value="length">length</option><option value="bucket">bucket</option>
        </select>
        <button disabled={!newFeature.name || !newFeature.path} onClick={() => {
          setFeatures({ ...features, [newFeature.name]: { path: newFeature.path,
            transform: newFeature.transform as "value" } });
          setNewFeature({ name: "", path: "", transform: "value" });
        }}>Add</button>
      </div>
    </fieldset>
  );
}
