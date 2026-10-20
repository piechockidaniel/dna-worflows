import type { ActivityDescriptor, Estimate, WorkflowDefinition } from "../api/types";
import { availableFields, undoFields } from "../lib/dataPicker";
import { setInput, updateNode } from "../lib/graph";
import { fieldsFor } from "../lib/schemaForm";
import { ValueEditor } from "./ValueEditor";

const UNITS: [string, number][] = [["seconds", 1], ["minutes", 60], ["hours", 3600], ["days", 86400]];

export function NodeInspector({ definition, nodeId, descriptors, onChange, readOnly }: {
  definition: WorkflowDefinition;
  nodeId: string;
  descriptors: Record<string, ActivityDescriptor>;
  onChange: (def: WorkflowDefinition) => void;
  readOnly?: boolean;
}) {
  const node = definition.nodes[nodeId];
  const descriptor = descriptors[node.type];
  if (!descriptor) return <div className="error">Unknown activity type {node.type}</div>;
  const options = availableFields(definition, nodeId, descriptors);
  const fields = fieldsFor(descriptor.inputs_schema);

  return (
    <fieldset className="inspector" disabled={readOnly}>
      <div className="field">
        <label htmlFor="node-name">Step name</label>
        <input id="node-name" value={node.name ?? ""} placeholder={node.id}
          onChange={(e) => onChange(updateNode(definition, nodeId, { name: e.target.value }))} />
        <div className="hint">{descriptor.display_name} · {descriptor.description}</div>
      </div>
      {fields.map((field) => (
        <ValueEditor key={`${nodeId}:${field.name}`} field={field} value={node.inputs[field.name]}
          options={options} definition={definition}
          onChange={(v) => onChange(setInput(definition, nodeId, field.name, v))} />
      ))}
      <UndoEditor definition={definition} nodeId={nodeId} descriptors={descriptors} onChange={onChange} />
      <EstimateEditor estimate={node.estimate ?? null} features={Object.keys(definition.forecast?.features ?? {})}
        onChange={(estimate) => onChange(updateNode(definition, nodeId, { estimate }))} />
      <div className="field">
        <label htmlFor="node-note">Notes for colleagues</label>
        <textarea id="node-note" rows={2} value={String(node.ui?.note ?? "")}
          onChange={(e) => onChange(updateNode(definition, nodeId, { ui: { ...(node.ui ?? {}), note: e.target.value } }))} />
      </div>
    </fieldset>
  );
}

/** "How long does this step usually take?" – a three-point estimate in the analyst's unit. */
export function EstimateEditor({ estimate, features, onChange }: {
  estimate: Estimate | null;
  features: string[];
  onChange: (estimate: Estimate | null) => void;
}) {
  const unit = pickUnit(estimate?.likely ?? 60);
  const factor = UNITS.find(([u]) => u === unit)![1];
  const show = (v: number | null | undefined) => (v === null || v === undefined ? "" : String(+(v / factor).toFixed(2)));
  const update = (patch: Partial<Estimate>) => {
    const next = { likely: estimate?.likely ?? factor, ...estimate, ...patch };
    onChange(next.likely === undefined || Number.isNaN(next.likely) ? null : next);
  };
  const num = (raw: string) => (raw === "" ? null : Number(raw) * factor);
  return (
    <div className="estimate">
      <div className="estimate-title">How long does this step usually take? <span className="hint">({unit})</span></div>
      <div className="estimate-row">
        <label>Fastest<input aria-label="fastest" type="number" value={show(estimate?.optimistic)}
          onChange={(e) => update({ optimistic: num(e.target.value) })} /></label>
        <label>Usually<input aria-label="usually" type="number" value={show(estimate?.likely)}
          onChange={(e) => e.target.value === "" ? onChange(null) : update({ likely: num(e.target.value)! })} /></label>
        <label>Slowest<input aria-label="slowest" type="number" value={show(estimate?.pessimistic)}
          onChange={(e) => update({ pessimistic: num(e.target.value) })} /></label>
      </div>
      {features.length > 0 && (
        <div className="estimate-row">
          <label>…plus per
            <select aria-label="driver feature" value={estimate?.driver?.feature ?? ""}
              onChange={(e) => update({ driver: e.target.value ? { feature: e.target.value,
                per_unit: estimate?.driver?.per_unit ?? factor } : null })}>
              <option value="">—</option>
              {features.map((f) => <option key={f}>{f}</option>)}
            </select>
          </label>
          {estimate?.driver && <label>{unit} each<input aria-label="per unit" type="number"
            value={show(estimate.driver.per_unit)}
            onChange={(e) => update({ driver: { ...estimate.driver!, per_unit: num(e.target.value) ?? 0 } })} /></label>}
        </div>
      )}
      <div className="hint">A rough guess is fine – DnA Workflows refines it with every real run.</div>
    </div>
  );
}

export function pickUnit(seconds: number): string {
  if (seconds >= 86400) return "days";
  if (seconds >= 3600) return "hours";
  if (seconds >= 60) return "minutes";
  return "seconds";
}

/** "If this step must be undone later, do this" – the saga compensation of a step. */
export function UndoEditor({ definition, nodeId, descriptors, onChange }: {
  definition: WorkflowDefinition; nodeId: string; descriptors: Record<string, ActivityDescriptor>;
  onChange: (def: WorkflowDefinition) => void;
}) {
  const node = definition.nodes[nodeId];
  const spec = node.compensate ?? null;
  const undoDescriptor = spec ? descriptors[spec.type] : undefined;
  const options = undoFields(definition, nodeId, descriptors);
  const actions = Object.values(descriptors).filter((d) => !d.is_trigger);
  return (
    <details className="estimate" open={!!spec}>
      <summary className="estimate-title">If this step must be undone later…</summary>
      <div className="field">
        <label htmlFor="undo-type">Undo with</label>
        <select id="undo-type" value={spec?.type ?? ""} onChange={(e) => onChange(updateNode(definition, nodeId, {
          compensate: e.target.value ? { type: e.target.value, inputs: {} } : null }))}>
          <option value="">nothing to undo</option>
          {actions.map((d) => <option key={d.type} value={d.type}>{d.display_name}</option>)}
        </select>
        <div className="hint">Runs automatically (newest step first) when the run is compensated.</div>
      </div>
      {spec && undoDescriptor && fieldsFor(undoDescriptor.inputs_schema).map((field) => (
        <ValueEditor key={`undo:${nodeId}:${field.name}`} field={field} value={spec.inputs[field.name]}
          options={options} definition={definition}
          onChange={(v) => {
            const inputs = { ...spec.inputs };
            if (v === undefined) delete inputs[field.name]; else inputs[field.name] = v;
            onChange(updateNode(definition, nodeId, { compensate: { ...spec, inputs } }));
          }} />))}
    </details>
  );
}
