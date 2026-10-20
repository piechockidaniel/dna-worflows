import { useId, useState } from "react";
import type { Binding, Json, WorkflowDefinition } from "../api/types";
import { describeBinding, isBinding, type FieldOption } from "../lib/dataPicker";
import { parseLiteral, type FieldSpec } from "../lib/schemaForm";

type Mode = "value" | "data" | "text";

function modeOf(value: Binding | undefined): Mode {
  if (isBinding(value as Binding)) return "$tmpl" in (value as Record<string, string>) ? "text" : "data";
  return "value";
}

/** One input: a fixed value, data picked from earlier steps, or a text with inserted data. */
export function ValueEditor({ field, value, onChange, options, definition }: {
  field: FieldSpec;
  value: Binding | undefined;
  onChange: (value: Binding | undefined) => void;
  options: FieldOption[];
  definition: WorkflowDefinition;
}) {
  const [mode, setMode] = useState<Mode>(modeOf(value));
  const id = `field-${field.name}-${useId()}`;
  const literal = value === undefined || isBinding(value as Binding) ? "" : value;

  return (
    <div className="field">
      <label htmlFor={id}>
        {field.label}{field.required && <span className="req" title="required">*</span>}
      </label>
      <div className="mode-switch" role="group" aria-label={`${field.label} source`}>
        {(["value", "data", "text"] as Mode[]).map((m) => (
          <button key={m} type="button" className={m === mode ? "on" : ""} onClick={() => setMode(m)}>
            {m === "value" ? "Fixed value" : m === "data" ? "From data" : "Text with data"}
          </button>
        ))}
      </div>
      {mode === "value" && <LiteralInput id={id} field={field} value={literal as Json}
        onChange={(v) => onChange(v === "" || v === null ? undefined : v)} />}
      {mode === "data" && (
        <select id={id} value={isBinding(value as Binding) ? JSON.stringify(value) : ""}
          onChange={(e) => onChange(e.target.value ? JSON.parse(e.target.value) : undefined)}>
          <option value="">— choose data —</option>
          {groups(options).map(([group, items]) => (
            <optgroup key={group} label={group}>
              {items.map((o) => <option key={o.token} value={JSON.stringify(o.binding)}>{o.label}</option>)}
            </optgroup>
          ))}
        </select>
      )}
      {mode === "text" && <TemplateInput id={id} value={value} options={options} onChange={onChange} />}
      {isBinding(value as Binding) && mode !== "value" && (
        <div className="hint">Uses: {describeBinding(value as Binding, definition)}</div>)}
      {field.description && <div className="hint">{field.description}</div>}
    </div>
  );
}

function LiteralInput({ id, field, value, onChange }: { id: string; field: FieldSpec; value: Json;
  onChange: (v: Json) => void }) {
  if (field.kind === "boolean") {
    return <input id={id} type="checkbox" checked={value === true} onChange={(e) => onChange(e.target.checked)} />;
  }
  if (field.kind === "enum") {
    return (
      <select id={id} value={value === null ? "" : String(value)} onChange={(e) => onChange(e.target.value)}>
        <option value="">—</option>
        {(field.options ?? []).map((o) => <option key={String(o)} value={String(o)}>{String(o)}</option>)}
      </select>
    );
  }
  if (field.kind === "list" || field.kind === "object" || field.kind === "any") {
    const text = value === null || value === "" ? "" : typeof value === "string" ? value : JSON.stringify(value, null, 2);
    return <textarea id={id} rows={3} defaultValue={text} placeholder={field.kind === "list" ? "[ … ]" : "{ … }"}
      onBlur={(e) => onChange(e.target.value === "" ? null : parseLiteral(field.kind, e.target.value))} />;
  }
  return <input id={id} type={field.kind === "text" ? "text" : "number"}
    value={value === null ? "" : String(value)}
    onChange={(e) => onChange(e.target.value === "" ? null : parseLiteral(field.kind, e.target.value))} />;
}

function TemplateInput({ id, value, options, onChange }: { id: string; value: Binding | undefined;
  options: FieldOption[]; onChange: (v: Binding | undefined) => void }) {
  const text = isBinding(value as Binding) && "$tmpl" in (value as Record<string, string>)
    ? (value as Record<string, string>).$tmpl : "";
  const set = (t: string) => onChange(t ? { $tmpl: t } : undefined);
  return (
    <div className="template">
      <textarea id={id} rows={2} value={text} onChange={(e) => set(e.target.value)}
        placeholder="e.g. Order {input.data.order_id} booked" />
      <select aria-label="insert data" value="" onChange={(e) => e.target.value && set(`${text}${e.target.value}`)}>
        <option value="">+ insert data…</option>
        {groups(options).map(([group, items]) => (
          <optgroup key={group} label={group}>
            {items.map((o) => <option key={o.token} value={o.token}>{o.label}</option>)}
          </optgroup>
        ))}
      </select>
    </div>
  );
}

function groups(options: FieldOption[]): [string, FieldOption[]][] {
  const map = new Map<string, FieldOption[]>();
  for (const o of options) map.set(o.group, [...(map.get(o.group) ?? []), o]);
  return [...map.entries()];
}
