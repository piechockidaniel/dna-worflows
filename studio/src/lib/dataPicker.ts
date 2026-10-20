// The data picker: analysts choose "which data" from a list instead of writing bindings.
// Produces the JSON binding forms the engine understands ({"$output": "node.path"}, {"$input": ...}, ...).
import type { ActivityDescriptor, Binding, JsonSchema, WorkflowDefinition } from "../api/types";
import { ancestors, roots } from "./graph";

export interface FieldOption {
  group: string;          // "Book in ERP", "Workflow input", "Variables", "Run"
  label: string;          // "body › erp_id"
  path: string[];         // for display
  binding: Binding;       // what gets stored
  token: string;          // for text templates: "{output.book.body.erp_id}"
}

export function flattenSchema(schema: JsonSchema | null | undefined, maxDepth = 3, prefix: string[] = []): string[][] {
  const result: string[][] = [];
  if (!schema || prefix.length >= maxDepth) return result;
  for (const [name, prop] of Object.entries(schema.properties ?? {})) {
    const path = [...prefix, name];
    result.push(path);
    result.push(...flattenSchema(prop, maxDepth, path));
  }
  return result;
}

function nodeLabel(def: WorkflowDefinition, id: string): string {
  return def.nodes[id]?.name || id;
}

export function availableFields(def: WorkflowDefinition, nodeId: string,
  descriptors: Record<string, ActivityDescriptor>): FieldOption[] {
  const options: FieldOption[] = [];
  // outputs of earlier steps
  for (const source of ancestors(def, nodeId).sort()) {
    const node = def.nodes[source];
    const group = nodeLabel(def, source);
    options.push({ group, label: "(whole result)", path: [source], binding: { $output: source },
      token: `{output.${source}}` });
    for (const path of flattenSchema(descriptors[node.type]?.output_schema)) {
      const dotted = [source, ...path].join(".");
      options.push({ group, label: path.join(" › "), path: [source, ...path], binding: { $output: dotted },
        token: `{output.${dotted}}` });
    }
  }
  // workflow input: shaped like the trigger's output when the workflow starts from a trigger
  const triggerRoots = roots(def).filter((r) => descriptors[def.nodes[r]?.type]?.is_trigger);
  const inputSchema = triggerRoots.length ? descriptors[def.nodes[triggerRoots[0]].type]?.output_schema : null;
  options.push({ group: "Workflow input", label: "(whole input)", path: [], binding: { $input: "" }, token: "{input}" });
  for (const path of flattenSchema(inputSchema)) {
    const dotted = path.join(".");
    options.push({ group: "Workflow input", label: path.join(" › "), path, binding: { $input: dotted },
      token: `{input.${dotted}}` });
  }
  for (const feature of Object.values(def.forecast?.features ?? {})) {
    if (!options.some((o) => o.group === "Workflow input" && JSON.stringify(o.binding) === JSON.stringify({ $input: feature.path }))) {
      options.push({ group: "Workflow input", label: feature.path.split(".").join(" › "), path: feature.path.split("."),
        binding: { $input: feature.path }, token: `{input.${feature.path}}` });
    }
  }
  // variables: declared and set by SetVariable steps
  const variables = new Set(Object.keys(def.variables ?? {}));
  for (const node of Object.values(def.nodes)) {
    if (node.type === "SetVariable" && typeof node.inputs.name === "string") variables.add(node.inputs.name);
  }
  for (const v of [...variables].sort()) {
    options.push({ group: "Variables", label: v, path: [v], binding: { $var: v }, token: `{var.${v}}` });
  }
  options.push({ group: "Run", label: "correlation id", path: ["correlation_id"], binding: { $ctx: "correlation_id" },
    token: "{ctx.correlation_id}" });
  options.push({ group: "Run", label: "execution id", path: ["execution_id"], binding: { $ctx: "execution_id" },
    token: "{ctx.execution_id}" });
  return options;
}

const REF_KEYS: Record<string, string> = { $output: "Step", $input: "Input", $var: "Variable", $ctx: "Run",
  $payload: "This step" };

export function isBinding(value: Binding): value is Record<string, string> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  return keys.length === 1 && (keys[0] in REF_KEYS || keys[0] === "$tmpl");
}

/** A human sentence for a stored binding: "Step ‘Book in ERP’ › body › erp_id". */
export function describeBinding(value: Binding, def?: WorkflowDefinition): string {
  if (!isBinding(value)) return typeof value === "string" ? value : JSON.stringify(value);
  const [key, arg] = Object.entries(value)[0];
  if (key === "$tmpl") return `Text: ${arg}`;
  const parts = String(arg).split(".").filter(Boolean);
  if (key === "$output" && parts.length) {
    const [node, ...rest] = parts;
    const name = def?.nodes[node]?.name || node;
    return [`${REF_KEYS[key]} ‘${name}’`, ...rest].join(" › ");
  }
  return [REF_KEYS[key], ...parts].join(" › ") || REF_KEYS[key];
}

/** Fields available to a step's *undo action*: what the step originally produced, plus the usual data. */
export function undoFields(def: WorkflowDefinition, nodeId: string,
  descriptors: Record<string, ActivityDescriptor>): FieldOption[] {
  const node = def.nodes[nodeId];
  const group = `Original result of ‘${node.name || nodeId}’`;
  const own: FieldOption[] = [{ group, label: "(whole result)", path: ["original", "output"],
    binding: { $payload: "original.output" }, token: "{payload.original.output}" }];
  for (const path of flattenSchema(descriptors[node.type]?.output_schema)) {
    const dotted = ["original", "output", ...path].join(".");
    own.push({ group, label: path.join(" › "), path, binding: { $payload: dotted }, token: `{payload.${dotted}}` });
  }
  return [...own, ...availableFields(def, nodeId, descriptors)];
}
