// Pure, immutable editing operations on workflow definitions (the designer never mutates state).
import type { ActivityDescriptor, EdgeSpec, NodeSpec, WorkflowDefinition, Binding } from "../api/types";

export function emptyDefinition(id: string, name: string): WorkflowDefinition {
  return { id, name, version: 1, description: null, nodes: {}, edges: [], variables: {}, metadata: {},
    forecast: { features: {} } };
}

export function uniqueNodeId(def: WorkflowDefinition, base: string): string {
  const stem = base.replace(/[^a-zA-Z0-9]+/g, "-").replace(/^-|-$/g, "").toLowerCase() || "step";
  if (!def.nodes[stem]) return stem;
  let i = 2;
  while (def.nodes[`${stem}-${i}`]) i += 1;
  return `${stem}-${i}`;
}

/** Default inputs from the activity's schema defaults (required fields without default stay empty). */
export function defaultInputs(descriptor: ActivityDescriptor): Record<string, Binding> {
  const inputs: Record<string, Binding> = {};
  for (const [name, prop] of Object.entries(descriptor.inputs_schema.properties ?? {})) {
    if (prop.default !== undefined && prop.default !== null) inputs[name] = prop.default;
  }
  return inputs;
}

export function addNode(def: WorkflowDefinition, descriptor: ActivityDescriptor,
  position: { x: number; y: number }, connectFrom?: { source: string; outcome: string }): [WorkflowDefinition, string] {
  const id = uniqueNodeId(def, descriptor.display_name || descriptor.type);
  const node: NodeSpec = { id, type: descriptor.type, name: descriptor.display_name, inputs: defaultInputs(descriptor),
    ui: { position } };
  let next: WorkflowDefinition = { ...def, nodes: { ...def.nodes, [id]: node } };
  if (connectFrom) next = connect(next, connectFrom.source, id, connectFrom.outcome);
  return [next, id];
}

export function removeNode(def: WorkflowDefinition, id: string): WorkflowDefinition {
  const nodes = { ...def.nodes };
  delete nodes[id];
  return { ...def, nodes, edges: def.edges.filter((e) => e.source !== id && e.target !== id) };
}

export function updateNode(def: WorkflowDefinition, id: string, patch: Partial<NodeSpec>): WorkflowDefinition {
  return { ...def, nodes: { ...def.nodes, [id]: { ...def.nodes[id], ...patch } } };
}

export function setInput(def: WorkflowDefinition, id: string, name: string, value: Binding | undefined): WorkflowDefinition {
  const inputs = { ...def.nodes[id].inputs };
  if (value === undefined) delete inputs[name];
  else inputs[name] = value;
  return updateNode(def, id, { inputs });
}

export function moveNode(def: WorkflowDefinition, id: string, position: { x: number; y: number }): WorkflowDefinition {
  return updateNode(def, id, { ui: { ...(def.nodes[id].ui ?? {}), position } });
}

export function connect(def: WorkflowDefinition, source: string, target: string, outcome = "Done"): WorkflowDefinition {
  if (source === target) return def;
  if (def.edges.some((e) => e.source === source && e.target === target && e.outcome === outcome)) return def;
  return { ...def, edges: [...def.edges, { source, target, outcome }] };
}

export function disconnect(def: WorkflowDefinition, edge: EdgeSpec): WorkflowDefinition {
  return { ...def, edges: def.edges.filter((e) => !sameEdge(e, edge)) };
}

export function setOutcome(def: WorkflowDefinition, edge: EdgeSpec, outcome: string): WorkflowDefinition {
  return { ...def, edges: def.edges.map((e) => (sameEdge(e, edge) ? { ...e, outcome } : e)) };
}

export function sameEdge(a: EdgeSpec, b: EdgeSpec): boolean {
  return a.source === b.source && a.target === b.target && a.outcome === b.outcome;
}

export function edgeId(e: EdgeSpec): string {
  return `${e.source}|${e.outcome}|${e.target}`;
}

/** Outcomes the analyst can choose for an edge leaving `node`. */
export function outcomeChoices(descriptor: ActivityDescriptor | undefined, def: WorkflowDefinition, node: string): string[] {
  const declared = (descriptor?.outcomes ?? ["Done"]).filter((o) => o !== "*");
  const used = def.edges.filter((e) => e.source === node).map((e) => e.outcome);
  const dynamic = descriptor?.outcomes.includes("*") ? ["Default"] : [];
  return [...new Set([...declared, ...dynamic, ...used])];
}

export function roots(def: WorkflowDefinition): string[] {
  const targets = new Set(def.edges.map((e) => e.target));
  return Object.keys(def.nodes).filter((n) => !targets.has(n));
}

/** Layered auto-layout for nodes without a stored position (left → right by depth). */
export function layout(def: WorkflowDefinition, dx = 260, dy = 130): Record<string, { x: number; y: number }> {
  const depth: Record<string, number> = {};
  const queue = roots(def).map((r) => [r, 0] as [string, number]);
  for (const n of Object.keys(def.nodes)) if (!queue.some(([q]) => q === n) && roots(def).length === 0) queue.push([n, 0]);
  while (queue.length) {
    const [n, d] = queue.shift()!;
    if (depth[n] !== undefined && depth[n] >= d) continue;
    if (d > Object.keys(def.nodes).length) continue; // cycles
    depth[n] = d;
    for (const e of def.edges) if (e.source === n) queue.push([e.target, d + 1]);
  }
  const rows: Record<number, number> = {};
  const positions: Record<string, { x: number; y: number }> = {};
  for (const id of Object.keys(def.nodes)) {
    const stored = def.nodes[id].ui?.position;
    if (stored) { positions[id] = stored; continue; }
    const d = depth[id] ?? 0;
    rows[d] = (rows[d] ?? -1) + 1;
    positions[id] = { x: 40 + d * dx, y: 40 + rows[d] * dy };
  }
  return positions;
}

/** Nodes that can run before `node` (its ancestors) – the data a step may use. */
export function ancestors(def: WorkflowDefinition, node: string): string[] {
  const seen = new Set<string>();
  const stack = [node];
  while (stack.length) {
    const current = stack.pop()!;
    for (const e of def.edges) {
      if (e.target === current && !seen.has(e.source)) {
        seen.add(e.source);
        stack.push(e.source);
      }
    }
  }
  seen.delete(node);
  return [...seen];
}
