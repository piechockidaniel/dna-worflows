import {
  Background, Controls, Handle, MarkerType, Position, ReactFlow, type Connection, type Edge, type Node,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useMemo } from "react";
import type { ActivityDescriptor, EdgeSpec, WorkflowDefinition } from "../api/types";
import { formatDuration } from "../lib/format";
import { connect, edgeId, layout, moveNode, outcomeChoices } from "../lib/graph";

export interface Highlight { tone: "ok" | "warn" | "bad" | "info" | "muted"; badge?: string }

interface StepData extends Record<string, unknown> {
  title: string;
  type: string;
  category: string;
  estimate?: string;
  highlight?: Highlight;
  note?: string;
  trigger: boolean;
}

function StepNode({ data, selected }: NodeProps<Node<StepData>>) {
  return (
    <div className={`step ${selected ? "selected" : ""} ${data.highlight ? `hl-${data.highlight.tone}` : ""}`}
      title={data.note || undefined}>
      {!data.trigger && <Handle type="target" position={Position.Left} />}
      <div className="step-cat">{data.category}</div>
      <div className="step-title">{data.title}</div>
      <div className="step-type">{data.type}</div>
      <div className="step-badges">
        {data.estimate && <span className="badge" title="designer estimate">⏱ {data.estimate}</span>}
        {data.highlight?.badge && <span className={`badge badge-${data.highlight.tone}`}>{data.highlight.badge}</span>}
        {data.note && <span className="badge" title={data.note}>✎</span>}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

const nodeTypes = { step: StepNode };

export type Selection = { kind: "node"; id: string } | { kind: "edge"; edge: EdgeSpec } | null;

export function FlowCanvas({ definition, descriptors, selection, onSelect, onChange, highlights = {}, readOnly }: {
  definition: WorkflowDefinition;
  descriptors: Record<string, ActivityDescriptor>;
  selection: Selection;
  onSelect: (s: Selection) => void;
  onChange: (def: WorkflowDefinition) => void;
  highlights?: Record<string, Highlight>;
  readOnly?: boolean;
}) {
  const positions = useMemo(() => layout(definition), [definition]);
  const nodes: Node<StepData>[] = Object.values(definition.nodes).map((n) => ({
    id: n.id, type: "step", position: positions[n.id],
    selected: selection?.kind === "node" && selection.id === n.id,
    data: {
      title: n.name || n.id, type: descriptors[n.type]?.display_name ?? n.type,
      category: descriptors[n.type]?.category ?? "?", trigger: descriptors[n.type]?.is_trigger ?? false,
      estimate: n.estimate ? formatDuration(n.estimate.likely) : undefined, highlight: highlights[n.id],
      note: typeof n.ui?.note === "string" ? n.ui.note : undefined,
    },
  }));
  const edges: Edge[] = definition.edges.map((e) => ({
    id: edgeId(e), source: e.source, target: e.target, label: e.outcome === "Done" ? undefined : e.outcome,
    markerEnd: { type: MarkerType.ArrowClosed }, data: { spec: e },
    selected: selection?.kind === "edge" && edgeId(selection.edge) === edgeId(e),
  }));

  const onConnect = (c: Connection) => {
    if (readOnly || !c.source || !c.target) return;
    const choices = outcomeChoices(descriptors[definition.nodes[c.source].type], definition, c.source);
    const used = new Set(definition.edges.filter((e) => e.source === c.source).map((e) => e.outcome));
    const outcome = choices.find((o) => !used.has(o)) ?? choices[0] ?? "Done";
    onChange(connect(definition, c.source, c.target, outcome));
  };

  return (
    <div className="canvas" data-testid="canvas">
      <ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.15, maxZoom: 1.1 }}
        proOptions={{ hideAttribution: true }}
        nodesDraggable={!readOnly} nodesConnectable={!readOnly}
        onNodeClick={(_, n) => onSelect({ kind: "node", id: n.id })}
        onEdgeClick={(_, e) => onSelect({ kind: "edge", edge: (e.data as { spec: EdgeSpec }).spec })}
        onPaneClick={() => onSelect(null)}
        onNodeDragStop={(_, n) => !readOnly && onChange(moveNode(definition, n.id, n.position))}
        onConnect={onConnect}>
        <Background />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}

export function Palette({ descriptors, onAdd, disabled }: {
  descriptors: Record<string, ActivityDescriptor>;
  onAdd: (descriptor: ActivityDescriptor) => void;
  disabled?: boolean;
}) {
  const byCategory = new Map<string, ActivityDescriptor[]>();
  for (const d of Object.values(descriptors).sort((a, b) => a.display_name.localeCompare(b.display_name))) {
    byCategory.set(d.category, [...(byCategory.get(d.category) ?? []), d]);
  }
  return (
    <div className="palette" aria-label="activities">
      {[...byCategory.entries()].sort().map(([category, items]) => (
        <details key={category} open={["Messaging", "Integration", "Control", "AI"].includes(category)}>
          <summary>{category}</summary>
          {items.map((d) => (
            <button key={d.type} className="palette-item" disabled={disabled} title={d.help || d.description}
              onClick={() => onAdd(d)}>
              <strong>{d.display_name}</strong><span>{d.description}</span>
            </button>
          ))}
        </details>
      ))}
    </div>
  );
}
