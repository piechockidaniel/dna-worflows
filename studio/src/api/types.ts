// Types mirroring the DnA Workflows API (see src/DnA Workflows/runtime/models.py and api/app.py).

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Binding = Json;

export interface Driver { feature: string; per_unit: number }
export interface Estimate { likely: number; optimistic?: number | null; pessimistic?: number | null; driver?: Driver | null }

export interface NodeSpec {
  id: string;
  type: string;
  name?: string | null;
  inputs: Record<string, Binding>;
  dependency?: string | null;
  retry?: Record<string, Json> | null;
  estimate?: Estimate | null;
  compensate?: { type: string; inputs: Record<string, Binding>; name?: string | null } | null;
  ui?: { position?: { x: number; y: number }; note?: string; [key: string]: Json | undefined };
}

export interface EdgeSpec { source: string; target: string; outcome: string }

export interface FeatureSpec { path: string; transform: "value" | "count" | "length" | "bucket"; buckets?: number[] }

export interface WorkflowDefinition {
  id: string;
  version: number;
  name?: string | null;
  description?: string | null;
  nodes: Record<string, NodeSpec>;
  edges: EdgeSpec[];
  variables: Record<string, Json>;
  metadata: Record<string, Json>;
  forecast: { features: Record<string, FeatureSpec> };
  compensate_on_fault?: boolean;
}

export interface JsonSchema {
  type?: string | string[];
  title?: string;
  description?: string;
  default?: Json;
  enum?: Json[];
  properties?: Record<string, JsonSchema>;
  required?: string[];
  items?: JsonSchema;
  anyOf?: JsonSchema[];
  allOf?: JsonSchema[];
  $ref?: string;
  $defs?: Record<string, JsonSchema>;
  additionalProperties?: boolean | JsonSchema;
  const?: Json;
}

export interface ActivityDescriptor {
  type: string;
  display_name: string;
  category: string;
  description: string;
  help: string;
  inputs_schema: JsonSchema;
  output_schema: JsonSchema | null;
  outcomes: string[];
  is_trigger: boolean;
}

export interface Capabilities {
  shell: string;
  version: string;
  features_enabled: string[];
  activities: ActivityDescriptor[];
}

export interface Principal {
  subject: string;
  name: string;
  kind: "user" | "service" | "api-key" | "anonymous";
  roles: string[];
  permissions: string[];
}

export type DraftStatus = "draft" | "in_review" | "changes_requested" | "approved" | "published";

export interface Draft {
  workflow_id: string;
  definition: WorkflowDefinition;
  status: DraftStatus;
  revision: number;
  base_version: number | null;
  created_by: string;
  updated_by: string;
  updated_at: string;
  problems: string[];
  comments: { id: string; author: string; at: string; text: string; node?: string | null }[];
  reviews: { reviewer: string; at: string; decision: "approve" | "request_changes"; note?: string | null }[];
  history: { at: string; actor: string; action: string; detail?: string | null }[];
  published_version: number | null;
}

export interface WorkItem {
  id: string; node_id: string; activity_type?: string | null; state: string; attempt: number; kind?: string;
  dependency?: string | null; parked_reason?: string | null; hold_id?: string | null;
  last_fault?: { kind: string; error_type: string; message: string } | null;
}

export interface Execution {
  id: string; definition_id: string; definition_version: number; status: string;
  input: Record<string, Json>; output: Json; outputs: Record<string, Json>; correlation_id?: string | null;
  trace_id?: string | null; created_at?: string | null; completed_at?: string | null;
  work: WorkItem[]; bookmarks: { id: string; node_id: string; stimulus_type: string; key: string }[];
  children?: string[]; parent_execution_id?: string | null; compensation_mode?: string | null;
  compensations?: { node_id: string }[];
  fault?: { kind: string; error_type: string; message: string } | null;
}

export interface JournalEvent {
  seq: number; id: string; kind: string; timestamp: string; execution_id?: string | null;
  node_id?: string | null; activity_type?: string | null; data: Record<string, Json>;
}

export interface Hold {
  id: string; scope: string; target: string; reason: string; status: "active" | "released"; source: string;
  created_by?: string | null; created_at: string; release_strategy: string;
  strategy_state: Record<string, Json>; released_by?: string | null; resumed_work: number;
}

export interface Proposal {
  id: string; kind: string; target: Record<string, Json>; reason: string; proposed_by: string;
  created_at: string; status: string; confidence?: number | null; evidence: Record<string, Json>;
  decided_by?: string | null;
}

export interface NodeForecast {
  node: string; mean: number; sd: number; p50: number; p90: number; basis: string; samples: number;
  level: string; expected_visits: number; remaining_mean: number; prior_mean?: number | null;
  history_mean?: number | null;
}

export interface WorkflowForecast {
  workflow: string; version: number; mean: number; sd: number; p50: number; p90: number; basis: string;
  features: Record<string, Json>; nodes: Record<string, NodeForecast>; critical_path: string[];
  history_lead_time?: { samples: number; mean: number; sd: number } | null;
}

export interface ExecutionForecast {
  execution_id: string; status: string; elapsed: number; remaining_mean: number; eta: string; eta_p90: string;
  basis: string; blocked_by: string[];
  frontier: { node: string; state: string; remaining: number; waiting_for?: string; hold_id?: string }[];
}

export interface TestRunResult {
  status: string; output?: Json; outputs?: Record<string, Json>; path?: string[]; simulated?: string[];
  waiting_for?: { node: string; stimulus: string }[]; fault?: { message: string } | null;
  would_send?: { kind: string; payload: Json; node: string }[]; problems?: string[];
}
