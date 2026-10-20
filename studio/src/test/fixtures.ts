import type { ActivityDescriptor, Principal, WorkflowDefinition } from "../api/types";
import type { Session } from "../auth/session";
import { ApiClient } from "../api/client";

const obj = (props: Record<string, string>) => ({ type: "object", properties: Object.fromEntries(
  Object.entries(props).map(([k, t]) => [k, { type: t }])) });

export const descriptors: Record<string, ActivityDescriptor> = {
  MessageReceived: { type: "MessageReceived", display_name: "Message Received", category: "Messaging", description: "Trigger",
    help: "", is_trigger: true, outcomes: ["Done"], output_schema: { type: "object", properties: {
      data: obj({ order_id: "integer", lines: "array" }), correlation_id: { type: "string" } } },
    inputs_schema: { type: "object", required: ["endpoint"], properties: { endpoint: { type: "string" },
      type: { anyOf: [{ type: "string" }, { type: "null" }], default: null }, correlate: { type: "boolean", default: false } } } },
  HttpRequest: { type: "HttpRequest", display_name: "Http Request", category: "Integration", description: "Call HTTP",
    help: "", is_trigger: false, outcomes: ["Done"], output_schema: obj({ status: "integer", body: "object" }),
    inputs_schema: { type: "object", properties: {
      method: { enum: ["GET", "POST"], type: "string", default: "GET" }, url: { anyOf: [{ type: "string" }, { type: "null" }] },
      json_body: { title: "Json Body" }, params: { type: "object", default: {} } } } },
  If: { type: "If", display_name: "If", category: "Control", description: "Branch", help: "", is_trigger: false,
    outcomes: ["True", "False"], output_schema: null,
    inputs_schema: { type: "object", required: ["condition"], properties: { condition: {} } } },
  Classify: { type: "Classify", display_name: "Classify", category: "AI", description: "Labels", help: "",
    is_trigger: false, outcomes: ["*"], output_schema: obj({ label: "string" }),
    inputs_schema: { type: "object", properties: { labels: { type: "array", items: { type: "string" } } } } },
  SetVariable: { type: "SetVariable", display_name: "Set Variable", category: "Data", description: "", help: "",
    is_trigger: false, outcomes: ["Done"], output_schema: null,
    inputs_schema: { type: "object", properties: { name: { type: "string" }, value: {} } } },
};

export function orderFlow(): WorkflowDefinition {
  return {
    id: "orders", version: 1, name: "Orders", description: null, variables: { region: "EU" }, metadata: {},
    forecast: { features: { lines: { path: "data.lines", transform: "count" } } },
    nodes: {
      receive: { id: "receive", type: "MessageReceived", name: "New order", inputs: { endpoint: "orders" } },
      book: { id: "book", type: "HttpRequest", name: "Book in ERP", inputs: { method: "POST" },
        estimate: { likely: 120 } },
      check: { id: "check", type: "If", inputs: { condition: { $output: "book.body.priority" } } },
      mark: { id: "mark", type: "SetVariable", inputs: { name: "booked", value: true } },
    },
    edges: [{ source: "receive", target: "book", outcome: "Done" }, { source: "book", target: "check", outcome: "Done" },
      { source: "check", target: "mark", outcome: "True" }],
  };
}

export function principal(permissions: string[], name = "ada"): Principal {
  return { subject: name, name, kind: "user", roles: [], permissions };
}

export function session(overrides: Partial<Session> = {}, fetchImpl?: typeof fetch): Session {
  const p = overrides.principal ?? principal(["workflows.read"]);
  return {
    api: new ApiClient({ fetch: fetchImpl ?? (async () => new Response("[]", { status: 200 })) }),
    principal: p,
    capabilities: { shell: "test", version: "0", features_enabled: ["authoring", "resilience", "forecasting"],
      activities: Object.values(descriptors) },
    descriptors, devMode: true,
    can: (perm) => p.permissions.includes(perm) || p.permissions.includes("admin"),
    logout: () => {}, switchPersona: () => {},
    ...overrides,
  };
}
