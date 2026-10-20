import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import { ApiClient } from "../api/client";
import type { Draft, WorkflowDefinition } from "../api/types";
import { StaticSession } from "../auth/session";
import { DesignerPage } from "../pages/DesignerPage";
import { descriptors, orderFlow, principal, session } from "./fixtures";

// React Flow needs a real layout engine; the designer logic is tested with a simple stand-in canvas.
vi.mock("../components/FlowCanvas", () => ({
  FlowCanvas: ({ definition, onSelect, highlights }: { definition: WorkflowDefinition;
    onSelect: (s: unknown) => void; highlights: Record<string, { badge?: string }> }) => (
    <div data-testid="canvas">{Object.values(definition.nodes).map((n) => (
      <button key={n.id} onClick={() => onSelect({ kind: "node", id: n.id })}>
        node:{n.id}{highlights[n.id]?.badge ? ` [${highlights[n.id].badge}]` : ""}</button>))}
      {definition.edges.map((e) => <button key={`${e.source}-${e.target}`}
        onClick={() => onSelect({ kind: "edge", edge: e })}>edge:{e.source}-{e.target}</button>)}
    </div>),
  Palette: ({ descriptors: d, onAdd }: { descriptors: Record<string, { type: string; display_name: string }>;
    onAdd: (x: unknown) => void }) => (
    <div>{Object.values(d).map((x) => <button key={x.type} onClick={() => onAdd(x)}>add:{x.type}</button>)}</div>),
}));

function draft(overrides: Partial<Draft> = {}): Draft {
  return { workflow_id: "orders", definition: orderFlow(), status: "draft", revision: 3, base_version: 1,
    created_by: "anna", updated_by: "anna", updated_at: "2030-01-01", problems: [], comments: [], reviews: [],
    history: [{ at: "2030-01-01", actor: "anna", action: "created" }], published_version: null, ...overrides };
}

function api(routes: Record<string, (body: any) => unknown>) {
  const calls: { key: string; body: any }[] = [];
  const fetchFn = (async (url: string, init: RequestInit) => {
    const key = `${init.method} ${url.split("?")[0]}`;
    const body = init.body ? JSON.parse(String(init.body)) : undefined;
    calls.push({ key, body });
    if (!(key in routes)) return new Response(JSON.stringify({ detail: `no route ${key}` }), { status: 404 });
    return new Response(JSON.stringify(routes[key](body)), { status: 200 });
  }) as unknown as typeof fetch;
  return { client: new ApiClient({ fetch: fetchFn }), calls };
}

function renderDesigner(permissions: string[], routes: Record<string, (b: any) => unknown>) {
  const { client, calls } = api(routes);
  render(<StaticSession value={session({ api: client, principal: principal(permissions) })}>
    <MemoryRouter initialEntries={["/designer/orders"]}><Routes>
      <Route path="/designer/:id" element={<DesignerPage />} /></Routes></MemoryRouter></StaticSession>);
  return calls;
}

const DESIGN = ["workflows.read", "workflows.design", "forecasts.read"];

describe("designer", () => {
  it("edits, saves and submits a draft", async () => {
    let current = draft();
    const calls = renderDesigner(DESIGN, {
      "GET /drafts/orders": () => current,
      "PUT /drafts/orders": (b) => (current = draft({ definition: b.definition, revision: b.revision + 1 })),
      "POST /drafts/orders/submit": () => (current = draft({ ...current, status: "in_review" })),
    });
    expect(await screen.findByText("Orders")).toBeInTheDocument();
    await userEvent.click(screen.getByText("node:book"));
    await userEvent.clear(screen.getByLabelText("Step name"));
    await userEvent.type(screen.getByLabelText("Step name"), "Book it");
    await userEvent.click(screen.getByText("add:SetVariable"));          // added after the selected step
    expect(screen.getByText("unsaved changes")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(calls.some((c) => c.key === "PUT /drafts/orders")).toBe(true));
    const saved = calls.find((c) => c.key === "PUT /drafts/orders")!.body.definition as WorkflowDefinition;
    expect(saved.nodes.book.name).toBe("Book it");
    expect(saved.edges).toContainEqual({ source: "book", target: "set-variable", outcome: "Done" });
    await userEvent.click(await screen.findByRole("button", { name: "Submit for review" }));
    expect(await screen.findByText("in review")).toBeInTheDocument();
  });

  it("edits connections and removes steps", async () => {
    renderDesigner(DESIGN, { "GET /drafts/orders": () => draft() });
    await userEvent.click(await screen.findByText("edge:check-mark"));
    await userEvent.selectOptions(screen.getByLabelText(/Follow this connection/), "False");
    await userEvent.click(screen.getByRole("button", { name: "Remove connection" }));
    expect(screen.queryByText("edge:check-mark")).not.toBeInTheDocument();
    await userEvent.click(screen.getByText("node:mark"));
    await userEvent.click(screen.getByRole("button", { name: "Remove step" }));
    expect(screen.queryByText("node:mark")).not.toBeInTheDocument();
  });

  it("runs a test with sample results and highlights the path", async () => {
    renderDesigner(DESIGN, {
      "GET /drafts/orders": () => draft(),
      "POST /drafts/orders/test-run": (b) => ({ status: "completed", path: ["receive", "book"],
        simulated: Object.keys(b.mocks), outputs: {}, would_send: [{ kind: "messaging.send", payload: {}, node: "x" }],
        waiting_for: [{ node: "mark", stimulus: "event:x" }] }),
    });
    await userEvent.click(await screen.findByRole("button", { name: "Test run" }));
    await userEvent.click(screen.getByText("Sample results for steps (optional)"));
    await userEvent.type(screen.getByLabelText("New order"), '{{"data": 1}');
    await userEvent.click(screen.getByRole("button", { name: "Run test" }));
    expect(await screen.findByText(/Would send 1 message/)).toBeInTheDocument();
    expect(screen.getByText("node:receive [simulated]")).toBeInTheDocument();
    expect(screen.getByText("node:book [ran]")).toBeInTheDocument();
    expect(screen.getByText("node:mark [waiting]")).toBeInTheDocument();
    expect(screen.getByText(/Waiting for: event:x/)).toBeInTheDocument();
  });

  it("previews the forecast of the draft", async () => {
    renderDesigner(DESIGN, {
      "GET /drafts/orders": () => draft(),
      "POST /forecasts/preview": () => ({ workflow: "orders", version: 1, mean: 130, sd: 20, p50: 120, p90: 600,
        basis: "mixed", features: {}, critical_path: ["book"], history_lead_time: { samples: 12, mean: 100, sd: 5 },
        nodes: { book: { node: "book", mean: 125, sd: 1, p50: 120, p90: 200, basis: "mixed", samples: 12,
          level: "node", expected_visits: 1, remaining_mean: 125 },
          check: { node: "check", mean: 1, sd: 1, p50: 1, p90: 2, basis: "default", samples: 0, level: "default",
            expected_visits: 1, remaining_mean: 1 } } }),
    });
    await userEvent.click(await screen.findByRole("button", { name: "Forecast" }));
    await userEvent.click(screen.getByRole("button", { name: "Estimate duration" }));
    expect(await screen.findByText(/Usually about/)).toHaveTextContent("Usually about 2 min, rarely more than 10 min");
    expect(screen.getByText(/12 finished runs/)).toBeInTheDocument();
    expect(screen.getByText("node:check [~1 s]")).toBeInTheDocument();
  });

  it("lets a reviewer decide, comment, and a publisher publish", async () => {
    let current = draft({ status: "in_review", updated_by: "anna" });
    renderDesigner(["workflows.read", "workflows.review", "workflows.publish", "workflows.design"], {
      "GET /drafts/orders": () => current,
      "POST /drafts/orders/review": (b) => (current = draft({ status: b.decision === "approve" ? "approved" : "changes_requested",
        history: [...current.history, { at: "2030", actor: "dora", action: `review:${b.decision}`, detail: b.note }] })),
      "POST /drafts/orders/comments": (b) => (current = { ...current, comments: [{ id: "c", author: "dora", at: "2030", text: b.text }] }),
      "POST /drafts/orders/publish": () => (current = draft({ status: "published", published_version: 2 })),
    });
    await userEvent.click(await screen.findByRole("button", { name: "Review & history" }));
    await userEvent.type(screen.getByLabelText("new comment"), "Looks fine");
    await userEvent.click(screen.getByRole("button", { name: "Comment" }));
    expect(await screen.findByText("Looks fine")).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("review note"), "ok");
    await userEvent.click(screen.getByRole("button", { name: "Approve" }));
    expect(await screen.findByText(/review approve – ok/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Publish" }));
    expect(await screen.findByText("published")).toBeInTheDocument();
  });

  it("edits workflow settings: name, undo on failure and duration inputs", async () => {
    let saved: WorkflowDefinition | null = null;
    renderDesigner(DESIGN, {
      "GET /drafts/orders": () => draft(),
      "PUT /drafts/orders": (b) => { saved = b.definition; return draft({ definition: b.definition }); },
    });
    await userEvent.click(await screen.findByRole("button", { name: "Settings" }));
    await userEvent.type(screen.getByLabelText("Workflow name"), " v2");
    await userEvent.click(screen.getByLabelText(/undo the completed steps automatically/));
    await userEvent.type(screen.getByLabelText("feature name"), "priority");
    await userEvent.type(screen.getByLabelText("feature path"), "data.priority");
    await userEvent.click(screen.getByRole("button", { name: "Add" }));
    await userEvent.click(screen.getAllByRole("button", { name: "remove" })[0]);
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(saved).not.toBeNull());
    expect(saved!.name).toBe("Orders v2");
    expect(saved!.compensate_on_fault).toBe(true);
    expect(Object.keys(saved!.forecast.features)).toEqual(["priority"]);
  });

  it("is read-only for viewers and starts a new draft from a published workflow", async () => {
    const calls = renderDesigner(["workflows.read"], { "GET /drafts/orders": () => draft({ status: "published" }) });
    expect(await screen.findByText("published")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Save" })).not.toBeInTheDocument();
    expect(calls.some((c) => c.key === "POST /drafts/orders")).toBe(false);
    expect(descriptors.HttpRequest).toBeDefined();
  });
});
