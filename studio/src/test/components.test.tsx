import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import { ApiClient, ApiError, qs } from "../api/client";
import type { Estimate, WorkflowDefinition } from "../api/types";
import { StaticSession, loadConfig } from "../auth/session";
import { EstimateEditor, NodeInspector, pickUnit } from "../components/NodeInspector";
import { ExecutionDetailPage } from "../pages/ExecutionsPage";
import { HomePage } from "../pages/HomePage";
import { OperationsPage } from "../pages/OperationsPage";
import { WorkflowsPage } from "../pages/WorkflowsPage";
import { descriptors, orderFlow, principal, session } from "./fixtures";

type Routes = Record<string, unknown | ((body: unknown) => unknown)>;

/** A fake API: "METHOD /path" → response body. Records calls. */
function fakeFetch(routes: Routes) {
  const calls: { method: string; path: string; body: unknown; headers: Record<string, string> }[] = [];
  const fn = vi.fn(async (url: string, init: RequestInit) => {
    const path = url.replace(/^https?:\/\/[^/]+/, "");
    const body = init.body ? JSON.parse(String(init.body)) : undefined;
    calls.push({ method: init.method!, path, body, headers: init.headers as Record<string, string> });
    const key = `${init.method} ${path.split("?")[0]}`;
    if (!(key in routes)) return new Response(JSON.stringify({ detail: `no route ${key}` }), { status: 404 });
    const value = routes[key];
    const result = typeof value === "function" ? (value as (b: unknown) => unknown)(body) : value;
    return new Response(JSON.stringify(result), { status: 200 });
  });
  return { fetch: fn as unknown as typeof fetch, calls };
}

function renderAt(path: string, element: React.ReactElement, s = session()) {
  return render(<StaticSession value={s}><MemoryRouter initialEntries={[path]}>
    <Routes><Route path="/executions/:id" element={element} /><Route path="*" element={element} /></Routes>
  </MemoryRouter></StaticSession>);
}

describe("api client", () => {
  it("sends bearer tokens or dev actor, decodes errors and empty responses", async () => {
    const { fetch, calls } = fakeFetch({ "GET /me": { name: "ada" }, "DELETE /drafts/x": null });
    const withToken = new ApiClient({ fetch, token: async () => "tok", actor: () => "ignored" });
    expect((await withToken.me()).name).toBe("ada");
    expect(calls[0].headers.Authorization).toBe("Bearer tok");
    expect(calls[0].headers["X-Actor"]).toBeUndefined();
    const dev = new ApiClient({ fetch, actor: () => "bob" });
    await dev.me();
    expect(calls[1].headers["X-Actor"]).toBe("bob");
    await expect(dev.workflows()).rejects.toBeInstanceOf(ApiError);
    const noContent = new ApiClient({ fetch: (async () => new Response(null, { status: 204 })) as unknown as typeof fetch });
    expect(await noContent.discardDraft("x")).toBeUndefined();
    expect(qs({ a: "1", b: undefined, c: "" })).toBe("?a=1");
  });

  it("loads the runtime config and falls back to development defaults", async () => {
    const ok = (async () => new Response(JSON.stringify({ apiBase: "/x", keycloak: null }))) as unknown as typeof fetch;
    expect((await loadConfig(ok)).apiBase).toBe("/x");
    const broken = (async () => { throw new Error("offline"); }) as unknown as typeof fetch;
    expect(await loadConfig(broken)).toEqual({ apiBase: "", keycloak: null });
  });
});

describe("designer building blocks", () => {
  it("lets an analyst pick data from earlier steps instead of typing bindings", async () => {
    let def: WorkflowDefinition = orderFlow();
    const onChange = (d: WorkflowDefinition) => { def = d; view.rerender(ui()); };
    const ui = () => <NodeInspector definition={def} nodeId="mark" descriptors={descriptors} onChange={onChange} />;
    const view = render(ui());
    const valueField = screen.getByRole("group", { name: "Value source" });
    await userEvent.click(valueField.querySelectorAll("button")[1]);       // "From data"
    await userEvent.selectOptions(screen.getByLabelText("Value", { selector: "select" }),
      screen.getByRole("option", { name: "body" }));
    expect(def.nodes.mark.inputs.value).toEqual({ $output: "book.body" });
    expect(screen.getByText("Uses: Step ‘Book in ERP’ › body")).toBeInTheDocument();
    await userEvent.clear(screen.getByLabelText("Step name"));
    await userEvent.type(screen.getByLabelText("Step name"), "Remember");
    expect(def.nodes.mark.name).toBe("Remember");
  });

  it("builds text templates with inserted data", async () => {
    let def: WorkflowDefinition = orderFlow();
    const ui = () => <NodeInspector definition={def} nodeId="mark" descriptors={descriptors}
      onChange={(d) => { def = d; view.rerender(ui()); }} />;
    const view = render(ui());
    const group = screen.getByRole("group", { name: "Name source" });
    await userEvent.click(group.querySelectorAll("button")[2]);            // "Text with data"
    const [insert] = screen.getAllByLabelText("insert data");
    await userEvent.selectOptions(insert, screen.getAllByRole("option", { name: "data › order_id" })[0]);
    // the first match is the trigger step's output ("New order" › data › order_id)
    expect(def.nodes.mark.inputs.name).toEqual({ $tmpl: "{output.receive.data.order_id}" });
  });

  it("edits three-point estimates in the analyst's unit, with drivers", async () => {
    let value: Estimate | null = { likely: 120 };
    const ui = () => <EstimateEditor estimate={value} features={["lines"]} onChange={(e) => { value = e; view.rerender(ui()); }} />;
    const view = render(ui());
    expect(screen.getByText("(minutes)")).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("slowest"), "5");
    expect(value!.pessimistic).toBe(300);
    await userEvent.selectOptions(screen.getByLabelText("driver feature"), "lines");
    expect(value!.driver).toEqual({ feature: "lines", per_unit: 60 });
    await userEvent.clear(screen.getByLabelText("usually"));
    expect(value).toBeNull();
    expect([pickUnit(5), pickUnit(7200), pickUnit(200000)]).toEqual(["seconds", "hours", "days"]);
  });
});

describe("pages", () => {
  it("shows each role the work it can pick up", async () => {
    const { fetch } = fakeFetch({
      "GET /drafts": [{ workflow_id: "a", status: "in_review", updated_by: "ben", definition: { name: "Alpha" } },
        { workflow_id: "b", status: "approved", updated_by: "ben", definition: { name: "Beta" } }],
      "GET /proposals": [{ id: "p1", kind: "hold.release", reason: "probe green", proposed_by: "ai:triage" }],
      "GET /holds": [{ id: "h1", scope: "dependency", target: "http:erp", reason: "down", created_at: "2030-01-01" }],
      "GET /executions": [],
    });
    const reviewer = session({ principal: principal(["workflows.read", "workflows.review"]),
      api: new ApiClient({ fetch }) });
    const view = renderAt("/", <HomePage />, reviewer);
    expect(await screen.findByText("Alpha")).toBeInTheDocument();
    expect(screen.getByText("Waiting for review (1)")).toBeInTheDocument();
    expect(screen.queryByText(/Approved, ready to publish/)).not.toBeInTheDocument();
    view.unmount();
    const approver = session({ principal: principal(["workflows.read", "proposals.read", "proposals.approve",
      "holds.read", "executions.read", "workflows.publish"]), api: new ApiClient({ fetch }) });
    renderAt("/", <HomePage />, approver);
    expect(await screen.findByText(/probe green/)).toBeInTheDocument();
    expect(await screen.findByText("dependency: http:erp")).toBeInTheDocument();
    expect(screen.getByText("Beta")).toBeInTheDocument();
  });

  it("creates a workflow from a plain name", async () => {
    const { fetch, calls } = fakeFetch({ "GET /workflows": [], "GET /drafts": [], "POST /drafts/order-to-erp": (b: unknown) => b });
    renderAt("/workflows", <WorkflowsPage />, session({ principal: principal(["workflows.read", "workflows.design"]),
      api: new ApiClient({ fetch }) }));
    await userEvent.type(await screen.findByLabelText("new workflow name"), "Order to ERP");
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => expect(calls.some((c) => c.path === "/drafts/order-to-erp")).toBe(true));
    const created = calls.find((c) => c.path === "/drafts/order-to-erp")!.body as { definition: WorkflowDefinition };
    expect(created.definition.name).toBe("Order to ERP");
  });

  it("lets an approver decide proposals, others only see them", async () => {
    const { fetch, calls } = fakeFetch({
      "GET /holds": [{ id: "h1", scope: "dependency", target: "http:erp", reason: "down", status: "active", source: "auto-trip",
        created_at: "2030-01-01", release_strategy: "probe-recommend", strategy_state: {}, resumed_work: 0 }],
      "GET /proposals": [{ id: "p1", kind: "hold.release", target: { hold_id: "h1" }, reason: "probe green",
        proposed_by: "strategy", status: "pending", confidence: 0.9, evidence: {}, created_at: "2030-01-01" }],
      "GET /parked": [{ execution_id: "e1", work_item_id: "w1" }],
      "POST /proposals/p1/approve": { id: "p1", status: "executed" },
    });
    renderAt("/operations", <OperationsPage />, session({ principal: principal(["holds.read", "proposals.read",
      "proposals.approve"]), api: new ApiClient({ fetch }) }));
    await userEvent.type(await screen.findByLabelText("decision note"), "checked");
    await userEvent.click(await screen.findByRole("button", { name: "Approve" }));
    await waitFor(() => expect(calls.find((c) => c.path === "/proposals/p1/approve")?.body).toEqual({ note: "checked" }));
    expect(screen.queryByRole("button", { name: "Release" })).not.toBeInTheDocument();   // no holds.manage
    expect(screen.getByText("90%")).toBeInTheDocument();
  });

  it("shows when a running execution will be done", async () => {
    const { fetch } = fakeFetch({
      "GET /executions/e1": { id: "e1", definition_id: "orders", status: "parked", correlation_id: "order-7",
        input: {}, outputs: {}, output: null, created_at: "2030-01-01T00:00:00Z",
        work: [{ id: "w1", node_id: "book", state: "parked", attempt: 3, parked_reason: "ERP down" }], bookmarks: [] },
      "GET /executions/e1/journal": [{ seq: 1, id: "x", kind: "activity.completed", timestamp: "2030-01-01T00:00:00Z",
        node_id: "receive", data: { latency_ms: 1500 } }],
      "GET /forecasts/executions/e1": { execution_id: "e1", status: "parked", elapsed: 60, remaining_mean: 5400,
        eta: "2030-01-01T02:00:00Z", eta_p90: "2030-01-01T03:00:00Z", basis: "mixed", blocked_by: ["h1"], frontier: [] },
    });
    renderAt("/executions/e1", <ExecutionDetailPage />, session({ principal: principal(["executions.read",
      "executions.operate", "journal.read", "forecasts.read"]), api: new ApiClient({ fetch }) }));
    expect(await screen.findByText("1 h 30 min")).toBeInTheDocument();
    expect(screen.getByText(/Blocked by hold h1/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Resume" })).toBeInTheDocument();
    expect(screen.getByText(/took 2 s/)).toBeInTheDocument();
  });
});

describe("undo (compensation)", () => {
  it("configures an undo action that uses the step's original result", async () => {
    let def: WorkflowDefinition = orderFlow();
    const ui = () => <NodeInspector definition={def} nodeId="book" descriptors={descriptors}
      onChange={(d) => { def = d; view.rerender(ui()); }} />;
    const view = render(ui());
    await userEvent.selectOptions(screen.getByLabelText("Undo with"), "HttpRequest");
    expect(def.nodes.book.compensate).toEqual({ type: "HttpRequest", inputs: {} });
    const url = screen.getAllByRole("group", { name: "Url source" }).at(-1)!;
    await userEvent.click(url.querySelectorAll("button")[1]);
    const select = screen.getAllByLabelText("Url", { selector: "select" }).at(-1)!;
    await userEvent.selectOptions(select, screen.getAllByRole("option", { name: "body" })[0]);
    expect(def.nodes.book.compensate!.inputs.url).toEqual({ $payload: "original.output.body" });
    await userEvent.selectOptions(screen.getByLabelText("Undo with"), "");
    expect(def.nodes.book.compensate).toBeNull();
  });
});
