import { describe, expect, it } from "vitest";
import { descriptors, orderFlow } from "../test/fixtures";
import { availableFields, describeBinding, flattenSchema, isBinding } from "./dataPicker";
import { basisLabel, formatDuration, slugify } from "./format";
import { addNode, ancestors, connect, defaultInputs, disconnect, emptyDefinition, layout, moveNode, outcomeChoices,
  removeNode, roots, setInput, setOutcome, uniqueNodeId } from "./graph";
import { can, visibleNav } from "./permissions";
import { fieldsFor, humanize, parseLiteral } from "./schemaForm";

describe("graph editing", () => {
  it("adds nodes with unique ids, defaults and optional connection", () => {
    const def = orderFlow();
    const [next, id] = addNode(def, descriptors.HttpRequest, { x: 1, y: 2 }, { source: "book", outcome: "Done" });
    expect(id).toBe("http-request");
    expect(next.nodes[id].inputs).toEqual({ method: "GET", params: {} });
    expect(next.edges).toContainEqual({ source: "book", target: id, outcome: "Done" });
    expect(def.nodes[id]).toBeUndefined();          // immutable
    const [again, id2] = addNode(next, descriptors.HttpRequest, { x: 0, y: 0 });
    expect(id2).toBe("http-request-2");
    expect(uniqueNodeId(again, "!!!")).toBe("step");
  });

  it("removes nodes with their edges, connects, disconnects and relabels", () => {
    let def = removeNode(orderFlow(), "book");
    expect(Object.keys(def.nodes)).not.toContain("book");
    expect(def.edges).toEqual([{ source: "check", target: "mark", outcome: "True" }]);
    def = connect(def, "receive", "check");
    expect(connect(def, "receive", "check")).toBe(def);            // duplicate ignored
    expect(connect(def, "mark", "mark")).toBe(def);                 // no self loops
    const edge = { source: "check", target: "mark", outcome: "True" };
    def = setOutcome(def, edge, "False");
    expect(def.edges.find((e) => e.source === "check")!.outcome).toBe("False");
    def = disconnect(def, { ...edge, outcome: "False" });
    expect(def.edges.some((e) => e.source === "check")).toBe(false);
  });

  it("edits inputs and positions, and offers outcome choices", () => {
    let def = setInput(orderFlow(), "book", "url", "https://erp");
    expect(def.nodes.book.inputs.url).toBe("https://erp");
    def = setInput(def, "book", "url", undefined);
    expect("url" in def.nodes.book.inputs).toBe(false);
    def = moveNode(def, "book", { x: 5, y: 6 });
    expect(def.nodes.book.ui?.position).toEqual({ x: 5, y: 6 });
    expect(outcomeChoices(descriptors.If, def, "check")).toEqual(["True", "False"]);
    expect(outcomeChoices(descriptors.Classify, def, "x")).toEqual(["Default"]);
    expect(outcomeChoices(undefined, def, "book")).toEqual(["Done"]);
    expect(defaultInputs(descriptors.MessageReceived)).toEqual({ correlate: false });
  });

  it("computes roots, ancestors and a layered layout", () => {
    const def = orderFlow();
    expect(roots(def)).toEqual(["receive"]);
    expect(ancestors(def, "mark").sort()).toEqual(["book", "check", "receive"]);
    const pos = layout(def);
    expect(pos.receive.x).toBeLessThan(pos.book.x);
    expect(pos.book.x).toBeLessThan(pos.check.x);
    const cyclic = connect(connect(emptyDefinition("c", "C"), "a", "b"), "b", "a");
    cyclic.nodes = { a: { id: "a", type: "If", inputs: {} }, b: { id: "b", type: "If", inputs: {} } };
    expect(Object.keys(layout(cyclic))).toEqual(["a", "b"]);
    expect(layout(moveNode(def, "mark", { x: 9, y: 9 })).mark).toEqual({ x: 9, y: 9 });
  });
});

describe("data picker", () => {
  it("offers outputs of earlier steps, the trigger-shaped input, variables and run context", () => {
    const options = availableFields(orderFlow(), "mark", descriptors);
    const labels = options.map((o) => `${o.group}: ${o.label}`);
    expect(labels).toContain("Book in ERP: body");
    expect(labels).toContain("Workflow input: data › order_id");
    expect(labels).toContain("Variables: region");
    expect(labels).toContain("Variables: booked");
    expect(labels).toContain("Run: correlation id");
    const erp = options.find((o) => o.label === "body" && o.group === "Book in ERP")!;
    expect(erp.binding).toEqual({ $output: "book.body" });
    expect(erp.token).toBe("{output.book.body}");
    // a step cannot use data of steps that run after it
    expect(availableFields(orderFlow(), "book", descriptors).some((o) => o.group === "Book in ERP")).toBe(false);
  });

  it("describes bindings in plain language", () => {
    const def = orderFlow();
    expect(describeBinding({ $output: "book.body.id" }, def)).toBe("Step ‘Book in ERP’ › body › id");
    expect(describeBinding({ $input: "data.order_id" })).toBe("Input › data › order_id");
    expect(describeBinding({ $tmpl: "Hi {input.x}" })).toBe("Text: Hi {input.x}");
    expect(describeBinding("plain")).toBe("plain");
    expect(describeBinding(5)).toBe("5");
    expect(isBinding({ $var: "x", other: 1 })).toBe(false);
    expect(flattenSchema({ properties: { a: { properties: { b: {} } } } })).toEqual([["a"], ["a", "b"]]);
  });
});

describe("schema forms", () => {
  it("maps JSON Schema properties to fields", () => {
    const fields = fieldsFor(descriptors.HttpRequest.inputs_schema);
    const byName = Object.fromEntries(fields.map((f) => [f.name, f]));
    expect(byName.method.kind).toBe("enum");
    expect(byName.method.options).toEqual(["GET", "POST"]);
    expect(byName.url.kind).toBe("text");
    expect(byName.url.nullable).toBe(true);
    expect(byName.json_body.kind).toBe("any");
    expect(byName.json_body.label).toBe("Json body");
    expect(fieldsFor(descriptors.MessageReceived.inputs_schema).find((f) => f.name === "endpoint")!.required).toBe(true);
    expect(fieldsFor({ $defs: { Kind: { enum: ["a"] } }, properties: { k: { $ref: "#/$defs/Kind" } } })[0].kind).toBe("enum");
    expect(fieldsFor({ properties: { n: { allOf: [{ type: "integer" }] } } })[0].kind).toBe("integer");
  });

  it("parses literals per kind", () => {
    expect(parseLiteral("number", "4.5")).toBe(4.5);
    expect(parseLiteral("integer", "")).toBeNull();
    expect(parseLiteral("number", "x")).toBe("x");
    expect(parseLiteral("boolean", "true")).toBe(true);
    expect(parseLiteral("list", "[1,2]")).toEqual([1, 2]);
    expect(parseLiteral("object", "{oops")).toBe("{oops");
    expect(parseLiteral("text", "hi")).toBe("hi");
    expect(humanize("json_body")).toBe("Json body");
    expect(humanize("maxTokens")).toBe("Max Tokens");
  });
});

describe("formatting and permissions", () => {
  it("formats durations for humans", () => {
    expect(formatDuration(0.25)).toBe("250 ms");
    expect(formatDuration(42)).toBe("42 s");
    expect(formatDuration(600)).toBe("10 min");
    expect(formatDuration(3600 * 3 + 1200)).toBe("3 h 20 min");
    expect(formatDuration(7200)).toBe("2 h");
    expect(formatDuration(86400 * 2 + 3600 * 4)).toBe("2 d 4 h");
    expect(formatDuration(86400)).toBe("1 d");
    expect(formatDuration(undefined)).toBe("–");
    expect(basisLabel("mixed", 12)).toBe("estimate + history (12 runs)");
    expect(basisLabel("weird")).toBe("weird");
    expect(slugify("Order to ERP – Ünïcode!")).toBe("order-to-erp-unicode");
  });

  it("gates navigation by permission", () => {
    const viewer = { subject: "a", name: "a", kind: "user" as const, roles: [], permissions: ["workflows.read"] };
    expect(visibleNav(viewer).map((n) => n.label)).toEqual(["My work", "Workflows"]);
    expect(can({ ...viewer, permissions: ["admin"] }, "anything")).toBe(true);
    expect(can(null, "workflows.read")).toBe(false);
  });
});
