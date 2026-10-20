// JSON Schema (from /capabilities) → form field descriptors. No hand-written forms per activity (G9).
import type { JsonSchema, Json } from "../api/types";

export type FieldKind = "text" | "number" | "integer" | "boolean" | "enum" | "list" | "object" | "any";

export interface FieldSpec {
  name: string;
  label: string;
  kind: FieldKind;
  required: boolean;
  nullable: boolean;
  description?: string;
  default?: Json;
  options?: Json[];
}

export function resolveRef(schema: JsonSchema, root: JsonSchema): JsonSchema {
  if (schema.$ref) {
    const name = schema.$ref.split("/").pop()!;
    const target = root.$defs?.[name];
    return target ? resolveRef({ ...target, ...omit(schema, "$ref") }, root) : schema;
  }
  if (schema.allOf?.length === 1) return resolveRef({ ...schema.allOf[0], ...omit(schema, "allOf") }, root);
  return schema;
}

function omit<T extends object>(obj: T, key: keyof T): Partial<T> {
  const copy = { ...obj };
  delete copy[key];
  return copy;
}

export function humanize(name: string): string {
  const spaced = name.replace(/_/g, " ").replace(/([a-z])([A-Z])/g, "$1 $2");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

function kindOf(schema: JsonSchema): FieldKind {
  if (schema.enum) return "enum";
  const type = Array.isArray(schema.type) ? schema.type.find((t) => t !== "null") : schema.type;
  switch (type) {
    case "string": return "text";
    case "number": return "number";
    case "integer": return "integer";
    case "boolean": return "boolean";
    case "array": return "list";
    case "object": return "object";
    default: return "any";
  }
}

export function fieldsFor(root: JsonSchema): FieldSpec[] {
  const required = new Set(root.required ?? []);
  return Object.entries(root.properties ?? {}).map(([name, raw]) => {
    let schema = resolveRef(raw, root);
    let nullable = false;
    if (schema.anyOf) {
      const variants = schema.anyOf.map((s) => resolveRef(s, root));
      nullable = variants.some((v) => v.type === "null");
      const concrete = variants.filter((v) => v.type !== "null");
      schema = concrete.length === 1 ? { ...concrete[0], title: schema.title, description: schema.description,
        default: schema.default } : { ...schema, anyOf: undefined, type: undefined };
    }
    return {
      name, label: humanize(name),
      kind: kindOf(schema), required: required.has(name), nullable, description: schema.description,
      default: schema.default, options: schema.enum,
    };
  });
}

/** Convert what the analyst typed into the literal value for the field kind. */
export function parseLiteral(kind: FieldKind, raw: string): Json {
  switch (kind) {
    case "number": case "integer": {
      if (raw.trim() === "") return null;
      const n = Number(raw);
      return Number.isNaN(n) ? raw : n;
    }
    case "boolean": return raw === "true";
    case "list": case "object": case "any":
      try { return JSON.parse(raw); } catch { return raw; }
    default: return raw;
  }
}
