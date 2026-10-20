import type {
  Capabilities, Draft, Execution, ExecutionForecast, Hold, JournalEvent, Principal, Proposal, TestRunResult,
  WorkflowDefinition, WorkflowForecast, Json,
} from "./types";

export class ApiError extends Error {
  constructor(public status: number, public detail: unknown) {
    super(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
}

export interface ApiOptions {
  baseUrl?: string;
  /** Returns a fresh bearer token (Keycloak) or null in development mode. */
  token?: () => Promise<string | null>;
  /** Development mode only: the name recorded in the audit trail. */
  actor?: () => string | null;
  fetch?: typeof fetch;
}

export class ApiClient {
  constructor(private options: ApiOptions = {}) {}

  async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = { Accept: "application/json" };
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const token = this.options.token ? await this.options.token() : null;
    if (token) headers.Authorization = `Bearer ${token}`;
    const actor = this.options.actor?.();
    if (actor && !token) headers["X-Actor"] = actor;
    const doFetch = this.options.fetch ?? fetch;
    const response = await doFetch(`${this.options.baseUrl ?? ""}${path}`, {
      method, headers, body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (response.status === 204) return undefined as T;
    const text = await response.text();
    const data = text ? JSON.parse(text) : undefined;
    if (!response.ok) throw new ApiError(response.status, data?.detail ?? data ?? response.statusText);
    return data as T;
  }

  me = () => this.request<Principal>("GET", "/me");
  capabilities = () => this.request<Capabilities>("GET", "/capabilities");
  // workflows & drafts
  workflows = () => this.request<WorkflowDefinition[]>("GET", "/workflows");
  versions = (id: string) => this.request<{ version: number; published_by?: string; published_at?: string }[]>(
    "GET", `/workflows/${enc(id)}/versions`);
  drafts = () => this.request<Draft[]>("GET", "/drafts");
  draft = (id: string) => this.request<Draft>("GET", `/drafts/${enc(id)}`);
  startDraft = (id: string, definition?: WorkflowDefinition) =>
    this.request<Draft>("POST", `/drafts/${enc(id)}`, { definition: definition ?? null });
  saveDraft = (id: string, definition: WorkflowDefinition, revision: number) =>
    this.request<Draft>("PUT", `/drafts/${enc(id)}`, { definition, revision });
  submitDraft = (id: string) => this.request<Draft>("POST", `/drafts/${enc(id)}/submit`);
  reviewDraft = (id: string, decision: "approve" | "request_changes", note?: string) =>
    this.request<Draft>("POST", `/drafts/${enc(id)}/review`, { decision, note });
  publishDraft = (id: string) => this.request<Draft>("POST", `/drafts/${enc(id)}/publish`);
  commentDraft = (id: string, text: string, node?: string) =>
    this.request<Draft>("POST", `/drafts/${enc(id)}/comments`, { text, node });
  discardDraft = (id: string) => this.request<void>("DELETE", `/drafts/${enc(id)}`);
  testRun = (id: string, input: Json, mocks: Record<string, { output: Json; outcome?: string }>, allowLive = false) =>
    this.request<TestRunResult>("POST", `/drafts/${enc(id)}/test-run`, { input, mocks, allow_live: allowLive });
  // executions
  executions = (query: Record<string, string | undefined> = {}) =>
    this.request<Execution[]>("GET", `/executions${qs(query)}`);
  execution = (id: string) => this.request<Execution>("GET", `/executions/${enc(id)}`);
  executionJournal = (id: string) => this.request<JournalEvent[]>("GET", `/executions/${enc(id)}/journal`);
  startExecution = (workflowId: string, input: Json, correlationId?: string) =>
    this.request<Execution>("POST", `/workflows/${enc(workflowId)}/executions`,
      { input, correlation_id: correlationId || null });
  cancel = (id: string, reason: string) => this.request<Execution>("POST", `/executions/${enc(id)}/cancel`, { reason });
  compensate = (id: string, reason: string) =>
    this.request<Execution>("POST", `/executions/${enc(id)}/compensate`, { reason });
  retry = (id: string, reason: string) => this.request<unknown>("POST", `/executions/${enc(id)}/retry`, { reason });
  resumeWork = (id: string, workItem: string, reason: string) =>
    this.request<unknown>("POST", `/executions/${enc(id)}/work/${enc(workItem)}/resume`, { reason });
  abandonWork = (id: string, workItem: string, reason: string) =>
    this.request<unknown>("POST", `/executions/${enc(id)}/work/${enc(workItem)}/abandon`, { reason });
  // operations
  holds = (activeOnly = false) => this.request<Hold[]>("GET", `/holds?active_only=${activeOnly}`);
  placeHold = (scope: string, target: string, reason: string) =>
    this.request<Hold>("POST", "/holds", { scope, target, reason });
  releaseHold = (id: string, reason: string) => this.request<Hold>("POST", `/holds/${enc(id)}/release`, { reason });
  parked = () => this.request<{ execution_id: string; work_item_id: string }[]>("GET", "/parked");
  proposals = (status?: string) => this.request<Proposal[]>("GET", `/proposals${qs({ status })}`);
  approve = (id: string, note?: string) => this.request<Proposal>("POST", `/proposals/${enc(id)}/approve`, { note });
  reject = (id: string, note?: string) => this.request<Proposal>("POST", `/proposals/${enc(id)}/reject`, { note });
  triage = (holdId: string) => this.request<Record<string, Json>>("POST", `/ai/triage/${enc(holdId)}`);
  // forecasts
  forecastWorkflow = (id: string, input: Json = {}, conditions: Json = {}) =>
    this.request<WorkflowForecast>("POST", `/forecasts/workflows/${enc(id)}`, { input, conditions });
  forecastPreview = (definition: WorkflowDefinition, input: Json = {}) =>
    this.request<WorkflowForecast>("POST", "/forecasts/preview", { definition, input });
  forecastExecution = (id: string) => this.request<ExecutionForecast>("GET", `/forecasts/executions/${enc(id)}`);
  forecastStats = (id: string) => this.request<Record<string, Json>[]>("GET", `/forecasts/workflows/${enc(id)}/stats`);
  forecastAccuracy = () => this.request<Record<string, Json>[]>("GET", "/forecasts/accuracy");
}

const enc = encodeURIComponent;

export function qs(query: Record<string, string | undefined>): string {
  const params = Object.entries(query).filter(([, v]) => v !== undefined && v !== "");
  return params.length ? `?${params.map(([k, v]) => `${enc(k)}=${enc(v as string)}`).join("&")}` : "";
}
