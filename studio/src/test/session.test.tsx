import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { SessionProvider, connect, useSession } from "../auth/session";
import { eventLabel } from "../lib/journal";

const kc = { init: vi.fn(async () => true), updateToken: vi.fn(async () => true), token: "jwt-123", logout: vi.fn() };
vi.mock("keycloak-js", () => ({ default: vi.fn(function Keycloak() { return kc; }) }));

describe("login modes", () => {
  it("uses Keycloak with PKCE when configured and sends fresh bearer tokens", async () => {
    const { api, devMode, logout } = await connect({ apiBase: "/api",
      keycloak: { url: "https://kc", realm: "DnA Workflows", clientId: "DnA Workflows-studio" } });
    expect(devMode).toBe(false);
    expect(kc.init).toHaveBeenCalledWith(expect.objectContaining({ onLoad: "login-required", pkceMethod: "S256" }));
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response('{"name":"ada"}'));
    await api.me();
    expect(kc.updateToken).toHaveBeenCalledWith(30);
    const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/api/me");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer jwt-123");
    logout();
    expect(kc.logout).toHaveBeenCalled();
    fetchSpy.mockRestore();
  });

  it("falls back to development mode with a persona", async () => {
    localStorage.setItem("DnA Workflows.actor", "reviewer");
    const { api, devMode, logout } = await connect({ apiBase: "", keycloak: null });
    expect(devMode).toBe(true);
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("{}"));
    await api.me();
    expect(((fetchSpy.mock.calls[0][1] as RequestInit).headers as Record<string, string>)["X-Actor"]).toBe("reviewer");
    logout();
    expect(localStorage.getItem("DnA Workflows.actor")).toBeNull();
    fetchSpy.mockRestore();
  });

  it("provides the session to the app, or explains a connection problem", async () => {
    const Probe = () => { const s = useSession(); return <div>user {s.principal.name} can design: {String(s.can("workflows.design"))}</div>; };
    const api = { me: async () => ({ name: "ada", permissions: ["admin"], roles: [], subject: "a", kind: "user" }),
      capabilities: async () => ({ shell: "s", version: "1", features_enabled: [], activities: [] }) };
    render(<SessionProvider connector={async () => ({ api: api as never, devMode: true, logout: () => {} })}><Probe /></SessionProvider>);
    expect(await screen.findByText("user ada can design: true")).toBeInTheDocument();
    render(<SessionProvider connector={async () => { throw new Error("offline"); }}><Probe /></SessionProvider>);
    expect(await screen.findByText("Cannot reach DnA Workflows: offline")).toBeInTheDocument();
    expect(() => render(<Probe />)).toThrow("useSession outside SessionProvider");
    expect(eventLabel("work.parked")).toBe("Step paused");
    expect(eventLabel("custom.kind")).toBe("custom.kind");
  });
});
