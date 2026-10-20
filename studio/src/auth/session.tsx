import Keycloak from "keycloak-js";
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { ApiClient } from "../api/client";
import type { ActivityDescriptor, Capabilities, Principal } from "../api/types";
import { can as canDo } from "../lib/permissions";

export interface StudioConfig {
  apiBase: string;
  keycloak: { url: string; realm: string; clientId: string } | null;
}

export interface Session {
  api: ApiClient;
  principal: Principal;
  capabilities: Capabilities;
  descriptors: Record<string, ActivityDescriptor>;
  devMode: boolean;
  can: (permission: string) => boolean;
  logout: () => void;
  switchPersona: (name: string) => void;
}

const ACTOR_KEY = "DnA Workflows.actor";

export async function loadConfig(fetcher: typeof fetch = fetch): Promise<StudioConfig> {
  try {
    const response = await fetcher(`${import.meta.env.BASE_URL}config.json`);
    if (response.ok) return (await response.json()) as StudioConfig;
  } catch { /* fall back to development defaults */ }
  return { apiBase: "", keycloak: null };
}

/** Builds the API client: Keycloak (authorization code + PKCE) when configured, development mode otherwise. */
export async function connect(config: StudioConfig): Promise<{ api: ApiClient; devMode: boolean; logout: () => void }> {
  if (config.keycloak) {
    const kc = new Keycloak(config.keycloak);
    await kc.init({ onLoad: "login-required", pkceMethod: "S256", checkLoginIframe: false });
    const api = new ApiClient({
      baseUrl: config.apiBase,
      token: async () => { await kc.updateToken(30); return kc.token ?? null; },
    });
    return { api, devMode: false, logout: () => void kc.logout() };
  }
  const api = new ApiClient({ baseUrl: config.apiBase, actor: () => localStorage.getItem(ACTOR_KEY) ?? "analyst" });
  return { api, devMode: true, logout: () => localStorage.removeItem(ACTOR_KEY) };
}

const SessionContext = createContext<Session | null>(null);

export function useSession(): Session {
  const session = useContext(SessionContext);
  if (!session) throw new Error("useSession outside SessionProvider");
  return session;
}

export function SessionProvider({ children, connector = connect }: {
  children: ReactNode;
  connector?: typeof connect;
}) {
  const [state, setState] = useState<Omit<Session, "can" | "switchPersona" | "descriptors"> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const { api, devMode, logout } = await connector(await loadConfig());
        const [principal, capabilities] = await Promise.all([api.me(), api.capabilities()]);
        if (!cancelled) setState({ api, principal, capabilities, devMode, logout });
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
  }, [connector, version]);

  const switchPersona = useCallback((name: string) => {
    localStorage.setItem(ACTOR_KEY, name);
    setVersion((v) => v + 1);
  }, []);

  const session = useMemo<Session | null>(() => state && {
    ...state,
    descriptors: Object.fromEntries(state.capabilities.activities.map((a) => [a.type, a])),
    can: (permission: string) => canDo(state.principal, permission),
    switchPersona,
  }, [state, switchPersona]);

  if (error) return <div className="fatal">Cannot reach DnA Workflows: {error}</div>;
  if (!session) return <div className="loading">Connecting to DnA Workflows…</div>;
  return <SessionContext.Provider value={session}>{children}</SessionContext.Provider>;
}

/** For tests and storybook-like rendering. */
export function StaticSession({ value, children }: { value: Session; children: ReactNode }) {
  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}
