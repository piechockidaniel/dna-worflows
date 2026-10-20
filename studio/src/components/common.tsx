import { useCallback, useEffect, useState, type ReactNode } from "react";

export function useAsync<T>(load: () => Promise<T>, deps: unknown[]): {
  data: T | undefined; error: string | null; loading: boolean; reload: () => void;
} {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    load().then((d) => { if (!cancelled) { setData(d); setError(null); } })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : String(e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick]);
  const reload = useCallback(() => setTick((t) => t + 1), []);
  return { data, error, loading, reload };
}

/** Runs an action, shows its error, reports busy state. */
export function useAction(): [(fn: () => Promise<unknown>) => Promise<boolean>, boolean, string | null, () => void] {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const run = useCallback(async (fn: () => Promise<unknown>) => {
    setBusy(true);
    setError(null);
    try { await fn(); return true; } catch (e) { setError(e instanceof Error ? e.message : String(e)); return false; }
    finally { setBusy(false); }
  }, []);
  return [run, busy, error, () => setError(null)];
}

const STATUS_TONE: Record<string, string> = {
  completed: "ok", published: "ok", approved: "ok", executed: "ok", released: "muted", delivered: "ok",
  running: "info", waiting: "info", in_review: "info", draft: "muted", pending: "info",
  parked: "warn", active: "warn", changes_requested: "warn", held: "warn", retrying: "warn",
  faulted: "bad", failed: "bad", cancelled: "muted", rejected: "muted", compensated: "warn",
};

export function StatusChip({ status }: { status: string }) {
  return <span className={`chip chip-${STATUS_TONE[status] ?? "muted"}`}>{status.replace(/_/g, " ")}</span>;
}

export function ErrorBanner({ error, onClose }: { error: string | null; onClose?: () => void }) {
  if (!error) return null;
  return <div className="error" role="alert">{error}{onClose && <button className="link" onClick={onClose}>×</button>}</div>;
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}

export function Section({ title, actions, children }: { title: string; actions?: ReactNode; children: ReactNode }) {
  return (
    <section className="card">
      <header className="card-head"><h2>{title}</h2><div className="actions">{actions}</div></header>
      {children}
    </section>
  );
}
