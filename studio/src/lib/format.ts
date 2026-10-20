/** Human-friendly durations for analysts: "45 s", "12 min", "3 h 20 min", "2 d 4 h". */
export function formatDuration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds)) return "–";
  const s = Math.max(0, seconds);
  if (s < 1) return `${Math.round(s * 1000)} ms`;
  if (s < 60) return `${Math.round(s)} s`;
  const minutes = s / 60;
  if (minutes < 60) return `${Math.round(minutes)} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    const rest = Math.round(minutes - hours * 60);
    return rest ? `${hours} h ${rest} min` : `${hours} h`;
  }
  const days = Math.floor(hours / 24);
  const restHours = hours - days * 24;
  return restHours ? `${days} d ${restHours} h` : `${days} d`;
}

export const BASIS_LABEL: Record<string, string> = {
  prior: "designer estimate",
  history: "history",
  mixed: "estimate + history",
  default: "no data yet",
  partial: "partly estimated",
  actual: "actual",
};

export function basisLabel(basis: string, samples?: number): string {
  const label = BASIS_LABEL[basis] ?? basis;
  return samples ? `${label} (${samples} runs)` : label;
}

export function formatTime(iso: string | null | undefined): string {
  if (!iso) return "–";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

/** "order-to-erp" from "Order to ERP" – ids are derived, analysts type names. */
export function slugify(name: string): string {
  return name.toLowerCase().normalize("NFKD").replace(/[^\w\s-]/g, "").trim().replace(/[\s_]+/g, "-")
    .replace(/-+/g, "-");
}
