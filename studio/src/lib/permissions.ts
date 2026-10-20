import type { Principal } from "../api/types";

export function can(principal: Principal | null, permission: string): boolean {
  if (!principal) return false;
  return principal.permissions.includes("admin") || principal.permissions.includes(permission);
}

export interface NavItem { path: string; label: string; permission: string }

export const NAV: NavItem[] = [
  { path: "/", label: "My work", permission: "workflows.read" },
  { path: "/workflows", label: "Workflows", permission: "workflows.read" },
  { path: "/executions", label: "Executions", permission: "executions.read" },
  { path: "/operations", label: "Operations", permission: "holds.read" },
  { path: "/forecasts", label: "Forecasts", permission: "forecasts.read" },
];

export function visibleNav(principal: Principal | null): NavItem[] {
  return NAV.filter((item) => can(principal, item.permission));
}
