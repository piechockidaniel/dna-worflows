import { lazy, Suspense } from "react";
import { BrowserRouter, NavLink, Route, Routes } from "react-router-dom";
import { useSession } from "./auth/session";
import { visibleNav } from "./lib/permissions";
import { ExecutionDetailPage, ExecutionsPage } from "./pages/ExecutionsPage";
import { ForecastsPage } from "./pages/ForecastsPage";
import { HomePage } from "./pages/HomePage";
import { OperationsPage } from "./pages/OperationsPage";
import { WorkflowsPage } from "./pages/WorkflowsPage";

// the designer pulls in the canvas library: load it only when needed
const DesignerPage = lazy(() => import("./pages/DesignerPage").then((m) => ({ default: m.DesignerPage })));

export const PERSONAS = ["analyst", "reviewer", "publisher", "operator", "approver"];

export function Layout() {
  const session = useSession();
  const { principal, devMode } = session;
  return (
    <div className="app">
      <nav className="nav">
        <div className="brand">〰 DnA Workflows</div>
        {visibleNav(principal).map((item) => (
          <NavLink key={item.path} to={item.path} end={item.path === "/"}>{item.label}</NavLink>))}
        <div className="spacer" />
        <div className="user">
          {devMode ? (
            <label className="hint">acting as
              <select aria-label="persona" value={principal.name} onChange={(e) => session.switchPersona(e.target.value)}>
                {[...new Set([principal.name, ...PERSONAS])].map((p) => <option key={p}>{p}</option>)}
              </select>
            </label>) : (
            <><div>{principal.name}</div><div className="hint">{principal.roles.filter((r) => r.startsWith("DnA Workflows-"))
              .map((r) => r.replace("DnA Workflows-", "")).join(", ")}</div>
              <button className="link" onClick={session.logout}>Sign out</button></>)}
        </div>
      </nav>
      <main>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/workflows" element={<WorkflowsPage />} />
          <Route path="/designer/:id" element={<Suspense fallback={<div className="page">Loading designer…</div>}>
            <DesignerPage /></Suspense>} />
          <Route path="/executions" element={<ExecutionsPage />} />
          <Route path="/executions/:id" element={<ExecutionDetailPage />} />
          <Route path="/operations" element={<OperationsPage />} />
          <Route path="/forecasts" element={<ForecastsPage />} />
        </Routes>
      </main>
    </div>
  );
}

export function App() {
  return <BrowserRouter basename="/studio"><Layout /></BrowserRouter>;
}
