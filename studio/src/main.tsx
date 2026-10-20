import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { SessionProvider } from "./auth/session";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <SessionProvider><App /></SessionProvider>
  </StrictMode>,
);
