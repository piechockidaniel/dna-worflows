/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// API routes proxied to a local DnA Workflows host during `npm run dev`.
const api = process.env.DnA Workflows_API ?? "http://localhost:8080";
const apiPaths = ["/me", "/capabilities", "/workflows", "/drafts", "/executions", "/journal", "/holds", "/parked",
  "/proposals", "/forecasts", "/events", "/messages", "/messaging", "/outbox", "/ai", "/health", "/metrics"];

export default defineConfig({
  base: "/studio/",
  plugins: [react()],
  server: { port: 5173, proxy: Object.fromEntries(apiPaths.map((p) => [p, api])) },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    css: false,
    coverage: {
      provider: "v8",
      include: ["src/**/*.{ts,tsx}"],
      exclude: ["src/main.tsx", "src/test/**", "src/**/*.test.*", "src/vite-env.d.ts"],
      reporter: ["text-summary", "json-summary"],
      // gate (P17.2): the logic the analyst relies on must stay well tested
      thresholds: { "src/lib/**": { lines: 85, branches: 75 }, lines: 70 },
    },
  },
});
