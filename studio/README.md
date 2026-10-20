# DnA Workflows Studio

Low-code GUI for domain analysts (plan phase P12): Vite 8 · React 19 · TypeScript 5.9 · React Flow 12 ·
keycloak-js. Everything is driven by the server's machine-readable descriptors (`/capabilities`): no form is
hand-written per activity.

```bash
npm ci
npm run dev        # http://localhost:5173/studio/ – proxies the API at http://localhost:8080 (DnA Workflows_API)
npm test           # Vitest + Testing Library
npm run coverage   # with the coverage gate (≥70 % lines overall, ≥85 % for src/lib)
npm run build      # dist/ – served by the DnA Workflows API under /studio (api.studio_dir)
```

Runtime configuration is fetched from `/studio/config.json` (served by the API from `api.studio_config`),
so one build serves every environment. Without a `keycloak` entry the Studio runs in development mode with
a persona switcher (the name is sent as `X-Actor`).

| Folder | Contents |
|---|---|
| `src/api` | typed API client and models |
| `src/auth` | Keycloak (authorization code + PKCE) / development session |
| `src/lib` | pure logic: graph editing, data picker, schema → form, formatting, permissions, journal labels |
| `src/components` | canvas, palette, value editor (fixed / from data / text with data), step inspector, estimate and undo editors |
| `src/pages` | My work, Workflows, Designer, Executions (+ ETA), Operations, Forecasts |
