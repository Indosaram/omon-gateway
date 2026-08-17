# omon-gateway Web Dashboard

The dashboard is a Vite + React + TypeScript + Tailwind SPA served by the Rust/Axum HTTP server in `omon-gateway`.

## Run the dashboard

From the repository root:

```bash
cargo run -- dashboard
```

`serve` is an alias:

```bash
cargo run -- serve --host 127.0.0.1 --port 9119
```

The default bind address is `127.0.0.1:9119`. The dashboard has no transport authentication, so binding to a non-loopback address is rejected unless the risk is explicitly acknowledged with `--insecure` (or `DASHBOARD_INSECURE=true`).

The standalone dashboard does not require a Discord token. Administrative APIs remain available without a model. Web chat becomes available when `DEFAULT_MODEL` and the matching provider credentials are configured.

## Run with the Discord gateway

The normal gateway command can start the dashboard in the same process lifecycle:

```bash
DASHBOARD_ENABLED=true cargo run -- run
```

Setting `DASHBOARD_PORT` also enables it:

```bash
DASHBOARD_PORT=9120 cargo run -- run
```

Supported dashboard environment variables:

- `DASHBOARD_ENABLED=true` — enable the background dashboard.
- `DASHBOARD_HOST=127.0.0.1` — bind address.
- `DASHBOARD_PORT=9119` — bind port; setting it also enables the dashboard.
- `DASHBOARD_INSECURE=true` — allow a non-loopback bind without transport authentication.
- `DASHBOARD_WEB_ROOT=web/dist` — override the built SPA directory.
- `DATABASE_URL=sqlite://omon_gateway.db` — shared SQLite database.
- `DEFAULT_MODEL` plus `OPENAI_API_KEY`/`ANTHROPIC_API_KEY` — enable agent-backed web chat.
- `OMON_WORKSPACE_ROOT` and `OMON_TOOL_ROOTS` — workspace/tool roots used by dashboard chat.

The dashboard server shuts down gracefully when the owning gateway process exits or receives its cancellation signal.

## Frontend development

Install dependencies and start Vite:

```bash
cd web
npm install
npm run dev
```

Vite listens on `127.0.0.1:5173` and proxies `/api` HTTP and WebSocket traffic to `http://127.0.0.1:9119`, so run the Rust dashboard server in another terminal while developing.

## Production build

```bash
cd web
npm run build
```

The build is emitted to `web/dist`. Axum serves that directory as an SPA. If `web/dist/index.html` is absent, the backend serves an informative fallback page with links to the live API instead of failing startup.

## Dashboard surface

The SPA includes:

- **Overview** — uptime, bot/session counts, database, memory, disk, cron and approval status.
- **Chat** — persistent multi-session web chat with progressive stream chunks, typing state, tool activity and approval events.
- **Cron** — create, pause, resume, trigger and delete jobs; inspect recent execution history/errors.
- **Sessions** — search sessions across platforms, inspect metadata/transcripts and delete/reset stored sessions.
- **Skills & Tools** — inspect registered tool schemas and discovered `SKILL.md` entries.
- **Settings & Approvals** — inspect redacted active configuration, pending approvals, persistent allowlist rules and recent memory.
- **Logs** — filter buffered structured logs and tail new tracing events over WebSocket.

## HTTP and WebSocket API

Core routes are under `/api`:

```text
GET    /api/status
GET    /api/health
GET    /api/readiness
GET    /api/sessions
GET    /api/sessions/:id
GET    /api/sessions/:id/messages
DELETE /api/sessions/:id
POST   /api/sessions/:id/chat
WS     /api/sessions/:id/ws

GET    /api/cron/jobs
POST   /api/cron/jobs
GET    /api/cron/jobs/:id
PUT    /api/cron/jobs/:id
DELETE /api/cron/jobs/:id
POST   /api/cron/jobs/:id/trigger
POST   /api/cron/jobs/:id/pause
POST   /api/cron/jobs/:id/resume
GET    /api/cron/runs

GET    /api/config
GET    /api/tools
GET    /api/skills
GET    /api/memory
GET    /api/approvals/pending
POST   /api/approvals/:id/resolve
GET    /api/approvals/allowlist
GET    /api/logs
WS     /api/logs/ws
```

Provider secrets are never returned by `/api/config`; only configuration-presence flags are exposed.

## Verification

From the repository root:

```bash
cargo fmt --check
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
npm --prefix web run build
```
