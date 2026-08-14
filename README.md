# Omon Gateway

[![Rust](https://img.shields.io/badge/Rust-1.85%2B-000000?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Release](https://img.shields.io/badge/release-v0.1.0-6f42c1)](https://github.com/omon-ai/omon-gateway/releases/tag/v0.1.0)
[![Discord](https://img.shields.io/badge/Discord-gateway-5865F2?logo=discord&logoColor=white)](https://discord.com/developers/applications)

**Ultra-fast, zero-GC async AI agent gateway and session multiplexer for Discord in 100% Rust.**

Omon Gateway connects one or more Discord bot identities to persistent, tool-capable AI agent sessions. It combines lock-free routing, actor-isolated execution, streaming model responses, SQLite-backed state, autonomous scheduled workflows, and native integrations in one self-hosted process.

## Architecture

Each Discord conversation is mapped to an isolated session actor. The multiplexer routes events without a global session lock, preserves per-session ordering, and retires idle actors through the scale-to-zero collector.

```text
+-------------------+     +----------------------+     +-------------------+     +------------------+
| Discord Ingress   | --> | Session Multiplexer  | --> | Agent Engine      | --> | Discord Egress   |
| messages, threads |     | DashMap + Tokio      |     | LLMs, memory,     |     | streaming edits, |
| commands, buttons |     | actor tasks          |     | tools, cron       |     | files, approvals |
+-------------------+     +----------------------+     +-------------------+     +------------------+
                                  |                           |
                                  v                           v
                          +---------------+            +---------------+
                          | SQLite state  |            | Native tools  |
                          | and ledger    |            | and MCP       |
                          +---------------+            +---------------+
```

## Core Features

- **⚡ Ultra-fast Lock-Free Async Session Multiplexing** - `DashMap` routing and Tokio actor tasks provide concurrent sessions with ordered, isolated execution and scale-to-zero cleanup.
- **🤖 Multi-Bot Parallel Sharding** - control multiple Discord bot identities in a single process by supplying a comma-separated token list.
- **🛠️ Native Tools** - PTY-style terminal execution, file CRUD, MCP over JSON-RPC, web search and fetch, Chrome CDP browser automation, and discovery of 140+ Hermes-compatible skills.
- **⏰ Autonomous Cron Engine** - a persistent SQLite scheduler runs shell, payload, and agent-backed prompt workflows and can synchronize Hermes job stores.
- **🎙️ Discord Voice Streaming** - Songbird-based Opus and decoded PCM ingestion feeds bounded, asynchronous audio pipelines.
- **🛡️ Smart Approval Guard** - interactive Discord buttons coordinate approval and rejection decisions for guarded actions.

Additional capabilities include OpenAI-compatible and Anthropic streaming APIs, tool-call rounds, persistent memory, delivery deduplication, Markdown-safe Discord chunking, and per-session model selection.

## Prerequisites

- Rust 1.85 or newer for native builds
- A Discord application with a bot token
- An API key for the selected model provider
- SQLite-compatible local storage
- Docker Engine with Docker Compose v2 for container deployment

Enable the **Message Content Intent** for each bot in the Discord Developer Portal. Invite bots with permissions to view channels, send messages, embed links, attach files, read message history, and use application commands. Voice deployments also require connect and speak permissions.

## Quickstart

### Local Cargo Build

```bash
git clone https://github.com/omon-ai/omon-gateway.git
cd omon-gateway
cp .env.example .env
# Edit .env with a Discord token, model, and provider credentials.
mkdir -p workspace
cargo build --release
./target/release/omon-gateway
```

For a development run with logs:

```bash
RUST_LOG=omon_gateway=debug cargo run --bin omon-gateway
```

Stop the process with `Ctrl+C`; the gateway shuts down Discord shards, the cron scheduler, session garbage collection, and the database pool cleanly.

### Docker Compose

```bash
cp .env.example .env
# Edit .env before starting the service.
mkdir -p data workspace
docker compose up --build -d
docker compose logs -f omon-gateway
```

Compose persists SQLite data under `./data` and exposes `./workspace` to agent tools. Stop the service with `docker compose down`. Do not mount sensitive host directories as the workspace.

### macOS LaunchAgent

Build the release binary, place the repository at a stable absolute path, and create `~/Library/LaunchAgents/ai.omon.gateway.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>ai.omon.gateway</string>
  <key>ProgramArguments</key>
  <array><string>/absolute/path/omon-gateway/target/release/omon-gateway</string></array>
  <key>WorkingDirectory</key><string>/absolute/path/omon-gateway</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/absolute/path/omon-gateway/gateway.log</string>
  <key>StandardErrorPath</key><string>/absolute/path/omon-gateway/gateway.log</string>
</dict>
</plist>
```

The process loads `.env` from its working directory. Replace every `/absolute/path` value, then run:

```bash
launchctl bootstrap gui/"$(id -u)" ~/Library/LaunchAgents/ai.omon.gateway.plist
launchctl kickstart -k gui/"$(id -u)"/ai.omon.gateway
```

### Linux systemd

Install the release binary and configuration in `/opt/omon-gateway`, then create `/etc/systemd/system/omon-gateway.service`:

```ini
[Unit]
Description=Omon Gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=omon
Group=omon
WorkingDirectory=/opt/omon-gateway
ExecStart=/opt/omon-gateway/omon-gateway
EnvironmentFile=/opt/omon-gateway/.env
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now omon-gateway
sudo journalctl -u omon-gateway -f
```

Grant the service user write access only to the configured database and workspace directories.

## Configuration Reference

Omon Gateway loads `.env` from its working directory and also accepts normal process environment variables. Comma-separated lists ignore surrounding whitespace.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DISCORD_BOT_TOKEN` | Conditional | None | Primary Discord bot token. At least one token must be provided through this setting or `DISCORD_BOT_TOKENS`. It may also contain a comma-separated list. |
| `DISCORD_BOT_TOKENS` | Conditional | None | Additional comma-separated Discord bot tokens started concurrently in the same process. Duplicate tokens are removed. |
| `DISCORD_ALLOWED_USERS` | No | All users | Comma-separated Discord user IDs allowed to interact with the gateway. An empty value disables the allowlist. |
| `DISCORD_FREE_RESPONSE_CHANNELS` | No | None | Comma-separated channel IDs where messages do not need to mention the bot. Direct messages and threads remain eligible automatically. |
| `DISCORD_HOME_CHANNEL` | No | None | Comma-separated home channel IDs, merged into the free-response channel list. |
| `DEFAULT_MODEL` | Yes | None | Initial model identifier. Names beginning with `claude` select Anthropic; other names select the OpenAI-compatible client. |
| `OPENAI_API_BASE` | No | Provider default | Base URL for OpenAI or an OpenAI-compatible API. A typical value is `https://api.openai.com/v1`. |
| `OPENAI_API_KEY` | Provider-dependent | None | Bearer token for the OpenAI-compatible provider. |
| `ANTHROPIC_BASE_URL` | No | Provider default | Base URL for the Anthropic Messages API. A typical value is `https://api.anthropic.com/v1`. |
| `ANTHROPIC_API_KEY` | Provider-dependent | None | API key used when the selected model name begins with `claude`. |
| `DATABASE_URL` | No | `sqlite://omon_gateway.db` | SQLite connection URL for sessions, messages, memory, delivery state, and cron jobs. Use `sqlite:///app/data/omon_gateway.db` in the container. |
| `OMON_WORKSPACE_ROOT` | No | `$HOME/.omon/workspace` | Root directory available to terminal and file tools. The gateway creates the directory when possible. |
| `APPROVAL_MODE` | No | `smart` in the template | Reserved approval-policy setting. `smart`, `always`, and `never` are documented policy values; the v0.1 runtime exposes the interactive guard while policy wiring remains forward-compatible. |

Optional Hermes integration uses `HERMES_HOME` for one job store and `OMON_HERMES_PROFILES` for additional comma-separated profile paths. Set `RUST_LOG` to a `tracing-subscriber` filter such as `info` or `omon_gateway=debug`.

## Native Tools

| Tool | Purpose |
| --- | --- |
| `terminal` | Run bounded shell commands within the configured workspace. |
| `file` | Read, write, search, and manage files under the workspace root. |
| `mcp` | Call configured Model Context Protocol servers over JSON-RPC transports. |
| `cron` | Inspect, add, and delete persistent scheduled jobs. |
| `web_search` | Search current public web content. |
| `web_fetch` | Fetch a URL and extract readable text. |
| `browser` | Control a Chrome-compatible browser through the Chrome DevTools Protocol. |
| `skills` | Discover and load Hermes-compatible `SKILL.md` instructions. |

Tool access is powerful. Run the gateway under a dedicated operating-system account, use a constrained workspace, restrict Discord users, and review container volume mounts.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo build --release
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution standards, [SECURITY.md](SECURITY.md) for private vulnerability reporting, and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community expectations.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
