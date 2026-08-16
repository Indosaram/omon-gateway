<div align="center">

```
 ██████╗ ███╗   ███╗ ██████╗ 
██╔═══██╗████╗ ████║██╔═══██╗
██║   ██║██╔████╔██║██║   ██║
██║   ██║██║╚██╔╝██║██║   ██║
╚██████╔╝██║ ╚═╝ ██║╚██████╔╝
 ╚═════╝ ╚═╝     ╚═╝ ╚═════╝ 
   G  A  T  E  W  A  Y
```

**High-performance, Zero-GC Discord Multiplexer Gateway for OMO (oh-my-openagent) in 100% Rust.**

[![Rust](https://img.shields.io/badge/Rust-1.78+-f74c00?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Tokio-Async_Runtime-232f3e?style=flat-square&logo=rust&logoColor=white)](https://tokio.rs/)
[![Discord](https://img.shields.io/badge/Discord-Gateway_v10-5865F2?style=flat-square&logo=discord&logoColor=white)](https://discord.com/developers/docs)
[![License](https://img.shields.io/badge/License-Apache_2.0-22c55e?style=flat-square)](LICENSE)
[![Zero GC](https://img.shields.io/badge/GC-Zero_Pause-a855f7?style=flat-square)]()
[![Platform](https://img.shields.io/badge/Platform-Linux_|_macOS_|_Docker-0ea5e9?style=flat-square)]()

---

### The Native Discord Bridge for OMO

**OMO Gateway** is a dedicated, ultra-fast Rust gateway that bridges **OMO (oh-my-openagent)** directly to Discord.<br/>
It multiplexes thousands of concurrent channels, threads, and DMs with sub-millisecond routing, multi-bot sharding, scale-to-zero memory reclamation, and sandboxed tool execution.

</div>

---

## ⚡ Why OMO Gateway?

**OMO (oh-my-openagent)** provides autonomous agent intelligence, deep planning, and subagent orchestration. However, running AI agents directly against Discord's WebSocket APIs creates operational bottlenecks:

- **Heavy Idle Memory**: Running individual Discord connections per agent consumes hundreds of megabytes.
- **Concurrency & Rate Limits**: Managing token streaming across many channels triggers Discord rate-limit bans without centralized debouncing.
- **Multi-Bot Management**: Running multiple bot identities requires running multiple redundant runtime instances.

**OMO Gateway solves this** by acting as a high-throughput, pure-Rust I/O multiplexer sitting between Discord and OMO.

---

## 🏗️ Architecture

```
[ Discord Ingress: DMs / Server Channels / Threads / Voice ]
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                    OMO GATEWAY (Pure Rust)                  │
│                                                             │
│  ┌─────────────────────────┐   ┌─────────────────────────┐  │
│  │   Session Multiplexer   │   │     Delivery Ledger     │  │
│  │   (Lock-Free DashMap)   │   │  (SQLite WAL Idempotent)│  │
│  └────────────┬────────────┘   └─────────────────────────┘  │
│               │                                             │
│               ▼                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │       Bounded Actor Worker Pool (tokio::task)         │  │
│  │     - Scale-to-Zero GC (Idle Session Eviction)        │  │
│  │     - Multi-Bot Sharding (N Bots in 1 Binary)         │  │
│  └────────────────────────────┬──────────────────────────┘  │
└───────────────────────────────┼─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                   OMO AGENT EXECUTION ENGINE                │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  LLM Streaming & Tool-Call Loop (OpenAI/Anthropic/...)│  │
│  └────────────────────────────┬──────────────────────────┘  │
│                               │                             │
│  ┌────────────────────────────┴──────────────────────────┐  │
│  │ Native Tools: PTY Terminal / File CRUD / MCP / Web    │  │
│  │ Dedicated Workspace Isolation (~/.omon/workspace)     │  │
│  └───────────────────────────────────────────────────────┘  │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    DISCORD RESPONSE EGRESS                  │
│  - Live-Edit Debounced Streaming (800ms sliding window)     │
│  - Interactive Smart Approvals ([Approve] / [Reject] UI)    │
│  - Songbird Real-Time Voice Audio Pipeline (Opus / PCM)     │
│  - Scheduled Cron Event Push Dispatch                       │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

- ⚡ **Lock-Free Session Multiplexing**: Routes messages across servers, threads, and DMs using composite session keys `(platform, guild_id, channel_id, thread_id, user_id)` with zero lock contention.
- 🤖 **Multi-Bot Parallel Sharding**: Run and control multiple Discord bot identities simultaneously from a single compiled ~20MB binary.
- 📉 **Scale-to-Zero GC**: Inactive sessions automatically flush their state to SQLite and evict worker tasks, reducing idle memory footprint to near zero.
- 🎙️ **Songbird Discord Voice**: Stream bidirectional Opus/PCM audio directly in Discord voice channels.
- ⏰ **Autonomous Cron Engine**: Persistent SQLite scheduler that triggers background agent prompt runs and command executions, pushing results directly to designated Discord channels.
- 🛠️ **Native Tool Suite**:
  - **PTY Terminal**: Execute commands in an isolated workspace (`~/.omon/workspace`).
  - **File Tools**: Sandboxed file read, write, and directory inspection.
  - **Browser & Web**: Chrome CDP control (port 9333), live web search, and text extraction.
  - **Model Context Protocol (MCP)**: Connect to external tools via stdio and SSE.
- 🛡️ **Smart Approval Guard**: Interactively request user confirmation in Discord via button components before running dangerous shell commands.

---

## 🔄 Hermes to OMO Gateway Migration Guide (for LLMs & Automations)

`omon-gateway migrate` performs the shipped one-click migration from Hermes Agent to OMO Gateway. Run it from the gateway directory so the imported configuration is written to that directory's `.env`:

```bash
# Preview the complete migration without writing files, changing the database,
# signaling processes, or invoking launchctl.
cargo run -- migrate --dry-run

# Import configuration and cron jobs, then retire the Hermes cron stores and gateway.
cargo run -- migrate
```

The compiled binary accepts the same subcommand (`omon-gateway migrate`). The default flow runs in this order:

1. Import Hermes configuration from `$HERMES_HOME` (default: `~/.hermes`), including the root `.env`, `config.yaml`, and profile `.env` files.
2. Authoritatively rewrite the gateway `.env`. If `.env` already exists, its complete previous contents are first copied to `.env.bak-<timestamp>`.
3. Synchronize Hermes cron jobs into the gateway SQLite `cron_jobs` table.
4. Verify each Hermes job exists in the gateway database, back up each non-empty `jobs.json` as `jobs.json.bak-omon-migration-<timestamp>`, and atomically replace its job list with an empty list.
5. Stop live Hermes gateway processes recorded by the root and profile `gateway.lock` files. On macOS, matching `~/Library/LaunchAgents/ai.hermes.gateway*.plist` services are booted out with `launchctl` and the plist files are renamed to `.plist.disabled` so launchd cannot restart them.

The backups and `.disabled` LaunchAgent files make the file cutover reversible; the original files are retained rather than deleted.

### Migration modes

| Command | Behavior |
|---|---|
| `omon-gateway migrate` | Full import and cutover. Writes the authoritative `.env`, imports cron jobs, empties backed-up Hermes cron stores after verification, and stops/disables the Hermes gateway. |
| `omon-gateway migrate --dry-run` | Read-only projection of configuration, cron, process, and LaunchAgent changes. It performs zero writes and does not run cron synchronization or destructive side effects. |
| `omon-gateway migrate --no-cutover` | Import only. Writes the gateway `.env` and synchronizes cron jobs, but does not empty Hermes cron stores or stop/disable Hermes services. |

Use `--no-cutover` when Hermes must remain available during a staged migration. Do not run both gateways against the same bot tokens and cron schedules after the final cutover.

### What the importer maps

The command performs these mappings automatically; this table is a reference, not a manual copy-and-paste procedure.

| Hermes Location | Hermes Key | OMO Gateway `.env` Key | Mapping behavior |
|---|---|---|---|
| `$HERMES_HOME/.env` | `DISCORD_BOT_TOKEN` | `DISCORD_BOT_TOKEN` | Preserves the primary token. |
| `$HERMES_HOME/profiles/*/.env` | `DISCORD_BOT_TOKEN` | `DISCORD_BOT_TOKENS` | Collects non-empty profile tokens in stable order and removes duplicates, including the primary token. |
| `$HERMES_HOME/.env` or profile `.env` | `DISCORD_ALLOWED_USERS` | `DISCORD_ALLOWED_USERS` | The root value wins; otherwise the first profile value is used. |
| `$HERMES_HOME/.env` or profile `.env` | `DISCORD_FREE_RESPONSE_CHANNELS` | `DISCORD_FREE_RESPONSE_CHANNELS` | The root value wins; otherwise the first profile value is used. |
| `$HERMES_HOME/.env` or profile `.env` | `DISCORD_HOME_CHANNEL` | `DISCORD_HOME_CHANNEL` | Preserved; the gateway also treats these as free-response channels at runtime. |
| `$HERMES_HOME/config.yaml` | `model.default` | `DEFAULT_MODEL` | Preserves the configured model identifier. |
| `$HERMES_HOME/config.yaml` | `model.base_url`, `model.api_key` | `ANTHROPIC_BASE_URL`, `ANTHROPIC_API_KEY` | Used when the default model name starts with `claude`. |
| `$HERMES_HOME/config.yaml` | `model.base_url`, `model.api_key` | `OPENAI_API_BASE`, `OPENAI_API_KEY` | Used for other model names. |
| `$HERMES_HOME/.env` or `config.yaml` | `APPROVAL_MODE` or `approvals.mode` | `APPROVAL_MODE` | The root `.env` value wins; otherwise the YAML approval mode is used. |

Only mapped keys are written to the authoritative `.env`; unrelated values from a previous gateway `.env` are available in its timestamped backup but are not carried forward automatically.

### Cron stores, workspace, and skills

- **Cron stores**: Hermes jobs are read from `$HERMES_HOME/cron/jobs.json` and `$HERMES_HOME/profiles/*/cron/jobs.json`. `OMON_HERMES_PROFILES` can restrict the profiles synchronized by the runtime; when unset, the default store and profile directories are discovered automatically.
- **Workspace**: When `OMON_WORKSPACE_ROOT` is unset, OMO Gateway isolates terminal and file tools under `$HOME/.omon/workspace`.
- **Skills**: OMO Gateway scans `$HERMES_HOME/skills` (default: `~/.hermes/skills`) and `~/.omon/skills` for `SKILL.md` bundles.

---

## 🚀 Quickstart

### 1. Build and Run from Source

```bash
# Clone the repository
git clone https://github.com/Indosaram/omon-gateway.git
cd omon-gateway

# Configure environment
cp .env.example .env
# Edit .env with your Discord bot tokens and LLM endpoint

# Build and run optimized release binary
cargo run --release
```

### 2. Run with Docker Compose

```bash
docker compose up -d
```

### 3. Run as macOS Background Service (LaunchAgent)

```bash
# Build release binary
cargo build --release --bin omon-gateway

# Install LaunchAgent
cp ai.omon.gateway.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/ai.omon.gateway.plist
```

---

## ⚙️ Configuration Reference

| Environment Variable | Default | Description |
|---|---|---|
| `DISCORD_BOT_TOKEN` | Required | Primary Discord Bot Token |
| `DISCORD_BOT_TOKENS` | Optional | Comma-separated tokens for Multi-Bot sharding |
| `DISCORD_ALLOWED_USERS` | Optional | Allowed Discord user IDs (empty allows all) |
| `DISCORD_ALLOWED_ROLES` | Optional | Comma-separated Discord role IDs allowed to interact with the bot |
| `DISCORD_ALLOW_ALL_USERS` | `false` | When true, bypasses user/role allowlists and grants access to all users |
| `DISCORD_ALLOWED_CHANNELS` | Optional | Comma-separated channel IDs allowed to process messages (DMs exempt) |
| `DISCORD_IGNORED_CHANNELS` | Optional | Comma-separated channel IDs ignored completely |
| `DISCORD_FREE_RESPONSE_CHANNELS` | Optional | Channels where the bot responds without @mention |
| `DISCORD_AUTO_THREAD` | `false` | When true, @mentions in guild text channels auto-create a public thread and route responses there |
| `DISCORD_THREAD_SESSIONS_PER_USER` | `true` | When false, all users in a thread share the same conversation session |
| `DISCORD_CHANNEL_CONTEXT` | `false` | When true, @mentions in guild channels backfill recent channel history as conversation context |
| `DISCORD_CHANNEL_CONTEXT_LIMIT` | `10` | Maximum preceding messages to backfill when channel context is enabled (clamped to <= 25) |
| `DISCORD_PROCESSING_REACTIONS` | `true` | When true, adds 👀 reaction when processing starts, swapping to ✅ on success or ❌ on failure |
| `DISCORD_CHUNK_PAGINATION` | `true` | When true, adds (i/N) pagination indicator headers to messages split across multiple chunks |
| `DISCORD_PROFILE_ROUTES` | Optional | JSON array of hierarchical profile routing rules mapping `(guild, channel, thread)` to custom models, prompts, and toolsets |
| `DEFAULT_MODEL` | `gpt-4o` | Default LLM model identifier |
| `OPENAI_API_BASE` | `https://api.openai.com/v1` | OpenAI-compatible endpoint URL |
| `OPENAI_API_KEY` | Optional | OpenAI API key |
| `ANTHROPIC_BASE_URL` | Optional | Anthropic Messages endpoint URL |
| `ANTHROPIC_API_KEY` | Optional | Anthropic API key |
| `DATABASE_URL` | `sqlite://omon_gateway.db` | SQLite database path (WAL mode) |
| `OMON_WORKSPACE_ROOT` | `$HOME/.omon/workspace` | Dedicated sandboxed working directory used by terminal and file tools |
| `HERMES_HOME` | `$HOME/.hermes` | Hermes root used by migration, cron synchronization, and Hermes skill discovery |
| `OMON_HERMES_PROFILES` | Auto-discover | Optional comma-separated Hermes cron profiles; when unset, synchronizes `default` plus discovered profile directories |
| `APPROVAL_MODE` | `smart` | Enforced terminal approval policy: `smart` gates dangerous commands, `always` gates every command, and `never`/`yolo` bypass approval |
| `APPROVAL_TIMEOUT_SECS` | `900` | Seconds to wait for a Discord command approval before the request expires (default 900) |
| `APPROVALS_DENY` | Optional | Comma-separated wildcard globs (`npm publish *,kubectl delete *`) unconditionally blocked before policy, YOLO, or allowlists |
| `DISCORD_APPROVAL_MENTIONS` | `false` | When true, @-mentions allowed users (<@uid>) on approval prompts for push notifications |

---

## 📜 License

Licensed under the [Apache License 2.0](LICENSE).
