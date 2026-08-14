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

If you are migrating an existing **Hermes Agent** installation to **OMO Gateway**, follow this automated recipe.

### 1. Configuration Mapping

| Hermes Location | Hermes Key | OMO Gateway `.env` Key | Example Value |
|---|---|---|---|
| `~/.hermes/.env` | `DISCORD_BOT_TOKEN` | `DISCORD_BOT_TOKEN` | `MTQ2...` |
| `~/.hermes/profiles/*/.env` | `DISCORD_BOT_TOKEN` | `DISCORD_BOT_TOKENS` | `token1,token2,token3` |
| `~/.hermes/.env` | `DISCORD_ALLOWED_USERS` | `DISCORD_ALLOWED_USERS` | `414011632306618368` |
| `~/.hermes/.env` | `DISCORD_FREE_RESPONSE_CHANNELS` | `DISCORD_FREE_RESPONSE_CHANNELS` | `1474213245014376569` |
| `~/.hermes/config.yaml` | `model.default` | `DEFAULT_MODEL` | `gpt-4o` / `gpt-5.6-luna` |
| `~/.hermes/config.yaml` | `model.base_url` / `providers.*.api` | `OPENAI_API_BASE` | `http://127.0.0.1:8317/v1` |
| `~/.hermes/config.yaml` | `model.api_key` / `providers.*.api_key` | `OPENAI_API_KEY` | `your-api-key` |
| `~/.hermes/config.yaml` | `approvals.mode` | `APPROVAL_MODE` | `smart` |

### 2. Multi-Bot Sharding Migration
In Hermes, multiple bot profiles (`advisor`, `marketer`, etc.) required running separate Python daemon processes.
In **OMO Gateway**, simply join all tokens with commas in `DISCORD_BOT_TOKENS`:
```bash
DISCORD_BOT_TOKENS="primary_bot_token,advisor_bot_token,marketer_bot_token"
```
OMO Gateway will automatically launch distinct gateway shards inside a single Tokio runtime.

### 3. Automated Cron Jobs Synchronization
Hermes stores scheduled jobs in `~/.hermes/cron/jobs.json` and `~/.hermes/profiles/*/cron/jobs.json`.
OMO Gateway includes `HermesStoreSynchronizer`:
- On startup, OMO Gateway automatically reads and imports all Hermes cron jobs into its persistent SQLite database (`cron_jobs` table).
- Both `prompt` jobs (which execute through the full agent LLM and tool-call loop) and `script` jobs are fully supported and dispatched to their target Discord channels (`deliver`/`origin`).

### 4. Workspace & Skills Migration
- **Workspace**: OMO Gateway isolates tool execution under `~/.omon/workspace` (configurable via `OMON_WORKSPACE_ROOT`).
- **Skills**: OMO Gateway automatically scans both `~/.hermes/skills/` and `~/.omon/skills/` for `SKILL.md` bundles.

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
| `DISCORD_FREE_RESPONSE_CHANNELS` | Optional | Channels where the bot responds without @mention |
| `DEFAULT_MODEL` | `gpt-4o` | Default LLM model identifier |
| `OPENAI_API_BASE` | `https://api.openai.com/v1` | OpenAI-compatible endpoint URL |
| `OPENAI_API_KEY` | Optional | OpenAI API key |
| `ANTHROPIC_BASE_URL` | Optional | Anthropic Messages endpoint URL |
| `ANTHROPIC_API_KEY` | Optional | Anthropic API key |
| `DATABASE_URL` | `sqlite://omon_gateway.db` | SQLite database path (WAL mode) |
| `OMON_WORKSPACE_ROOT` | `~/.omon/workspace` | Dedicated sandboxed working directory |
| `APPROVAL_MODE` | `smart` | `smart` (button approval) or `yolo` (auto-execute) |

---

## 📜 License

Licensed under the [Apache License 2.0](LICENSE).
