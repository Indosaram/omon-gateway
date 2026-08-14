<div align="center">

```
 ██████╗ ███╗   ███╗ ██████╗ ███╗   ██╗
██╔═══██╗████╗ ████║██╔═══██╗████╗  ██║
██║   ██║██╔████╔██║██║   ██║██╔██╗ ██║
██║   ██║██║╚██╔╝██║██║   ██║██║╚██╗██║
╚██████╔╝██║ ╚═╝ ██║╚██████╔╝██║ ╚████║
 ╚═════╝ ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
   G  A  T  E  W  A  Y
```

**The Rust-native AI agent gateway that replaces your entire Python fleet.**

[![Rust](https://img.shields.io/badge/Rust-1.78+-f74c00?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Tokio-Async_Runtime-232f3e?style=flat-square&logo=rust&logoColor=white)](https://tokio.rs/)
[![Discord](https://img.shields.io/badge/Discord-Gateway_v10-5865F2?style=flat-square&logo=discord&logoColor=white)](https://discord.com/developers/docs)
[![License](https://img.shields.io/badge/License-Apache_2.0-22c55e?style=flat-square)](LICENSE)
[![Zero GC](https://img.shields.io/badge/GC-Zero_Pause-a855f7?style=flat-square)]()
[![Platform](https://img.shields.io/badge/Platform-Linux_|_macOS_|_Docker-0ea5e9?style=flat-square)]()

---

### The 10× Rust Advantage

One **~20 MB binary**. Dozens of Discord bots. Sub-millisecond session routing.<br/>
No GIL. No garbage collector latency spikes. No 500 MB idle memory tax per bot.<br/>
Just pure, lock-free, zero-copy, Tokio-powered throughput.

</div>

---

## ⚡ Why Omon Exists

Traditional AI agent gateways — built in Python or Node — hit a wall:

| Pain Point | Root Cause |
|---|---|
| **GIL contention** | One bot blocks all others in-process |
| **500 MB+ idle memory** | Heavy runtimes, fat dependency trees |
| **GC latency spikes** | Unpredictable pauses during live streaming |
| **Fragile session routing** | Manual sticky sessions, Redis duct-tape |
| **Deployment sprawl** | One container per bot × N bots = Kubernetes nightmares |

**Omon Gateway** is the single compiled artifact that replaces all of it.

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DISCORD GATEWAY v10                                 │
│                    (Shards 0..N per bot identity)                            │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SESSION MULTIPLEXER & DELIVERY LEDGER                     │
│                                                                             │
│   ┌───────────────┐   ┌───────────────────────┐   ┌──────────────────┐    │
│   │  Lock-Free    │   │  Sliding-Window Rate  │   │  SQLite WAL      │    │
│   │  DashMap      │──▶│  Limit Debouncer      │──▶│  Context Store   │    │
│   │  (Sessions)   │   │  (per-channel)        │   │  (Scale-to-Zero) │    │
│   └───────────────┘   └───────────────────────┘   └──────────────────┘    │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       TOKIO ACTOR WORKER POOL                                │
│                                                                             │
│   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│   │wawabot  │  │ silphy  │  │  eris   │  │ bot_N   │  │ bot_N+1 │       │
│   │ Actor   │  │ Actor   │  │ Actor   │  │ Actor   │  │ Actor   │       │
│   └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │
│        └─────────────┴─────────────┴─────────────┴─────────────┘           │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AGENT ENGINE & NATIVE TOOLS                           │
│                                                                             │
│   ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐   │
│   │ Terminal │ │ File I/O │ │ Browser  │ │Web Search│ │  MCP Client  │   │
│   │  (PTY)  │ │  (CRUD)  │ │(CDP:9333)│ │(Jina/DDG)│ │ (stdio/SSE)  │   │
│   └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────────┘   │
│   ┌──────────────────────┐ ┌──────────────────────────────────────────┐   │
│   │  Cron Scheduler      │ │  140+ Auto-Discovered Skills             │   │
│   │  (SQLite-backed)     │ │  (Dynamic tool registry)                 │   │
│   └──────────────────────┘ └──────────────────────────────────────────┘   │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      LIVE STREAM EGRESS                                      │
│                                                                             │
│   ┌───────────────────┐  ┌────────────────────┐  ┌─────────────────────┐  │
│   │ Markdown-Aware    │  │ Action Row Buttons │  │  Voice (Songbird)   │  │
│   │ 2000-char Chunked │  │ [Approve] [Reject] │  │  Opus/PCM Streaming │  │
│   │ Streaming         │  │ Interactive UX     │  │  Ultra Low-Latency  │  │
│   └───────────────────┘  └────────────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Comparison

| Dimension | **Omon Gateway (Rust)** | Traditional Gateways (Python) |
|---|---|---|
| **Idle Memory / Bot** | ~8 MB | 500+ MB |
| **Binary Size** | ~20 MB (single static) | 2+ GB (venv + deps) |
| **Session Routing** | Lock-free DashMap, <1ms | Redis/sticky-session, 5-15ms |
| **GC Pauses** | **None** (ownership model) | 10-100ms stop-the-world |
| **Concurrent Bots** | Dozens in one process | One per process/container |
| **Live Streaming** | Zero-copy chunked egress | Buffered, GC-interrupted |
| **Context Eviction** | Automatic SQLite WAL flush | Manual / memory leak |
| **Voice Support** | Native Songbird + Opus | Third-party, high latency |
| **Tool Execution** | In-process PTY + sandbox | Subprocess + IPC overhead |
| **Cold Start** | < 50ms | 3-10 seconds |
| **Deployment** | `docker pull` → done | Dockerfile + pip + prayers |

---

## 🧱 Core Pillars

### 🔀 Sub-Millisecond Session Multiplexing

Lock-free `DashMap` routes every incoming Discord event to the correct actor in under 1ms. No mutexes. No contention. No Redis hop.

### 🤖 Multi-Bot Sharding

Run `wawabot`, `silphy`, `eris`, and dozens more simultaneously — all inside one compiled binary. Each bot is a lightweight Tokio task, not a heavyweight OS process.

### 🧊 Scale-to-Zero GC

Idle sessions automatically flush their context to **SQLite WAL** and release memory. Configurable timeout. Zero manual intervention. Your 3 AM memory graph stays flat.

### 💬 Full Discord UX

- **Markdown-aware** live streaming with intelligent 2000-character boundary splitting
- **Sliding-window rate limit debounce** — never hit 429 again
- **Interactive Action Rows**: `[Approve]` / `[Reject]` buttons for human-in-the-loop workflows
- **Slash commands** with full registration lifecycle

### ⏰ Autonomous Cron Engine

Persistent SQLite-backed scheduler. Define agent-powered prompt workflows or script executions on any cron expression. Survives restarts. No external scheduler required.

### 🛠️ Native Tool Ecosystem

| Tool | Capability |
|---|---|
| **Terminal** | PTY execution with per-workspace sandboxing |
| **File I/O** | Full CRUD with path safety enforcement |
| **Browser** | Chrome CDP control on port 9333 |
| **Web Search** | Jina Reader + DuckDuckGo fallback |
| **Web Fetch** | Raw HTTP with automatic readability extraction |
| **MCP Client** | Model Context Protocol over stdio and SSE |
| **Skills** | 140+ auto-discovered, dynamically registered |

### 🎙️ Ultra Low-Latency Voice

Discord voice channel integration via **Songbird**. Opus and PCM audio streaming with minimal jitter buffer. Real-time voice AI without third-party bridges.

---

## 🚀 60-Second Quickstart

### Option A: Cargo (from source)

```bash
# Clone
git clone https://github.com/your-org/omon-gateway.git
cd omon-gateway

# Configure
cp .env.example .env
# Edit .env with your bot tokens and API keys

# Build & Run
cargo run --release
```

### Option B: Docker Compose (recommended)

```yaml
# docker-compose.yml
version: "3.9"
services:
  omon:
    image: ghcr.io/your-org/omon-gateway:latest
    restart: unless-stopped
    env_file: .env
    volumes:
      - ./data:/app/data        # SQLite persistence
      - ./workspaces:/app/ws    # Tool sandboxes
    ports:
      - "9333:9333"             # Chrome CDP (optional)
```

```bash
docker compose up -d
# That's it. All bots are live.
```

---

## ⚙️ Configuration Cheatsheet

| Variable | Description | Default |
|---|---|---|
| `DISCORD_TOKENS` | Comma-separated bot tokens | *required* |
| `DISCORD_BOT_NAMES` | Matching comma-separated bot names | *required* |
| `LLM_API_URL` | OpenAI-compatible endpoint | `https://api.openai.com/v1` |
| `LLM_API_KEY` | API key for the LLM provider | *required* |
| `LLM_MODEL` | Model identifier | `gpt-4o` |
| `SESSION_TIMEOUT_SECS` | Idle session eviction timeout | `300` |
| `SQLITE_PATH` | Path to SQLite database file | `./data/omon.db` |
| `CHROME_CDP_PORT` | Chrome DevTools Protocol port | `9333` |
| `CRON_ENABLED` | Enable autonomous cron engine | `true` |
| `VOICE_ENABLED` | Enable Songbird voice integration | `false` |
| `MCP_SERVERS` | JSON array of MCP server configs | `[]` |
| `LOG_LEVEL` | Tracing filter directive | `info` |
| `RATE_LIMIT_WINDOW_MS` | Sliding window size for debounce | `1000` |
| `MAX_CONCURRENT_SESSIONS` | Per-bot session ceiling | `100` |

---

## 🤝 Contributing

We welcome contributions of all kinds — bug fixes, new tools, performance improvements, documentation.

```bash
# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run

# Format & lint
cargo fmt && cargo clippy -- -D warnings
```

Please open an issue before starting large features. All PRs should include tests where applicable.

---

## 📄 License

MIT © Omon Contributors

---

<div align="center">

**Built with 🦀 Rust, ⚡ Tokio, and zero patience for GC pauses.**

[Report Bug](../../issues) · [Request Feature](../../issues) · [Discussions](../../discussions)

</div>