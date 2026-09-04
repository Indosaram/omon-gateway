# Omon Gateway: Agent Callpath, Runtime Architecture & Engineering Conventions

This document maps the agent execution callpath from the dashboard playground chat handler through the multiplexer and LLM client, details the role and lifecycle of `dashboard_runtime.rs`, describes configuration and environment variable conventions, catalogues relevant dependencies for WebSocket/NDJSON transport, and outlines testing conventions across actor and dashboard test suites.

---

## 1. Dashboard Playground Chat Handler (`POST /api/sessions/{id}/chat`)

### 1.1 Route Registration & Schemas
- **Route Registration**: `src/dashboard.rs:582` registers the route `.route("/api/sessions/{id}/chat", post(post_chat))`.
- **Request JSON Schema** (`src/dashboard.rs:985-988`):
  ```json
  {
    "message": "string (required, non-empty)"
  }
  ```
  Represented in Rust by `ChatRequest`:
  ```rust
  #[derive(Debug, Deserialize)]
  struct ChatRequest {
    message: String,
  }
  ```
- **Response JSON Schema** (`src/dashboard.rs:1005-1011`):
  - **Success (`202 Accepted`)**:
    ```json
    {
      "queued": true,
      "session_id": "<storage_session_key>"
    }
    ```
  - **Client Error (`400 Bad Request`)**: `{"error": "message must not be empty", "status": 400}` (`src/dashboard.rs:992-997`).
  - **Server Error (`503 Service Unavailable`)**: `{"error": "chat runtime is not configured; set DEFAULT_MODEL and provider credentials", "status": 503}` (`src/dashboard.rs:998-1003`).

### 1.2 LLM Driving & Execution Flow
- **Bootstrapping the LLM**: In standalone mode, `src/dashboard_runtime.rs:121-125` creates an `LlmClient` instance using `llm_config_from_environment(model)` (`src/dashboard_runtime.rs:242-260`).
- **LiveAgentRunner Setup**: `src/dashboard_runtime.rs:123-140` packages the `LlmClient`, `SqlitePool`, `MemoryStore`, `ToolRegistry`, and `WebDashboardDispatcher` into an `Arc<LiveAgentRunner>`.
- **Multiplexer Routing**:
  1. `post_chat` resolves the session key via `resolve_session_key(&state.pool, &id)` (`src/dashboard.rs:1001`).
  2. It constructs an `InboundEvent::message(key.clone(), Uuid::new_v4().to_string(), message)` (`src/dashboard.rs:1002`).
  3. It calls `mux.route(event).await` (`src/dashboard.rs:1003`), forwarding the event to the dedicated `SessionActor` worker mailbox channel.
  4. The `SessionActor` invokes `LiveAgentRunner::execute` (`src/main.rs:770`), preparing context and conversation history via `LiveAgentRunner::messages` (`src/main.rs:641-715`) and streaming completions via `LlmClient::chat_stream`.

### 1.3 Message Persistence & Session Keys
- **Session Key Resolution**:
  - `resolve_session_key` (`src/dashboard.rs:1154-1167`) checks if `id` is an exact existing `session_key` or attempts parsing via `parse_storage_key` (`src/dashboard.rs:1170-1215`).
  - If new/unmatched, it falls back to `web_session_key(id)` (`src/dashboard.rs:1131-1138`):
    - Platform: `"web"`
    - Guild ID: `None`
    - Channel ID: `id` (path parameter)
    - Thread ID: `None`
    - User ID: `"dashboard"`
    - Storage key format: `3:web|-|<len>:<id>|-|9:dashboard`
- **Inbound Message Persistence**: `SessionActor::persist_inbound` (`src/multiplexer/actor.rs:376-395`) executes an `INSERT INTO messages (id, session_key, role, content, metadata_json, created_at, platform_message_id) VALUES (?, ?, 'user', ?, ?, ?, ?)` on the database pool before agent processing begins.
- **Outbound Message Persistence**: `LiveAgentRunner::persist_message` (`src/main.rs:748-767`) inserts `assistant` and `tool` output messages under the same `session.key.storage_key()`.

### 1.4 Blocking vs Streaming Characteristics
- **HTTP Handler (`post_chat`)**: Purely **non-blocking and asynchronous**. It accepts the request, verifies input, enqueues the event into the multiplexer channel, and returns `202 Accepted` immediately.
- **Agent Actor Processing**: Runs concurrently within a spawned Tokio task (`SessionActor::run` at `src/multiplexer/actor.rs:141`).
- **Streaming Output Delivery**: Emitted in real-time over WebSocket via `session_ws` (`src/dashboard.rs:1014-1025`) and `handle_session_socket` (`src/dashboard.rs:1026-1120`). Intermediate token deltas, tool invocation notifications, and final outputs are broadcasted through `WebDashboardDispatcher` events (`src/dashboard.rs:172-270`).

---

## 2. Dashboard Runtime Role & Architecture (`src/dashboard_runtime.rs`)

### 2.1 Boot Sequence & Subsystems Initialized
`dashboard_runtime::run_standalone` (`src/dashboard_runtime.rs:37-195`) runs the standalone dashboard gateway without Discord gateway dependencies:
1. **Storage Pool**: Initializes SQLite connection pool via `omon_gateway::storage::init_pool(&database_url)` (`src/dashboard_runtime.rs:49`).
2. **Memory & Approvals**: Instantiates `MemoryStore` (`:50`), `SmartApprovalGuard` (`:52`), loads allowlist (`:53`), and binds `DiscordApprovalRequester` (`:59-62`).
3. **Dispatcher**: Instantiates `WebDashboardDispatcher` (`src/dashboard_runtime.rs:57-58`) as the central `Arc<dyn OutboundDispatcher>`.
4. **Tool Registry**: Registers `TerminalTool`, `FileTool`, `McpTool`, `CronTool`, `WebSearchTool`, `WebFetchTool`, `BrowserTool`, and `SkillsTool` (`src/dashboard_runtime.rs:77-118`).
5. **LLM & Multiplexer**: If `DEFAULT_MODEL` is set, constructs `LlmClient`, `LiveAgentRunner`, and `SessionMultiplexer` (`src/dashboard_runtime.rs:121-150`).
6. **Background Tasks**: Starts `ScaleToZero` monitor (`:146`) and `CronScheduler` (`:154-169`).
7. **HTTP Server**: Spawns Axum web server and dashboard API router (`src/dashboard_runtime.rs:188-191`).

### 2.2 LlmClient Lifecycle
- Initialized once on startup in `run_standalone` (`src/dashboard_runtime.rs:122`) via `LlmClient::new(llm_config_from_environment(model))`.
- Held inside `LiveAgentRunner` (`src/dashboard_runtime.rs:123-140`) wrapped in an `Arc`.
- Cloned across spawned `SessionActor` instances on demand.
- Lives for the entire duration of the dashboard process until `shutdown.cancelled()` triggers graceful teardown (`src/dashboard_runtime.rs:190-194`).

### 2.3 Difference from the Discord Actor Path
| Aspect | Standalone Dashboard Path (`src/dashboard_runtime.rs`) | Discord Gateway Path (`src/main.rs`) |
| :--- | :--- | :--- |
| **Ingress Triggers** | HTTP REST (`post_chat`) & WebSocket (`session_ws`) | Discord Gateway Shards (`serenity::Client`, `poise::Framework`) |
| **Dispatcher** | `WebDashboardDispatcher` (`src/dashboard.rs:172`) emitting Axum WS events | `DiscordDispatcher` (`src/discord/dispatcher.rs`) sending REST messages / reactions to Discord API |
| **Session Key Platform** | Platform `"web"`, User `"dashboard"` | Platform `"discord"`, Guild/Channel/Thread/User IDs |
| **Shared Core** | Identical `SessionMultiplexer`, `SessionActor`, `LiveAgentRunner`, `ToolRegistry`, SQLite Schema |

---

## 3. Configuration & Environment Variable Conventions

### 3.1 `OMON_*` Environment Variable Parsing Conventions
The codebase employs explicit strongly-typed helper routines for loading environment variables:
- **`src/models/messenger_policy.rs:59-106` (`MessageContextPolicyMatrix::from_environment`)**:
  - Uses typed helpers `env_bool(key, default)` and `env_usize(key, default)` (`src/models/messenger_policy.rs:109-126`).
  - Examples: `OMON_MESSAGE_CONTEXT_ALLOW_CURRENT`, `OMON_MESSAGE_CONTEXT_MAX_RECENT`.
- **`src/main.rs:430-580` (`Settings::from_env`)**:
  - Uses `optional_env`, `required_env`, `parse_bool_from`, and `parse_u64_list`.
  - Reads `OMON_WORKSPACE_ROOT` (`src/main.rs:480`), `OMON_TOOL_ROOTS` (`src/main.rs:490`), `OMON_CRON_SCRIPT_TIMEOUT_SECS` (`src/main.rs:548`), and `OMON_APPROVALS_DENY` (`src/main.rs:555`).
- **`src/dashboard.rs:56-105` (`DashboardSettings::from_env`)**:
  - Provides self-contained fallback defaults (`DEFAULT_DASHBOARD_PORT = 3000`, `DEFAULT_DASHBOARD_HOST = "127.0.0.1"`).

### 3.2 `DATABASE_URL` Handling
- `src/main.rs:512-513`:
  ```rust
  database_url: env::var("DATABASE_URL")
      .unwrap_or_else(|_| "sqlite://omon_gateway.db".to_owned()),
  ```
- `src/dashboard_runtime.rs:43-44`:
  ```rust
  let database_url =
      env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://omon_gateway.db".into());
  ```
- Pool Initialization: `omon_gateway::storage::init_pool(&database_url).await?` executes `sqlx::migrate!()` automatically against SQLite (supporting both file paths and `sqlite::memory:`).

### 3.3 Fitting `OMON_AGENT_BACKEND` & `OMON_OMO_APPSERVER_URL`
To introduce `OMON_AGENT_BACKEND` and `OMON_OMO_APPSERVER_URL` conforming to existing conventions:
1. **Type Definition**: Define an enum `AgentBackendMode` (`HermesLive`, `OmoAppServer`) in `src/models/mod.rs` with `parse(Option<&str>) -> Self` and `Default`.
2. **Settings Structs**: Add `pub agent_backend: AgentBackendMode` and `pub omo_appserver_url: Option<String>` to `Settings` (`src/main.rs:410-435`) and `DashboardSettings` (`src/dashboard.rs:56-70`).
3. **Environment Parsing**:
   - In `Settings::from_env` (`src/main.rs:510+`):
     ```rust
     agent_backend: AgentBackendMode::parse(optional_env("OMON_AGENT_BACKEND").as_deref()),
     omo_appserver_url: optional_env("OMON_OMO_APPSERVER_URL"),
     ```
   - In `dashboard_config_view` (`src/dashboard_runtime.rs:290-330`): expose them in the JSON payload under `"backend"` for inspection in the dashboard UI.

---

## 4. Cargo.toml Dependencies for WebSocket & NDJSON Transport

Key transport dependencies defined in `/Users/indo/code/project/omon-gateway/Cargo.toml`:

| Dependency | Exact Version / Features Spec in `Cargo.toml` | Line Ref | Role / Capability |
| :--- | :--- | :--- | :--- |
| `async-trait` | `async-trait = "0.1"` | `Cargo.toml:34` | Trait abstractions for async agent runners and dispatchers |
| `reqwest` | `reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls", "stream"] }` | `Cargo.toml:35` | HTTP client with byte streaming (`stream` feature enabled for NDJSON consumption) |
| `songbird` | `songbird = { version = "0.5", default-features = false, features = ["driver", "gateway", "serenity", "rustls", "receive", "tungstenite"] }` | `Cargo.toml:38` | Discord voice; transitively includes `tungstenite` |
| `axum` | `axum = { version = "0.8", features = ["json", "ws"] }` | `Cargo.toml:21` | Dashboard HTTP & WebSocket server implementation (`ws` feature enabled) |
| `tokio` | `tokio = { version = "1.43", features = ["full", "test-util"] }` | `Cargo.toml:19` | Async runtime, timers, I/O primitives, testing utilities |
| `tokio-util` | `tokio-util = "0.7"` | `Cargo.toml:20` | Codecs (`LinesCodec` for NDJSON), CancellationToken |
| `futures-util` | `futures-util = "0.3"` | `Cargo.toml:36` | `StreamExt` and `SinkExt` for async stream transformation |
| `serde_json` | `serde_json = "1.0"` | `Cargo.toml:29` | Parsing JSON lines / payloads |

---

## 5. Test Conventions & In-Memory SQLite Patterns

### 5.1 Representative Actor Unit Tests (`src/multiplexer/actor.rs`)
- **Turn FIFO Queueing (`src/multiplexer/actor.rs:561-622`)**:
  - `#[tokio::test] async fn actor_queues_turns_in_order_without_cancellation()`
  - Uses `TurnRecordingRunner` with `tokio::sync::Barrier` and `mpsc::unbounded_channel` to verify that concurrent inbound messages are queued and executed strictly in order without aborting active turns.
- **Immediate Stop Cancellation (`src/multiplexer/actor.rs:624-671`)**:
  - `#[tokio::test] async fn actor_stop_cancels_active_turn_immediately()`
  - Sends `ActorCommand::Stop { reply }` to an actor executing a blocking turn and asserts the turn task is cancelled and aborts promptly.
- **Profile Routing & Route Preservation (`src/multiplexer/actor.rs:673-745`)**:
  - `#[tokio::test] async fn actor_load_applies_profile_routes_to_fresh_and_preserves_explicit_model()`
  - Verifies that routing overrides configure fresh sessions correctly while preserving custom explicit overrides.

### 5.2 Dashboard Mock HTTP Server Pattern (`src/main.rs:2738`)
- **`spawn_two_turn_tool_llm_server` (`src/main.rs:2738-2770`)**:
  - Binds to dynamic loopback port `tokio::net::TcpListener::bind("127.0.0.1:0").await`.
  - Directly handles TCP sockets and emits SSE mock streams (`text/event-stream` with delta chunks and `[DONE]`).
  - Provides deterministic, fast OpenAI-compatible HTTP endpoints for live runner tests without external network calls.

### 5.3 `sqlite::memory:` Database Isolation Patterns
- **Actor Tests**: `let db = Database::connect("sqlite::memory:").await.unwrap();` (`src/multiplexer/actor.rs:562, 625, 676`).
- **Runner Tests**: `let pool = omon_gateway::storage::init_pool("sqlite::memory:").await.unwrap();` (`src/main.rs:2774-2776`, `src/dashboard.rs:1880`).
- **Pattern Summary**: Every test creates a completely fresh in-memory SQLite connection with migrations executed automatically, guaranteeing zero state leakage across test cases and avoiding temporary database file cleanup.
