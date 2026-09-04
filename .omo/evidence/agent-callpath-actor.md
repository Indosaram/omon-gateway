# Evidence Brief: Discord-Turn Agent Call Path & Actor Architecture

This document maps the complete Discord-turn agent call path in `omon-gateway`, tracing how an inbound Discord event flows through the multiplexer actor, triggers LLM streaming with tool execution, delivers throttled output back to Discord, persists session state, and injects bot profile routing configuration.

---

## 1. Inbound Discord Event -> Multiplexer Actor -> LLM Call Chain

### Call Chain Overview
1. **Discord Gateway Ingress**: Discord messages arrive via Serenity event handler in `src/discord/adapter.rs` (`FullEvent::Message`, lines 808–940).
2. **Inbound Coalescing & Normalization**: The message is converted to `InboundEvent` via `message_to_inbound_with_config` (`src/discord/adapter.rs:1111–1145`). Rapid split messages are debounced and coalesced in `SplitMessageDebouncer::enqueue` (`src/discord/adapter.rs:566–614`).
3. **Delivery Ledger & Multiplexer Routing**: `route_claimed_event` (`src/discord/adapter.rs:1060–1097`) claims the delivery in `DeliveryLedgerService` and dispatches `event` to `SessionMultiplexer::route` (`src/multiplexer/router.rs:230–244`).
4. **Session Handle & Actor Mailbox**: `SessionMultiplexer::handle_for` (`src/multiplexer/router.rs:318–334`) retrieves or spawns the dedicated actor task via `spawn_actor` (`src/multiplexer/router.rs:336–368`). The event is queued into an `mpsc::channel(64)` bounded channel as `ActorCommand::Event(Box::new(event))` (`src/multiplexer/router.rs:87–90`).
5. **Actor Run Loop**: `SessionActor::run` in `src/multiplexer/actor.rs:91–300` processes the mailbox.
   - Dedup check: queries SQLite transcript dedup index via `crate::storage::has_platform_message_id` (`src/multiplexer/actor.rs:103–128`).
   - Persists inbound user message via `SessionActor::persist_inbound` (`src/multiplexer/actor.rs:130–133, 373–393`).
   - Executes turn via `runner.run_cancelable(&mut turn_context, *event, cancellation)` (`src/multiplexer/actor.rs:155–159`).
6. **Agent Runner Execution**: `LiveAgentRunner::run` (`src/main.rs:1203–1215`) invokes `LiveAgentRunner::execute` (`src/main.rs:776–1054`).
7. **LLM Invocation**: Inside `LiveAgentRunner::execute`, the runner calls `llm.stream_with_tool_calls(&messages, &definitions)` (`src/main.rs:846–847`, `src/agent/llm.rs:188–289`).

### LlmClient Creation & Injection
- **Default LlmClient Initialization** (`src/main.rs:2285–2297`):
  ```rust
  let llm = LlmClient::new(config.llm_config(config.default_model.clone()))?;
  let runner = Arc::new(LiveAgentRunner {
      pool: pool.clone(),
      memory,
      tools: tools.clone(),
      llm: llm.clone(),
      dispatcher: shared_dispatcher.clone(),
      workspace_root: config.workspace_root.clone(),
      streams: ParkingMutex::new(HashMap::new()),
      processing_reactions: config.processing_reactions,
      runtime_footer: config.runtime_footer,
  });
  ```
- **Gateway Config Provider Mapping** (`src/main.rs:582–598`):
  `config.llm_config(model)` detects model prefix (e.g. `claude` -> `LlmProvider::Anthropic`, else `LlmProvider::OpenAi`) and assigns API keys/base URLs.
- **Dynamic Per-Turn Model Override** (`src/main.rs:834–843`):
  Inside `LiveAgentRunner::execute`, if `session.state.active_model` differs from `self.llm.config().model`, a per-turn `LlmClient` instance is constructed:
  ```rust
  let llm = if let Some(custom) = execution_llm {
      custom.clone()
  } else {
      match session.state.active_model.as_deref() {
          Some(model) if model != self.llm.config().model => {
              let mut config = self.llm.config().clone();
              config.model = model.to_owned();
              LlmClient::new(config)?
          }
          _ => self.llm.clone(),
      }
  };
  ```

### Exact Method Signatures Invoked Per Turn
- **`AgentRunner::run`** (`src/multiplexer/actor.rs:23`):
  `async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()>`
- **`AgentRunner::run_cancelable`** (`src/multiplexer/actor.rs:25–34`):
  `async fn run_cancelable(&self, session: &mut SessionContext, event: InboundEvent, cancellation: CancellationToken) -> Result<()>`
- **`LiveAgentRunner::execute`** (`src/main.rs:776–784`):
  `async fn execute(&self, session: &mut SessionContext, event: InboundEvent, enabled_tools: Option<&[String]>, execution_tools: Option<&ToolRegistry>, stream_output: bool, execution_llm: Option<&LlmClient>) -> Result<String>`
- **`LlmClient::stream_with_tool_calls`** (`src/agent/llm.rs:188–198`):
  `pub async fn stream_with_tool_calls(&self, messages: &[ChatMessage], tools: &[ToolDefinition]) -> Result<(LlmStream, oneshot::Receiver<Result<Vec<ToolCall>, OmonError>>), OmonError>`
- **`ToolRegistry::execute_with_context`** (`src/tools/mod.rs:136–143`):
  `pub async fn execute_with_context(&self, name: &str, input: Value, session: Option<&SessionKey>) -> Result<Value>`

---

## 2. The Tool-Call Loop

### Loop Structure & Round Limits
- **Owning Module**: Tool execution loop logic is owned by `LiveAgentRunner::execute` in `src/main.rs:845–1040`, while tool stream parsing is owned by `src/agent/llm.rs:515–615` and tool dispatch is in `src/tools/mod.rs`.
- **Round Limit Behavior**: The tool loop in `LiveAgentRunner::execute` (`src/main.rs:845`) is an **unbounded `loop { ... }`** without a hardcoded turn limit. It terminates when either:
  1. `calls.is_empty()`: the LLM generates a text response without tool calls (`src/main.rs:878–984`), returning `Ok(response)`.
  2. An error occurs during LLM streaming, JSON parsing, tool dispatch, or message persistence (`?` operator returns `Err(...)`).

### Parsing, Execution, and Re-Feeding Flow
1. **Streaming & Tool Call Accumulation** (`src/main.rs:846–877`, `src/agent/llm.rs:207–275`):
   - `llm.stream_with_tool_calls(&messages, &definitions)` spawns a background Tokio task reading the SSE response stream.
   - For Anthropic, OpenAI, DeepSeek, and Ollama, `accumulate_stream_tool_calls` (`src/agent/llm.rs:515–589`) buffers partial JSON chunks and parameter deltas into `BTreeMap<usize, PendingToolCall>`.
   - Text chunks are parsed via `parse_stream_line` (`src/agent/llm.rs:617–640`) and sent across `LlmStream`. `ThinkStripper` (`src/main.rs:850–874`) strips `<think>...</think>` tags before emitting chunks to Discord.
2. **Completing Tool Calls** (`src/agent/llm.rs:591–615`, `src/main.rs:875–877`):
   - At stream end, `finish_stream_tool_calls` deserializes accumulated arguments into `serde_json::Value` and sends `Result<Vec<ToolCall>, OmonError>` across the oneshot channel.
   - `LiveAgentRunner::execute` receives `calls = tool_calls.await??`.
3. **Loop Termination Check** (`src/main.rs:878–984`):
   - If `calls.is_empty()`, the runner processes media directives (`extract_media_directives`), checks for silence sentinels (`is_silence_response`), appends optional runtime footer context (`append_runtime_footer`), emits final Discord content via `self.emit_final(session, final_text).await?`, records the assistant message in the database via `self.persist_message(session, "assistant", &response, ...)`, and exits the loop.
4. **Tool Execution & Status Notification** (`src/main.rs:985–1038`):
   - If `calls` is non-empty, pending text is flushed to Discord (`src/main.rs:985–996`).
   - The assistant's tool-call message is appended to `messages` and persisted to SQLite messages table (`src/main.rs:997–1004`).
   - For each `ToolCall` in `calls`:
     - Emits typing/status update to Discord: `self.emit_tool_status(session, &call.name)` emitting `"⚙️ Running tool <name>..."` (`src/main.rs:1006–1008, 1056–1068`).
     - Dispatches execution via `tools.execute_with_context(&call.name, call.arguments.clone(), tool_session).await` (`src/main.rs:1010–1013`).
     - Formats result content with truncation: `truncate_large_content(&s, MAX_TOOL_CONTENT_CHARS)` (`src/main.rs:1017`).
     - Constructs result `ChatMessage`: role is `"user"` for `LlmProvider::Anthropic` and `"tool"` for OpenAI/others, with `message.tool_call_id = Some(call.id)` (`src/main.rs:1021–1028`).
     - Appends result to `messages` and persists to SQLite (`src/main.rs:1029–1037`).
5. **Re-Feeding**: The updated `messages` vector containing the conversation history, assistant tool request, and tool output messages is passed back to `llm.stream_with_tool_calls` on the next loop iteration (`src/main.rs:845–847`).

---

## 3. Streaming & Discord Delivery Throttling

### `StreamChunk` Struct
Defined in `src/models/events.rs:167–173`:
```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamChunk {
    pub stream_id: Uuid,
    pub sequence: u64,
    pub content: String,
    pub is_final: bool,
}
```

### Actor Output Delivery Path
1. **Delta Batching in Agent Runner** (`src/main.rs:626–630, 1070–1188`):
   - `LiveAgentRunner` maintains `streams: ParkingMutex<HashMap<String, StreamEmissionState>>` keyed by `session.key.storage_key()`.
   - `StreamEmissionState` tracks `stream_id: Uuid`, `next_sequence: u64`, and cumulative `content: String`.
   - Chunks are batched until `pending.chars().count() >= STREAM_BATCH_CHARS` (32 chars) (`src/main.rs:863`).
   - `LiveAgentRunner::emit` / `emit_final` constructs `StreamChunk` and sends `OutboundAction::Stream { session, chunk }` to `SharedDispatcher` (`src/main.rs:1070–1188`).
   - For final chunks, delivery obligations are registered in `DeliveryLedgerService` (`src/main.rs:1104–1108, 1148–1153`).
2. **Dispatcher Routing to Discord Egress** (`src/discord/adapter.rs:1696–1741, 2015–2017`):
   - `DiscordEgress::dispatch(OutboundAction::Stream)` invokes `DiscordEgress::stream(session, chunk)`.
   - Manages active streams in `streams: Arc<Mutex<HashMap<(String, Uuid), Arc<ActiveDiscordStream>>>>` keyed by `(bot_identity, stream_id)`.
   - On sequence 0 (stream initialization): sends an initial placeholder message `"\u{200b}"` to Discord (`src/discord/adapter.rs:1708–1715`), receives `MessageId`, and instantiates `LiveEditThrottler`.
   - Out-of-order or duplicate sequence chunks are dropped via `last_sequence: Mutex<Option<u64>>` (`src/discord/adapter.rs:1720–1724`).
   - Forwards chunk content and finality flag to `active.throttler.update(&chunk.content, chunk.is_final)` (`src/discord/adapter.rs:1725–1728`).
   - On `chunk.is_final == true`, the stream entry is pruned from `self.streams` (`src/discord/adapter.rs:1731–1739`).

### Throttler & Debouncing Mechanisms
- **Egress Edit Throttler** (`src/discord/throttler.rs:12–160`):
  - `LiveEditThrottler<T>` enforces `DEFAULT_DEBOUNCE = Duration::from_millis(800)` (`src/discord/throttler.rs:13`).
  - Uses atomic revision tracking (`revision: AtomicU64`, line 81) and `wait_for_debounce` (lines 162–177). Non-final intermediate edits coalesce: if newer tokens arrive during the 800ms debounce window, sleeping stale updates exit immediately without Discord HTTP calls (`src/discord/throttler.rs:83–85, 100–104`).
  - Intermediate edits (`!is_final`): preview is truncated to `DISCORD_MESSAGE_LIMIT` (2000 chars) via `truncate_live_preview` (`src/discord/throttler.rs:118–124, 197–208`) and updates the initial placeholder message (`edit_message`).
  - Final edits (`is_final`): content is split using `chunk_markdown` / `chunk_markdown_paginated` (`src/discord/throttler.rs:126–145, 219–300`) with code-fence preservation and pagination headers `(i/N)`. Existing Discord messages are edited, extra chunks are sent via `send_message`, and stale trailing messages are deleted via `delete_message`.
- **Inbound Message Debouncer** (`src/discord/adapter.rs:490–624`):
  - Inbound user messages are batched by `SplitMessageDebouncer` (`DEFAULT_DEBOUNCE_DURATION = 600ms`) using `coalesce_inbound_events` (`src/discord/adapter.rs:504–546`) to combine rapidly typed multi-message Discord splits before routing into the multiplexer actor.

---

## 4. Session Lifecycle & Storage Layout

### Session Models & State Fields
Defined in `src/models/session.rs:8–78`:
- **`SessionContext`**:
  ```rust
  pub struct SessionContext {
      pub key: SessionKey,
      pub state: SessionState,
      pub created_at: DateTime<Utc>,
      pub updated_at: DateTime<Utc>,
  }
  ```
- **`SessionState`** (`state_json` in DB):
  ```rust
  pub struct SessionState {
      pub active_model: Option<String>,
      pub system_prompt: Option<String>,
      pub enabled_toolsets: Option<Vec<String>>,
      pub yolo: bool,
      pub suspended: bool,
      pub metadata: HashMap<String, serde_json::Value>,
  }
  ```

### Storage Operations
1. **Load Context** (`src/multiplexer/actor.rs:407–495`):
   - `SELECT state_json, created_at, updated_at FROM sessions WHERE session_key = ?` bound by `key.storage_key()`.
   - If row exists: deserializes `state_json` into `SessionState`. Checks `bot_profiles` table (`SELECT model, system_prompt, enabled_toolsets FROM bot_profiles WHERE bot_id = ?`) for overrides if `key.bot_id` is present, then applies `profile_router.match_session(&key)`.
   - If row is missing: initializes `SessionContext::new(key)`, checks `bot_profiles` table, and runs `profile_router.apply_to_session(&mut context)`.
2. **Save / Flush Context** (`src/multiplexer/actor.rs:373–405, 497–513`):
   - `ensure_session`: `INSERT INTO sessions (session_key, platform, guild_id, channel_id, thread_id, user_id, state_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_key) DO NOTHING`.
   - `flush`: `UPDATE sessions SET state_json = ?, updated_at = ? WHERE session_key = ?`.
   - Triggered on actor idle eviction (`EvictIfIdle`), user stop (`ActorCommand::Stop`), channel close / shutdown, or turn completion when marked dirty (`self.dirty = true`).

### Exact `SessionKey::storage_key()` String Layout
Implemented in `src/models/session.rs:33–47`:
- **Format**: Each field is serialized as length-prefixed `"{length}:{value}"` if `Some`, or `"-"` if `None`. Components are joined by the pipe delimiter `"|"`.
- **Layout**:
  ```text
  {len}:{platform}|{len}:{guild_id}|{len}:{channel_id}|{len}:{thread_id}|{len}:{user_id}[|{len}:{bot_id}]
  ```
  *(Note: `{len}:{bot_id}` is appended only if `bot_id.is_some()` for backward compatibility).*
- **Examples**:
  - Without bot_id: `7:discord|19:123456789012345678|19:987654321098765432|-|18:112233445566778899`
  - With bot_id: `7:discord|19:123456789012345678|19:987654321098765432|19:555555555555555555|18:112233445566778899|19:999999999999999999`

### Deterministic Per-Session Agent ID Derivation
Because `session.key.storage_key()` is strictly canonical, unambiguous, and collision-resistant across platforms, guilds, channels, threads, users, and bot identities, a deterministic per-session agent ID can be derived directly by hashing or namespacing `session.key.storage_key()`:
- **UUID v5**: `uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, session.key.storage_key().as_bytes())`
- **Hash ID**: `sha256(session.key.storage_key())`

---

## 5. Bot Profile Routing & System Prompt / Model Injection

### Profile Routing Structs & Specificity
Defined in `src/multiplexer/profile_routing.rs:34–66, 173–176`:
- **`ProfileRoute`**:
  ```rust
  pub struct ProfileRoute {
      pub name: Option<String>,
      pub guild: Option<u64>,          // serde alias: guild_id
      pub channel: Option<u64>,        // serde alias: channel_id, chat_id
      pub thread: Option<u64>,         // serde alias: thread_id
      pub enabled: bool,
      pub model: Option<String>,
      pub system_prompt: Option<String>,
      pub enabled_toolsets: Option<Vec<String>>, // serde alias: toolsets
  }
  ```
- **Hierarchical Precedence (`specificity`)** (`src/multiplexer/profile_routing.rs:50–66`):
  - `thread` match: `+8` (highest)
  - `channel` match: `+4` (middle)
  - `guild` match: `+2` (lowest)
  - Routes in `ProfileRouter` are pre-sorted descending by specificity (`src/multiplexer/profile_routing.rs:178–184`), ensuring the most specific Discord context rule matches first.

### Plumbing from Profile Router to Actor
1. **Gateway Initialization** (`src/main.rs:2298–2305`):
   - `ProfileRouter::new(config.profile_routes.clone())` parses and sorts JSON-configured routes.
   - Passed into `SessionMultiplexer::with_profile_router(..., profile_router.clone())`.
2. **Actor Spawning & Context Loading** (`src/multiplexer/router.rs:343–356`, `src/multiplexer/actor.rs:70–89, 407–495`):
   - `SessionMultiplexer::spawn_actor` passes `Some(profile_router)` into `SessionActor::load`.
   - `load_context` resolves profile overrides in two tiers:
     - **Database Tier**: `bot_profiles` table lookup by `key.bot_id` (`src/multiplexer/actor.rs:420–436, 467–482`).
     - **Routing Tier**: `profile_router.match_session(&key)` / `apply_to_session(&mut context)` (`src/multiplexer/actor.rs:438–454, 484–487`, `src/multiplexer/profile_routing.rs:89–106, 218–228`).
     - Unset fields in `SessionState` (`active_model`, `system_prompt`, `enabled_toolsets`) are populated from the matching route.

### Injection into the Agent Turn & LLM Request
1. **System Prompt Injection** (`src/main.rs:680–710`):
   - Inside `LiveAgentRunner::messages`, the runner checks `session.state.system_prompt.as_deref()`.
   - If present, it replaces the default OMON agent preamble with the custom profile prompt string.
   - Pushed into `messages` as `ChatMessage::new("system", system_prompt)`.
2. **Model Selection & Client Dynamic Dispatch** (`src/main.rs:834–843`):
   - Inside `LiveAgentRunner::execute`, if `session.state.active_model` is present and differs from `self.llm.config().model`, `LiveAgentRunner` dynamically clones the configuration and creates a matching `LlmClient::new(config)?`.
3. **Tool Filtering** (`src/main.rs:830, 1204–1210`):
   - `session.state.enabled_toolsets` filters registered tools via `LiveAgentRunner::tool_definitions(tools, tool_filter)`.
4. **Provider Payload Formatting** (`src/agent/llm.rs:291–354`):
   - Anthropic (`anthropic_payload`, lines 329–354): extracts system messages into the top-level `payload["system"]` string and sets `payload["model"]`.
   - OpenAI / DeepSeek / Ollama (`openai_payload`, lines 291–327): includes system messages within the `payload["messages"]` array and sets `payload["model"]`.

---

## 6. Comprehensive File & Line Reference Matrix

| Component / Path | File | Key Lines |
|---|---|---|
| Inbound Discord message handler | `src/discord/adapter.rs` | 808–940 |
| Message to InboundEvent conversion | `src/discord/adapter.rs` | 1111–1145 |
| Split message inbound debouncer | `src/discord/adapter.rs` | 490–624 |
| Delivery ledger claim & router handoff | `src/discord/adapter.rs` | 1060–1097 |
| OutboundDispatcher for DiscordEgress | `src/discord/adapter.rs` | 1745–2025 |
| Egress stream initialization & delivery | `src/discord/adapter.rs` | 1696–1741 |
| LiveEditThrottler debounce & chunk splitting | `src/discord/throttler.rs` | 12–160, 219–300 |
| SessionMultiplexer route & handle lifecycle | `src/multiplexer/router.rs` | 230–244, 318–368 |
| SessionActor run loop & cancellation select | `src/multiplexer/actor.rs` | 91–300 |
| SessionContext load & DB profile override | `src/multiplexer/actor.rs` | 407–495 |
| SessionContext flush & persistence | `src/multiplexer/actor.rs` | 373–405, 497–513 |
| ProfileRoute & specificity matcher | `src/multiplexer/profile_routing.rs` | 34–106, 173–236 |
| LiveAgentRunner run implementation | `src/main.rs` | 1203–1249 |
| LiveAgentRunner tool execution loop | `src/main.rs` | 776–1054 |
| StreamEmissionState & chunk dispatch | `src/main.rs` | 626–630, 1070–1188 |
| Default LlmClient & runner initialization | `src/main.rs` | 582–598, 2285–2305 |
| LlmClient streaming & tool stream parser | `src/agent/llm.rs` | 188–289, 515–615 |
| SessionKey, SessionState, SessionContext | `src/models/session.rs` | 8–78 |
| StreamChunk & OutboundAction models | `src/models/events.rs` | 89–100, 167–215 |
