// allow: SIZE_OK — OMO WebSocket protocol state machine
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex as ParkingMutex;
use serde_json::{json, Value};
use sqlx::SqlitePool;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

use super::agent_workspace::{agent_workspace_slug, resolve_workspace};
use super::backend::AgentBackend;

use super::omo_config::OmoBackendConfig;
use super::omo_protocol::{
    approval_denial_response, initialize_request, is_approval_request, thread_resume_request,
    thread_start_request, turn_start_request,
};
use crate::models::{
    render_user_prompt, InboundEvent, OutboundAction, SessionContext, StreamChunk,
};
use crate::{OmonError, OutboundDispatcher, Result};

type WsStream = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Approval denials tolerated per turn before the gateway aborts it: a
/// policy-denial loop otherwise burns the entire turn deadline flailing.
pub const APPROVAL_DENIAL_TURN_LIMIT: u32 = 5;

pub struct OmoBackend {
    pub config: OmoBackendConfig,
    pub pool: Option<SqlitePool>,
    pub dispatcher: Arc<dyn OutboundDispatcher>,
    pub thread_ids: Arc<ParkingMutex<HashMap<String, String>>>,
}

impl OmoBackend {
    pub fn new(config: OmoBackendConfig, dispatcher: Arc<dyn OutboundDispatcher>) -> Self {
        Self {
            config,
            pool: None,
            dispatcher,
            thread_ids: Arc::new(ParkingMutex::new(HashMap::new())),
        }
    }

    pub fn with_pool(mut self, pool: SqlitePool) -> Self {
        self.pool = Some(pool);
        self
    }

    async fn connect_ws(&self) -> Result<WsStream> {
        let mut request = self
            .config
            .appserver_url
            .as_str()
            .into_client_request()
            .map_err(|e| OmonError::Config(format!("invalid appserver url: {e}")))?;

        if let Some(token) = &self.config.auth_token {
            let header_val = format!("Bearer {token}")
                .parse()
                .map_err(|e| OmonError::Config(format!("invalid auth token header: {e}")))?;
            request.headers_mut().insert("Authorization", header_val);
        }

        let mut last_err = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut attempt = 0;
        while tokio::time::Instant::now() < deadline {
            attempt += 1;
            if attempt > 1 {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let connect_fut = tokio_tungstenite::connect_async(request.clone());
            match tokio::time::timeout(self.config.connect_timeout, connect_fut).await {
                Ok(Ok((ws, _))) => return Ok(ws),
                Ok(Err(e)) => {
                    last_err = Some(OmonError::Llm(format!(
                        "failed to connect to omo app-server at {}: {e}",
                        self.config.appserver_url
                    )));
                }
                Err(_) => {
                    last_err = Some(OmonError::Llm(format!(
                        "timeout connecting to omo app-server at {}",
                        self.config.appserver_url
                    )));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            OmonError::Llm(format!(
                "failed to connect to omo app-server at {}",
                self.config.appserver_url
            ))
        }))
    }

    async fn do_initialize(&self, ws: &mut WsStream) -> Result<()> {
        ws.send(initialize_request())
            .await
            .map_err(|e| OmonError::Llm(format!("failed to send initialize: {e}")))?;

        while let Some(msg) = tokio::time::timeout(self.config.request_timeout, ws.next())
            .await
            .map_err(|_| OmonError::Llm("timeout waiting for initialize response".into()))?
        {
            let msg =
                msg.map_err(|e| OmonError::Llm(format!("ws error during initialize: {e}")))?;
            if let Message::Text(text) = msg {
                let val: Value = serde_json::from_str(text.as_str())
                    .map_err(|e| OmonError::Llm(format!("invalid json: {e}")))?;
                if val.get("id").and_then(Value::as_u64) == Some(1) {
                    if let Some(err) = val.get("error") {
                        return Err(OmonError::Llm(format!("initialize error: {err}")));
                    }
                    return Ok(());
                }
            }
        }
        Err(OmonError::Llm("closed before initialize response".into()))
    }

    /// Resolves the remote OMO app-server thread ID for the session.
    ///
    /// # Persona & Model Flow across Session Lifecycle:
    ///
    /// 1. **Initial Thread Creation (`thread/start`)**:
    ///    When a session does not yet have an assigned `omo_thread_id` (in session metadata or
    ///    the in-memory cache), `thread/start` is dispatched with:
    ///    - `developerInstructions`: Initial `session.state.system_prompt` resolved from the bot
    ///      profile route or database profile override.
    ///    - `model`: `session.state.active_model` or fallback `config.default_model`.
    ///
    ///    The returned `thread.id` is saved into `session.state.metadata["omo_thread_id"]`
    ///    and cached in `self.thread_ids`.
    ///
    /// 2. **Mid-Session Profile or Model Changes**:
    ///    - **Model Changes**: The active model (`session.state.active_model`) is forwarded per-turn
    ///      in `turn/start(model: ...)`. Any runtime `/model` switch or updated route model takes
    ///      effect immediately on the next turn on the existing remote thread.
    ///    - **System Prompt / Persona Changes**: Per the OMO app-server protocol, `developerInstructions`
    ///      are bound immutably to the remote thread at `thread/start`. If a profile's system prompt
    ///      changes mid-session, the existing thread retains its initial developer instructions.
    ///      To re-bind updated system instructions, a new session thread must be started (e.g. by
    ///      clearing `omo_thread_id` from session metadata or establishing a fresh conversation).
    async fn resolve_thread_id(
        &self,
        ws: &mut WsStream,
        session: &mut SessionContext,
    ) -> Result<String> {
        let storage_key = session.key.storage_key();
        let is_cron = session.key.user_id.starts_with("cron:");
        if is_cron {
            session.state.metadata.remove("omo_thread_id");
            self.thread_ids.lock().remove(&storage_key);
        } else if let Some(id) = session
            .state
            .metadata
            .get("omo_thread_id")
            .and_then(Value::as_str)
            .map(String::from)
            .or_else(|| self.thread_ids.lock().get(&storage_key).cloned())
        {
            let mut start_replacement = false;
            ws.send(thread_resume_request(&id)).await.map_err(|error| {
                OmonError::Llm(format!("failed to send thread/resume: {error}"))
            })?;
            while let Some(message) = tokio::time::timeout(self.config.request_timeout, ws.next())
                .await
                .map_err(|_| OmonError::Llm("timeout waiting for thread/resume response".into()))?
            {
                let message = message.map_err(|error| {
                    OmonError::Llm(format!("ws error in thread/resume: {error}"))
                })?;
                if let Message::Text(text) = message {
                    let response: Value = serde_json::from_str(text.as_str())
                        .map_err(|error| OmonError::Llm(format!("invalid json: {error}")))?;
                    if response.get("id").and_then(Value::as_u64) == Some(2) {
                        if let Some(error) = response.get("error") {
                            let message = error
                                .get("message")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            if message.contains("no rollout found")
                                || message.contains("thread not found")
                            {
                                session.state.metadata.remove("omo_thread_id");
                                self.thread_ids.lock().remove(&storage_key);
                                start_replacement = true;
                                break;
                            }
                            return Err(OmonError::Llm(format!("thread/resume error: {error}")));
                        }
                        session
                            .state
                            .metadata
                            .insert("omo_thread_id".into(), json!(id));
                        self.thread_ids.lock().insert(storage_key, id.clone());
                        return Ok(id);
                    }
                }
            }
            if !start_replacement {
                return Err(OmonError::Llm(
                    "closed before thread/resume response".into(),
                ));
            }
        }

        let model = session
            .state
            .active_model
            .as_deref()
            .or(self.config.default_model.as_deref());

        let workspace = if self.config.per_agent_workspace {
            if let Some(root) = &self.config.workspace_root {
                let slug = agent_workspace_slug(
                    &session.key.platform,
                    &session.key.user_id,
                    session.key.bot_id.as_deref(),
                );
                let ws = resolve_workspace(root, &slug);
                tokio::fs::create_dir_all(&ws.cwd).await.map_err(|err| {
                    OmonError::Config(format!(
                        "failed to provision agent workspace at {}: {err}",
                        ws.cwd.display()
                    ))
                })?;
                let shared_dir = root.join("shared");
                tokio::fs::create_dir_all(&shared_dir)
                    .await
                    .map_err(|err| {
                        OmonError::Config(format!(
                            "failed to provision agent workspace at {}: {err}",
                            shared_dir.display()
                        ))
                    })?;
                let omo_dir = ws.cwd.join(".omo");
                tokio::fs::create_dir_all(&omo_dir).await.map_err(|err| {
                    OmonError::Config(format!(
                        "failed to provision agent workspace at {}: {err}",
                        omo_dir.display()
                    ))
                })?;
                let omo_json_path = omo_dir.join("omo.json");
                if !tokio::fs::try_exists(&omo_json_path).await.unwrap_or(false) {
                    let content = serde_json::to_string_pretty(&json!({
                        "memory": {
                            "agent": slug
                        }
                    }))
                    .map_err(|err| {
                        OmonError::Config(format!(
                            "failed to provision agent workspace at {}: {err}",
                            omo_json_path.display()
                        ))
                    })?;
                    tokio::fs::write(&omo_json_path, content.as_bytes())
                        .await
                        .map_err(|err| {
                            OmonError::Config(format!(
                                "failed to provision agent workspace at {}: {err}",
                                omo_json_path.display()
                            ))
                        })?;
                }
                Some(ws)
            } else {
                None
            }
        } else {
            None
        };

        ws.send(thread_start_request(
            session.state.system_prompt.as_deref(),
            model,
            workspace.as_ref(),
        ))
        .await
        .map_err(|e| OmonError::Llm(format!("failed to send thread/start: {e}")))?;

        while let Some(msg) = tokio::time::timeout(self.config.request_timeout, ws.next())
            .await
            .map_err(|_| OmonError::Llm("timeout waiting for thread/start response".into()))?
        {
            let msg = msg.map_err(|e| OmonError::Llm(format!("ws error in thread/start: {e}")))?;
            if let Message::Text(text) = msg {
                let val: Value = serde_json::from_str(text.as_str())
                    .map_err(|e| OmonError::Llm(format!("invalid json: {e}")))?;
                if val.get("id").and_then(Value::as_u64) == Some(2) {
                    if let Some(err) = val.get("error") {
                        return Err(OmonError::Llm(format!("thread/start error: {err}")));
                    }
                    if let Some(id) = val.pointer("/result/thread/id").and_then(Value::as_str) {
                        let id_str = id.to_string();
                        if !is_cron {
                            session
                                .state
                                .metadata
                                .insert("omo_thread_id".into(), json!(id_str));
                            self.thread_ids.lock().insert(storage_key, id_str.clone());
                        }
                        return Ok(id_str);
                    }
                }
            }
        }
        Err(OmonError::Llm(
            "failed to obtain thread id from thread/start".into(),
        ))
    }

    async fn emit_chunk(
        &self,
        session: &SessionContext,
        stream_id: Uuid,
        sequence: u64,
        content: String,
        is_final: bool,
    ) -> Result<()> {
        let chunk = StreamChunk {
            stream_id,
            sequence,
            content,
            is_final,
        };
        self.dispatcher
            .dispatch(OutboundAction::Stream {
                session: session.key.clone(),
                chunk,
            })
            .await
    }
}

impl OmoBackend {
    async fn run_once(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()> {
        let mut ws = self.connect_ws().await?;
        self.do_initialize(&mut ws).await?;
        let thread_id = self.resolve_thread_id(&mut ws, session).await?;

        let user_prompt = render_user_prompt(&event);
        let model = session
            .state
            .active_model
            .as_deref()
            .or(self.config.default_model.as_deref());
        ws.send(turn_start_request(&thread_id, &user_prompt, model))
            .await
            .map_err(|e| OmonError::Llm(format!("failed to send turn/start: {e}")))?;

        let is_cron_session = session.key.user_id.starts_with("cron:")
            || session
                .state
                .metadata
                .get("cron_scheduler_delivery")
                .and_then(Value::as_bool)
                .unwrap_or(false);

        // Immediately notify Discord that the agent is typing while preparing/reasoning
        if !is_cron_session {
            let _ = self
                .dispatcher
                .dispatch(OutboundAction::Typing {
                    session: session.key.clone(),
                    active: true,
                })
                .await;
        }

        let stream_id = Uuid::new_v4();
        let mut sequence: u64 = 0;
        let mut full_content = String::new();
        let mut tool_call_counts: std::collections::BTreeMap<String, usize> = Default::default();
        let mut total_tool_calls: usize = 0;
        let mut started_ids: std::collections::HashSet<String> = Default::default();
        let mut approval_denials: u32 = 0;

        let started_at = std::time::Instant::now();
        let mut turn_id: Option<String> = None;

        while let Some(msg) = tokio::time::timeout(self.config.request_timeout, ws.next())
            .await
            .map_err(|_| OmonError::Llm("timeout during turn streaming".into()))?
        {
            let msg = msg.map_err(|e| OmonError::Llm(format!("ws streaming error: {e}")))?;
            if let Message::Text(text) = msg {
                let val: Value = serde_json::from_str(text.as_str())
                    .map_err(|e| OmonError::Llm(format!("invalid json: {e}")))?;
                let method = val.get("method").and_then(Value::as_str).unwrap_or("");

                if method != "turn/completed" && started_at.elapsed() > self.config.total_timeout {
                    if let Some(turn_id) = &turn_id {
                        let interrupt = json!({
                            "jsonrpc": "2.0",
                            "id": 9_001,
                            "method": "turn/interrupt",
                            "params": { "threadId": thread_id, "turnId": turn_id }
                        });
                        let _ = ws.send(Message::text(interrupt.to_string())).await;
                        // Give the frame a moment to reach the daemon before the
                        // connection drops.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    return Err(OmonError::Llm(format!(
                        "turn exceeded total deadline of {:?}; turn/interrupt sent",
                        self.config.total_timeout
                    )));
                }

                if val.get("id").and_then(Value::as_u64) == Some(3) {
                    if let Some(error) = val.get("error") {
                        return Err(OmonError::Llm(format!("turn/start error: {error}")));
                    }
                    if let Some(id) = val.pointer("/result/turn/id").and_then(Value::as_str) {
                        turn_id = Some(id.to_string());
                    }
                    continue;
                }

                if let (Some(req_id), Some(method)) =
                    (val.get("id"), val.get("method").and_then(Value::as_str))
                {
                    if is_approval_request(method) {
                        approval_denials += 1;
                        if approval_denials >= APPROVAL_DENIAL_TURN_LIMIT {
                            tracing::error!(
                                denials = approval_denials,
                                "approval denial loop: aborting turn instead of flailing until the deadline"
                            );
                            if let Some(turn_id) = &turn_id {
                                let interrupt = json!({
                                    "jsonrpc": "2.0",
                                    "id": 9_002,
                                    "method": "turn/interrupt",
                                    "params": { "threadId": thread_id, "turnId": turn_id }
                                });
                                let _ = ws.send(Message::text(interrupt.to_string())).await;
                            }
                            return Err(OmonError::Llm(format!(
                                "tool approval denied {approval_denials} times by gateway policy; turn aborted"
                            )));
                        }
                        let _ = ws.send(approval_denial_response(req_id)).await;
                        continue;
                    }
                }

                match method {
                    "turn/started" => {
                        approval_denials = 0;
                        if let Some(tid) = val.pointer("/params/turnId").and_then(Value::as_str) {
                            turn_id = Some(tid.to_string());
                        }
                    }
                    "item/started" | "item/completed" => {
                        approval_denials = 0;
                        let is_started =
                            val.get("method").and_then(Value::as_str) == Some("item/started");
                        let Some(item) = val.pointer("/params/item") else {
                            continue;
                        };
                        let item_id = item.get("id").and_then(Value::as_str).unwrap_or("");

                        if is_started && !item_id.is_empty() && started_ids.insert(item_id.to_string()) {
                            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
                            if !matches!(item_type, "agentMessage" | "userMessage" | "reasoning" | "") {
                                total_tool_calls += 1;
                                let tool_name = item
                                    .get("command")
                                    .or_else(|| item.get("tool"))
                                    .or_else(|| item.get("name"))
                                    .or_else(|| item.get("path"))
                                    .and_then(Value::as_str)
                                    .unwrap_or(item_type);
                                let short_name = tool_name
                                    .split_whitespace()
                                    .next()
                                    .unwrap_or(tool_name);
                                *tool_call_counts.entry(short_name.to_string()).or_insert(0) += 1;
                            }
                        }

                        if !is_started {
                            match item.get("type").and_then(Value::as_str) {
                                Some("agentMessage") => {
                                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                                        full_content.clear();
                                        full_content.push_str(text);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    "item/agentMessage/delta" => {
                        if let Some(delta) = val.pointer("/params/delta").and_then(Value::as_str) {
                            if !delta.is_empty() {
                                full_content.push_str(delta);
                                let _ = self
                                    .emit_chunk(
                                        session,
                                        stream_id,
                                        sequence,
                                        full_content.clone(),
                                        false,
                                    )
                                    .await;
                                sequence = sequence.saturating_add(1);
                            }
                        }
                    }
                    "turn/completed" | "thread/status/changed" => {
                        if method == "turn/completed" {
                            let is_for_current_thread = val
                                .pointer("/params/threadId")
                                .and_then(Value::as_str)
                                .map(|id| id == thread_id)
                                .unwrap_or(true);
                            if !is_for_current_thread {
                                continue;
                            }
                            if val.pointer("/params/turn/status").and_then(Value::as_str) == Some("failed") {
                                let err_msg = val
                                    .pointer("/params/turn/error")
                                    .and_then(Value::as_str)
                                    .or_else(|| val.pointer("/params/turn/error/message").and_then(Value::as_str))
                                    .unwrap_or("turn failed");
                                return Err(OmonError::Llm(format!("omo turn failed: {err_msg}")));
                            }
                        }

                        let has_content = !full_content.is_empty() || total_tool_calls > 0;
                        if !has_content {
                            // A terminal frame with no streamed content (observed:
                            // the daemon raced a premature turn/completed at +6s
                            // while the real turn was still running) must not end
                            // the turn — ignore premature completion and keep streaming.
                            continue;
                        }

                        let rendered = if is_cron_session || total_tool_calls == 0 {
                            if full_content.trim().is_empty() {
                                "✅ Done.".to_string()
                            } else {
                                full_content.clone()
                            }
                        } else {
                            let breakdown: Vec<String> = tool_call_counts
                                .iter()
                                .map(|(tool, count)| {
                                    if *count > 1 {
                                        format!("`{tool}` ×{count}")
                                    } else {
                                        format!("`{tool}`")
                                    }
                                })
                                .collect();
                            let summary_badge = if breakdown.is_empty() {
                                format!("-# 🛠️ 도구 {total_tool_calls}회 실행됨")
                            } else {
                                format!(
                                    "-# 🛠️ 도구 {total_tool_calls}회 실행 ({})",
                                    breakdown.join(", ")
                                )
                            };

                            if full_content.trim().is_empty() {
                                format!("✅ Done.\n\n{summary_badge}")
                            } else {
                                format!("{}\n\n{}", full_content, summary_badge)
                            }
                        };
                        // A terminal frame with no streamed content (observed:
                        // the daemon raced a premature turn/completed at +6s
                        // while the real turn was still running) must not end
                        // the turn — record a success whose delivery is empty
                        // or fail it outright. Ignore the frame and keep
                        // streaming until real completion or the deadlines.
                        let delivered = self
                            .emit_chunk(session, stream_id, sequence, rendered.clone(), true)
                            .await;
                        if let Some(pool) = &self.pool {
                            ensure_session_row(pool, session).await?;
                            persist_message(pool, session, &rendered).await?;
                        }
                        if delivered.is_ok() {
                            if let Some(ack_command) = session
                                .state
                                .metadata
                                .get("cron_ack_command")
                                .and_then(Value::as_str)
                                .filter(|command| !command.trim().is_empty())
                            {
                                crate::cron::ack::run_ack_logged(ack_command).await;
                            }
                        }
                        return Ok(());
                    }
                    "error" | "turn/error" => {
                        let err_msg = val
                            .pointer("/params/message")
                            .or_else(|| val.pointer("/params/error"))
                            .or_else(|| val.pointer("/params/error/message"))
                            .and_then(Value::as_str)
                            .unwrap_or("unknown error from omo app-server");
                        return Err(OmonError::Llm(format!(
                            "omo app-server turn error: {err_msg}"
                        )));
                    }
                    _ => {}
                }
            }
        }
        // Reaching the end of the stream without a terminal frame means the
        // daemon connection closed mid-turn. Failing here (retryable — the
        // message matches the connection-error retry contract) prevents a
        // silent Ok with whatever partial content was collected.
        Err(OmonError::Llm(
            "connection closed before turn completion".into(),
        ))
    }
}

#[async_trait]
impl AgentBackend for OmoBackend {
    async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()> {
        // One automatic retry when the daemon connection drops mid-turn
        // (ECONNRESET from a daemon restart); the persisted threadId keeps
        // the conversation continuous across the retry.
        let outcome = self.run_once(session, event.clone()).await;
        let retryable = matches!(
            &outcome,
            Err(OmonError::Llm(msg))
                if msg.contains("Connection reset")
                    || msg.contains("os error 54")
                    || msg.contains("Broken pipe")
                    || msg.contains("connection closed")
                    || msg.contains("Handshake not finished")
                    || msg.contains("Connection refused")
                    || msg.contains("os error 61")
                    || msg.contains("failed to connect to omo app-server")
        );
        if retryable {
            tracing::warn!("omo daemon connection reset or refused mid-turn; retrying once after cooldown");
            tokio::time::sleep(Duration::from_millis(500)).await;
            return self.run_once(session, event).await;
        }
        outcome
    }
}

async fn ensure_session_row(pool: &sqlx::SqlitePool, session: &SessionContext) -> Result<()> {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT OR IGNORE INTO sessions (session_key, platform, guild_id, channel_id, thread_id, user_id, state_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(session.key.storage_key())
    .bind(&session.key.platform)
    .bind(&session.key.guild_id)
    .bind(&session.key.channel_id)
    .bind(&session.key.thread_id)
    .bind(&session.key.user_id)
    .bind(serde_json::to_string(&session.state).unwrap_or_else(|_| "{}".to_string()))
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|e| OmonError::Llm(format!("ensure session row: {e}")))?;
    Ok(())
}

async fn persist_message(pool: &SqlitePool, session: &SessionContext, content: &str) -> Result<()> {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO messages (id, session_key, role, content, metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(session.key.storage_key())
    .bind("assistant")
    .bind(content)
    .bind("{}")
    .bind(now)
    .execute(pool)
    .await
    .map_err(|e| OmonError::Database(e.to_string()))?;
    Ok(())
}
