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

use super::backend::AgentBackend;
use super::omo_activity::{
    command_output_excerpt, format_activity_line, reasoning_blockquote, OUTPUT_CAP, REASONING_CAP,
};
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

        let connect_fut = tokio_tungstenite::connect_async(request);
        let (ws, _) = tokio::time::timeout(self.config.connect_timeout, connect_fut)
            .await
            .map_err(|_| {
                OmonError::Llm(format!(
                    "timeout connecting to omo app-server at {}",
                    self.config.appserver_url
                ))
            })?
            .map_err(|e| {
                OmonError::Llm(format!(
                    "failed to connect to omo app-server at {}: {e}",
                    self.config.appserver_url
                ))
            })?;

        Ok(ws)
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
        if let Some(id) = session
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
        ws.send(thread_start_request(
            session.state.system_prompt.as_deref(),
            model,
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
                        session
                            .state
                            .metadata
                            .insert("omo_thread_id".into(), json!(id_str));
                        self.thread_ids.lock().insert(storage_key, id_str.clone());
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
    ) {
        let chunk = StreamChunk {
            stream_id,
            sequence,
            content,
            is_final,
        };
        let _ = self
            .dispatcher
            .dispatch(OutboundAction::Stream {
                session: session.key.clone(),
                chunk,
            })
            .await;
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

        let stream_id = Uuid::new_v4();
        let mut sequence: u64 = 0;
        let mut full_content = String::new();
        let mut activity_lines: Vec<String> = Vec::new();
        let mut started_ids: std::collections::HashSet<String> = Default::default();
        let mut completed_ids: std::collections::HashSet<String> = Default::default();
        let mut excerpt_ids: std::collections::HashSet<String> = Default::default();

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
                        let _ = ws.send(approval_denial_response(req_id)).await;
                        continue;
                    }
                }

                match method {
                    "turn/started" => {
                        if let Some(tid) = val.pointer("/params/turnId").and_then(Value::as_str) {
                            turn_id = Some(tid.to_string());
                        }
                    }
                    "item/started" | "item/completed" => {
                        let is_started =
                            val.get("method").and_then(Value::as_str) == Some("item/started");
                        let Some(item) = val.pointer("/params/item") else {
                            continue;
                        };
                        let item_id = item.get("id").and_then(Value::as_str).unwrap_or("");

                        if let Some(line) = format_activity_line(item) {
                            if is_started {
                                if !item_id.is_empty() && started_ids.contains(item_id) {
                                    continue;
                                }
                                if !item_id.is_empty() {
                                    started_ids.insert(item_id.to_string());
                                }
                                activity_lines.push(line);
                            } else if !started_ids.contains(item_id)
                                && !completed_ids.contains(item_id)
                            {
                                if !item_id.is_empty() {
                                    completed_ids.insert(item_id.to_string());
                                }
                                activity_lines.push(line);
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
                                Some("reasoning") => {
                                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                                        if item_id.is_empty()
                                            || completed_ids.insert(item_id.to_string())
                                        {
                                            activity_lines
                                                .push(reasoning_blockquote(text, REASONING_CAP));
                                        }
                                    }
                                }
                                Some("commandExecution") => {
                                    if let Some(output) =
                                        item.get("aggregatedOutput").and_then(Value::as_str)
                                    {
                                        if !output.trim().is_empty()
                                            && (item_id.is_empty()
                                                || excerpt_ids.insert(item_id.to_string()))
                                        {
                                            activity_lines.push(format!(
                                                "⤷ {}",
                                                command_output_excerpt(output, OUTPUT_CAP)
                                            ));
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }

                        let rendered = if activity_lines.is_empty() {
                            full_content.clone()
                        } else {
                            format!("{}\n\n{}", activity_lines.join("\n"), full_content)
                        };
                        self.emit_chunk(session, stream_id, sequence, rendered, false)
                            .await;
                        sequence = sequence.saturating_add(1);
                    }
                    "item/agentMessage/delta" => {
                        if let Some(delta) = val.pointer("/params/delta").and_then(Value::as_str) {
                            if !delta.is_empty() {
                                full_content.push_str(delta);
                                self.emit_chunk(
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
                    "turn/completed" | "thread/status/changed"
                        if method == "turn/completed"
                            || (val.pointer("/params/threadId").and_then(Value::as_str)
                                == Some(thread_id.as_str())
                                && val.pointer("/params/status/type").and_then(Value::as_str)
                                    == Some("idle")
                                && !full_content.is_empty()) =>
                    {
                        let rendered = if activity_lines.is_empty() {
                            full_content.clone()
                        } else {
                            format!("{}\n\n{}", activity_lines.join("\n"), full_content)
                        };
                        self.emit_chunk(session, stream_id, sequence, rendered.clone(), true)
                            .await;
                        if let Some(pool) = &self.pool {
                            ensure_session_row(pool, session).await?;
                            persist_message(pool, session, &rendered).await?;
                        }
                        return Ok(());
                    }
                    "error" | "turn/error" => {
                        let err_msg = val
                            .pointer("/params/message")
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
        Ok(())
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
        );
        if retryable {
            tracing::warn!("omo daemon connection reset mid-turn; retrying once");
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
