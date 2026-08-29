// allow: SIZE_OK — OMO WebSocket protocol state machine
use std::collections::HashMap;
use std::sync::Arc;

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
    approval_denial_response, initialize_request, is_approval_request, thread_start_request,
    turn_start_request,
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
            session
                .state
                .metadata
                .insert("omo_thread_id".into(), json!(id));
            self.thread_ids.lock().insert(storage_key, id.clone());
            return Ok(id);
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

#[async_trait]
impl AgentBackend for OmoBackend {
    async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()> {
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

        while let Some(msg) = tokio::time::timeout(self.config.request_timeout, ws.next())
            .await
            .map_err(|_| OmonError::Llm("timeout during turn streaming".into()))?
        {
            let msg = msg.map_err(|e| OmonError::Llm(format!("ws streaming error: {e}")))?;
            if let Message::Text(text) = msg {
                let val: Value = serde_json::from_str(text.as_str())
                    .map_err(|e| OmonError::Llm(format!("invalid json: {e}")))?;

                if let (Some(req_id), Some(method)) =
                    (val.get("id"), val.get("method").and_then(Value::as_str))
                {
                    if is_approval_request(method) {
                        let _ = ws.send(approval_denial_response(req_id)).await;
                        continue;
                    }
                }

                match val.get("method").and_then(Value::as_str).unwrap_or("") {
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
                    "turn/completed" => {
                        let rendered = if activity_lines.is_empty() {
                            full_content.clone()
                        } else {
                            format!("{}\n\n{}", activity_lines.join("\n"), full_content)
                        };
                        self.emit_chunk(session, stream_id, sequence, rendered.clone(), true)
                            .await;
                        if let Some(pool) = &self.pool {
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
