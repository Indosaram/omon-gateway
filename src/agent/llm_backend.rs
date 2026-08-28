// allow: SIZE_OK — direct LLM multi-turn tool execution loop and streaming delivery engine
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use parking_lot::Mutex as ParkingMutex;
use serde_json::json;
use sqlx::SqlitePool;
use tracing::info;
use uuid::Uuid;

use super::backend::AgentBackend;
use crate::agent::llm::{ChatMessage, LlmClient, LlmProvider, ToolDefinition};
use crate::memory::MemoryStore;
use crate::models::{InboundEvent, OutboundAction, SessionContext, StreamChunk};
use crate::security::neutralize_untrusted_inline_text;
use crate::tools::ToolRegistry;
use crate::{
    append_runtime_footer, extract_media_directives, is_silence_response,
    reaction_emoji_for_outcome, render_user_prompt, DeliveryLedgerService, OmonError,
    OutboundDispatcher, Result, DISCORD_ATTACHMENT_MAX_BYTES,
};

const STREAM_BATCH_CHARS: usize = 1_500;
const MAX_TOOL_CONTENT_CHARS: usize = 100_000;

#[derive(Clone, Debug)]
pub struct StreamEmissionState {
    pub stream_id: Uuid,
    pub next_sequence: u64,
    pub content: String,
}

/// Direct LLM execution backend implementing [`AgentBackend`].
///
/// Coordinates prompt assembly, message history retrieval, tool call loop execution,
/// streaming delivery via [`OutboundDispatcher`], and SQLite transcript persistence.
pub struct LlmBackend {
    pub pool: SqlitePool,
    pub memory: MemoryStore,
    pub tools: ToolRegistry,
    pub llm: LlmClient,
    pub dispatcher: Arc<dyn OutboundDispatcher>,
    pub workspace_root: PathBuf,
    pub streams: ParkingMutex<HashMap<String, StreamEmissionState>>,
    pub processing_reactions: bool,
    pub runtime_footer: bool,
}

impl LlmBackend {
    pub async fn messages(
        &self,
        session: &SessionContext,
        event: &InboundEvent,
    ) -> Result<Vec<ChatMessage>> {
        let history: Vec<(String, String)> = sqlx::query_as(
            "SELECT role, content FROM (
                SELECT sequence, role, content
                FROM messages
                WHERE session_key = ?
                ORDER BY sequence DESC
                LIMIT 100
             ) ORDER BY sequence ASC",
        )
        .bind(session.key.storage_key())
        .fetch_all(&self.pool)
        .await?;

        let memories = self.memory.search(&session.key, &event.content, 5).await?;
        let cron_jobs: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT id, expression, payload_json FROM cron_jobs WHERE enabled = 1 ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let cron_summary = if cron_jobs.is_empty() {
            "No active cron jobs registered.".to_string()
        } else {
            cron_jobs
                .iter()
                .map(|(id, expr, payload)| {
                    let safe_id = neutralize_untrusted_inline_text(id, 64);
                    let safe_expr = neutralize_untrusted_inline_text(expr, 64);
                    let safe_payload = neutralize_untrusted_inline_text(payload, 240);
                    format!("- [{safe_id}] Schedule `{safe_expr}` | Payload: {safe_payload}")
                })
                .collect::<Vec<_>>()
                .join("\n")
        };

        let mut messages = Vec::new();

        let safe_session_key = neutralize_untrusted_inline_text(&session.key.to_string(), 240);
        let system_prompt = if let Some(prompt) = session.state.system_prompt.as_deref() {
            prompt.to_string()
        } else {
            format!(
                "You are OMON Agent, an autonomous coding and task orchestration assistant running on the Rust-based Omon Gateway multiplexer.\n\n\
                [System & Workspace Environment]\n\
                - Agent Identity: OMON Agent\n\
                - Runtime Engine: Omon Gateway (Rust / Tokio / DashMap Session Multiplexer)\n\
                - Dedicated Workspace Directory: {}\n\
                - Current Session: {}\n\
                - Available Tools: terminal (execute shell commands / scripts in workspace), file (read/write/search files), mcp (connect to MCP servers), cron (inspect/add/delete scheduled background jobs), memory (long-term memory search).\n\n\
                [Active Background Cron Jobs]\n\
                {}\n\n\
                You operate inside your dedicated workspace at `{}`. You have full access to tools to execute commands, create files, manage cron jobs, and perform tasks. When asked about who you are, your workspace, or your cron jobs, answer accurately using the above environment facts and the `cron` tool.",
                self.workspace_root.display(),
                safe_session_key,
                cron_summary,
                self.workspace_root.display()
            )
        };
        messages.push(ChatMessage::new("system", system_prompt));

        if !memories.is_empty() {
            let context = memories
                .into_iter()
                .map(|memory| {
                    format!(
                        "- {}",
                        neutralize_untrusted_inline_text(&memory.content, 400)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            messages.push(ChatMessage::new(
                "system",
                format!("Relevant persistent memory:\n{context}"),
            ));
        }
        messages.extend(history.into_iter().map(|(role, content)| {
            let content = truncate_large_content(&content, MAX_TOOL_CONTENT_CHARS);
            ChatMessage::new(role, content)
        }));
        let messages = repair_message_sequence(messages);
        Ok(messages)
    }

    pub fn tool_definitions(
        tools: &ToolRegistry,
        enabled: Option<&[String]>,
    ) -> Vec<ToolDefinition> {
        tools
            .names()
            .into_iter()
            .filter(|name| tool_enabled(name, enabled))
            .filter_map(|name| tools.get(&name))
            .map(|tool| ToolDefinition {
                name: tool.name().to_owned(),
                description: tool.description().to_owned(),
                input_schema: tool.input_schema(),
            })
            .collect()
    }

    pub async fn persist_message(
        &self,
        session: &SessionContext,
        role: &str,
        content: &str,
        metadata: serde_json::Value,
    ) -> Result<()> {
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(session.key.storage_key())
        .bind(role)
        .bind(content)
        .bind(
            serde_json::to_string(&metadata)
                .map_err(|error| OmonError::Database(error.to_string()))?,
        )
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn execute(
        &self,
        session: &mut SessionContext,
        event: InboundEvent,
        enabled_tools: Option<&[String]>,
        execution_tools: Option<&ToolRegistry>,
        stream_output: bool,
        execution_llm: Option<&LlmClient>,
    ) -> Result<String> {
        if stream_output {
            self.streams.lock().remove(&session.key.storage_key());
            let _ = self
                .dispatcher
                .dispatch(OutboundAction::Typing {
                    session: session.key.clone(),
                    active: true,
                })
                .await;
        }

        let outcome: Result<String> = async {
            info!(session = %session.key, user = %event.session.user_id, content = %event.content, "Starting agent execution for message");
            let mut messages = self.messages(session, &event).await?;
            let mut user_content = render_user_prompt(&event);
            let lower = user_content.to_lowercase();
            let is_ulw = lower.contains("ulw")
                || lower.contains("ultrawork")
                || lower.contains("울트라워크")
                || lower.contains("/ulw");

            if is_ulw {
                let ulw_directive = "\n\n<ultrawork-mode>\n\
                    **MANDATORY**: First user-visible line this turn MUST be exactly:\n\
                    `ULTRAWORK MODE ENABLED!`\n\n\
                    [CODE RED] Maximum precision. Outcome-first. Evidence-driven.\n\
                    - Decompose work into systematic, evidence-bound steps.\n\
                    - Actively use available tools (terminal, file, web_search, browser, mcp, skills) to inspect, execute, and verify.\n\
                    - Never claim completion without executing and verifying real artifacts.\n\
                    </ultrawork-mode>";
                user_content = format!("{}{}", user_content, ulw_directive);
            }

            let attachments = event.attachments.clone();
            if let Some(message) = messages
                .iter_mut()
                .rev()
                .find(|message| message.role == "user" && message.content == user_content)
            {
                message.attachments = attachments;
            } else {
                messages.push(ChatMessage::new("user", &user_content).with_attachments(attachments));
            }
            let mut messages = repair_message_sequence(messages);
            ensure_agent_session(&self.pool, session).await?;
            let tools = execution_tools.unwrap_or(&self.tools);
            let tool_filter = enabled_tools.or(session.state.enabled_toolsets.as_deref());
            let definitions = Self::tool_definitions(tools, tool_filter);
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

            loop {
                let (mut stream, tool_calls) =
                    llm.stream_with_tool_calls(&messages, &definitions).await?;
                let mut response = String::new();
                let mut pending = String::new();
                let mut stripper = ThinkStripper::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    if chunk.content.is_empty() {
                        continue;
                    }
                    let clean = stripper.push(&chunk.content);
                    if clean.is_empty() {
                        continue;
                    }
                    response.push_str(&clean);
                    pending.push_str(&clean);
                    if stream_output && pending.chars().count() >= STREAM_BATCH_CHARS {
                        self.emit(session, std::mem::take(&mut pending), false)
                            .await?;
                    }
                }
                let tail = stripper.finish();
                if !tail.is_empty() {
                    response.push_str(&tail);
                    pending.push_str(&tail);
                }
                let calls = tool_calls
                    .await
                    .map_err(|_| OmonError::Llm("LLM tool-call stream closed unexpectedly".into()))??;
                if calls.is_empty() {
                    let (stripped_text, media_paths) = extract_media_directives(&response);

                    let mut uploaded_media = false;
                    for path_str in &media_paths {
                        let path = PathBuf::from(path_str);
                        if path.is_file() {
                            match std::fs::metadata(&path) {
                                Ok(meta) if meta.len() <= DISCORD_ATTACHMENT_MAX_BYTES => {
                                    self.dispatcher
                                        .dispatch(OutboundAction::UploadFile {
                                            session: session.key.clone(),
                                            path,
                                        })
                                        .await?;
                                    uploaded_media = true;
                                }
                                Ok(meta) => {
                                    tracing::warn!(
                                        session = %session.key,
                                        path = %path.display(),
                                        size = meta.len(),
                                        "MEDIA file exceeds Discord attachment size limit; skipping upload"
                                    );
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        session = %session.key,
                                        path = %path.display(),
                                        %error,
                                        "Failed to read MEDIA file metadata"
                                    );
                                }
                            }
                        } else {
                            tracing::warn!(
                                session = %session.key,
                                path = %path.display(),
                                "MEDIA directive path does not exist; skipping upload"
                            );
                        }
                    }

                    let is_silent = is_silence_response(&stripped_text);
                    if is_silent {
                        if !uploaded_media {
                            if stripped_text.trim().is_empty() {
                                tracing::warn!(session = %session.key, "LLM returned an empty response; suppressing delivery");
                            } else {
                                tracing::info!(session = %session.key, response = %response, "LLM returned silence sentinel or narration; suppressing delivery");
                            }
                            if stream_output {
                                let mut streams = self.streams.lock();
                                streams.remove(&session.key.storage_key());
                            }
                            return Ok(response);
                        }
                        if stream_output {
                            let mut streams = self.streams.lock();
                            streams.remove(&session.key.storage_key());
                        }
                        self.persist_message(session, "assistant", &response, json!({}))
                            .await?;
                        return Ok(response);
                    }

                    let final_text = if self.runtime_footer {
                        let active_model = session
                            .state
                            .active_model
                            .as_deref()
                            .or(Some(llm.config().model.as_str()));
                        let total_chars: usize = messages.iter().map(|m| m.content.len()).sum();
                        let approx_tokens = total_chars / 4;
                        let context_limit = 128_000usize;
                        let pct = ((approx_tokens as f64 / context_limit as f64) * 100.0)
                            .round()
                            .min(100.0) as u8;
                        append_runtime_footer(
                            &stripped_text,
                            active_model,
                            Some(pct),
                            Some(&self.workspace_root),
                        )
                    } else {
                        stripped_text
                    };

                    if stream_output {
                        self.emit_final(session, final_text).await?;
                    } else if !final_text.trim().is_empty() {
                        self.dispatcher
                            .dispatch(OutboundAction::SendMessage {
                                session: session.key.clone(),
                                content: final_text,
                                reply_to: None,
                            })
                            .await?;
                    }
                    self.persist_message(session, "assistant", &response, json!({}))
                        .await?;
                    return Ok(response);
                }
                if stream_output {
                    if !pending.is_empty() {
                        let _ = self.emit(session, std::mem::take(&mut pending), true).await;
                    } else {
                        let has_active_stream = self
                            .streams
                            .lock()
                            .contains_key(&session.key.storage_key());
                        if has_active_stream {
                            let _ = self.emit(session, String::new(), true).await;
                        }
                    }
                }
                if !response.is_empty() {
                    messages.push(ChatMessage::new("assistant", response));
                }
                let mut assistant = ChatMessage::new("assistant", "");
                assistant.tool_calls = calls.clone();
                messages.push(assistant);
                self.persist_message(session, "assistant", "", json!({"tool_calls": calls}))
                    .await?;
                for call in calls {
                    if stream_output {
                        let _ = self.emit_tool_status(session, &call.name).await;
                    }

                    let tool_session = stream_output.then_some(&session.key);
                    let result = tools
                        .execute_with_context(&call.name, call.arguments.clone(), tool_session)
                        .await;
                    let content = match result {
                        Ok(value) => {
                            let s = value.to_string();
                            truncate_large_content(&s, MAX_TOOL_CONTENT_CHARS)
                        }
                        Err(error) => json!({"error": error.to_string()}).to_string(),
                    };
                    let mut message = ChatMessage::new(
                        if llm.config().provider == LlmProvider::Anthropic {
                            "user"
                        } else {
                            "tool"
                        },
                        content.clone(),
                    );
                    message.tool_call_id = Some(call.id.clone());
                    messages.push(message);
                    self.persist_message(
                        session,
                        "tool",
                        &content,
                        json!({"tool_call_id": call.id, "tool": call.name}),
                    )
                    .await?;
                }
            }
        }
        .await;

        if stream_output {
            let _ = self
                .dispatcher
                .dispatch(OutboundAction::Typing {
                    session: session.key.clone(),
                    active: false,
                })
                .await;
        }

        if self.processing_reactions && !event.platform_message_id.is_empty() {
            let emoji = reaction_emoji_for_outcome(outcome.is_ok());
            let _ = self
                .dispatcher
                .dispatch(OutboundAction::React {
                    session: session.key.clone(),
                    message_id: event.platform_message_id.clone(),
                    emoji: emoji.to_string(),
                    remove_others: true,
                })
                .await;
        }

        outcome
    }

    pub async fn emit_tool_status(&self, session: &SessionContext, tool_name: &str) -> Result<()> {
        let status_msg = format!("⚙️ Running tool `{tool_name}`...");
        let chunk = StreamChunk {
            stream_id: Uuid::new_v4(),
            sequence: 0,
            content: status_msg,
            is_final: true,
        };
        self.dispatcher
            .dispatch(OutboundAction::Stream {
                session: session.key.clone(),
                chunk,
            })
            .await
    }

    pub async fn emit(
        &self,
        session: &SessionContext,
        content: String,
        final_chunk: bool,
    ) -> Result<()> {
        let session_key = session.key.storage_key();
        let chunk = {
            let mut streams = self.streams.lock();
            let state = streams
                .entry(session_key.clone())
                .or_insert_with(|| StreamEmissionState {
                    stream_id: Uuid::new_v4(),
                    next_sequence: 0,
                    content: String::new(),
                });
            state.content.push_str(&content);
            let chunk = StreamChunk {
                stream_id: state.stream_id,
                sequence: state.next_sequence,
                content: state.content.clone(),
                is_final: final_chunk,
            };
            state.next_sequence = state.next_sequence.saturating_add(1);
            chunk
        };
        let stream_id = chunk.stream_id;
        let obligation_id = format!("obl_{stream_id}");
        let ledger = DeliveryLedgerService::new(self.pool.clone());

        if final_chunk {
            let _ = ledger
                .record_obligation(&obligation_id, &session.key, &chunk.content)
                .await;
            let _ = ledger.mark_obligation_attempting(&obligation_id).await;
        }

        let result = self
            .dispatcher
            .dispatch(OutboundAction::Stream {
                session: session.key.clone(),
                chunk,
            })
            .await;

        if final_chunk {
            match &result {
                Ok(_) => {
                    let _ = ledger.mark_obligation_delivered(&obligation_id).await;
                }
                Err(error) => {
                    let _ = ledger
                        .mark_obligation_failed(&obligation_id, &error.to_string())
                        .await;
                }
            }
            let mut streams = self.streams.lock();
            if streams
                .get(&session_key)
                .is_some_and(|state| state.stream_id == stream_id)
            {
                streams.remove(&session_key);
            }
        }
        result
    }

    pub async fn emit_final(&self, session: &SessionContext, content: String) -> Result<()> {
        let session_key = session.key.storage_key();
        let chunk = {
            let mut streams = self.streams.lock();
            let state = streams
                .entry(session_key.clone())
                .or_insert_with(|| StreamEmissionState {
                    stream_id: Uuid::new_v4(),
                    next_sequence: 0,
                    content: String::new(),
                });
            state.content = content;
            StreamChunk {
                stream_id: state.stream_id,
                sequence: state.next_sequence,
                content: state.content.clone(),
                is_final: true,
            }
        };
        let stream_id = chunk.stream_id;
        let obligation_id = format!("obl_{stream_id}");
        let ledger = DeliveryLedgerService::new(self.pool.clone());

        let _ = ledger
            .record_obligation(&obligation_id, &session.key, &chunk.content)
            .await;
        let _ = ledger.mark_obligation_attempting(&obligation_id).await;

        let result = self
            .dispatcher
            .dispatch(OutboundAction::Stream {
                session: session.key.clone(),
                chunk,
            })
            .await;

        match &result {
            Ok(_) => {
                let _ = ledger.mark_obligation_delivered(&obligation_id).await;
            }
            Err(error) => {
                let _ = ledger
                    .mark_obligation_failed(&obligation_id, &error.to_string())
                    .await;
            }
        }

        let mut streams = self.streams.lock();
        if streams
            .get(&session_key)
            .is_some_and(|state| state.stream_id == stream_id)
        {
            streams.remove(&session_key);
        }
        result
    }
}

#[async_trait]
impl AgentBackend for LlmBackend {
    async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()> {
        let enabled_tools = session.state.enabled_toolsets.clone().or_else(|| {
            session
                .state
                .metadata
                .get("enabled_toolsets")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
        });
        self.execute(session, event, enabled_tools.as_deref(), None, true, None)
            .await
            .map(|_| ())
    }

    async fn cancel(&self, session: &SessionContext) -> Result<()> {
        let _ = self
            .dispatcher
            .dispatch(OutboundAction::Typing {
                session: session.key.clone(),
                active: false,
            })
            .await;
        let stream = self.streams.lock().remove(&session.key.storage_key());
        if let Some(stream) = stream {
            let obligation_id = format!("obl_{}", stream.stream_id);
            let ledger = DeliveryLedgerService::new(self.pool.clone());
            let _ = ledger
                .record_obligation(&obligation_id, &session.key, &stream.content)
                .await;
            let _ = ledger.mark_obligation_attempting(&obligation_id).await;
            let result = self
                .dispatcher
                .dispatch(OutboundAction::Stream {
                    session: session.key.clone(),
                    chunk: StreamChunk {
                        stream_id: stream.stream_id,
                        sequence: stream.next_sequence,
                        content: stream.content,
                        is_final: true,
                    },
                })
                .await;
            match &result {
                Ok(_) => {
                    let _ = ledger.mark_obligation_delivered(&obligation_id).await;
                }
                Err(error) => {
                    let _ = ledger
                        .mark_obligation_failed(&obligation_id, &error.to_string())
                        .await;
                }
            }
            result?;
        }
        Ok(())
    }
}

fn tool_enabled(name: &str, enabled: Option<&[String]>) -> bool {
    let Some(enabled) = enabled else { return true };
    enabled.iter().any(|toolset| {
        toolset == name
            || (toolset == "web" && matches!(name, "web_search" | "web_fetch"))
            || (toolset == "cron" && name == "cron")
    })
}

async fn ensure_agent_session(pool: &SqlitePool, session: &SessionContext) -> Result<()> {
    sqlx::query(
        "INSERT INTO sessions (session_key, platform, guild_id, channel_id, thread_id, user_id, state_json, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_key) DO NOTHING",
    )
    .bind(session.key.storage_key())
    .bind(&session.key.platform)
    .bind(&session.key.guild_id)
    .bind(&session.key.channel_id)
    .bind(&session.key.thread_id)
    .bind(&session.key.user_id)
    .bind(
        serde_json::to_string(&session.state)
            .map_err(|error| OmonError::Database(error.to_string()))?,
    )
    .bind(session.created_at)
    .bind(session.updated_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Truncates large tool content while preserving head and tail context.
pub fn truncate_large_content(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let head_chars = (max_chars * 2) / 5;
    let tail_chars = max_chars.saturating_sub(head_chars);
    let head: String = text.chars().take(head_chars).collect();
    let total_chars = text.chars().count();
    let tail: String = text
        .chars()
        .skip(total_chars.saturating_sub(tail_chars))
        .collect();
    let omitted = total_chars.saturating_sub(head_chars + tail_chars);
    format!("{head}\n\n... [content truncated: {omitted} characters omitted] ...\n\n{tail}")
}

const THINK_OPEN_TAGS: &[&str] = &[
    "<think>",
    "<thinking>",
    "<reasoning>",
    "<thought>",
    "<reasoning_scratchpad>",
];

const THINK_CLOSE_TAGS: &[&str] = &[
    "</think>",
    "</thinking>",
    "</reasoning>",
    "</thought>",
    "</reasoning_scratchpad>",
];

#[derive(Clone, Debug)]
pub struct ThinkStripper {
    in_block: bool,
    buffer: String,
    last_emitted_ended_newline: bool,
}

impl Default for ThinkStripper {
    fn default() -> Self {
        Self::new()
    }
}

impl ThinkStripper {
    pub fn new() -> Self {
        Self {
            in_block: false,
            buffer: String::new(),
            last_emitted_ended_newline: true,
        }
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.in_block = false;
        self.buffer.clear();
        self.last_emitted_ended_newline = true;
    }

    pub fn push(&mut self, chunk: &str) -> String {
        if chunk.is_empty() {
            return String::new();
        }
        self.buffer.push_str(chunk);
        let mut buf = std::mem::take(&mut self.buffer);
        let mut out = String::new();

        while !buf.is_empty() {
            if self.in_block {
                let (close_idx, close_len) = Self::find_first_tag(&buf, THINK_CLOSE_TAGS);
                if close_idx == -1 {
                    let held = Self::max_partial_suffix(&buf, THINK_CLOSE_TAGS);
                    if held > 0 {
                        self.buffer = buf[buf.len() - held..].to_string();
                    }
                    return out;
                }
                let close_idx = close_idx as usize;
                buf = buf[close_idx + close_len..].to_string();
                self.in_block = false;
            } else {
                let pair = Self::find_earliest_closed_pair(&buf);
                let (open_idx, open_len) =
                    Self::find_open_at_boundary(&buf, &out, self.last_emitted_ended_newline);

                if let Some((start_idx, end_idx)) = pair {
                    if open_idx == -1 || start_idx <= open_idx as usize {
                        let preceding = &buf[..start_idx];
                        if !preceding.is_empty() {
                            let cleaned = Self::strip_orphan_close_tags(preceding);
                            if !cleaned.is_empty() {
                                self.last_emitted_ended_newline = cleaned.ends_with('\n');
                                out.push_str(&cleaned);
                            }
                        }
                        buf = buf[end_idx..].to_string();
                        continue;
                    }
                }

                if open_idx != -1 {
                    let open_idx = open_idx as usize;
                    let preceding = &buf[..open_idx];
                    if !preceding.is_empty() {
                        let cleaned = Self::strip_orphan_close_tags(preceding);
                        if !cleaned.is_empty() {
                            self.last_emitted_ended_newline = cleaned.ends_with('\n');
                            out.push_str(&cleaned);
                        }
                    }
                    self.in_block = true;
                    buf = buf[open_idx + open_len..].to_string();
                    continue;
                }

                let held_open = Self::max_partial_suffix(&buf, THINK_OPEN_TAGS);
                let held_close = Self::max_partial_suffix(&buf, THINK_CLOSE_TAGS);
                let held = held_open.max(held_close);

                if held > 0 {
                    let emittable = &buf[..buf.len() - held];
                    self.buffer = buf[buf.len() - held..].to_string();
                    if !emittable.is_empty() {
                        let cleaned = Self::strip_orphan_close_tags(emittable);
                        if !cleaned.is_empty() {
                            self.last_emitted_ended_newline = cleaned.ends_with('\n');
                            out.push_str(&cleaned);
                        }
                    }
                } else {
                    let cleaned = Self::strip_orphan_close_tags(&buf);
                    if !cleaned.is_empty() {
                        self.last_emitted_ended_newline = cleaned.ends_with('\n');
                        out.push_str(&cleaned);
                    }
                    self.buffer.clear();
                }
                return out;
            }
        }

        out
    }

    pub fn finish(&mut self) -> String {
        if self.in_block {
            self.buffer.clear();
            self.in_block = false;
            self.last_emitted_ended_newline = true;
            String::new()
        } else {
            let tail = std::mem::take(&mut self.buffer);
            self.last_emitted_ended_newline = true;
            if tail.is_empty() {
                String::new()
            } else {
                Self::strip_orphan_close_tags(&tail)
            }
        }
    }

    fn find_first_tag(buf: &str, tags: &[&str]) -> (isize, usize) {
        let buf_lower = buf.to_lowercase();
        let mut best_idx: isize = -1;
        let mut best_len = 0;
        for &tag in tags {
            if let Some(idx) = buf_lower.find(tag) {
                let idx = idx as isize;
                if best_idx == -1 || idx < best_idx {
                    best_idx = idx;
                    best_len = tag.len();
                }
            }
        }
        (best_idx, best_len)
    }

    fn find_earliest_closed_pair(buf: &str) -> Option<(usize, usize)> {
        let buf_lower = buf.to_lowercase();
        let mut best: Option<(usize, usize)> = None;
        for (&open_tag, &close_tag) in THINK_OPEN_TAGS.iter().zip(THINK_CLOSE_TAGS.iter()) {
            if let Some(open_idx) = buf_lower.find(open_tag) {
                if let Some(close_rel) = buf_lower[open_idx + open_tag.len()..].find(close_tag) {
                    let close_idx = open_idx + open_tag.len() + close_rel;
                    let end_idx = close_idx + close_tag.len();
                    if best.is_none() || open_idx < best.unwrap().0 {
                        best = Some((open_idx, end_idx));
                    }
                }
            }
        }
        best
    }

    fn find_open_at_boundary(
        buf: &str,
        already_emitted: &str,
        last_emitted_ended_newline: bool,
    ) -> (isize, usize) {
        let buf_lower = buf.to_lowercase();
        let mut best_idx: isize = -1;
        let mut best_len = 0;
        for &tag in THINK_OPEN_TAGS {
            let mut search_start = 0;
            while search_start < buf_lower.len() {
                if let Some(rel) = buf_lower[search_start..].find(tag) {
                    let idx = search_start + rel;
                    if Self::is_block_boundary(
                        buf,
                        idx,
                        already_emitted,
                        last_emitted_ended_newline,
                    ) {
                        let idx = idx as isize;
                        if best_idx == -1 || idx < best_idx {
                            best_idx = idx;
                            best_len = tag.len();
                        }
                        break;
                    }
                    search_start = idx + 1;
                } else {
                    break;
                }
            }
        }
        (best_idx, best_len)
    }

    fn is_block_boundary(
        buf: &str,
        idx: usize,
        already_emitted: &str,
        last_emitted_ended_newline: bool,
    ) -> bool {
        if idx == 0 {
            if !already_emitted.is_empty() {
                already_emitted.ends_with('\n')
            } else {
                last_emitted_ended_newline
            }
        } else {
            let preceding = &buf[..idx];
            if let Some(last_nl) = preceding.rfind('\n') {
                preceding[last_nl + 1..].trim().is_empty()
            } else {
                let prior_newline = if !already_emitted.is_empty() {
                    already_emitted.ends_with('\n')
                } else {
                    last_emitted_ended_newline
                };
                prior_newline && preceding.trim().is_empty()
            }
        }
    }

    fn max_partial_suffix(buf: &str, tags: &[&str]) -> usize {
        if buf.is_empty() {
            return 0;
        }
        let buf_lower = buf.to_lowercase();
        let max_tag_len = tags.iter().map(|t| t.len()).max().unwrap_or(0);
        let max_check = buf_lower.len().min(max_tag_len.saturating_sub(1));
        for i in (1..=max_check).rev() {
            let start_idx = buf_lower.len() - i;
            if !buf_lower.is_char_boundary(start_idx) {
                continue;
            }
            let suffix = &buf_lower[start_idx..];
            for &tag in tags {
                if tag.len() > i && tag.starts_with(suffix) {
                    return i;
                }
            }
        }
        0
    }

    pub fn strip_orphan_close_tags(text: &str) -> String {
        let text_lower = text.to_lowercase();
        if !text_lower.contains("</") {
            return text.to_string();
        }
        let mut out = String::with_capacity(text.len());
        let mut byte_pos = 0;
        let bytes = text.as_bytes();
        let text_len = text.len();

        while byte_pos < text_len {
            let mut matched = false;
            if byte_pos + 1 < text_len && bytes[byte_pos] == b'<' && bytes[byte_pos + 1] == b'/' {
                for &tag in THINK_CLOSE_TAGS {
                    let tag_len = tag.len();
                    if byte_pos + tag_len <= text_len
                        && text_lower[byte_pos..byte_pos + tag_len] == *tag
                    {
                        let mut j = byte_pos + tag_len;
                        while j < text_len
                            && (bytes[j] == b' '
                                || bytes[j] == b'\t'
                                || bytes[j] == b'\n'
                                || bytes[j] == b'\r')
                        {
                            j += 1;
                        }
                        byte_pos = j;
                        matched = true;
                        break;
                    }
                }
            }
            if !matched {
                let next_char = text[byte_pos..].chars().next().unwrap();
                out.push(next_char);
                byte_pos += next_char.len_utf8();
            }
        }
        out
    }
}

/// Sanitizes and repairs message role sequence for LLM providers:
/// 1. Extracts all system messages and keeps them leading (merging consecutive ones).
/// 2. Merges consecutive same-role non-system messages with "\n\n" rather than dropping them.
/// 3. Handles sequences starting with assistant gracefully by prepending a user turn.
/// 4. Ensures the resulting non-system sequence strictly alternates and ends on user.
pub fn repair_message_sequence(msgs: Vec<ChatMessage>) -> Vec<ChatMessage> {
    if msgs.is_empty() {
        return Vec::new();
    }

    let mut system_msgs = Vec::new();
    let mut non_system_raw = Vec::new();

    for msg in msgs {
        if msg.role == "system" {
            system_msgs.push(msg);
        } else {
            non_system_raw.push(msg);
        }
    }

    // Merge consecutive system messages into leading system messages
    let mut leading_system: Vec<ChatMessage> = Vec::new();
    for msg in system_msgs {
        if let Some(prev) = leading_system.last_mut() {
            if !prev.content.is_empty() && !msg.content.is_empty() {
                prev.content.push_str("\n\n");
                prev.content.push_str(&msg.content);
            } else if prev.content.is_empty() {
                prev.content = msg.content;
            }
            prev.attachments.extend(msg.attachments);
        } else {
            leading_system.push(msg);
        }
    }

    if non_system_raw.is_empty() {
        return leading_system;
    }

    // Merge consecutive same-role messages
    let mut merged_non_system: Vec<ChatMessage> = Vec::new();
    for msg in non_system_raw {
        if let Some(prev) = merged_non_system.last_mut() {
            if prev.role == msg.role {
                if !prev.content.is_empty() && !msg.content.is_empty() {
                    prev.content.push_str("\n\n");
                    prev.content.push_str(&msg.content);
                } else if prev.content.is_empty() {
                    prev.content = msg.content;
                }
                prev.attachments.extend(msg.attachments);
                prev.tool_calls.extend(msg.tool_calls);
                if prev.tool_call_id.is_none() {
                    prev.tool_call_id = msg.tool_call_id;
                }
                continue;
            }
        }
        merged_non_system.push(msg);
    }

    // If starting with assistant or tool, prepend a graceful user message
    if let Some(first) = merged_non_system.first() {
        if first.role == "assistant" || first.role == "tool" {
            merged_non_system.insert(0, ChatMessage::new("user", "Continue"));
        }
    }

    // Ensure ending on user message
    if let Some(last) = merged_non_system.last() {
        if last.role == "assistant" || last.role == "tool" {
            merged_non_system.push(ChatMessage::new("user", "Continue"));
        }
    }

    let mut result = leading_system;
    result.extend(merged_non_system);
    result
}
