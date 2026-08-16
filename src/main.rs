use std::collections::HashMap;
use std::env;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use omon_gateway::migrate::MigrateArgs;
use omon_gateway::storage::init_pool;
use omon_gateway::{
    augmented_path_from_environment, cron_runs_retention_days_from_environment,
    extract_media_directives, format_context_from_block, is_silence_response,
    neutralize_untrusted_inline_text, parse_context_from_ids, parse_profile_routes,
    parse_wake_gate, prune_terminal_cron_runs, render_user_prompt, resolve_predecessor_output,
    truncate_context_output, AgentRunner, ApprovalPolicy, AttachmentDownloader, ChatMessage,
    CronJob, CronScheduler, CronTaskExecutor, CronTool, DeliveryLedgerService, DiscordAdapter,
    DiscordApprovalRequester, DiscordEgress, FileTool, HermesJob, HermesStoreSynchronizer,
    InboundEvent, LlmClient, LlmConfig, LlmProvider, McpTool, MemoryStore, MultiplexerConfig,
    OmonError, OutboundAction, OutboundDispatcher, PoiseData, ProfileRoute, ProfileRouter,
    RestartLoopGuard, Result, ScaleToZero, SessionContext, SessionKey, SessionMultiplexer,
    SmartApprovalGuard, TerminalTool, ToolDefinition, ToolRegistry, DISCORD_ATTACHMENT_MAX_BYTES,
    MAX_CONTEXT_CHARS,
};
use parking_lot::Mutex as ParkingMutex;
use serde_json::json;
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const STREAM_BATCH_CHARS: usize = 1_500;

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

#[derive(Debug, Parser)]
#[command(name = "omon-gateway")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run,
    Migrate(MigrateArgs),
}

impl Cli {
    fn into_command(self) -> Command {
        self.command.unwrap_or(Command::Run)
    }
}

struct Config {
    discord_bot_tokens: Vec<String>,
    database_url: String,
    default_model: String,
    openai_api_base: Option<String>,
    openai_api_key: Option<String>,
    anthropic_base_url: Option<String>,
    anthropic_api_key: Option<String>,
    workspace_root: PathBuf,
    free_response_channels: Vec<u64>,
    allowed_users: Vec<u64>,
    allowed_roles: Vec<u64>,
    allow_all_users: bool,
    thread_sessions_per_user: bool,
    thread_require_mention: bool,
    allowed_channels: Vec<u64>,
    ignored_channels: Vec<u64>,
    auto_thread: bool,
    channel_context: bool,
    channel_context_limit: usize,
    processing_reactions: bool,
    approval_policy: ApprovalPolicy,
    approval_timeout_secs: u64,
    approval_mentions: bool,
    approvals_deny: Vec<String>,
    profile_routes: Vec<ProfileRoute>,
    runtime_footer: bool,
    allow_bots: omon_gateway::AllowBotsMode,
    channel_topic_context: bool,
}

impl Config {
    fn from_env() -> Result<Self> {
        let mut free_response_channels: Vec<u64> = env::var("DISCORD_FREE_RESPONSE_CHANNELS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|p| p.trim().parse::<u64>().ok())
                    .collect()
            })
            .unwrap_or_default();
        if let Ok(home) = env::var("DISCORD_HOME_CHANNEL") {
            for h in home.split(',') {
                if let Ok(id) = h.trim().parse::<u64>() {
                    if !free_response_channels.contains(&id) {
                        free_response_channels.push(id);
                    }
                }
            }
        }
        let allowed_users = env::var("DISCORD_ALLOWED_USERS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|p| p.trim().parse::<u64>().ok())
                    .collect()
            })
            .unwrap_or_default();
        let allowed_roles = parse_u64_list(optional_env("DISCORD_ALLOWED_ROLES").as_deref());
        let allow_all_users =
            parse_bool_from(optional_env("DISCORD_ALLOW_ALL_USERS").as_deref(), false);
        let thread_sessions_per_user = parse_bool_from(
            optional_env("DISCORD_THREAD_SESSIONS_PER_USER").as_deref(),
            true,
        );
        let thread_require_mention = parse_bool_from(
            optional_env("DISCORD_THREAD_REQUIRE_MENTION").as_deref(),
            false,
        );
        let allowed_channels = parse_u64_list(optional_env("DISCORD_ALLOWED_CHANNELS").as_deref());
        let ignored_channels = parse_u64_list(optional_env("DISCORD_IGNORED_CHANNELS").as_deref());

        let mut tokens = Vec::new();
        if let Ok(tok) = env::var("DISCORD_BOT_TOKEN") {
            for t in tok.split(',') {
                let trimmed = t.trim().trim_matches('"').trim_matches('\'');
                if !trimmed.is_empty() {
                    tokens.push(trimmed.to_string());
                }
            }
        }
        if let Ok(toks) = env::var("DISCORD_BOT_TOKENS") {
            for t in toks.split(',') {
                let trimmed = t.trim().trim_matches('"').trim_matches('\'');
                if !trimmed.is_empty() && !tokens.contains(&trimmed.to_string()) {
                    tokens.push(trimmed.to_string());
                }
            }
        }
        if tokens.is_empty() {
            return Err(OmonError::Config(
                "missing required environment variable DISCORD_BOT_TOKEN".into(),
            ));
        }

        let workspace_root = env::var_os("OMON_WORKSPACE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = env::var_os("HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("."));
                home.join(".omon").join("workspace")
            });
        let _ = std::fs::create_dir_all(&workspace_root);

        let mut profile_routes =
            parse_profile_routes(&optional_env("DISCORD_PROFILE_ROUTES").unwrap_or_default());
        let channel_prompt_routes = omon_gateway::parse_channel_prompts(
            &optional_env("DISCORD_CHANNEL_PROMPTS").unwrap_or_default(),
        );
        profile_routes.extend(channel_prompt_routes);

        Ok(Self {
            discord_bot_tokens: tokens,
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "sqlite://omon_gateway.db".to_owned()),
            default_model: required_env("DEFAULT_MODEL")?,
            openai_api_base: optional_env("OPENAI_API_BASE"),
            openai_api_key: optional_env("OPENAI_API_KEY"),
            anthropic_base_url: optional_env("ANTHROPIC_BASE_URL"),
            anthropic_api_key: optional_env("ANTHROPIC_API_KEY"),
            workspace_root,
            free_response_channels,
            allowed_users,
            allowed_roles,
            allow_all_users,
            thread_sessions_per_user,
            thread_require_mention,
            allowed_channels,
            ignored_channels,
            auto_thread: parse_bool_from(optional_env("DISCORD_AUTO_THREAD").as_deref(), false),
            channel_context: parse_bool_from(
                optional_env("DISCORD_CHANNEL_CONTEXT").as_deref(),
                false,
            ),
            channel_topic_context: parse_bool_from(
                optional_env("DISCORD_CHANNEL_TOPIC_CONTEXT").as_deref(),
                false,
            ),
            channel_context_limit: optional_env("DISCORD_CHANNEL_CONTEXT_LIMIT")
                .and_then(|val| val.trim().parse::<usize>().ok())
                .unwrap_or(omon_gateway::DEFAULT_CHANNEL_CONTEXT_LIMIT)
                .min(omon_gateway::MAX_CHANNEL_CONTEXT_LIMIT),
            processing_reactions: parse_bool_from(
                optional_env("DISCORD_PROCESSING_REACTIONS").as_deref(),
                true,
            ),
            approval_policy: ApprovalPolicy::parse(optional_env("APPROVAL_MODE").as_deref()),
            approval_timeout_secs: approval_timeout_secs_from(
                optional_env("APPROVAL_TIMEOUT_SECS").as_deref(),
            ),
            approval_mentions: parse_bool_from(
                optional_env("DISCORD_APPROVAL_MENTIONS").as_deref(),
                false,
            ),
            approvals_deny: env::var("APPROVALS_DENY")
                .or_else(|_| env::var("OMON_APPROVALS_DENY"))
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|p| p.trim().to_string())
                        .filter(|p| !p.is_empty())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            profile_routes,
            runtime_footer: parse_bool_from(
                optional_env("DISCORD_RUNTIME_FOOTER").as_deref(),
                false,
            ),
            allow_bots: omon_gateway::AllowBotsMode::parse(
                optional_env("DISCORD_ALLOW_BOTS").as_deref(),
            ),
        })
    }

    fn llm_config(&self, model: impl Into<String>) -> LlmConfig {
        let model = model.into();
        let anthropic = model.starts_with("claude");
        let mut config = LlmConfig::new(
            if anthropic {
                LlmProvider::Anthropic
            } else {
                LlmProvider::OpenAi
            },
            model,
        );
        if anthropic {
            config.base_url = self.anthropic_base_url.clone();
            config.api_key = self.anthropic_api_key.clone();
        } else {
            config.base_url = self.openai_api_base.clone();
            config.api_key = self.openai_api_key.clone();
        }
        config
    }
}

#[derive(Default)]
struct SharedDispatcher {
    inner: RwLock<Option<Arc<dyn OutboundDispatcher>>>,
}

impl SharedDispatcher {
    async fn set(&self, dispatcher: Arc<dyn OutboundDispatcher>) {
        *self.inner.write().await = Some(dispatcher);
    }
}

#[async_trait]
impl OutboundDispatcher for SharedDispatcher {
    async fn dispatch(&self, action: OutboundAction) -> Result<()> {
        let dispatcher =
            self.inner.read().await.clone().ok_or_else(|| {
                OmonError::Config("outbound dispatcher is not initialized".into())
            })?;
        dispatcher.dispatch(action).await
    }
}

struct StreamEmissionState {
    stream_id: Uuid,
    next_sequence: u64,
    content: String,
}

struct LiveAgentRunner {
    pool: SqlitePool,
    memory: MemoryStore,
    tools: ToolRegistry,
    llm: LlmClient,
    dispatcher: Arc<dyn OutboundDispatcher>,
    workspace_root: PathBuf,
    streams: ParkingMutex<HashMap<String, StreamEmissionState>>,
    processing_reactions: bool,
    runtime_footer: bool,
}

impl LiveAgentRunner {
    async fn messages(
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
        messages.extend(
            history
                .into_iter()
                .map(|(role, content)| ChatMessage::new(role, content)),
        );
        let messages = repair_message_sequence(messages);
        Ok(messages)
    }

    fn tool_definitions(tools: &ToolRegistry, enabled: Option<&[String]>) -> Vec<ToolDefinition> {
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

    async fn persist_message(
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

    async fn execute(
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
                        omon_gateway::append_runtime_footer(
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
                        Ok(value) => value.to_string(),
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
            let emoji = omon_gateway::reaction_emoji_for_outcome(outcome.is_ok());
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

    async fn emit_tool_status(&self, session: &SessionContext, tool_name: &str) -> Result<()> {
        let status_msg = format!("⚙️ Running tool `{tool_name}`...");
        let chunk = omon_gateway::StreamChunk {
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

    async fn emit(
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
            let chunk = omon_gateway::StreamChunk {
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

    async fn emit_final(&self, session: &SessionContext, content: String) -> Result<()> {
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
            omon_gateway::StreamChunk {
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
impl AgentRunner for LiveAgentRunner {
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
                    chunk: omon_gateway::StreamChunk {
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

/// Startup recovery sweep: finds undelivered obligations from previous dead processes,
/// claims them, and re-dispatches them to the platform (tagging recovered deliveries).
pub async fn recover_pending_delivery_obligations(
    pool: &SqlitePool,
    dispatcher: Arc<dyn OutboundDispatcher>,
) -> Result<usize> {
    let ledger = DeliveryLedgerService::new(pool.clone());
    let recoverable = ledger.sweep_recoverable(3, 86400).await?;
    let count = recoverable.len();
    for obligation in recoverable {
        let session_key: SessionKey = if let Ok(Some((platform, guild_id, channel_id, thread_id, user_id))) =
            sqlx::query_as::<_, (String, Option<String>, String, Option<String>, String)>(
                "SELECT platform, guild_id, channel_id, thread_id, user_id FROM sessions WHERE session_key = ?",
            )
            .bind(&obligation.session_key)
            .fetch_optional(pool)
            .await
        {
            SessionKey::new(platform, guild_id, channel_id, thread_id, user_id)
        } else {
            SessionKey::new(
                "discord",
                None::<String>,
                &obligation.channel_id,
                obligation.thread_id.as_deref(),
                "recovered-delivery",
            )
        };

        let content = if obligation.state == "pending" {
            obligation.content.clone()
        } else {
            format!(
                "{}{}",
                omon_gateway::ledger::RECOVERED_REPLY_MARKER,
                obligation.content
            )
        };

        let stream_id = Uuid::new_v4();
        let chunk = omon_gateway::StreamChunk {
            stream_id,
            sequence: 0,
            content,
            is_final: true,
        };

        let _ = ledger.mark_obligation_attempting(&obligation.id).await;
        let result = dispatcher
            .dispatch(OutboundAction::Stream {
                session: session_key,
                chunk,
            })
            .await;

        match result {
            Ok(_) => {
                let _ = ledger.mark_obligation_delivered(&obligation.id).await;
            }
            Err(error) => {
                let _ = ledger
                    .mark_obligation_failed(&obligation.id, &error.to_string())
                    .await;
            }
        }
    }
    Ok(count)
}

/// Startup recovery: finds sessions marked resume_pending from a previous run/crash/restart,
/// reconstructs their last unfinished user turn, and re-dispatches them through the multiplexer.
pub async fn recover_resume_pending_sessions(
    pool: &SqlitePool,
    multiplexer: &SessionMultiplexer,
) -> Result<usize> {
    let pending_keys = omon_gateway::storage::fetch_resume_pending_session_keys(pool).await?;
    let mut resumed_count = 0;
    for session_key in pending_keys {
        let storage_key = session_key.storage_key();
        let is_suspended = omon_gateway::storage::is_session_suspended(pool, &storage_key).await?;
        let cleared =
            omon_gateway::storage::clear_session_resume_pending(pool, &storage_key).await?;
        if !cleared {
            continue;
        }
        if is_suspended {
            info!(
                session = %session_key,
                "skipping restart recovery for suspended session"
            );
            continue;
        }

        if let Some(unfinished) =
            omon_gateway::storage::find_last_unfinished_user_turn(pool, &storage_key).await?
        {
            let attachments: Vec<omon_gateway::MessageAttachment> =
                serde_json::from_str(&unfinished.metadata_json).unwrap_or_default();
            let event = InboundEvent {
                id: Uuid::new_v4(),
                session: session_key.clone(),
                platform_message_id: String::new(),
                delivery_id: None,
                content: unfinished.content,
                attachments,
                received_at: chrono::Utc::now(),
            };
            info!(
                session = %session_key,
                "re-dispatching unfinished user turn on restart recovery"
            );
            if let Err(error) = multiplexer.route(event).await {
                tracing::error!(
                    session = %session_key,
                    %error,
                    "failed to route resumed session event"
                );
            } else {
                resumed_count += 1;
            }
        }
    }
    Ok(resumed_count)
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

struct AgentCronExecutor {
    runner: Arc<LiveAgentRunner>,
}

#[async_trait]
impl CronTaskExecutor for AgentCronExecutor {
    async fn execute(&self, job: &CronJob) -> Result<Option<String>> {
        let payload = job.payload()?;
        if payload.get("schedule").is_none() {
            return execute_native_cron(&self.runner, job, &payload).await;
        }
        let hermes: HermesJob = serde_json::from_value(payload).map_err(|error| {
            OmonError::Config(format!("invalid Hermes job {}: {error}", job.id))
        })?;
        let script_output = if let Some(script) = hermes.script.as_deref() {
            Some(run_cron_script(&hermes, script, &self.runner.workspace_root).await?)
        } else {
            None
        };
        if hermes.no_agent {
            return Ok(script_output.filter(|output| !output.trim().is_empty()));
        }
        if let Some(output) = script_output.as_deref() {
            if !parse_wake_gate(output) {
                tracing::info!(job_id = %hermes.id, "wakeAgent:false detected in script output, skipping agent execution");
                return Ok(None);
            }
        }
        if hermes.prompt.trim().is_empty() && script_output.is_none() {
            return Err(OmonError::Config(format!(
                "Hermes job {} has neither prompt nor executable script",
                hermes.id
            )));
        }
        let cron_hint = "[IMPORTANT: You are running as a scheduled cron job. DELIVERY: Your final response will be automatically delivered to the user — do NOT use send_message or try to deliver the output yourself. Just produce your report/output as your final response and the system handles the rest. SILENT: If there is genuinely nothing new to report, respond with exactly \"[SILENT]\" (nothing else) to suppress delivery. Never combine [SILENT] with content — either report your findings normally, or say [SILENT] and nothing more.]\n\n";
        let mut prompt = cron_hint.to_string();

        let workdir = if let Some(custom_workdir) = hermes.workdir.as_ref() {
            let roots = authorized_cron_roots(&hermes, &self.runner.workspace_root).ok();
            if let Some(roots) = roots {
                canonical_authorized_directory(custom_workdir, &roots, "Hermes workdir")
                    .unwrap_or_else(|_| self.runner.workspace_root.clone())
            } else {
                self.runner.workspace_root.clone()
            }
        } else {
            self.runner.workspace_root.clone()
        };

        if let Some(instructions) = resolve_workspace_instructions(&workdir) {
            prompt.push_str(&instructions);
            prompt.push_str("\n\n");
        }

        let context_ids = parse_context_from_ids(hermes.context_from.as_ref());
        if !context_ids.is_empty() {
            let home_path = hermes_home(&hermes).ok();
            for source_id in &context_ids {
                if let Some(output) =
                    resolve_predecessor_output(&self.runner.pool, home_path.as_deref(), source_id)
                        .await
                {
                    if !output.trim().is_empty() {
                        let truncated = truncate_context_output(output.trim(), MAX_CONTEXT_CHARS);
                        prompt.push_str(&format_context_from_block(source_id, &truncated));
                        prompt.push_str("\n\n");
                    }
                }
            }
        }

        let skills = load_cron_skills(&hermes)?;
        if !skills.is_empty() {
            prompt.push_str(&skills);
            if !hermes.prompt.trim().is_empty() {
                prompt.push_str("\n\n[Task]\n");
            }
        }
        prompt.push_str(&hermes.prompt);
        if let Some(output) = script_output.filter(|output| !output.trim().is_empty()) {
            prompt.push_str("\n\n[Script output]\n");
            prompt.push_str(&output);
        }
        let destination = hermes.discord_destination()?;
        let session_key = destination
            .as_ref()
            .map(|target| {
                SessionKey::new(
                    "discord",
                    None::<String>,
                    target.chat_id.clone(),
                    target.thread_id.clone(),
                    target
                        .user_id
                        .clone()
                        .unwrap_or_else(|| format!("cron:{}", hermes.id)),
                )
            })
            .unwrap_or_else(|| {
                SessionKey::new(
                    "local",
                    None::<String>,
                    hermes.id.clone(),
                    None::<String>,
                    format!("cron:{}", hermes.id),
                )
            });
        let mut session = SessionContext::new(session_key.clone());
        session.state.active_model = hermes.model.clone();
        session
            .state
            .metadata
            .insert("hermes_cron_job_id".into(), json!(hermes.id));
        let event = InboundEvent::message(session_key, format!("cron:{}", job.id), prompt);
        let execution_tools =
            build_cron_tools(&hermes, &self.runner.tools, &self.runner.workspace_root)?;
        let cron_llm =
            if hermes.provider.is_some() || hermes.base_url.is_some() || hermes.model.is_some() {
                let config = build_cron_llm_config(
                    self.runner.llm.config(),
                    hermes.provider.as_deref(),
                    hermes.base_url.as_deref(),
                    hermes.model.as_deref(),
                );
                Some(LlmClient::new(config)?)
            } else {
                None
            };
        let response = self
            .runner
            .execute(
                &mut session,
                event,
                hermes.enabled_toolsets.as_deref(),
                execution_tools.as_ref(),
                false,
                cron_llm.as_ref(),
            )
            .await?;
        if response.trim().is_empty() || omon_gateway::is_cron_silence_response(&response) {
            Ok(None)
        } else {
            Ok(Some(response))
        }
    }
}

async fn execute_native_cron(
    runner: &Arc<LiveAgentRunner>,
    job: &CronJob,
    payload: &serde_json::Value,
) -> Result<Option<String>> {
    let script_output =
        if let Some(script) = payload.get("script").and_then(serde_json::Value::as_str) {
            let workspace = canonical_directory(&runner.workspace_root, "workspace root")?;
            let mut command = tokio::process::Command::new("sh");
            command
                .arg("-c")
                .arg(script)
                .current_dir(workspace)
                .kill_on_drop(true);
            let augmented_path = augmented_path_from_environment();
            if !augmented_path.is_empty() {
                command.env("PATH", augmented_path);
            }
            let output =
                tokio::time::timeout(std::time::Duration::from_secs(15 * 60), command.output())
                    .await
                    .map_err(|_| {
                        OmonError::ToolExecution(format!("cron script timed out for {}", job.id))
                    })?
                    .map_err(|error| {
                        OmonError::ToolExecution(format!("failed to execute cron script: {error}"))
                    })?;
            if !output.status.success() {
                return Err(OmonError::ToolExecution(format!(
                    "cron script failed with {:?}: {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim()
                )));
            }
            Some(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            None
        };
    if let Some(output) = script_output.as_deref() {
        if !parse_wake_gate(output) {
            tracing::info!(job_id = %job.id, "wakeAgent:false detected in script output, skipping agent execution");
            return Ok(None);
        }
    }
    let prompt = payload
        .get("prompt")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if prompt.is_empty() {
        return Ok(script_output.filter(|output| !output.trim().is_empty()));
    }
    let mut task = prompt.to_owned();
    if let Some(output) = script_output.filter(|output| !output.trim().is_empty()) {
        task.push_str("\n\n[Script output]\n");
        task.push_str(&output);
    }
    let channel = payload
        .get("deliver")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.strip_prefix("discord:"));
    let session_key = SessionKey::new(
        if channel.is_some() {
            "discord"
        } else {
            "local"
        },
        None::<String>,
        channel.unwrap_or(&job.id),
        None::<String>,
        format!("cron:{}", job.id),
    );
    let mut session = SessionContext::new(session_key.clone());
    let event = InboundEvent::message(session_key, format!("cron:{}", job.id), task);
    let enabled = payload
        .get("enabled_toolsets")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        });
    let response = runner
        .execute(&mut session, event, enabled.as_deref(), None, false, None)
        .await?;
    if response.trim().is_empty() || omon_gateway::is_cron_silence_response(&response) {
        Ok(None)
    } else {
        Ok(Some(response))
    }
}

fn load_cron_skills(job: &HermesJob) -> Result<String> {
    let mut names = job.skills.clone();
    if let Some(skill) = job.skill.as_ref().filter(|skill| !names.contains(skill)) {
        names.push(skill.clone());
    }
    if names.is_empty() {
        return Ok(String::new());
    }
    let Ok(home) = hermes_home(job) else {
        if job.prompt.trim().is_empty() {
            return Err(OmonError::Config(format!(
                "Hermes job {} has an empty prompt and all skills are missing: {}",
                job.id,
                names.join(", ")
            )));
        }
        return Ok(format!(
            "⚠️ Skill(s) not found and skipped: {}",
            names.join(", ")
        ));
    };
    let Ok(root) = canonical_directory(&home.join("skills"), "Hermes skills root") else {
        if job.prompt.trim().is_empty() {
            return Err(OmonError::Config(format!(
                "Hermes job {} has an empty prompt and all skills are missing: {}",
                job.id,
                names.join(", ")
            )));
        }
        return Ok(format!(
            "⚠️ Skill(s) not found and skipped: {}",
            names.join(", ")
        ));
    };

    let mut expanded_names = Vec::new();
    for name in names {
        if let Some(members) = resolve_skill_bundle(&root, Some(&home), &name) {
            for member in members {
                if !expanded_names.contains(&member) {
                    expanded_names.push(member);
                }
            }
        } else if !expanded_names.contains(&name) {
            expanded_names.push(name);
        }
    }

    let mut assembled = String::new();
    let mut skipped = Vec::new();
    for name in expanded_names {
        let path = match find_skill_file(&root, &name) {
            Some(p) => p,
            None => {
                warn!(job_id = %job.id, skill = %name, "Cron job skill not found, skipping");
                skipped.push(name);
                continue;
            }
        };
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(error) => {
                warn!(job_id = %job.id, skill = %name, %error, "Failed to read cron skill file, skipping");
                skipped.push(name);
                continue;
            }
        };
        if !assembled.is_empty() {
            assembled.push_str("\n\n");
        }
        assembled.push_str(&format!("[Skill: {name}]\n{content}"));
    }

    if assembled.is_empty() && !skipped.is_empty() && job.prompt.trim().is_empty() {
        return Err(OmonError::Config(format!(
            "Hermes job {} has an empty prompt and all skills were missing: {}",
            job.id,
            skipped.join(", ")
        )));
    }

    if !skipped.is_empty() {
        let warning = format!("⚠️ Skill(s) not found and skipped: {}", skipped.join(", "));
        if !assembled.is_empty() {
            Ok(format!("{warning}\n\n{assembled}"))
        } else {
            Ok(warning)
        }
    } else {
        Ok(assembled)
    }
}

fn resolve_skill_bundle(
    skills_root: &Path,
    hermes_home: Option<&Path>,
    name: &str,
) -> Option<Vec<String>> {
    if Path::new(name).components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return None;
    }

    fn parse_bundle_manifest(content: &str) -> Option<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct Manifest {
            #[serde(default)]
            skills: Vec<String>,
        }
        if let Ok(m) = serde_yaml::from_str::<Manifest>(content) {
            if !m.skills.is_empty() {
                return Some(m.skills);
            }
        }
        if let Ok(m) = serde_json::from_str::<Manifest>(content) {
            if !m.skills.is_empty() {
                return Some(m.skills);
            }
        }
        None
    }

    // 1. Check <hermes_home>/skill-bundles/<name>.yaml / .yml / .json
    if let Some(home) = hermes_home {
        let bundles_dir = home.join("skill-bundles");
        for ext in &["yaml", "yml", "json"] {
            let path = bundles_dir.join(format!("{name}.{ext}"));
            if path.is_file() {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if let Some(skills) = parse_bundle_manifest(&content) {
                        return Some(skills);
                    }
                }
            }
        }
    }

    // 2. Check <skills_root>/<name>.yaml / .yml / .json
    for ext in &["yaml", "yml", "json"] {
        let path = skills_root.join(format!("{name}.{ext}"));
        if path.is_file() {
            if let Ok(content) = std::fs::read_to_string(&path) {
                if let Some(skills) = parse_bundle_manifest(&content) {
                    return Some(skills);
                }
            }
        }
    }

    // 3. Check <skills_root>/<name>/bundle.yaml / .yml / .json / manifest.yaml ...
    let dir = skills_root.join(name);
    if dir.is_dir() {
        for filename in &[
            "bundle.yaml",
            "bundle.yml",
            "bundle.json",
            "manifest.yaml",
            "manifest.yml",
            "manifest.json",
        ] {
            let path = dir.join(filename);
            if path.is_file() {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if let Some(skills) = parse_bundle_manifest(&content) {
                        return Some(skills);
                    }
                }
            }
        }

        // 4. Directory containing multiple member skills (subdirectories with SKILL.md),
        // provided the directory itself does not have a direct SKILL.md.
        if !dir.join("SKILL.md").exists() {
            if let Ok(entries) = std::fs::read_dir(&dir) {
                let mut members = Vec::new();
                for entry in entries.flatten() {
                    let sub_path = entry.path();
                    if sub_path.is_dir() && sub_path.join("SKILL.md").is_file() {
                        if let Some(file_name) = sub_path.file_name().and_then(|n| n.to_str()) {
                            members.push(format!("{name}/{file_name}"));
                        }
                    }
                }
                if !members.is_empty() {
                    members.sort();
                    return Some(members);
                }
            }
        }
    }

    None
}

fn find_skill_file(root: &Path, name: &str) -> Option<PathBuf> {
    if Path::new(name).components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return None;
    }
    let direct = root.join(name).join("SKILL.md");
    if let Ok(candidate) = std::fs::canonicalize(&direct) {
        if candidate.starts_with(root) && candidate.is_file() {
            return Some(candidate);
        }
    }
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).ok()?.flatten() {
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).ok()?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                if path.file_name().is_some_and(|value| value == name) {
                    let candidate = path.join("SKILL.md");
                    if let Ok(candidate) = std::fs::canonicalize(candidate) {
                        if candidate.starts_with(root) && candidate.is_file() {
                            return Some(candidate);
                        }
                    }
                }
                pending.push(path);
            }
        }
    }
    None
}

fn build_cron_tools(
    job: &HermesJob,
    defaults: &ToolRegistry,
    workspace_root: &Path,
) -> Result<Option<ToolRegistry>> {
    let Some(workdir) = job.workdir.as_ref() else {
        return Ok(None);
    };
    let roots = authorized_cron_roots(job, workspace_root)?;
    let workdir = canonical_authorized_directory(workdir, &roots, "Hermes workdir")?;
    let mut tools = defaults.clone();
    tools.register(TerminalTool::new(&workdir));
    tools.register(FileTool::new(&workdir));
    Ok(Some(tools))
}

fn parse_llm_provider(name: &str) -> Option<LlmProvider> {
    let lower = name.trim().to_lowercase();
    match lower.as_str() {
        "openai" | "gpt" => Some(LlmProvider::OpenAi),
        "anthropic" | "claude" => Some(LlmProvider::Anthropic),
        "deepseek" => Some(LlmProvider::DeepSeek),
        "ollama" => Some(LlmProvider::Ollama),
        _ => None,
    }
}

pub fn build_cron_llm_config(
    base: &LlmConfig,
    provider: Option<&str>,
    base_url: Option<&str>,
    model: Option<&str>,
) -> LlmConfig {
    let mut config = base.clone();

    if let Some(m) = model.filter(|m| !m.trim().is_empty()) {
        config.model = m.trim().to_string();
    }

    if let Some(p) = provider.filter(|p| !p.trim().is_empty()) {
        if let Some(parsed) = parse_llm_provider(p) {
            config.provider = parsed;
        } else {
            warn!(provider = %p, "Unknown LLM provider override, keeping base provider");
        }
    }

    if let Some(b) = base_url.filter(|b| !b.trim().is_empty()) {
        config.base_url = Some(b.trim().to_string());
    }

    config
}

pub fn resolve_workspace_instructions(workdir: &Path) -> Option<String> {
    const MAX_WORKSPACE_INSTRUCTION_CHARS: usize = 8000;
    for filename in &["AGENTS.md", "agents.md", "CLAUDE.md", "claude.md"] {
        let candidate = workdir.join(filename);
        if candidate.is_file() {
            if let Ok(content) = std::fs::read_to_string(&candidate) {
                let trimmed = content.trim();
                if !trimmed.is_empty() {
                    let truncated: String =
                        if trimmed.chars().count() > MAX_WORKSPACE_INSTRUCTION_CHARS {
                            trimmed
                                .chars()
                                .take(MAX_WORKSPACE_INSTRUCTION_CHARS)
                                .collect()
                        } else {
                            trimmed.to_string()
                        };
                    return Some(format!("[Workspace instructions]\n{truncated}"));
                }
            }
        }
    }
    None
}

async fn run_cron_script(job: &HermesJob, script: &str, workspace_root: &Path) -> Result<String> {
    let home = hermes_home(job)?;
    let scripts_root = canonical_directory(&home.join("scripts"), "Hermes scripts root")?;
    let candidate = Path::new(script);
    if candidate.is_absolute()
        || candidate.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(OmonError::Config(format!(
            "Hermes job {} script path escapes its scripts root: {script}",
            job.id
        )));
    }
    let path = std::fs::canonicalize(scripts_root.join(candidate)).map_err(|error| {
        OmonError::Config(format!(
            "failed to resolve Hermes script for {}: {error}",
            job.id
        ))
    })?;
    if !path.starts_with(&scripts_root) || !path.is_file() {
        return Err(OmonError::Config(format!(
            "Hermes job {} script escapes its scripts root: {}",
            job.id,
            path.display()
        )));
    }

    let roots = authorized_cron_roots(job, workspace_root)?;
    let workdir = match job.workdir.as_ref() {
        Some(workdir) => canonical_authorized_directory(workdir, &roots, "Hermes workdir")?,
        None => home,
    };
    let mut command = if matches!(
        path.extension().and_then(|value| value.to_str()),
        Some("sh" | "bash")
    ) {
        let mut command = tokio::process::Command::new("bash");
        command.arg(&path);
        command
    } else {
        let mut command = tokio::process::Command::new("python3");
        command.arg(&path);
        command
    };
    let augmented_path = augmented_path_from_environment();
    if !augmented_path.is_empty() {
        command.env("PATH", augmented_path);
    }
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(15 * 60),
        command.current_dir(workdir).kill_on_drop(true).output(),
    )
    .await
    .map_err(|_| {
        OmonError::ToolExecution(format!("Hermes cron script timed out: {}", path.display()))
    })?
    .map_err(|error| {
        OmonError::ToolExecution(format!("failed to execute {}: {error}", path.display()))
    })?;
    if !output.status.success() {
        return Err(OmonError::ToolExecution(format!(
            "Hermes cron script {} failed with {:?}: {}",
            path.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn hermes_home(job: &HermesJob) -> Result<PathBuf> {
    let home = job
        .extra
        .get("_omon_hermes_home")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| OmonError::Config(format!("Hermes job {} is missing its home", job.id)))?;
    canonical_directory(&home, "Hermes home")
}

fn authorized_cron_roots(job: &HermesJob, workspace_root: &Path) -> Result<Vec<PathBuf>> {
    let workspace_root = canonical_directory(workspace_root, "workspace root")?;
    let home = hermes_home(job)?;
    if home == workspace_root {
        Ok(vec![workspace_root])
    } else {
        Ok(vec![workspace_root, home])
    }
}

fn canonical_authorized_directory(path: &Path, roots: &[PathBuf], kind: &str) -> Result<PathBuf> {
    let path = canonical_directory(path, kind)?;
    if roots.iter().any(|root| path.starts_with(root)) {
        Ok(path)
    } else {
        Err(OmonError::Config(format!(
            "{kind} is outside authorized workspace/Hermes roots: {}",
            path.display()
        )))
    }
}

fn canonical_directory(path: &Path, kind: &str) -> Result<PathBuf> {
    let path = std::fs::canonicalize(path)
        .map_err(|error| OmonError::Config(format!("failed to resolve {kind}: {error}")))?;
    if !path.is_dir() {
        return Err(OmonError::Config(format!(
            "{kind} is not a directory: {}",
            path.display()
        )));
    }
    Ok(path)
}

fn hermes_skill_dirs(hermes_root: &Path, home: &Path) -> Vec<PathBuf> {
    vec![
        hermes_root.join("skills"),
        home.join(".omon").join("skills"),
    ]
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().into_command() {
        Command::Run => run_gateway().await,
        Command::Migrate(args) => omon_gateway::migrate::run_migrate(args).await,
    }
}

async fn run_gateway() -> Result<()> {
    let config = Config::from_env()?;
    let pool = init_pool(&config.database_url).await?;
    let memory = MemoryStore::new(pool.clone());

    let approval_guard = SmartApprovalGuard::new().with_pool(pool.clone());
    let loaded_allowlist = approval_guard.load_persisted_allowlist().await?;
    info!(
        loaded_allowlist,
        "loaded persisted approval allowlist entries"
    );
    let approval_requester = Arc::new(DiscordApprovalRequester::new(
        approval_guard.clone(),
        std::time::Duration::from_secs(config.approval_timeout_secs),
    ));
    let mut tools = ToolRegistry::new();
    tools.register(
        TerminalTool::new(&config.workspace_root)
            .with_approval(
                config.approval_policy,
                approval_requester.clone(),
                std::time::Duration::from_secs(config.approval_timeout_secs + 5),
            )
            .with_deny_globs(config.approvals_deny.clone()),
    );
    tools.register(FileTool::new(&config.workspace_root));
    tools.register(McpTool::default());
    tools.register(CronTool::new(pool.clone()));
    tools.register(omon_gateway::WebSearchTool);
    tools.register(omon_gateway::WebFetchTool);
    tools.register(omon_gateway::BrowserTool::default());
    let home = env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    let hermes_root = env::var_os("HERMES_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".hermes"));
    tools.register(
        omon_gateway::SkillsTool::new(hermes_skill_dirs(&hermes_root, &home))
            .with_pool(pool.clone()),
    );
    let tool_names = tools.names();

    let llm = LlmClient::new(config.llm_config(config.default_model.clone()))?;
    let shared_dispatcher = Arc::new(SharedDispatcher::default());
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
    let profile_router = ProfileRouter::new(config.profile_routes.clone());
    let multiplexer = SessionMultiplexer::with_profile_router(
        pool.clone(),
        runner.clone(),
        Some(shared_dispatcher.clone()),
        MultiplexerConfig::default(),
        profile_router.clone(),
    );
    let scale_to_zero = ScaleToZero::start(multiplexer.clone());

    let mut bot_http_clients = HashMap::new();
    let mut default_bot_id = None;
    for token in &config.discord_bot_tokens {
        let http = Arc::new(serenity::http::Http::new(token));
        let bot_id = http.get_current_user().await?.id.to_string();
        if default_bot_id.is_none() {
            default_bot_id = Some(bot_id.clone());
        }
        if bot_http_clients.insert(bot_id.clone(), http).is_some() {
            return Err(OmonError::Config(format!(
                "multiple Discord tokens resolve to the same bot identity {bot_id}"
            )));
        }
    }
    let default_bot_id = default_bot_id
        .ok_or_else(|| OmonError::Config("no Discord bot identities were configured".into()))?;
    let discord_egress = Arc::new(
        DiscordEgress::with_bot_clients(default_bot_id.clone(), bot_http_clients)?
            .with_approval_mentions(config.allowed_users.clone(), config.approval_mentions),
    );
    shared_dispatcher.set(discord_egress.clone()).await;
    approval_requester
        .set_dispatcher(discord_egress.clone())
        .await;
    approval_requester
        .set_heartbeat(multiplexer.activity_heartbeat())
        .await;

    let retention_days = cron_runs_retention_days_from_environment()?;
    let pruned = prune_terminal_cron_runs(&pool, retention_days, chrono::Utc::now()).await?;
    info!(pruned, retention_days, "pruned old terminal cron runs");

    let cron_sync = HermesStoreSynchronizer::from_environment(pool.clone())?;
    let imported = cron_sync.sync().await?;
    info!(imported, "synchronized Hermes cron stores");

    let recovered = recover_pending_delivery_obligations(&pool, discord_egress.clone()).await?;
    info!(
        recovered,
        "recovered pending outbound delivery obligations on boot"
    );

    let restart_guard_path = config.workspace_root.join("restart_loop.json");
    let restart_guard = RestartLoopGuard::new(restart_guard_path);
    let pending_sessions_count =
        omon_gateway::storage::count_resume_pending_sessions(&pool).await?;
    if pending_sessions_count > 0 {
        if restart_guard.check_and_record() {
            warn!(
                pending_sessions_count,
                "Restart-loop breaker TRIPPED: skipping auto-resume of in-flight sessions to break crash loop"
            );
        } else {
            let recovered_sessions = recover_resume_pending_sessions(&pool, &multiplexer).await?;
            info!(
                recovered_sessions,
                "recovered resume_pending sessions on boot"
            );
        }
    }

    let scheduler = CronScheduler::with_dispatcher(
        pool.clone(),
        Arc::new(AgentCronExecutor {
            runner: runner.clone(),
        }),
        discord_egress,
    )
    .with_hermes_sync(cron_sync);
    scheduler.start().await;

    let mut poise_data = PoiseData::new(multiplexer.clone(), pool.clone());
    poise_data.profile_router = profile_router;
    poise_data.llm = Some(llm.clone());
    poise_data.tools = tool_names;
    poise_data.tool_registry = tools.clone();
    poise_data.free_response_channels = config.free_response_channels.clone();
    poise_data.allowed_users = config.allowed_users.clone();
    poise_data.allowed_roles = config.allowed_roles.clone();
    poise_data.allow_all_users = config.allow_all_users;
    poise_data.thread_sessions_per_user = config.thread_sessions_per_user;
    poise_data.thread_require_mention = config.thread_require_mention;
    poise_data.allow_bots = config.allow_bots;
    poise_data.allowed_channels = config.allowed_channels.clone();
    poise_data.ignored_channels = config.ignored_channels.clone();
    poise_data.auto_thread = config.auto_thread;
    poise_data.channel_topic_context = config.channel_topic_context;
    poise_data.channel_context = config.channel_context;
    poise_data.channel_context_limit = config.channel_context_limit;
    poise_data.processing_reactions = config.processing_reactions;
    poise_data.approval_mentions = config.approval_mentions;
    poise_data.approvals_deny = config.approvals_deny.clone();
    poise_data.attachment_downloader = Some(AttachmentDownloader::new(&config.workspace_root)?);
    poise_data.primary_bot_id = Some(default_bot_id.parse().map_err(|_| {
        OmonError::Config(format!(
            "invalid primary Discord bot identity {default_bot_id}"
        ))
    })?);
    let adapter = DiscordAdapter::new(poise_data).with_approval_guard(approval_guard);

    let mut clients = Vec::new();
    let mut shard_managers = Vec::new();
    for token in &config.discord_bot_tokens {
        let client = adapter.client(token).await?;
        shard_managers.push(client.shard_manager.clone());
        clients.push(client);
    }

    info!(
        model = %config.default_model,
        database = %config.database_url,
        bot_count = clients.len(),
        "omon-gateway listening on Discord"
    );

    let mut join_set = tokio::task::JoinSet::new();
    for mut client in clients {
        join_set.spawn(async move { client.start().await });
    }

    let drain_watcher = omon_gateway::DrainWatcher::new(
        config.workspace_root.clone(),
        std::time::Duration::from_secs(3),
    );
    let mut drain_rx = drain_watcher.receiver();
    let _drain_handle = drain_watcher.spawn();

    tokio::select! {
        Some(res) = join_set.join_next() => {
            if let Ok(Err(err)) = res {
                tracing::error!("Discord client exited with error: {:?}", err);
            }
        }
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|error| OmonError::Config(format!("failed to listen for Ctrl+C: {error}")))?;
            info!("shutdown signal received");
            let _ = multiplexer.mark_in_flight_resume_pending().await;
            for sm in shard_managers {
                sm.shutdown_all().await;
            }
        }
        changed = drain_rx.changed() => {
            if changed.is_ok() && *drain_rx.borrow() {
                warn!("drain request detected via .drain_request.json marker; shutting down gracefully");
                let _ = multiplexer.mark_in_flight_resume_pending().await;
                for sm in shard_managers {
                    sm.shutdown_all().await;
                }
            }
        }
    }

    scheduler.shutdown().await;
    scale_to_zero.shutdown().await;
    pool.close().await;
    warn!("omon-gateway stopped");
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    optional_env(name)
        .ok_or_else(|| OmonError::Config(format!("missing required environment variable {name}")))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn approval_timeout_secs_from(raw: Option<&str>) -> u64 {
    raw.and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(900)
}

pub fn parse_bool_from(raw: Option<&str>, default: bool) -> bool {
    match raw {
        Some(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        None => default,
    }
}

pub fn parse_u64_list(raw: Option<&str>) -> Vec<u64> {
    raw.map(|s| {
        s.split(',')
            .filter_map(|p| p.trim().parse::<u64>().ok())
            .collect()
    })
    .unwrap_or_default()
}

#[cfg(test)]
mod runner_tests {
    use std::collections::HashMap;
    use std::fs;

    use clap::Parser;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{
        approval_timeout_secs_from, canonical_authorized_directory, hermes_skill_dirs,
        load_cron_skills, tool_enabled, Cli, Command,
    };
    use omon_gateway::HermesJob;

    #[test]
    fn parses_approval_timeout_secs_from_env() {
        assert_eq!(approval_timeout_secs_from(Some("120")), 120);
        assert_eq!(approval_timeout_secs_from(Some(" 300 ")), 300);
        assert_eq!(approval_timeout_secs_from(None), 900);
        assert_eq!(approval_timeout_secs_from(Some("")), 900);
        assert_eq!(approval_timeout_secs_from(Some("   ")), 900);
        assert_eq!(approval_timeout_secs_from(Some("0")), 900);
        assert_eq!(approval_timeout_secs_from(Some("-10")), 900);
        assert_eq!(approval_timeout_secs_from(Some("invalid")), 900);
    }

    #[test]
    fn parses_bool_from_env_variants() {
        assert!(super::parse_bool_from(Some("true"), false));
        assert!(super::parse_bool_from(Some("True"), false));
        assert!(super::parse_bool_from(Some("1"), false));
        assert!(super::parse_bool_from(Some("yes"), false));
        assert!(super::parse_bool_from(Some("on"), false));
        assert!(!super::parse_bool_from(Some("false"), true));
        assert!(!super::parse_bool_from(Some("0"), true));
        assert!(!super::parse_bool_from(Some("no"), true));
        assert!(!super::parse_bool_from(Some("off"), true));
        assert!(!super::parse_bool_from(Some(""), false));
        assert!(super::parse_bool_from(None, true));
        assert!(!super::parse_bool_from(None, false));
    }

    #[test]
    fn hermes_skill_dirs_use_documented_roots() {
        let dirs = hermes_skill_dirs(std::path::Path::new("/x"), std::path::Path::new("/h"));

        assert_eq!(
            dirs,
            vec![
                std::path::PathBuf::from("/x/skills"),
                std::path::PathBuf::from("/h/.omon/skills"),
            ]
        );
        assert!(dirs
            .iter()
            .all(|path| !path.to_string_lossy().contains("workspace/.hermes")));
    }

    #[test]
    fn cli_defaults_to_run_without_a_subcommand() {
        let cli = Cli::try_parse_from(["omon-gateway"]).unwrap();
        assert!(matches!(cli.into_command(), Command::Run));
    }

    #[test]
    fn cli_maps_explicit_run_to_the_run_path() {
        let cli = Cli::try_parse_from(["omon-gateway", "run"]).unwrap();
        assert!(matches!(cli.into_command(), Command::Run));
    }

    #[test]
    fn cli_parses_migrate_flags() {
        let cli =
            Cli::try_parse_from(["omon-gateway", "migrate", "--dry-run", "--no-cutover"]).unwrap();
        match cli.into_command() {
            Command::Migrate(args) => {
                assert!(args.dry_run);
                assert!(args.no_cutover);
            }
            command => panic!("expected migrate command, got {command:?}"),
        }
    }

    #[test]
    fn cli_rejects_unknown_subcommands() {
        assert!(Cli::try_parse_from(["omon-gateway", "bogus"]).is_err());
    }

    #[test]
    fn maps_hermes_web_toolset_to_both_web_tools() {
        let enabled = vec!["web".to_string()];
        assert!(tool_enabled("web_search", Some(&enabled)));
        assert!(tool_enabled("web_fetch", Some(&enabled)));
        assert!(!tool_enabled("terminal", Some(&enabled)));
    }

    #[test]
    fn rejects_cron_workdir_outside_authorized_roots() {
        let base = std::env::temp_dir().join(format!("omon-cron-roots-{}", uuid::Uuid::new_v4()));
        let workspace = base.join("workspace");
        let hermes = base.join("hermes");
        let outside = base.join("outside");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&hermes).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let roots = vec![
            fs::canonicalize(&workspace).unwrap(),
            fs::canonicalize(&hermes).unwrap(),
        ];

        assert!(canonical_authorized_directory(&workspace, &roots, "workdir").is_ok());
        assert!(canonical_authorized_directory(&hermes, &roots, "workdir").is_ok());
        assert!(canonical_authorized_directory(&outside, &roots, "workdir").is_err());

        let _ = fs::remove_dir_all(base);
    }

    struct MockTool;

    #[async_trait::async_trait]
    impl omon_gateway::Tool for MockTool {
        fn name(&self) -> &str {
            "mock_tool"
        }

        fn description(&self) -> &str {
            "Mock test tool"
        }

        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }

        async fn execute(
            &self,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, omon_gateway::OmonError> {
            Ok(serde_json::json!({"status": "ok"}))
        }
    }

    #[derive(Default)]
    struct CapturingDispatcher {
        actions: tokio::sync::Mutex<Vec<omon_gateway::OutboundAction>>,
    }

    #[async_trait::async_trait]
    impl omon_gateway::OutboundDispatcher for CapturingDispatcher {
        async fn dispatch(&self, action: omon_gateway::OutboundAction) -> omon_gateway::Result<()> {
            self.actions.lock().await.push(action);
            Ok(())
        }
    }

    async fn spawn_two_turn_tool_llm_server() -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            // Turn 1: LLM returns tool call for "mock_tool"
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let body = "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"mock_tool\",\"arguments\":\"{}\"}}]}}]}\n\ndata: [DONE]\n\n";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }

            // Turn 2: LLM returns final text content
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let body = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Final result content\"}}]}\n\ndata: [DONE]\n\n";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        (format!("http://{address}/v1"), handle)
    }

    async fn build_test_runner(
        base_url: String,
        dispatcher: std::sync::Arc<CapturingDispatcher>,
    ) -> (super::LiveAgentRunner, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let pool = omon_gateway::storage::init_pool("sqlite::memory:")
            .await
            .unwrap();
        let memory = omon_gateway::MemoryStore::new(pool.clone());
        let mut tools = omon_gateway::ToolRegistry::new();
        tools.register(MockTool);

        let mut config =
            omon_gateway::LlmConfig::new(omon_gateway::LlmProvider::OpenAi, "gpt-test");
        config.base_url = Some(base_url);
        let llm = omon_gateway::LlmClient::new(config).unwrap();

        let runner = super::LiveAgentRunner {
            pool,
            memory,
            tools,
            llm,
            dispatcher,
            workspace_root: temp_dir.path().to_path_buf(),
            streams: parking_lot::Mutex::new(std::collections::HashMap::new()),
            processing_reactions: true,
            runtime_footer: false,
        };
        (runner, temp_dir)
    }

    #[tokio::test]
    async fn test_execute_suppresses_delivery_for_silence_sentinel_and_empty() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let body = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"[SILENT]\"}}]}\n\ndata: [DONE]\n\n";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) =
            build_test_runner(format!("http://{address}/v1"), dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-silence",
            None::<String>,
            "user-silence",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event =
            omon_gateway::InboundEvent::message(session_key.clone(), "msg-silence", "Hello");

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();
        assert_eq!(response, "[SILENT]");

        let actions = dispatcher.actions.lock().await;
        let stream_actions: Vec<_> = actions
            .iter()
            .filter(|a| matches!(a, omon_gateway::OutboundAction::Stream { .. }))
            .collect();
        assert!(
            stream_actions.is_empty(),
            "Expected zero Stream outbound actions for silence sentinel, found: {stream_actions:?}"
        );

        // Assistant message should NOT be persisted in DB
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT role, content FROM messages WHERE session_key = $1 AND role = 'assistant'",
        )
        .bind(session_key.storage_key())
        .fetch_all(&runner.pool)
        .await
        .unwrap();
        assert!(
            rows.is_empty(),
            "Expected 0 assistant messages in DB for silence sentinel"
        );

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn execute_suppresses_tool_status_when_stream_output_is_false() {
        let (base_url, server_handle) = spawn_two_turn_tool_llm_server().await;
        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) = build_test_runner(base_url, dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-1",
            None::<String>,
            "user-1",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event = omon_gateway::InboundEvent::message(session_key, "msg-1", "Run a tool");

        let response = runner
            .execute(&mut session, event, None, None, false, None)
            .await
            .unwrap();
        assert_eq!(response, "Final result content");

        let actions = dispatcher.actions.lock().await.clone();
        let has_tool_status = actions.iter().any(|action| match action {
            omon_gateway::OutboundAction::Stream { chunk, .. } => {
                chunk.content.contains("Running tool")
            }
            _ => false,
        });
        assert!(
            !has_tool_status,
            "Non-streaming (cron) run must not emit tool-call status chunks"
        );
        let stream_actions: Vec<_> = actions
            .iter()
            .filter(|a| matches!(a, omon_gateway::OutboundAction::Stream { .. }))
            .collect();
        assert!(
            stream_actions.is_empty(),
            "Non-streaming run should not dispatch any stream actions, got: {actions:?}"
        );

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn execute_emits_tool_status_when_stream_output_is_true() {
        let (base_url, server_handle) = spawn_two_turn_tool_llm_server().await;
        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) = build_test_runner(base_url, dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-1",
            None::<String>,
            "user-1",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event = omon_gateway::InboundEvent::message(session_key.clone(), "msg-1", "Run a tool");

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();
        assert_eq!(response, "Final result content");
        assert!(
            !response.contains("Running tool"),
            "Final assistant response must not contain tool status lines"
        );

        let actions = dispatcher.actions.lock().await.clone();
        let tool_status_actions: Vec<_> = actions
            .iter()
            .filter(|action| match action {
                omon_gateway::OutboundAction::Stream { chunk, .. } => {
                    chunk.content.contains("Running tool `mock_tool`")
                }
                _ => false,
            })
            .collect();
        assert!(
            !tool_status_actions.is_empty(),
            "Streaming run must emit tool-call status chunks"
        );

        // Verify final assistant stream chunk does NOT contain tool-status text
        let final_stream_actions: Vec<_> = actions
            .iter()
            .filter(|action| match action {
                omon_gateway::OutboundAction::Stream { chunk, .. } => {
                    chunk.is_final && chunk.content.contains("Final result content")
                }
                _ => false,
            })
            .collect();
        assert!(
            !final_stream_actions.is_empty(),
            "Expected final assistant prose stream chunk"
        );
        for action in &final_stream_actions {
            if let omon_gateway::OutboundAction::Stream { chunk, .. } = action {
                assert!(
                    !chunk.content.contains("Running tool"),
                    "Final stream chunk content polluted with tool-status: {:?}",
                    chunk.content
                );
            }
        }

        // Verify assistant message persisted in db has only model prose
        let history: Vec<(String, String)> = sqlx::query_as(
            "SELECT role, content FROM messages WHERE session_key = ? AND role = 'assistant' ORDER BY sequence ASC",
        )
        .bind(session_key.storage_key())
        .fetch_all(&runner.pool)
        .await
        .unwrap();
        for (_, content) in history {
            assert!(
                !content.contains("Running tool"),
                "Persisted assistant message polluted with tool-status: {content}"
            );
        }

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn execute_dispatches_typing_start_and_stop_for_streaming_turns_and_omits_for_non_streaming(
    ) {
        // 1. Streaming (interactive) turn: dispatches Typing { active: true } first,
        // intermediate stream / tool chunks, and Typing { active: false } last.
        let (base_url, server_handle) = spawn_two_turn_tool_llm_server().await;
        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) = build_test_runner(base_url, dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-1",
            None::<String>,
            "user-1",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event = omon_gateway::InboundEvent::message(session_key.clone(), "msg-1", "Run a tool");

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();
        assert_eq!(response, "Final result content");

        let actions = dispatcher.actions.lock().await.clone();
        assert!(
            actions.len() >= 2,
            "Expected at least start and stop typing actions, got {actions:?}"
        );

        assert_eq!(
            actions.first(),
            Some(&omon_gateway::OutboundAction::Typing {
                session: session_key.clone(),
                active: true,
            }),
            "First action dispatched in interactive turn must be Typing {{ active: true }}"
        );

        assert!(
            actions.contains(&omon_gateway::OutboundAction::Typing {
                session: session_key.clone(),
                active: false,
            }),
            "Interactive turn must dispatch Typing {{ active: false }}"
        );

        assert_eq!(
            actions.last(),
            Some(&omon_gateway::OutboundAction::React {
                session: session_key.clone(),
                message_id: "msg-1".into(),
                emoji: "✅".into(),
                remove_others: true,
            }),
            "Last action dispatched on success must be React with check mark"
        );

        let typing_stop_idx = actions
            .iter()
            .position(|a| {
                matches!(
                    a,
                    omon_gateway::OutboundAction::Typing { active: false, .. }
                )
            })
            .expect("typing stop action present");
        let intermediate_actions = &actions[1..typing_stop_idx];
        assert!(
            !intermediate_actions.is_empty(),
            "Expected intermediate stream/tool actions between typing start and stop"
        );
        assert!(
            intermediate_actions
                .iter()
                .all(|a| !matches!(a, omon_gateway::OutboundAction::Typing { .. })),
            "Intermediate actions must not be typing actions"
        );

        server_handle.await.unwrap();

        // 2. Non-streaming turn: dispatches neither start nor stop typing.
        let (base_url_non_stream, server_handle_non_stream) =
            spawn_two_turn_tool_llm_server().await;
        let dispatcher_non_stream = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner_non_stream, _dir_non_stream) =
            build_test_runner(base_url_non_stream, dispatcher_non_stream.clone()).await;

        let mut session_non_stream = omon_gateway::SessionContext::new(session_key.clone());
        let event_non_stream =
            omon_gateway::InboundEvent::message(session_key.clone(), "msg-2", "Run a tool");

        let response_non_stream = runner_non_stream
            .execute(
                &mut session_non_stream,
                event_non_stream,
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
        assert_eq!(response_non_stream, "Final result content");

        let non_stream_actions = dispatcher_non_stream.actions.lock().await.clone();
        let non_stream_typing_or_stream: Vec<_> = non_stream_actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    omon_gateway::OutboundAction::Typing { .. }
                        | omon_gateway::OutboundAction::Stream { .. }
                )
            })
            .collect();
        assert!(
            non_stream_typing_or_stream.is_empty(),
            "Non-streaming turn must not dispatch typing or stream actions, got: {non_stream_actions:?}"
        );
        assert_eq!(
            non_stream_actions.last(),
            Some(&omon_gateway::OutboundAction::React {
                session: session_key.clone(),
                message_id: "msg-2".into(),
                emoji: "✅".into(),
                remove_others: true,
            }),
            "Non-streaming turn must still dispatch success reaction"
        );
        server_handle_non_stream.await.unwrap();
    }

    async fn spawn_single_turn_llm_server(
        content: String,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let escaped = serde_json::to_string(&content).unwrap();
                let body = format!(
                    "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":{escaped}}}}}]}}\n\ndata: [DONE]\n\n"
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        (format!("http://{address}/v1"), server_handle)
    }

    #[tokio::test]
    async fn test_execute_uploads_media_directive_and_delivers_stripped_text() {
        let temp_dir = tempfile::tempdir().unwrap();
        let image_path = temp_dir.path().join("chart.png");
        std::fs::write(&image_path, b"fake png data").unwrap();

        let llm_text = format!(
            "Here is the generated chart:\nMEDIA:{}\nEnjoy!",
            image_path.display()
        );
        let (base_url, server_handle) = spawn_single_turn_llm_server(llm_text).await;
        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) = build_test_runner(base_url, dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-media",
            None::<String>,
            "user-media",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event =
            omon_gateway::InboundEvent::message(session_key.clone(), "msg-media", "Draw a chart");

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();
        assert!(response.contains("MEDIA:"));

        let actions = dispatcher.actions.lock().await.clone();

        // Verify UploadFile was dispatched for the local file
        let uploads: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                omon_gateway::OutboundAction::UploadFile { path, session: s } => {
                    Some((path.clone(), s.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].0, image_path);
        assert_eq!(uploads[0].1, session_key);

        // Verify final stream chunk delivered stripped text without MEDIA line
        let final_stream = actions.iter().find(|a| match a {
            omon_gateway::OutboundAction::Stream { chunk, .. } => chunk.is_final,
            _ => false,
        });
        assert!(final_stream.is_some());
        if let Some(omon_gateway::OutboundAction::Stream { chunk, .. }) = final_stream {
            assert!(!chunk.content.contains("MEDIA:"));
            assert_eq!(chunk.content, "Here is the generated chart:\nEnjoy!");
        }

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_execute_uploads_media_only_and_suppresses_empty_text_delivery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let report_path = temp_dir.path().join("report.pdf");
        std::fs::write(&report_path, b"fake pdf data").unwrap();

        let llm_text = format!("MEDIA:{}", report_path.display());
        let (base_url, server_handle) = spawn_single_turn_llm_server(llm_text).await;
        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) = build_test_runner(base_url, dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-media",
            None::<String>,
            "user-media",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event = omon_gateway::InboundEvent::message(
            session_key.clone(),
            "msg-media-only",
            "Generate report",
        );

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();
        assert!(response.contains("MEDIA:"));

        let actions = dispatcher.actions.lock().await.clone();

        // Verify UploadFile was dispatched
        let uploads: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                omon_gateway::OutboundAction::UploadFile { path, .. } => Some(path.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(uploads, vec![report_path]);

        // Verify no stream chunk was delivered with empty text
        let has_final_stream = actions.iter().any(|a| match a {
            omon_gateway::OutboundAction::Stream { chunk, .. } => chunk.is_final,
            _ => false,
        });
        assert!(
            !has_final_stream,
            "Must suppress empty text message when only media is delivered"
        );

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn execute_suppresses_reactions_when_processing_reactions_is_false() {
        let (base_url, server_handle) = spawn_two_turn_tool_llm_server().await;
        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (mut runner, _dir) = build_test_runner(base_url, dispatcher.clone()).await;
        runner.processing_reactions = false;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-1",
            None::<String>,
            "user-1",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event = omon_gateway::InboundEvent::message(session_key, "msg-1", "Run a tool");

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();
        assert_eq!(response, "Final result content");

        let actions = dispatcher.actions.lock().await.clone();
        assert!(
            actions
                .iter()
                .all(|a| !matches!(a, omon_gateway::OutboundAction::React { .. })),
            "Must not dispatch React actions when processing_reactions is false, got: {actions:?}"
        );
        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn execute_dispatches_typing_stop_on_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let body = "{\"error\": \"Internal server error\"}";
                let response = format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) =
            build_test_runner(format!("http://{address}/v1"), dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-1",
            None::<String>,
            "user-1",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event = omon_gateway::InboundEvent::message(session_key.clone(), "msg-err", "Hello");

        let result = runner
            .execute(&mut session, event, None, None, true, None)
            .await;
        assert!(result.is_err(), "Expected error from 500 response");

        let actions = dispatcher.actions.lock().await.clone();
        assert_eq!(
            actions,
            vec![
                omon_gateway::OutboundAction::Typing {
                    session: session_key.clone(),
                    active: true,
                },
                omon_gateway::OutboundAction::Typing {
                    session: session_key.clone(),
                    active: false,
                },
                omon_gateway::OutboundAction::React {
                    session: session_key,
                    message_id: "msg-err".into(),
                    emoji: "❌".into(),
                    remove_others: true,
                },
            ],
            "Both typing start and stop and failure reaction must be dispatched even when execution fails"
        );

        server_handle.await.unwrap();
    }

    #[test]
    fn repair_message_sequence_merges_consecutive_user_messages() {
        let msgs = vec![
            omon_gateway::ChatMessage::new("system", "sys prompt"),
            omon_gateway::ChatMessage::new("user", "first message"),
            omon_gateway::ChatMessage::new("user", "second message"),
        ];
        let repaired = super::repair_message_sequence(msgs);
        assert_eq!(repaired.len(), 2);
        assert_eq!(repaired[0].role, "system");
        assert_eq!(repaired[0].content, "sys prompt");
        assert_eq!(repaired[1].role, "user");
        assert_eq!(repaired[1].content, "first message\n\nsecond message");
    }

    #[test]
    fn repair_message_sequence_merges_consecutive_assistant_messages() {
        let msgs = vec![
            omon_gateway::ChatMessage::new("system", "sys prompt"),
            omon_gateway::ChatMessage::new("user", "question"),
            omon_gateway::ChatMessage::new("assistant", "partial answer"),
            omon_gateway::ChatMessage::new("assistant", "full answer"),
            omon_gateway::ChatMessage::new("user", "follow up"),
        ];
        let repaired = super::repair_message_sequence(msgs);
        assert_eq!(repaired.len(), 4);
        assert_eq!(repaired[0].role, "system");
        assert_eq!(repaired[1].role, "user");
        assert_eq!(repaired[1].content, "question");
        assert_eq!(repaired[2].role, "assistant");
        assert_eq!(repaired[2].content, "partial answer\n\nfull answer");
        assert_eq!(repaired[3].role, "user");
        assert_eq!(repaired[3].content, "follow up");
    }

    #[test]
    fn repair_message_sequence_keeps_system_messages_leading() {
        let msgs = vec![
            omon_gateway::ChatMessage::new("user", "user 1"),
            omon_gateway::ChatMessage::new("system", "system 1"),
            omon_gateway::ChatMessage::new("assistant", "assistant 1"),
            omon_gateway::ChatMessage::new("system", "system 2"),
            omon_gateway::ChatMessage::new("user", "user 2"),
        ];
        let repaired = super::repair_message_sequence(msgs);
        assert_eq!(repaired[0].role, "system");
        assert_eq!(repaired[0].content, "system 1\n\nsystem 2");
        assert_eq!(repaired[1].role, "user");
        assert_eq!(repaired[1].content, "user 1");
        assert_eq!(repaired[2].role, "assistant");
        assert_eq!(repaired[2].content, "assistant 1");
        assert_eq!(repaired[3].role, "user");
        assert_eq!(repaired[3].content, "user 2");
    }

    #[test]
    fn repair_message_sequence_strictly_alternates_and_ends_on_user() {
        let msgs = vec![
            omon_gateway::ChatMessage::new("system", "sys prompt"),
            omon_gateway::ChatMessage::new("user", "user 1"),
            omon_gateway::ChatMessage::new("assistant", "assistant 1"),
        ];
        let repaired = super::repair_message_sequence(msgs);
        assert_eq!(repaired.len(), 4);
        assert_eq!(repaired[0].role, "system");
        assert_eq!(repaired[1].role, "user");
        assert_eq!(repaired[1].content, "user 1");
        assert_eq!(repaired[2].role, "assistant");
        assert_eq!(repaired[2].content, "assistant 1");
        assert_eq!(repaired[3].role, "user");
        assert_eq!(repaired[3].content, "Continue");
    }

    #[test]
    fn repair_message_sequence_handles_leading_assistant_gracefully() {
        let msgs = vec![
            omon_gateway::ChatMessage::new("system", "sys prompt"),
            omon_gateway::ChatMessage::new("assistant", "unprompted greeting"),
            omon_gateway::ChatMessage::new("user", "my answer"),
        ];
        let repaired = super::repair_message_sequence(msgs);
        assert_eq!(repaired.len(), 4);
        assert_eq!(repaired[0].role, "system");
        assert_eq!(repaired[1].role, "user");
        assert_eq!(repaired[1].content, "Continue");
        assert_eq!(repaired[2].role, "assistant");
        assert_eq!(repaired[2].content, "unprompted greeting");
        assert_eq!(repaired[3].role, "user");
        assert_eq!(repaired[3].content, "my answer");
    }

    #[tokio::test]
    async fn test_emit_records_and_completes_delivery_obligation() {
        let (base_url, server_handle) = spawn_two_turn_tool_llm_server().await;
        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) = build_test_runner(base_url, dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-emit-obl",
            None::<String>,
            "user-1",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event =
            omon_gateway::InboundEvent::message(session_key.clone(), "msg-emit", "Hello obl");

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();
        assert_eq!(response, "Final result content");

        // Check delivery_obligations table
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT id, state, content FROM delivery_obligations WHERE channel_id = 'chan-emit-obl'",
        )
        .fetch_all(&runner.pool)
        .await
        .unwrap();

        assert!(
            !rows.is_empty(),
            "Expected at least 1 delivery obligation recorded"
        );
        // All recorded obligations should be marked 'delivered' after successful dispatch
        for (id, state, content) in rows {
            assert_eq!(
                state, "delivered",
                "obligation {id} state should be delivered"
            );
            assert!(content.contains("Final result content"));
        }

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_recover_pending_delivery_obligations_redispatches_dead_process_rows() {
        let pool = omon_gateway::storage::init_pool("sqlite::memory:")
            .await
            .unwrap();
        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let dead_pid = 999_999_i64;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            Some("guild-1"),
            "chan-recover",
            None::<String>,
            "user-recover",
        );
        super::ensure_agent_session(
            &pool,
            &omon_gateway::SessionContext::new(session_key.clone()),
        )
        .await
        .unwrap();

        let ledger = omon_gateway::ledger::DeliveryLedgerService::new(pool.clone());
        // 1. Pending obligation from dead process
        let _ = ledger
            .record_obligation("obl-rec-pending", &session_key, "first dead text")
            .await;
        sqlx::query("UPDATE delivery_obligations SET owner_pid = ? WHERE id = 'obl-rec-pending'")
            .bind(dead_pid)
            .execute(&pool)
            .await
            .unwrap();

        // 2. Attempting obligation from dead process (crashed mid-send)
        let _ = ledger
            .record_obligation("obl-rec-attempting", &session_key, "second dead text")
            .await;
        sqlx::query("UPDATE delivery_obligations SET state = 'attempting', owner_pid = ? WHERE id = 'obl-rec-attempting'")
            .bind(dead_pid)
            .execute(&pool)
            .await
            .unwrap();

        let recovered_count =
            super::recover_pending_delivery_obligations(&pool, dispatcher.clone())
                .await
                .unwrap();
        assert_eq!(recovered_count, 2);

        // Verify actions dispatched
        let actions = dispatcher.actions.lock().await.clone();
        assert_eq!(actions.len(), 2);

        let contents: Vec<String> = actions
            .iter()
            .map(|a| match a {
                omon_gateway::OutboundAction::Stream { chunk, .. } => chunk.content.clone(),
                _ => String::new(),
            })
            .collect();

        // Pending obligation should NOT have duplicate marker
        assert_eq!(contents[0], "first dead text");
        // Attempting obligation SHOULD have the recovered duplicate marker
        assert!(contents[1].contains("♻️ Recovered reply"));
        assert!(contents[1].contains("second dead text"));

        // Both obligations should now be marked 'delivered' in the database
        let obl1: omon_gateway::ledger::DeliveryObligation = ledger
            .get_obligation("obl-rec-pending")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(obl1.state, "delivered");
        let obl2: omon_gateway::ledger::DeliveryObligation = ledger
            .get_obligation("obl-rec-attempting")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(obl2.state, "delivered");
    }

    #[tokio::test]
    async fn test_recover_resume_pending_sessions_redispatches_unfinished_turn() {
        let pool = omon_gateway::storage::init_pool("sqlite::memory:")
            .await
            .unwrap();

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            Some("guild-1"),
            "chan-rec-turn",
            None::<String>,
            "user-rec-turn",
        );
        super::ensure_agent_session(
            &pool,
            &omon_gateway::SessionContext::new(session_key.clone()),
        )
        .await
        .unwrap();

        // Persist an unfinished user turn
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json)
             VALUES ('msg-unfin', ?, 'user', 'resume this prompt', '[]')",
        )
        .bind(session_key.storage_key())
        .execute(&pool)
        .await
        .unwrap();

        // Mark resume_pending
        omon_gateway::storage::mark_session_resume_pending(&pool, &session_key.storage_key())
            .await
            .unwrap();

        let pending = omon_gateway::storage::fetch_resume_pending_session_keys(&pool)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);

        struct MockRunner;
        #[async_trait::async_trait]
        impl omon_gateway::AgentRunner for MockRunner {
            async fn run(
                &self,
                _session: &mut omon_gateway::SessionContext,
                event: omon_gateway::InboundEvent,
            ) -> omon_gateway::Result<()> {
                assert_eq!(event.content, "resume this prompt");
                Ok(())
            }
        }

        let multiplexer = omon_gateway::SessionMultiplexer::new(
            pool.clone(),
            std::sync::Arc::new(MockRunner),
            omon_gateway::MultiplexerConfig::default(),
        );

        let recovered = super::recover_resume_pending_sessions(&pool, &multiplexer)
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        // Resume pending flag must now be cleared
        let pending_after = omon_gateway::storage::fetch_resume_pending_session_keys(&pool)
            .await
            .unwrap();
        assert!(pending_after.is_empty());

        // A second recovery sweep should find 0 sessions and not resume twice
        let recovered_second = super::recover_resume_pending_sessions(&pool, &multiplexer)
            .await
            .unwrap();
        assert_eq!(recovered_second, 0);
    }

    #[tokio::test]
    async fn test_suspended_session_suppresses_auto_resume() {
        let pool = omon_gateway::storage::init_pool("sqlite::memory:")
            .await
            .unwrap();
        let session_key = omon_gateway::SessionKey::new(
            "discord",
            Some("guild-1"),
            "chan-suspended",
            None::<String>,
            "user-suspended",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        session.state.suspended = true;
        super::ensure_agent_session(&pool, &session).await.unwrap();

        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json)
             VALUES ('msg-suspended', ?, 'user', 'should not be resumed', '[]')",
        )
        .bind(session_key.storage_key())
        .execute(&pool)
        .await
        .unwrap();

        // Mark session as resume_pending
        omon_gateway::storage::mark_session_resume_pending(&pool, &session_key.storage_key())
            .await
            .unwrap();

        struct PanicRunner;
        #[async_trait::async_trait]
        impl omon_gateway::AgentRunner for PanicRunner {
            async fn run(
                &self,
                _session: &mut omon_gateway::SessionContext,
                _event: omon_gateway::InboundEvent,
            ) -> omon_gateway::Result<()> {
                panic!("Suspended session must not be auto-resumed!");
            }
        }

        let multiplexer = omon_gateway::SessionMultiplexer::new(
            pool.clone(),
            std::sync::Arc::new(PanicRunner),
            omon_gateway::MultiplexerConfig::default(),
        );

        let recovered = super::recover_resume_pending_sessions(&pool, &multiplexer)
            .await
            .unwrap();
        assert_eq!(
            recovered, 0,
            "Suspended session must be skipped by recovery"
        );

        // The resume_pending flag should be cleared so it won't repeatedly re-attempt
        let pending = omon_gateway::storage::fetch_resume_pending_session_keys(&pool)
            .await
            .unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn test_restart_loop_guard_suppresses_crash_loop_auto_resume() {
        let pool = omon_gateway::storage::init_pool("sqlite::memory:")
            .await
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let guard_file = temp.path().join("restart_loop.json");
        let guard = omon_gateway::RestartLoopGuard::with_config(&guard_file, 3, 60);

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            Some("guild-1"),
            "chan-poison",
            None::<String>,
            "user-poison",
        );
        super::ensure_agent_session(
            &pool,
            &omon_gateway::SessionContext::new(session_key.clone()),
        )
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json)
             VALUES ('msg-poison', ?, 'user', 'crash daemon command', '[]')",
        )
        .bind(session_key.storage_key())
        .execute(&pool)
        .await
        .unwrap();

        omon_gateway::storage::mark_session_resume_pending(&pool, &session_key.storage_key())
            .await
            .unwrap();

        // Simulate 2 previous boots within window
        guard.record_boot_at(10.0);
        guard.record_boot_at(20.0);

        // 3rd boot at t=30.0 trips the breaker!
        let tripped = guard.check_and_record_at(30.0);
        assert!(tripped, "Breaker must be tripped on 3rd boot");

        let dispatch_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dispatch_counter_clone = dispatch_counter.clone();

        struct PoisonMockRunner {
            counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl omon_gateway::AgentRunner for PoisonMockRunner {
            async fn run(
                &self,
                _session: &mut omon_gateway::SessionContext,
                _event: omon_gateway::InboundEvent,
            ) -> omon_gateway::Result<()> {
                self.counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        }

        let multiplexer = omon_gateway::SessionMultiplexer::new(
            pool.clone(),
            std::sync::Arc::new(PoisonMockRunner {
                counter: dispatch_counter_clone,
            }),
            omon_gateway::MultiplexerConfig::default(),
        );

        // Because breaker is tripped, gateway startup skips auto-resume:
        let pending_count = omon_gateway::storage::count_resume_pending_sessions(&pool)
            .await
            .unwrap();
        assert_eq!(pending_count, 1);
        if !tripped {
            let _ = super::recover_resume_pending_sessions(&pool, &multiplexer).await;
        }

        // Verify that no task was dispatched
        assert_eq!(
            dispatch_counter.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        // Session remains marked resume_pending for manual resolution / next real user event
        assert_eq!(
            omon_gateway::storage::count_resume_pending_sessions(&pool)
                .await
                .unwrap(),
            1
        );
    }

    fn drive_stripper(chunks: &[&str]) -> String {
        let mut stripper = super::ThinkStripper::new();
        let mut out = String::new();
        for chunk in chunks {
            out.push_str(&stripper.push(chunk));
        }
        out.push_str(&stripper.finish());
        out
    }

    #[test]
    fn test_think_stripper_closed_pairs() {
        assert_eq!(
            drive_stripper(&["<think>reasoning</think>Hello world"]),
            "Hello world"
        );
        assert_eq!(
            drive_stripper(&["Hello <think>internal thoughts</think> world"]),
            "Hello  world"
        );
    }

    #[test]
    fn test_think_stripper_all_tag_variants_and_case() {
        for tag in [
            "think",
            "thinking",
            "reasoning",
            "thought",
            "reasoning_scratchpad",
        ] {
            let chunk = format!("<{tag}>secret scratchpad</{tag}>Visible answer");
            assert_eq!(drive_stripper(&[&chunk]), "Visible answer");
        }
        assert_eq!(drive_stripper(&["<THINK>mixed case</Think>Hello"]), "Hello");
        assert_eq!(
            drive_stripper(&["<Reasoning>planning</REASONING>Done"]),
            "Done"
        );
    }

    #[test]
    fn test_think_stripper_unterminated_open_drops_to_end() {
        assert_eq!(drive_stripper(&["<think>reasoning without close"]), "");
        assert_eq!(drive_stripper(&["Hello\n<think>reasoning text"]), "Hello\n");
        assert_eq!(
            drive_stripper(&["Hello\n  <thought>reasoning text"]),
            "Hello\n  "
        );
    }

    #[test]
    fn test_think_stripper_prose_mentioning_tag_preserved() {
        let text = "Use the <think> tag for reasoning models";
        assert_eq!(drive_stripper(&[text]), text);
    }

    #[test]
    fn test_think_stripper_orphan_close_tags() {
        assert_eq!(drive_stripper(&["Hello</think>world"]), "Helloworld");
        assert_eq!(drive_stripper(&["Hello</think> world"]), "Helloworld");
        assert_eq!(drive_stripper(&["A</think>B</thinking>C"]), "ABC");
    }

    #[test]
    fn test_think_stripper_split_tags_across_chunks() {
        assert_eq!(
            drive_stripper(&["<", "think>reasoning</think>done"]),
            "done"
        );
        assert_eq!(
            drive_stripper(&["<", "thi", "nk>internal thoughts</think>Answer"]),
            "Answer"
        );
        assert_eq!(
            drive_stripper(&["<think>reasoning<", "/think>after"]),
            "after"
        );
        assert_eq!(
            drive_stripper(&["<think>reasoning<", "/", "think>after"]),
            "after"
        );
    }

    #[test]
    fn test_think_stripper_non_tag_flushed() {
        assert_eq!(drive_stripper(&["Is 3 < 5?"]), "Is 3 < 5?");
        assert_eq!(drive_stripper(&["Is 3 <"]), "Is 3 <");
    }

    #[tokio::test]
    async fn test_execute_strips_think_blocks_from_streamed_responses() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let chunk1 = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"<think>Internal deep thought\"}}]}\n\n";
                let chunk2 = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\" and hidden steps</think>Hello user!\"}}]}\n\n";
                let chunk_done = "data: [DONE]\n\n";
                let body = format!("{chunk1}{chunk2}{chunk_done}");
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) =
            build_test_runner(format!("http://{address}/v1"), dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            None::<String>,
            "chan-think",
            None::<String>,
            "user-1",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        let event = omon_gateway::InboundEvent::message(session_key.clone(), "msg-think", "Hello!");

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();

        assert_eq!(response, "Hello user!");

        let actions = dispatcher.actions.lock().await.clone();
        for action in &actions {
            if let omon_gateway::OutboundAction::Stream { chunk, .. } = action {
                assert!(
                    !chunk.content.contains("Internal deep thought"),
                    "Dispatched stream chunk contained reasoning text: {:?}",
                    chunk.content
                );
                assert!(
                    !chunk.content.contains("<think>"),
                    "Dispatched stream chunk contained <think> tag: {:?}",
                    chunk.content
                );
            }
        }

        // Check persisted messages
        let history: Vec<(String, String)> = sqlx::query_as(
            "SELECT role, content FROM messages WHERE session_key = ? ORDER BY sequence ASC",
        )
        .bind(session_key.storage_key())
        .fetch_all(&runner.pool)
        .await
        .unwrap();

        let assistant_msg = history.iter().find(|(role, _)| role == "assistant");
        assert!(assistant_msg.is_some());
        let content = &assistant_msg.unwrap().1;
        assert_eq!(content, "Hello user!");

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_live_agent_runner_applies_session_custom_system_prompt_and_toolsets() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let captured_requests = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let captured = captured_requests.clone();

        let server_handle = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0_u8; 8192];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]).into_owned();
                captured.lock().await.push(req_str);

                let body = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Profile response\"}}]}\n\ndata: [DONE]\n\n";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let dispatcher = std::sync::Arc::new(CapturingDispatcher::default());
        let (runner, _dir) =
            build_test_runner(format!("http://{address}/v1"), dispatcher.clone()).await;

        let session_key = omon_gateway::SessionKey::new(
            "discord",
            Some("guild-routed"),
            "chan-profile",
            None::<String>,
            "user-1",
        );
        let mut session = omon_gateway::SessionContext::new(session_key.clone());
        session.state.system_prompt = Some("Custom profile prompt for this channel".into());
        session.state.enabled_toolsets = Some(vec!["terminal".into()]);

        let event = omon_gateway::InboundEvent::message(
            session_key.clone(),
            "msg-profile",
            "Hello profile",
        );

        let response = runner
            .execute(&mut session, event, None, None, true, None)
            .await
            .unwrap();

        assert_eq!(response, "Profile response");

        let reqs = captured_requests.lock().await.clone();
        assert!(!reqs.is_empty(), "LLM must receive request");
        let first_req = &reqs[0];
        assert!(
            first_req.contains("Custom profile prompt for this channel"),
            "Payload sent to LLM must contain the profile system prompt: {first_req}"
        );

        server_handle.await.unwrap();
    }

    #[test]
    fn test_load_cron_skills_missing_skill_resilience() {
        let temp_dir =
            std::env::temp_dir().join(format!("omon-test-skills-{}", uuid::Uuid::new_v4()));
        let skills_dir = temp_dir.join("skills");
        let skill_a_dir = skills_dir.join("skill_a");
        std::fs::create_dir_all(&skill_a_dir).unwrap();
        std::fs::write(skill_a_dir.join("SKILL.md"), "Instructions for skill A").unwrap();

        let mut extra = HashMap::new();
        extra.insert(
            "_omon_hermes_home".into(),
            serde_json::Value::String(temp_dir.to_string_lossy().into_owned()),
        );

        // 1. Partial missing skills with prompt -> warning prepended, job does not fail
        let partial_job = HermesJob {
            id: "job_partial".into(),
            name: "Partial".into(),
            prompt: "Summarize status".into(),
            skills: vec!["skill_a".into(), "skill_missing".into()],
            skill: None,
            model: None,
            provider: None,
            base_url: None,
            script: None,
            no_agent: false,
            context_from: None,
            schedule: omon_gateway::HermesSchedule::default(),
            schedule_display: "".into(),
            repeat: omon_gateway::HermesRepeat::default(),
            enabled: true,
            state: "".into(),
            created_at: None,
            next_run_at: None,
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_delivery_error: None,
            deliver: None,
            origin: None,
            enabled_toolsets: None,
            workdir: None,
            attach_to_session: None,
            extra: extra.clone(),
        };
        let loaded = load_cron_skills(&partial_job).unwrap();
        assert!(loaded.contains("⚠️ Skill(s) not found and skipped: skill_missing"));
        assert!(loaded.contains("[Skill: skill_a]\nInstructions for skill A"));

        // 2. All skills missing but non-empty prompt -> returns warning only, succeeds
        let missing_with_prompt_job = HermesJob {
            id: "job_missing".into(),
            name: "Missing".into(),
            prompt: "Do something anyway".into(),
            skills: vec!["missing_1".into(), "missing_2".into()],
            skill: None,
            model: None,
            provider: None,
            base_url: None,
            script: None,
            no_agent: false,
            context_from: None,
            schedule: omon_gateway::HermesSchedule::default(),
            schedule_display: "".into(),
            repeat: omon_gateway::HermesRepeat::default(),
            enabled: true,
            state: "".into(),
            created_at: None,
            next_run_at: None,
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_delivery_error: None,
            deliver: None,
            origin: None,
            enabled_toolsets: None,
            workdir: None,
            attach_to_session: None,
            extra: extra.clone(),
        };
        let loaded_warn = load_cron_skills(&missing_with_prompt_job).unwrap();
        assert_eq!(
            loaded_warn,
            "⚠️ Skill(s) not found and skipped: missing_1, missing_2"
        );

        // 3. All skills missing and EMPTY prompt -> fails with Config error
        let empty_prompt_missing_job = HermesJob {
            id: "job_empty".into(),
            name: "Empty".into(),
            prompt: "".into(),
            skills: vec!["missing_skill".into()],
            skill: None,
            model: None,
            provider: None,
            base_url: None,
            script: None,
            no_agent: false,
            context_from: None,
            schedule: omon_gateway::HermesSchedule::default(),
            schedule_display: "".into(),
            repeat: omon_gateway::HermesRepeat::default(),
            enabled: true,
            state: "".into(),
            created_at: None,
            next_run_at: None,
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_delivery_error: None,
            deliver: None,
            origin: None,
            enabled_toolsets: None,
            workdir: None,
            attach_to_session: None,
            extra,
        };
        let err = load_cron_skills(&empty_prompt_missing_job).unwrap_err();
        assert!(err
            .to_string()
            .contains("empty prompt and all skills were missing"));

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_resolve_skill_bundle_and_expansion() {
        let temp_dir =
            std::env::temp_dir().join(format!("omon-test-bundles-{}", uuid::Uuid::new_v4()));
        let skills_dir = temp_dir.join("skills");
        let bundles_dir = temp_dir.join("skill-bundles");
        std::fs::create_dir_all(&skills_dir).unwrap();
        std::fs::create_dir_all(&bundles_dir).unwrap();

        // 1. Create a regular single skill
        let s1_dir = skills_dir.join("skill_1");
        std::fs::create_dir_all(&s1_dir).unwrap();
        std::fs::write(s1_dir.join("SKILL.md"), "Skill 1 instructions").unwrap();

        // 2. Create another single skill
        let s2_dir = skills_dir.join("skill_2");
        std::fs::create_dir_all(&s2_dir).unwrap();
        std::fs::write(s2_dir.join("SKILL.md"), "Skill 2 instructions").unwrap();

        // 3. Create a YAML bundle in skill-bundles/
        std::fs::write(
            bundles_dir.join("backend_bundle.yaml"),
            "name: backend_bundle\nskills:\n  - skill_1\n  - skill_2\n",
        )
        .unwrap();

        // 4. Create a multi-skill directory bundle in skills/group_bundle/
        let group_dir = skills_dir.join("group_bundle");
        let member_a = group_dir.join("member_a");
        let member_b = group_dir.join("member_b");
        std::fs::create_dir_all(&member_a).unwrap();
        std::fs::create_dir_all(&member_b).unwrap();
        std::fs::write(member_a.join("SKILL.md"), "Member A instructions").unwrap();
        std::fs::write(member_b.join("SKILL.md"), "Member B instructions").unwrap();

        // Verify resolve_skill_bundle for YAML manifest bundle
        let resolved_yaml =
            super::resolve_skill_bundle(&skills_dir, Some(&temp_dir), "backend_bundle");
        assert_eq!(
            resolved_yaml,
            Some(vec!["skill_1".to_string(), "skill_2".to_string()])
        );

        // Verify resolve_skill_bundle for directory bundle
        let resolved_dir =
            super::resolve_skill_bundle(&skills_dir, Some(&temp_dir), "group_bundle");
        assert_eq!(
            resolved_dir,
            Some(vec![
                "group_bundle/member_a".to_string(),
                "group_bundle/member_b".to_string()
            ])
        );

        // Verify single skill returns None (not a bundle)
        let resolved_single = super::resolve_skill_bundle(&skills_dir, Some(&temp_dir), "skill_1");
        assert_eq!(resolved_single, None);

        // Verify load_cron_skills expands the bundle and loads skill bodies
        let mut extra = HashMap::new();
        extra.insert(
            "_omon_hermes_home".into(),
            serde_json::Value::String(temp_dir.to_string_lossy().into_owned()),
        );

        let bundle_job = HermesJob {
            id: "job_bundle".into(),
            name: "Bundle Job".into(),
            prompt: "Perform bundle task".into(),
            skills: vec!["backend_bundle".into()],
            skill: None,
            model: None,
            provider: None,
            base_url: None,
            script: None,
            no_agent: false,
            context_from: None,
            schedule: omon_gateway::HermesSchedule::default(),
            schedule_display: "".into(),
            repeat: omon_gateway::HermesRepeat::default(),
            enabled: true,
            state: "".into(),
            created_at: None,
            next_run_at: None,
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_delivery_error: None,
            deliver: None,
            origin: None,
            enabled_toolsets: None,
            workdir: None,
            attach_to_session: None,
            extra,
        };

        let loaded = load_cron_skills(&bundle_job).unwrap();
        assert!(
            loaded.contains("[Skill: skill_1]\nSkill 1 instructions"),
            "Loaded skills must include skill_1: {loaded}"
        );
        assert!(
            loaded.contains("[Skill: skill_2]\nSkill 2 instructions"),
            "Loaded skills must include skill_2: {loaded}"
        );

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_build_cron_llm_config_overrides() {
        let base = omon_gateway::LlmConfig::new(omon_gateway::LlmProvider::OpenAi, "gpt-4o-mini");

        // 1. Override model only
        let cfg1 = super::build_cron_llm_config(&base, None, None, Some("gpt-4o"));
        assert_eq!(cfg1.model, "gpt-4o");
        assert_eq!(cfg1.provider, omon_gateway::LlmProvider::OpenAi);
        assert_eq!(cfg1.base_url, None);

        // 2. Override provider only
        let cfg2 = super::build_cron_llm_config(&base, Some("anthropic"), None, None);
        assert_eq!(cfg2.provider, omon_gateway::LlmProvider::Anthropic);
        assert_eq!(cfg2.model, "gpt-4o-mini");

        // 3. Override base_url only
        let cfg3 =
            super::build_cron_llm_config(&base, None, Some("http://127.0.0.1:11434/api"), None);
        assert_eq!(cfg3.base_url.as_deref(), Some("http://127.0.0.1:11434/api"));
        assert_eq!(cfg3.model, "gpt-4o-mini");

        // 4. Override all three
        let cfg4 = super::build_cron_llm_config(
            &base,
            Some("deepseek"),
            Some("https://api.deepseek.com/v1"),
            Some("deepseek-chat"),
        );
        assert_eq!(cfg4.provider, omon_gateway::LlmProvider::DeepSeek);
        assert_eq!(
            cfg4.base_url.as_deref(),
            Some("https://api.deepseek.com/v1")
        );
        assert_eq!(cfg4.model, "deepseek-chat");

        // 5. Empty / whitespace overrides preserve base
        let cfg5 = super::build_cron_llm_config(&base, Some("  "), Some(""), Some(" "));
        assert_eq!(cfg5.model, "gpt-4o-mini");
        assert_eq!(cfg5.provider, omon_gateway::LlmProvider::OpenAi);
        assert_eq!(cfg5.base_url, None);
    }

    #[test]
    fn test_resolve_workspace_instructions() {
        let temp_dir =
            std::env::temp_dir().join(format!("omon-test-instructions-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();

        // 1. None when neither AGENTS.md nor CLAUDE.md exists
        assert_eq!(super::resolve_workspace_instructions(&temp_dir), None);

        // 2. Loads AGENTS.md
        std::fs::write(
            temp_dir.join("AGENTS.md"),
            "Rule 1: Always format code with cargo fmt.\n",
        )
        .unwrap();
        let loaded = super::resolve_workspace_instructions(&temp_dir).unwrap();
        assert_eq!(
            loaded,
            "[Workspace instructions]\nRule 1: Always format code with cargo fmt."
        );

        // 3. Precedence: AGENTS.md beats CLAUDE.md
        std::fs::write(
            temp_dir.join("CLAUDE.md"),
            "Claude rules that should be ignored.",
        )
        .unwrap();
        let loaded_prec = super::resolve_workspace_instructions(&temp_dir).unwrap();
        assert!(loaded_prec.contains("Rule 1: Always format code with cargo fmt."));
        assert!(!loaded_prec.contains("Claude rules"));

        // 4. CLAUDE.md when AGENTS.md removed
        std::fs::remove_file(temp_dir.join("AGENTS.md")).unwrap();
        let loaded_claude = super::resolve_workspace_instructions(&temp_dir).unwrap();
        assert_eq!(
            loaded_claude,
            "[Workspace instructions]\nClaude rules that should be ignored."
        );

        // 5. Truncation when exceeding 8000 chars
        let long_content = "X".repeat(8500);
        std::fs::write(temp_dir.join("CLAUDE.md"), &long_content).unwrap();
        let loaded_trunc = super::resolve_workspace_instructions(&temp_dir).unwrap();
        assert_eq!(
            loaded_trunc,
            format!("[Workspace instructions]\n{}", "X".repeat(8000))
        );

        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
