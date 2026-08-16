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
    prune_terminal_cron_runs, render_user_prompt, AgentRunner, ApprovalPolicy,
    AttachmentDownloader, ChatMessage, CronJob, CronScheduler, CronTaskExecutor, CronTool,
    DiscordAdapter, DiscordApprovalRequester, DiscordEgress, FileTool, HermesJob,
    HermesStoreSynchronizer, InboundEvent, LlmClient, LlmConfig, LlmProvider, McpTool, MemoryStore,
    MultiplexerConfig, OmonError, OutboundAction, OutboundDispatcher, PoiseData, Result,
    ScaleToZero, SessionContext, SessionKey, SessionMultiplexer, SmartApprovalGuard, TerminalTool,
    ToolDefinition, ToolRegistry,
};
use parking_lot::Mutex as ParkingMutex;
use serde_json::json;
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const STREAM_BATCH_CHARS: usize = 1_500;

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
    approval_policy: ApprovalPolicy,
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
            approval_policy: ApprovalPolicy::parse(optional_env("APPROVAL_MODE").as_deref()),
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
                    format!("- [{id}] Schedule `{expr}` | Payload: {payload}")
                })
                .collect::<Vec<_>>()
                .join("\n")
        };

        let mut messages = Vec::new();

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
                session.key,
                cron_summary,
                self.workspace_root.display()
            )
        };
        messages.push(ChatMessage::new("system", system_prompt));

        if !memories.is_empty() {
            let context = memories
                .into_iter()
                .map(|memory| format!("- {}", memory.content))
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
    ) -> Result<String> {
        if stream_output {
            self.streams.lock().remove(&session.key.storage_key());
            let _ = self
                .dispatcher
                .dispatch(OutboundAction::Typing {
                    session: session.key.clone(),
                })
                .await;
        }
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
        ensure_agent_session(&self.pool, session).await?;
        let tools = execution_tools.unwrap_or(&self.tools);
        let definitions = Self::tool_definitions(tools, enabled_tools);
        let llm = match session.state.active_model.as_deref() {
            Some(model) if model != self.llm.config().model => {
                let mut config = self.llm.config().clone();
                config.model = model.to_owned();
                LlmClient::new(config)?
            }
            _ => self.llm.clone(),
        };

        loop {
            let (mut stream, tool_calls) =
                llm.stream_with_tool_calls(&messages, &definitions).await?;
            let mut response = String::new();
            let mut pending = String::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if chunk.content.is_empty() {
                    continue;
                }
                response.push_str(&chunk.content);
                pending.push_str(&chunk.content);
                if stream_output && pending.chars().count() >= STREAM_BATCH_CHARS {
                    self.emit(session, std::mem::take(&mut pending), false)
                        .await?;
                }
            }
            let calls = tool_calls
                .await
                .map_err(|_| OmonError::Llm("LLM tool-call stream closed unexpectedly".into()))??;
            if calls.is_empty() {
                if stream_output {
                    if !pending.is_empty() {
                        self.emit(session, pending, true).await?;
                    } else if response.is_empty() {
                        self.emit(
                            session,
                            "The model returned an empty response.".into(),
                            true,
                        )
                        .await?;
                    } else {
                        self.emit(session, String::new(), true).await?;
                    }
                }
                self.persist_message(session, "assistant", &response, json!({}))
                    .await?;
                return Ok(response);
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
                    let status_msg = format!("\n\n⚙️ Running tool `{}`...", call.name);
                    let _ = self.emit(session, status_msg, false).await;
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
        let result = self
            .dispatcher
            .dispatch(OutboundAction::Stream {
                session: session.key.clone(),
                chunk,
            })
            .await;
        if final_chunk {
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
}

#[async_trait]
impl AgentRunner for LiveAgentRunner {
    async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()> {
        self.execute(session, event, None, None, true)
            .await
            .map(|_| ())
    }

    async fn cancel(&self, session: &SessionContext) -> Result<()> {
        let stream = self.streams.lock().remove(&session.key.storage_key());
        if let Some(stream) = stream {
            self.dispatcher
                .dispatch(OutboundAction::Stream {
                    session: session.key.clone(),
                    chunk: omon_gateway::StreamChunk {
                        stream_id: stream.stream_id,
                        sequence: stream.next_sequence,
                        content: stream.content,
                        is_final: true,
                    },
                })
                .await?;
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
        if hermes.prompt.trim().is_empty() && script_output.is_none() {
            return Err(OmonError::Config(format!(
                "Hermes job {} has neither prompt nor executable script",
                hermes.id
            )));
        }
        let mut prompt = load_cron_skills(&hermes)?;
        if !prompt.is_empty() && !hermes.prompt.trim().is_empty() {
            prompt.push_str("\n\n[Task]\n");
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
        self.runner
            .execute(
                &mut session,
                event,
                hermes.enabled_toolsets.as_deref(),
                execution_tools.as_ref(),
                false,
            )
            .await
            .map(Some)
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
    runner
        .execute(&mut session, event, enabled.as_deref(), None, false)
        .await
        .map(Some)
}

fn load_cron_skills(job: &HermesJob) -> Result<String> {
    let mut names = job.skills.clone();
    if let Some(skill) = job.skill.as_ref().filter(|skill| !names.contains(skill)) {
        names.push(skill.clone());
    }
    if names.is_empty() {
        return Ok(String::new());
    }
    let home = hermes_home(job)?;
    let root = canonical_directory(&home.join("skills"), "Hermes skills root")?;
    let mut assembled = String::new();
    for name in names {
        let path = find_skill_file(&root, &name).ok_or_else(|| {
            OmonError::Config(format!(
                "Hermes job {} references missing skill `{name}`",
                job.id
            ))
        })?;
        let content = std::fs::read_to_string(&path).map_err(|error| {
            OmonError::Config(format!("failed to read skill {}: {error}", path.display()))
        })?;
        if !assembled.is_empty() {
            assembled.push_str("\n\n");
        }
        assembled.push_str(&format!("[Skill: {name}]\n{content}"));
    }
    Ok(assembled)
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

    let approval_guard = SmartApprovalGuard::new();
    let approval_requester = Arc::new(DiscordApprovalRequester::new(
        approval_guard.clone(),
        std::time::Duration::from_secs(120),
    ));
    let mut tools = ToolRegistry::new();
    tools.register(TerminalTool::new(&config.workspace_root).with_approval(
        config.approval_policy,
        approval_requester.clone(),
        std::time::Duration::from_secs(125),
    ));
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
    tools.register(omon_gateway::SkillsTool::new(hermes_skill_dirs(
        &hermes_root,
        &home,
    )));
    let tool_names = tools.names();

    let llm = LlmClient::new(config.llm_config(config.default_model.clone()))?;
    let shared_dispatcher = Arc::new(SharedDispatcher::default());
    let runner = Arc::new(LiveAgentRunner {
        pool: pool.clone(),
        memory,
        tools: tools.clone(),
        llm,
        dispatcher: shared_dispatcher.clone(),
        workspace_root: config.workspace_root.clone(),
        streams: ParkingMutex::new(HashMap::new()),
    });
    let multiplexer = SessionMultiplexer::with_dispatcher(
        pool.clone(),
        runner.clone(),
        Some(shared_dispatcher.clone()),
        MultiplexerConfig::default(),
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
    let discord_egress = Arc::new(DiscordEgress::with_bot_clients(
        default_bot_id.clone(),
        bot_http_clients,
    )?);
    shared_dispatcher.set(discord_egress.clone()).await;
    approval_requester
        .set_dispatcher(discord_egress.clone())
        .await;

    let retention_days = cron_runs_retention_days_from_environment()?;
    let pruned = prune_terminal_cron_runs(&pool, retention_days, chrono::Utc::now()).await?;
    info!(pruned, retention_days, "pruned old terminal cron runs");

    let cron_sync = HermesStoreSynchronizer::from_environment(pool.clone())?;
    let imported = cron_sync.sync().await?;
    info!(imported, "synchronized Hermes cron stores");
    let scheduler = CronScheduler::with_dispatcher(
        pool.clone(),
        Arc::new(AgentCronExecutor {
            runner: runner.clone(),
        }),
        discord_egress,
    )
    .with_hermes_sync(cron_sync);
    scheduler.start().await;

    let mut poise_data = PoiseData::new(multiplexer, pool.clone());
    poise_data.tools = tool_names;
    poise_data.tool_registry = tools.clone();
    poise_data.free_response_channels = config.free_response_channels.clone();
    poise_data.allowed_users = config.allowed_users.clone();
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

    tokio::select! {
        Some(res) = join_set.join_next() => {
            if let Ok(Err(err)) = res {
                tracing::error!("Discord client exited with error: {:?}", err);
            }
        }
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|error| OmonError::Config(format!("failed to listen for Ctrl+C: {error}")))?;
            info!("shutdown signal received");
            for sm in shard_managers {
                sm.shutdown_all().await;
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

#[cfg(test)]
mod runner_tests {
    use std::fs;

    use clap::Parser;

    use super::{canonical_authorized_directory, hermes_skill_dirs, tool_enabled, Cli, Command};

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
        };
        (runner, temp_dir)
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
            .execute(&mut session, event, None, None, false)
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
        assert!(
            actions.is_empty(),
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
        let event = omon_gateway::InboundEvent::message(session_key, "msg-1", "Run a tool");

        let response = runner
            .execute(&mut session, event, None, None, true)
            .await
            .unwrap();
        assert_eq!(response, "Final result content");

        let actions = dispatcher.actions.lock().await.clone();
        let has_tool_status = actions.iter().any(|action| match action {
            omon_gateway::OutboundAction::Stream { chunk, .. } => {
                chunk.content.contains("Running tool `mock_tool`")
            }
            _ => false,
        });
        assert!(
            has_tool_status,
            "Streaming run must emit tool-call status chunks"
        );

        server_handle.await.unwrap();
    }
}
