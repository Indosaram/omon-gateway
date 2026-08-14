use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use omon_gateway::storage::init_pool;
use omon_gateway::{
    AgentRunner, ChatMessage, CronJob, CronScheduler, CronTaskExecutor, CronTool, DiscordAdapter,
    DiscordEgress, FileTool, HermesJob, HermesStoreSynchronizer, InboundEvent, LlmClient,
    LlmConfig, LlmProvider, McpTool, MemoryStore, MultiplexerConfig, OmonError, OutboundAction,
    OutboundDispatcher, PoiseData, Result, ScaleToZero, SessionContext, SessionKey,
    SessionMultiplexer, TerminalTool, ToolDefinition, ToolRegistry,
};
use serde_json::json;
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_TOOL_ROUNDS: usize = 8;
const STREAM_BATCH_CHARS: usize = 1_500;

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

struct LiveAgentRunner {
    pool: SqlitePool,
    memory: MemoryStore,
    tools: ToolRegistry,
    llm: LlmClient,
    dispatcher: Arc<dyn OutboundDispatcher>,
    workspace_root: PathBuf,
}

impl LiveAgentRunner {
    async fn messages(
        &self,
        session: &SessionContext,
        event: &InboundEvent,
    ) -> Result<Vec<ChatMessage>> {
        let history: Vec<(String, String)> = sqlx::query_as(
            "SELECT role, content FROM messages WHERE session_key = ? ORDER BY created_at, id LIMIT 100",
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
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(session.key.storage_key())
        .bind(role)
        .bind(content)
        .bind(serde_json::to_string(&metadata).map_err(|error| OmonError::Database(error.to_string()))?)
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
            let _ = self
                .dispatcher
                .dispatch(OutboundAction::Typing {
                    session: session.key.clone(),
                })
                .await;
        }
        info!(session = %session.key, user = %event.session.user_id, content = %event.content, "Starting agent execution for message");
        let mut messages = self.messages(session, &event).await?;
        if !messages
            .iter()
            .any(|message| message.role == "user" && message.content == event.content)
        {
            messages.push(ChatMessage::new("user", &event.content));
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

        for round in 0..MAX_TOOL_ROUNDS {
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
                let result = tools.execute(&call.name, call.arguments.clone()).await;
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
            if round + 1 == MAX_TOOL_ROUNDS {
                return Err(OmonError::Llm(format!(
                    "model exceeded the {MAX_TOOL_ROUNDS}-round tool-call limit"
                )));
            }
        }
        unreachable!()
    }

    async fn emit(
        &self,
        session: &SessionContext,
        content: String,
        final_chunk: bool,
    ) -> Result<()> {
        self.dispatcher
            .dispatch(OutboundAction::Stream {
                session: session.key.clone(),
                chunk: omon_gateway::StreamChunk {
                    stream_id: Uuid::new_v4(),
                    sequence: 0,
                    content,
                    is_final: final_chunk,
                },
            })
            .await
    }
}

#[async_trait]
impl AgentRunner for LiveAgentRunner {
    async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()> {
        self.execute(session, event, None, None, true)
            .await
            .map(|_| ())
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
        "INSERT INTO sessions (session_key, platform, guild_id, channel_id, thread_id, user_id, state_json, created_at, updated_at)\n         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_key) DO NOTHING",
    )
    .bind(session.key.storage_key())
    .bind(&session.key.platform)
    .bind(&session.key.guild_id)
    .bind(&session.key.channel_id)
    .bind(&session.key.thread_id)
    .bind(&session.key.user_id)
    .bind(serde_json::to_string(&session.state).map_err(|error| OmonError::Database(error.to_string()))?)
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
            Some(run_cron_script(&hermes, script).await?)
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
        let execution_tools = build_cron_tools(&hermes, &self.runner.tools)?;
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
            let output = tokio::time::timeout(
                std::time::Duration::from_secs(15 * 60),
                tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(script)
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .map_err(|_| OmonError::ToolExecution(format!("cron script timed out for {}", job.id)))?
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
    let home = job
        .extra
        .get("_omon_hermes_home")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| OmonError::Config(format!("Hermes job {} is missing its home", job.id)))?;
    let root = home.join("skills");
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

fn find_skill_file(root: &std::path::Path, name: &str) -> Option<PathBuf> {
    let direct = root.join(name).join("SKILL.md");
    if direct.is_file() {
        return Some(direct);
    }
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|value| value == name) {
                    let candidate = path.join("SKILL.md");
                    if candidate.is_file() {
                        return Some(candidate);
                    }
                }
                pending.push(path);
            }
        }
    }
    None
}

fn build_cron_tools(job: &HermesJob, defaults: &ToolRegistry) -> Result<Option<ToolRegistry>> {
    let Some(workdir) = job.workdir.as_ref() else {
        return Ok(None);
    };
    if !workdir.is_absolute() || !workdir.is_dir() {
        return Err(OmonError::Config(format!(
            "Hermes job {} has invalid workdir {}",
            job.id,
            workdir.display()
        )));
    }
    let mut tools = defaults.clone();
    tools.register(TerminalTool::new(workdir));
    tools.register(FileTool::new(workdir));
    Ok(Some(tools))
}

async fn run_cron_script(job: &HermesJob, script: &str) -> Result<String> {
    let home = job
        .extra
        .get("_omon_hermes_home")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| OmonError::Config(format!("Hermes job {} is missing its home", job.id)))?;
    let path = {
        let candidate = PathBuf::from(script);
        if candidate.is_absolute() {
            candidate
        } else {
            home.join("scripts").join(candidate)
        }
    };
    let workdir = job.workdir.clone().unwrap_or_else(|| home.clone());
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

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let pool = init_pool(&config.database_url).await?;
    let memory = MemoryStore::new(pool.clone());

    let mut tools = ToolRegistry::new();
    tools.register(TerminalTool::new(&config.workspace_root));
    tools.register(FileTool::new(&config.workspace_root));
    tools.register(McpTool::default());
    tools.register(CronTool::new(pool.clone()));
    tools.register(omon_gateway::WebSearchTool);
    tools.register(omon_gateway::WebFetchTool);
    tools.register(omon_gateway::BrowserTool::default());
    let skills_dirs = vec![
        config.workspace_root.join(".hermes").join("skills"),
        env::var_os("HERMES_HOME")
            .map(PathBuf::from)
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".hermes")))
            .unwrap_or_default()
            .join("skills"),
    ];
    tools.register(omon_gateway::SkillsTool::new(skills_dirs));
    let tool_names = tools.names();

    let llm = LlmClient::new(config.llm_config(config.default_model.clone()))?;
    let shared_dispatcher = Arc::new(SharedDispatcher::default());
    let runner = Arc::new(LiveAgentRunner {
        pool: pool.clone(),
        memory,
        tools,
        llm,
        dispatcher: shared_dispatcher.clone(),
        workspace_root: config.workspace_root.clone(),
    });
    let multiplexer = SessionMultiplexer::with_dispatcher(
        pool.clone(),
        runner.clone(),
        Some(shared_dispatcher.clone()),
        MultiplexerConfig::default(),
    );
    let scale_to_zero = ScaleToZero::start(multiplexer.clone());

    let primary_token = config.discord_bot_tokens[0].clone();
    let http = Arc::new(serenity::http::Http::new(&primary_token));
    let discord_egress = Arc::new(DiscordEgress::new(http));
    shared_dispatcher.set(discord_egress.clone()).await;

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
    poise_data.free_response_channels = config.free_response_channels.clone();
    poise_data.allowed_users = config.allowed_users.clone();
    let adapter = DiscordAdapter::new(poise_data);

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
    use super::tool_enabled;

    #[test]
    fn maps_hermes_web_toolset_to_both_web_tools() {
        let enabled = vec!["web".to_string()];
        assert!(tool_enabled("web_search", Some(&enabled)));
        assert!(tool_enabled("web_fetch", Some(&enabled)));
        assert!(!tool_enabled("terminal", Some(&enabled)));
    }
}
