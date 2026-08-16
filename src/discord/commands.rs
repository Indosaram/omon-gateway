use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use poise::serenity_prelude as serenity;
use sqlx::SqlitePool;

use super::approval::SmartApprovalGuard;
use super::attachments::AttachmentDownloader;
use crate::{OmonError, SessionKey, SessionMultiplexer};

pub type CommandError = Box<dyn std::error::Error + Send + Sync>;
pub type PoiseContext<'a> = poise::Context<'a, PoiseData, CommandError>;

#[derive(Clone)]
pub struct PoiseData {
    pub multiplexer: SessionMultiplexer,
    pub pool: SqlitePool,
    pub started_at: Instant,
    pub tools: Vec<String>,
    pub mcp_endpoints: Vec<String>,
    pub approvals: SmartApprovalGuard,
    pub free_response_channels: Vec<u64>,
    pub allowed_users: Vec<u64>,
    /// Thread IDs the bot is actively participating in (created or @mentioned).
    /// Kept in-memory: fast, zero-overhead, sufficient for active gateway runtime lifecycle.
    pub active_threads: Arc<RwLock<HashSet<u64>>>,
    pub auto_thread: bool,
    pub primary_bot_id: Option<u64>,
    pub attachment_downloader: Option<AttachmentDownloader>,
    pub tool_registry: crate::ToolRegistry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayStats {
    pub active_sessions: usize,
    pub memory_count: i64,
    pub uptime: Duration,
    pub ledger_count: i64,
}

impl PoiseData {
    pub fn new(multiplexer: SessionMultiplexer, pool: SqlitePool) -> Self {
        Self {
            multiplexer,
            pool,
            started_at: Instant::now(),
            tools: Vec::new(),
            mcp_endpoints: Vec::new(),
            approvals: SmartApprovalGuard::new(),
            free_response_channels: Vec::new(),
            allowed_users: Vec::new(),
            active_threads: Arc::new(RwLock::new(HashSet::new())),
            auto_thread: false,
            primary_bot_id: None,
            attachment_downloader: None,
            tool_registry: crate::ToolRegistry::new(),
        }
    }

    pub fn mark_thread_active(&self, thread_id: u64) {
        if let Ok(mut set) = self.active_threads.write() {
            set.insert(thread_id);
        }
    }

    pub fn is_thread_active(&self, thread_id: u64) -> bool {
        self.active_threads
            .read()
            .map(|set| set.contains(&thread_id))
            .unwrap_or(false)
    }

    pub async fn stats(&self) -> Result<GatewayStats, sqlx::Error> {
        let memory_count = sqlx::query_scalar("SELECT COUNT(*) FROM memories")
            .fetch_one(&self.pool)
            .await?;
        let ledger_count = sqlx::query_scalar("SELECT COUNT(*) FROM delivery_ledger")
            .fetch_one(&self.pool)
            .await?;
        Ok(GatewayStats {
            active_sessions: self.multiplexer.active_sessions(),
            memory_count,
            uptime: self.started_at.elapsed(),
            ledger_count,
        })
    }
}

pub fn all() -> Vec<poise::Command<PoiseData, CommandError>> {
    vec![model(), reset(), stop(), status(), tools(), skill(), cron()]
}

pub fn is_user_allowed(allowed_users: &[u64], user_id: u64) -> bool {
    allowed_users.is_empty() || allowed_users.contains(&user_id)
}

pub async fn command_check(ctx: PoiseContext<'_>) -> Result<bool, CommandError> {
    if is_user_allowed(&ctx.data().allowed_users, ctx.author().id.get()) {
        return Ok(true);
    }

    ctx.send(
        poise::CreateReply::default()
            .content("You are not authorized to use this command.")
            .ephemeral(true),
    )
    .await?;
    Ok(false)
}

#[poise::command(slash_command)]
/// Execute or inspect an OMO skill
pub async fn skill(
    ctx: PoiseContext<'_>,
    #[description = "Skill action: list, search, read, run"] action: String,
    #[description = "Skill name or query"] name_or_query: Option<String>,
) -> Result<(), CommandError> {
    ctx.defer().await?;
    let data = ctx.data();
    let query_val = name_or_query.clone().unwrap_or_default();

    match action.as_str() {
        "list" => {
            let res = data
                .tool_registry
                .execute("skills", serde_json::json!({"action": "list"}))
                .await;
            match res {
                Ok(val) => {
                    let total = val
                        .get("total_skills")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let skills = val
                        .get("skills")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let list_str = skills
                        .iter()
                        .take(30)
                        .filter_map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    ctx.say(format!("📚 **Available OMO Skills** ({total} total):\n`{list_str}`\n*(Use `/skill action:read name_or_query:<skill_name>` to inspect)*")).await?;
                }
                Err(e) => {
                    ctx.say(format!("❌ Failed to list skills: {e}")).await?;
                }
            }
        }
        "search" => {
            let res = data
                .tool_registry
                .execute(
                    "skills",
                    serde_json::json!({"action": "search", "query": query_val}),
                )
                .await;
            match res {
                Ok(val) => {
                    let count = val.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                    let matches = val
                        .get("matches")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let list_str = matches
                        .iter()
                        .filter_map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join("\n- ");
                    ctx.say(format!("🔍 **Skill Search Results for `{query_val}`** ({count} matches):\n- {list_str}")).await?;
                }
                Err(e) => {
                    ctx.say(format!("❌ Search failed: {e}")).await?;
                }
            }
        }
        "read" => {
            let res = data
                .tool_registry
                .execute(
                    "skills",
                    serde_json::json!({"action": "read", "name": query_val}),
                )
                .await;
            match res {
                Ok(val) => {
                    let content = val
                        .get("content")
                        .and_then(|v| v.as_str())
                        .unwrap_or("No content");
                    let preview = if content.len() > 1800 {
                        &content[..1800]
                    } else {
                        content
                    };
                    ctx.say(format!(
                        "📖 **Skill: `{query_val}`**\n```markdown\n{preview}\n```"
                    ))
                    .await?;
                }
                Err(e) => {
                    ctx.say(format!("❌ Could not read skill `{query_val}`: {e}"))
                        .await?;
                }
            }
        }
        "run" => {
            ctx.say(format!(
                "🚀 Injecting skill `{query_val}` into current OMO session..."
            ))
            .await?;
            let session_key = session_key(ctx).await?;
            let prompt = format!("Execute skill: {}", query_val);
            let event = crate::InboundEvent::message(session_key, ctx.id().to_string(), prompt);
            let _ = data.multiplexer.route(event).await;
        }
        _ => {
            ctx.say("Usage: `/skill action:<list|search|read|run> name_or_query:<name>`")
                .await?;
        }
    }
    Ok(())
}

#[poise::command(slash_command)]
/// Inspect or manage background cron jobs
pub async fn cron(
    ctx: PoiseContext<'_>,
    #[description = "Cron action: list, add, delete"] action: Option<String>,
) -> Result<(), CommandError> {
    ctx.defer().await?;
    let data = ctx.data();
    let act = action.unwrap_or_else(|| "list".to_string());

    match act.as_str() {
        "list" => {
            let res = data
                .tool_registry
                .execute("cron", serde_json::json!({"action": "list"}))
                .await;
            match res {
                Ok(val) => {
                    let count = val.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                    let jobs = val
                        .get("cron_jobs")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let mut lines = Vec::new();
                    for j in jobs.iter().take(15) {
                        let id = j.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                        let expr = j
                            .get("expression")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        let next = j
                            .get("next_run_at")
                            .and_then(|v| v.as_str())
                            .unwrap_or("paused");
                        lines.push(format!("• `{id}`: `{expr}` (Next: `{next}`)"));
                    }
                    let body = lines.join("\n");
                    ctx.say(format!(
                        "⏰ **Active OMO Cron Jobs** ({count} total):\n{body}"
                    ))
                    .await?;
                }
                Err(e) => {
                    ctx.say(format!("❌ Failed to list cron jobs: {e}")).await?;
                }
            }
        }
        _ => {
            ctx.say("Usage: `/cron [action:list]`").await?;
        }
    }
    Ok(())
}

#[poise::command(slash_command)]
/// Switch the model used by this Discord session.
pub async fn model(
    ctx: PoiseContext<'_>,
    #[description = "Model name"] name: String,
) -> Result<(), CommandError> {
    let key = session_key(ctx).await?;
    ensure_session(&ctx.data().pool, &key).await?;
    let state_json: String =
        sqlx::query_scalar("SELECT state_json FROM sessions WHERE session_key = ?")
            .bind(key.storage_key())
            .fetch_one(&ctx.data().pool)
            .await?;
    let mut state: crate::SessionState = serde_json::from_str(&state_json)?;
    state.active_model = Some(name.clone());
    sqlx::query(
        "UPDATE sessions SET state_json = ?, updated_at = CURRENT_TIMESTAMP WHERE session_key = ?",
    )
    .bind(serde_json::to_string(&state)?)
    .bind(key.storage_key())
    .execute(&ctx.data().pool)
    .await?;
    ctx.say(format!("Model switched to `{name}`.")).await?;
    Ok(())
}

#[poise::command(slash_command)]
/// Clear conversation context and persistent memory for this session.
pub async fn reset(ctx: PoiseContext<'_>) -> Result<(), CommandError> {
    let key = session_key(ctx).await?;
    let storage_key = key.storage_key();
    let mut transaction = ctx.data().pool.begin().await?;
    sqlx::query("DELETE FROM messages WHERE session_key = ?")
        .bind(&storage_key)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM memories WHERE session_key = ?")
        .bind(&storage_key)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("UPDATE sessions SET state_json = '{}', updated_at = CURRENT_TIMESTAMP WHERE session_key = ?")
        .bind(&storage_key)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    ctx.say("Session context and memory cleared.").await?;
    Ok(())
}

#[poise::command(slash_command, prefix_command)]
/// Stop the active agent turn for this Discord session.
pub async fn stop(ctx: PoiseContext<'_>) -> Result<(), CommandError> {
    let key = session_key(ctx).await?;
    let message = if ctx.data().multiplexer.stop(&key).await? {
        "Stopped the active turn."
    } else {
        "No active turn to stop."
    };
    ctx.say(message).await?;
    Ok(())
}

#[poise::command(slash_command)]
/// Show gateway health and persistence statistics.
pub async fn status(ctx: PoiseContext<'_>) -> Result<(), CommandError> {
    let stats = ctx.data().stats().await?;
    ctx.say(format!(
        "Active sessions: {}\nMemory entries: {}\nUptime: {}s\nLedger entries: {}",
        stats.active_sessions,
        stats.memory_count,
        stats.uptime.as_secs(),
        stats.ledger_count
    ))
    .await?;
    Ok(())
}

#[poise::command(slash_command)]
/// List configured tools and MCP endpoints.
pub async fn tools(ctx: PoiseContext<'_>) -> Result<(), CommandError> {
    let tools = if ctx.data().tools.is_empty() {
        "(none)".to_owned()
    } else {
        ctx.data().tools.join(", ")
    };
    let endpoints = if ctx.data().mcp_endpoints.is_empty() {
        "(none)".to_owned()
    } else {
        ctx.data().mcp_endpoints.join("\n")
    };
    ctx.say(format!("Tools: {tools}\nMCP endpoints:\n{endpoints}"))
        .await?;
    Ok(())
}

async fn session_key(ctx: PoiseContext<'_>) -> Result<SessionKey, CommandError> {
    let guild_id = ctx.guild_id().map(|id| id.to_string());
    let channel_id = ctx.channel_id();
    let thread_id = if guild_id.is_some() {
        match channel_id.to_channel(ctx.serenity_context()).await? {
            serenity::Channel::Guild(channel) if is_thread(channel.kind) => {
                Some(channel_id.to_string())
            }
            _ => None,
        }
    } else {
        None
    };
    Ok(SessionKey::new(
        "discord",
        guild_id,
        channel_id.to_string(),
        thread_id,
        ctx.author().id.to_string(),
    )
    .with_bot_id(ctx.serenity_context().cache.current_user().id.to_string()))
}

fn is_thread(kind: serenity::ChannelType) -> bool {
    matches!(
        kind,
        serenity::ChannelType::NewsThread
            | serenity::ChannelType::PublicThread
            | serenity::ChannelType::PrivateThread
    )
}

async fn ensure_session(pool: &SqlitePool, key: &SessionKey) -> Result<(), OmonError> {
    sqlx::query(
        "INSERT INTO sessions (session_key, platform, guild_id, channel_id, thread_id, user_id, state_json)
         VALUES (?, ?, ?, ?, ?, ?, '{}') ON CONFLICT(session_key) DO NOTHING",
    )
    .bind(key.storage_key())
    .bind(&key.platform)
    .bind(&key.guild_id)
    .bind(&key.channel_id)
    .bind(&key.thread_id)
    .bind(&key.user_id)
    .execute(pool)
    .await?;
    Ok(())
}
