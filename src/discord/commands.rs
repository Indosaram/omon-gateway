use std::time::{Duration, Instant};

use poise::serenity_prelude as serenity;
use sqlx::SqlitePool;

use super::approval::SmartApprovalGuard;
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
        }
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
    vec![model(), reset(), status(), tools()]
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
    ))
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
