use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use poise::serenity_prelude as serenity;
use sqlx::SqlitePool;

use super::approval::SmartApprovalGuard;
use super::attachments::AttachmentDownloader;
use crate::{ChatMessage, LlmClient, OmonError, ProfileRouter, SessionKey, SessionMultiplexer};

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
    pub allowed_roles: Vec<u64>,
    pub allow_all_users: bool,
    pub thread_sessions_per_user: bool,
    pub allowed_channels: Vec<u64>,
    pub ignored_channels: Vec<u64>,
    /// Thread IDs the bot is actively participating in (created or @mentioned).
    /// Kept in-memory: fast, zero-overhead, sufficient for active gateway runtime lifecycle.
    pub active_threads: Arc<RwLock<HashSet<u64>>>,
    pub thread_require_mention: bool,
    pub auto_thread: bool,
    pub channel_context: bool,
    pub channel_context_limit: usize,
    pub processing_reactions: bool,
    pub approval_mentions: bool,
    pub approvals_deny: Vec<String>,
    pub runtime_footer: bool,
    pub primary_bot_id: Option<u64>,
    pub attachment_downloader: Option<AttachmentDownloader>,
    pub tool_registry: crate::ToolRegistry,
    pub llm: Option<LlmClient>,
    pub profile_router: ProfileRouter,
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
        let profile_router = multiplexer.profile_router().clone();
        Self {
            multiplexer,
            pool,
            started_at: Instant::now(),
            tools: Vec::new(),
            mcp_endpoints: Vec::new(),
            approvals: SmartApprovalGuard::new(),
            free_response_channels: Vec::new(),
            allowed_users: Vec::new(),
            allowed_roles: Vec::new(),
            allow_all_users: false,
            thread_sessions_per_user: true,
            allowed_channels: Vec::new(),
            ignored_channels: Vec::new(),
            active_threads: Arc::new(RwLock::new(HashSet::new())),
            thread_require_mention: false,
            auto_thread: false,
            channel_context: false,
            channel_context_limit: 10,
            processing_reactions: true,
            approval_mentions: false,
            approvals_deny: Vec::new(),
            runtime_footer: false,
            primary_bot_id: None,
            attachment_downloader: None,
            tool_registry: crate::ToolRegistry::new(),
            llm: None,
            profile_router,
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
    vec![
        model(),
        reset(),
        stop(),
        status(),
        tools(),
        skill(),
        skills(),
        memory(),
        cron(),
        steer(),
        undo(),
        retry(),
        compress(),
        title(),
        thread(),
        deny(),
        yolo(),
    ]
}

pub fn is_user_allowed(allowed_users: &[u64], user_id: u64) -> bool {
    is_user_authorized(user_id, &[], allowed_users, &[], false)
}

/// Evaluates user authorization based on user ID allowlist, role membership, and allow-all bypass.
pub fn is_user_authorized(
    user_id: u64,
    user_roles: &[u64],
    allowed_users: &[u64],
    allowed_roles: &[u64],
    allow_all_users: bool,
) -> bool {
    if allow_all_users {
        return true;
    }
    if allowed_users.is_empty() && allowed_roles.is_empty() {
        return true;
    }
    if !allowed_users.is_empty() && allowed_users.contains(&user_id) {
        return true;
    }
    if !allowed_roles.is_empty() && user_roles.iter().any(|r| allowed_roles.contains(r)) {
        return true;
    }
    false
}

pub async fn command_check(ctx: PoiseContext<'_>) -> Result<bool, CommandError> {
    let data = ctx.data();
    let user_roles: Vec<u64> = match ctx.author_member().await {
        Some(member) => member.roles.iter().map(|r| r.get()).collect(),
        None => Vec::new(),
    };
    if is_user_authorized(
        ctx.author().id.get(),
        &user_roles,
        &data.allowed_users,
        &data.allowed_roles,
        data.allow_all_users,
    ) {
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
    #[description = "Skill action: list, search, read, run, pending, approve, reject"]
    action: String,
    #[description = "Skill name, query, or pending ID"] name_or_query: Option<String>,
) -> Result<(), CommandError> {
    skill_dispatch(ctx, &action, name_or_query).await
}

#[poise::command(slash_command)]
/// Execute, inspect, or manage capability skills
pub async fn skills(
    ctx: PoiseContext<'_>,
    #[description = "Skill action: list, search, read, run, pending, approve, reject"]
    action: Option<String>,
    #[description = "Skill name, query, or pending ID"] name_or_query: Option<String>,
) -> Result<(), CommandError> {
    let act = action.unwrap_or_else(|| "list".to_string());
    skill_dispatch(ctx, &act, name_or_query).await
}

async fn skill_dispatch(
    ctx: PoiseContext<'_>,
    action: &str,
    name_or_query: Option<String>,
) -> Result<(), CommandError> {
    ctx.defer().await?;
    let data = ctx.data();
    let query_val = name_or_query.clone().unwrap_or_default();
    let pool = &data.pool;

    match action {
        "pending" => {
            let pending = crate::storage::list_pending_writes(pool, Some("skill")).await?;
            if pending.is_empty() {
                ctx.say("No pending skill writes.").await?;
                return Ok(());
            }
            let mut lines = vec![format!("**Pending skill writes ({})**:", pending.len())];
            for item in pending {
                let payload_val: serde_json::Value =
                    serde_json::from_str(&item.payload).unwrap_or(serde_json::json!({}));
                let name = payload_val
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&item.id);
                lines.push(format!("• `{}` — skill `{}`", item.id, name));
            }
            lines.push("\n*Apply: `/skills approve <id>` | Reject: `/skills reject <id>`*".into());
            ctx.say(lines.join("\n")).await?;
        }
        "approve" | "apply" => {
            let id = query_val.trim();
            if id.is_empty() {
                ctx.say(
                    "Please specify the pending skill write ID to approve: `/skills approve <id>`",
                )
                .await?;
                return Ok(());
            }
            match crate::storage::approve_pending_write(pool, id, None).await? {
                Some(msg) => {
                    ctx.say(format!("✅ {msg}")).await?;
                }
                None => {
                    ctx.say(format!("Pending skill write `{id}` not found."))
                        .await?;
                }
            }
        }
        "reject" | "deny" | "drop" => {
            let id = query_val.trim();
            if id.is_empty() {
                ctx.say(
                    "Please specify the pending skill write ID to reject: `/skills reject <id>`",
                )
                .await?;
                return Ok(());
            }
            if crate::storage::reject_pending_write(pool, id).await? {
                ctx.say(format!(
                    "🗑️ Rejected and discarded pending skill write `{id}`."
                ))
                .await?;
            } else {
                ctx.say(format!("Pending skill write `{id}` not found."))
                    .await?;
            }
        }
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
            ctx.say("Usage: `/skills action:<list|search|read|run|pending|approve|reject> name_or_query:<name_or_id>`")
                .await?;
        }
    }
    Ok(())
}

#[poise::command(slash_command)]
/// Inspect or review persistent memories and pending memory writes
pub async fn memory(
    ctx: PoiseContext<'_>,
    #[description = "Memory action: pending, approve, reject, list"] action: Option<String>,
    #[description = "Pending write ID or query"] id_or_query: Option<String>,
) -> Result<(), CommandError> {
    ctx.defer().await?;
    let action_str = action.unwrap_or_else(|| "list".to_string()).to_lowercase();
    let pool = &ctx.data().pool;
    let key = session_key(ctx).await?;

    match action_str.as_str() {
        "pending" => {
            let pending = crate::storage::list_pending_writes(pool, Some("memory")).await?;
            if pending.is_empty() {
                ctx.say("No pending memory writes.").await?;
                return Ok(());
            }
            let mut lines = vec![format!("**Pending memory writes ({})**:", pending.len())];
            for item in pending {
                let payload_val: serde_json::Value =
                    serde_json::from_str(&item.payload).unwrap_or(serde_json::json!({}));
                let content = payload_val
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&item.payload);
                let preview = preview_text(content, 80);
                lines.push(format!("• `{}` — `{}`", item.id, preview));
            }
            lines.push("\n*Apply: `/memory approve <id>` | Reject: `/memory reject <id>`*".into());
            ctx.say(lines.join("\n")).await?;
        }
        "approve" | "apply" => {
            let Some(id) = id_or_query else {
                ctx.say("Please specify the pending write ID to approve: `/memory approve <id>`")
                    .await?;
                return Ok(());
            };
            match crate::storage::approve_pending_write(pool, &id, None).await? {
                Some(msg) => {
                    ctx.say(format!("✅ {msg}")).await?;
                }
                None => {
                    ctx.say(format!("Pending write `{id}` not found.")).await?;
                }
            }
        }
        "reject" | "deny" | "drop" => {
            let Some(id) = id_or_query else {
                ctx.say("Please specify the pending write ID to reject: `/memory reject <id>`")
                    .await?;
                return Ok(());
            };
            if crate::storage::reject_pending_write(pool, &id).await? {
                ctx.say(format!("🗑️ Rejected and discarded pending write `{id}`."))
                    .await?;
            } else {
                ctx.say(format!("Pending write `{id}` not found.")).await?;
            }
        }
        _ => {
            let memories: Vec<(String, String)> = sqlx::query_as(
                "SELECT id, content FROM memories WHERE session_key = ? ORDER BY updated_at DESC LIMIT 10",
            )
            .bind(key.storage_key())
            .fetch_all(pool)
            .await
            .unwrap_or_default();

            if memories.is_empty() {
                ctx.say("No memories stored for this session.").await?;
            } else {
                let mut lines = vec![format!("**Stored memories ({})**:", memories.len())];
                for (id, content) in memories {
                    let preview = preview_text(&content, 100);
                    lines.push(format!("• `{id}`: {preview}"));
                }
                ctx.say(lines.join("\n")).await?;
            }
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
    let user_id = if thread_id.is_some() && !ctx.data().thread_sessions_per_user {
        "shared".to_string()
    } else {
        ctx.author().id.to_string()
    };
    Ok(SessionKey::new(
        "discord",
        guild_id,
        channel_id.to_string(),
        thread_id,
        user_id,
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

#[poise::command(slash_command)]
/// Inject steering guidance into the session's ongoing or next turn.
pub async fn steer(
    ctx: PoiseContext<'_>,
    #[description = "Steering guidance for the agent"] text: String,
) -> Result<(), CommandError> {
    let key = session_key(ctx).await?;
    let prompt = format_steer_prompt(&text);
    let event = crate::InboundEvent::message(key, ctx.id().to_string(), prompt);
    ctx.data().multiplexer.route(event).await?;
    ctx.send(
        poise::CreateReply::default()
            .content(format!("🎯 Steering guidance queued: `{text}`"))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command)]
/// Remove the most recent exchange from the session conversation history.
pub async fn undo(ctx: PoiseContext<'_>) -> Result<(), CommandError> {
    let key = session_key(ctx).await?;
    let storage_key = key.storage_key();

    match undo_last_exchange(&ctx.data().pool, &storage_key).await? {
        Some(result) => {
            let user_preview = preview_text(&result.user_content, 120);
            let response = if let Some(assistant_content) = result.assistant_content {
                let assistant_preview = preview_text(&assistant_content, 120);
                format!(
                    "↩️ **Undid last exchange** ({} messages removed):\n• **User:** `{user_preview}`\n• **Assistant:** `{assistant_preview}`",
                    result.deleted_count
                )
            } else {
                format!(
                    "↩️ **Undid last message** ({} message removed):\n• **User:** `{user_preview}`",
                    result.deleted_count
                )
            };
            ctx.say(response).await?;
        }
        None => {
            ctx.say("No conversation history to undo.").await?;
        }
    }
    Ok(())
}

#[poise::command(slash_command)]
/// Re-run the last user message in this session.
pub async fn retry(ctx: PoiseContext<'_>) -> Result<(), CommandError> {
    let key = session_key(ctx).await?;
    let storage_key = key.storage_key();

    let user_row: Option<(i64, String)> = sqlx::query_as(
        "SELECT sequence, content FROM messages WHERE session_key = ? AND role = 'user' ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&storage_key)
    .fetch_optional(&ctx.data().pool)
    .await?;

    let Some((user_seq, user_content)) = user_row else {
        ctx.send(
            poise::CreateReply::default()
                .content("No previous user message found to retry.")
                .ephemeral(true),
        )
        .await?;
        return Ok(());
    };

    sqlx::query("DELETE FROM messages WHERE session_key = ? AND sequence >= ?")
        .bind(&storage_key)
        .bind(user_seq)
        .execute(&ctx.data().pool)
        .await?;

    let preview = preview_text(&user_content, 100);
    let event = crate::InboundEvent::message(key, ctx.id().to_string(), user_content);
    ctx.data().multiplexer.route(event).await?;

    ctx.send(
        poise::CreateReply::default()
            .content(format!("🔄 Retrying last message: `{preview}`"))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command)]
/// Compress conversation history into a concise summary.
pub async fn compress(ctx: PoiseContext<'_>) -> Result<(), CommandError> {
    ctx.defer().await?;
    let key = session_key(ctx).await?;
    let storage_key = key.storage_key();

    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT sequence, role, content FROM messages WHERE session_key = ? ORDER BY sequence ASC",
    )
    .bind(&storage_key)
    .fetch_all(&ctx.data().pool)
    .await?;

    if rows.len() < 2 {
        ctx.say("Conversation history is too short to compress.")
            .await?;
        return Ok(());
    }

    let history: Vec<(String, String)> = rows.into_iter().map(|(_, r, c)| (r, c)).collect();
    let chars_before: usize = history.iter().map(|(_, c)| c.len()).sum();
    let prompt = build_compression_prompt(&history);

    let summary = if let Some(llm) = &ctx.data().llm {
        match llm.stream(&[ChatMessage::new("user", prompt)], &[]).await {
            Ok(mut stream) => {
                let mut acc = String::new();
                while let Some(chunk) = stream.next().await {
                    if let Ok(chunk) = chunk {
                        acc.push_str(&chunk.content);
                    }
                }
                if acc.trim().is_empty() {
                    fallback_summary(&history)
                } else {
                    acc.trim().to_string()
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "LLM compression stream failed, using fallback summary");
                fallback_summary(&history)
            }
        }
    } else {
        fallback_summary(&history)
    };

    let summary_content = format!("[Conversation Summary]\n{summary}");
    let chars_after = summary_content.len();

    let mut tx = ctx.data().pool.begin().await?;
    sqlx::query("DELETE FROM messages WHERE session_key = ?")
        .bind(&storage_key)
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "INSERT INTO messages (id, session_key, role, content, metadata_json, created_at)
         VALUES (?, ?, 'system', ?, '{\"compressed\": true}', CURRENT_TIMESTAMP)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(&storage_key)
    .bind(&summary_content)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let (before, after, pct) = calculate_compression_stats(chars_before, chars_after);
    ctx.say(format!(
        "🗜️ **Conversation Compressed**\n• **Before:** {before} chars\n• **After:** {after} chars\n• **Reduction:** {pct:.1}%"
    ))
    .await?;
    Ok(())
}

#[poise::command(slash_command)]
/// Set or rename the current Discord thread title.
pub async fn title(
    ctx: PoiseContext<'_>,
    #[description = "New title for the thread"] text: String,
) -> Result<(), CommandError> {
    let channel_id = ctx.channel_id();
    let channel = channel_id.to_channel(ctx.serenity_context()).await?;
    match channel {
        serenity::Channel::Guild(guild_channel) if is_thread(guild_channel.kind) => {
            let builder = serenity::EditThread::new().name(text.trim());
            guild_channel
                .id
                .edit_thread(ctx.serenity_context(), builder)
                .await?;
            ctx.say(format!("🏷️ Thread renamed to `{}`.", text.trim()))
                .await?;
        }
        _ => {
            ctx.send(
                poise::CreateReply::default()
                    .content("❌ `/title` can only be used inside a thread.")
                    .ephemeral(true),
            )
            .await?;
        }
    }
    Ok(())
}

#[poise::command(slash_command)]
/// Create a new thread and optionally start a session in it.
pub async fn thread(
    ctx: PoiseContext<'_>,
    #[description = "Thread name"] name: String,
    #[description = "Optional first message to send in the thread"] message: Option<String>,
) -> Result<(), CommandError> {
    let channel_id = ctx.channel_id();
    let channel = channel_id.to_channel(ctx.serenity_context()).await?;
    match channel {
        serenity::Channel::Guild(guild_channel) => {
            if is_thread(guild_channel.kind) {
                ctx.send(
                    poise::CreateReply::default()
                        .content("❌ Cannot create a thread inside an existing thread.")
                        .ephemeral(true),
                )
                .await?;
                return Ok(());
            }

            let builder = serenity::CreateThread::new(name.trim());
            let created_thread = channel_id
                .create_thread(ctx.serenity_context(), builder)
                .await?;
            let thread_id_u64 = created_thread.id.get();
            ctx.data().mark_thread_active(thread_id_u64);

            if let Some(starter_msg) = message.filter(|m| !m.trim().is_empty()) {
                let user_id = if !ctx.data().thread_sessions_per_user {
                    "shared".to_string()
                } else {
                    ctx.author().id.to_string()
                };
                let thread_key = SessionKey::new(
                    "discord",
                    ctx.guild_id().map(|id| id.to_string()),
                    created_thread.id.to_string(),
                    Some(created_thread.id.to_string()),
                    user_id,
                )
                .with_bot_id(ctx.serenity_context().cache.current_user().id.to_string());

                let event =
                    crate::InboundEvent::message(thread_key, ctx.id().to_string(), starter_msg);
                let _ = ctx.data().multiplexer.route(event).await;
            }

            ctx.say(format!("🧵 Created thread <#{}>.", created_thread.id))
                .await?;
        }
        _ => {
            ctx.send(
                poise::CreateReply::default()
                    .content("❌ Threads can only be created in server text channels.")
                    .ephemeral(true),
            )
            .await?;
        }
    }
    Ok(())
}

#[poise::command(slash_command)]
/// Deny a pending dangerous command approval with an optional reason.
pub async fn deny(
    ctx: PoiseContext<'_>,
    #[description = "Optional reason explaining why the command was denied"] reason: Option<String>,
) -> Result<(), CommandError> {
    let key = session_key(ctx).await?;
    let resolved = ctx
        .data()
        .approvals
        .resolve_session_deny(&key, reason.clone())
        .await;
    if resolved {
        let msg = match reason {
            Some(r) if !r.trim().is_empty() => {
                format!("❌ Denied pending command approval: `{}`", r.trim())
            }
            _ => "❌ Denied pending command approval.".to_string(),
        };
        ctx.say(msg).await?;
    } else {
        ctx.send(
            poise::CreateReply::default()
                .content("No pending approval found to deny.")
                .ephemeral(true),
        )
        .await?;
    }
    Ok(())
}

#[poise::command(slash_command)]
/// Toggle YOLO mode (approval bypass) for this session.
pub async fn yolo(
    ctx: PoiseContext<'_>,
    #[description = "Enable or disable YOLO mode (on/off)"] mode: Option<String>,
) -> Result<(), CommandError> {
    let key = session_key(ctx).await?;
    ensure_session(&ctx.data().pool, &key).await?;
    let state_json: String =
        sqlx::query_scalar("SELECT state_json FROM sessions WHERE session_key = ?")
            .bind(key.storage_key())
            .fetch_one(&ctx.data().pool)
            .await?;
    let mut state: crate::SessionState = serde_json::from_str(&state_json).unwrap_or_default();
    let new_yolo = match mode
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("on" | "enable" | "true" | "yes" | "1") => true,
        Some("off" | "disable" | "false" | "no" | "0") => false,
        _ => !state.yolo,
    };
    state.yolo = new_yolo;
    sqlx::query(
        "UPDATE sessions SET state_json = ?, updated_at = CURRENT_TIMESTAMP WHERE session_key = ?",
    )
    .bind(serde_json::to_string(&state)?)
    .bind(key.storage_key())
    .execute(&ctx.data().pool)
    .await?;

    ctx.data().approvals.set_yolo(&key, new_yolo).await;

    let status_str = if new_yolo { "enabled" } else { "disabled" };
    let note = if new_yolo {
        "\n⚠️ Unconditional hardline and deny rules are still enforced."
    } else {
        ""
    };
    ctx.send(
        poise::CreateReply::default()
            .content(format!(
                "⚡ YOLO mode **{status_str}** for this session.{note}"
            ))
            .ephemeral(true),
    )
    .await?;
    Ok(())
}

pub fn format_steer_prompt(text: &str) -> String {
    format!("[Steering] {}", text.trim())
}

pub fn preview_text(text: &str, max_len: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() > max_len {
        let truncated: String = trimmed.chars().take(max_len.saturating_sub(3)).collect();
        format!("{truncated}...")
    } else {
        trimmed.to_string()
    }
}

pub fn build_compression_prompt(history: &[(String, String)]) -> String {
    let transcript = history
        .iter()
        .map(|(role, content)| format!("{role}: {content}"))
        .collect::<Vec<_>>()
        .join("\n\n");
    format!(
        "Summarize the following conversation concisely, preserving key context, user requests, agent findings, facts, and decisions:\n\n{transcript}\n\nSummary:"
    )
}

pub fn fallback_summary(history: &[(String, String)]) -> String {
    let mut lines = Vec::new();
    for (role, content) in history {
        let first_line = content.lines().next().unwrap_or("").trim();
        let preview = if first_line.chars().count() > 80 {
            let truncated: String = first_line.chars().take(77).collect();
            format!("{truncated}...")
        } else {
            first_line.to_string()
        };
        if !preview.is_empty() {
            lines.push(format!("- {role}: {preview}"));
        }
    }
    lines.join("\n")
}

pub fn calculate_compression_stats(before_chars: usize, after_chars: usize) -> (usize, usize, f64) {
    let pct = if before_chars > 0 && before_chars >= after_chars {
        ((before_chars - after_chars) as f64 / before_chars as f64) * 100.0
    } else {
        0.0
    };
    (before_chars, after_chars, pct)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndoResult {
    pub user_content: String,
    pub assistant_content: Option<String>,
    pub deleted_count: u64,
}

pub async fn undo_last_exchange(
    pool: &SqlitePool,
    session_key: &str,
) -> Result<Option<UndoResult>, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let user_row: Option<(i64, String)> = sqlx::query_as(
        "SELECT sequence, content FROM messages WHERE session_key = ? AND role = 'user' ORDER BY sequence DESC LIMIT 1",
    )
    .bind(session_key)
    .fetch_optional(&mut *tx)
    .await?;

    let Some((user_seq, user_content)) = user_row else {
        return Ok(None);
    };

    let assistant_row: Option<(String,)> = sqlx::query_as(
        "SELECT content FROM messages WHERE session_key = ? AND role = 'assistant' AND sequence >= ? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(session_key)
    .bind(user_seq)
    .fetch_optional(&mut *tx)
    .await?;

    let assistant_content = assistant_row.map(|(c,)| c);

    let delete_res = sqlx::query("DELETE FROM messages WHERE session_key = ? AND sequence >= ?")
        .bind(session_key)
        .bind(user_seq)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    Ok(Some(UndoResult {
        user_content,
        assistant_content,
        deleted_count: delete_res.rows_affected(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[test]
    fn test_all_commands_count() {
        let commands = all();
        assert_eq!(commands.len(), 17);
        let names: HashSet<&str> = commands.iter().map(|c| c.name.as_str()).collect();
        for expected in &[
            "model", "reset", "stop", "status", "tools", "skill", "skills", "memory", "cron",
            "steer", "undo", "retry", "compress", "title", "thread", "deny", "yolo",
        ] {
            assert!(names.contains(expected), "missing command {expected}");
        }
    }

    #[test]
    fn test_format_steer_prompt() {
        assert_eq!(
            format_steer_prompt("focus on Rust implementation"),
            "[Steering] focus on Rust implementation"
        );
        assert_eq!(
            format_steer_prompt("  padded whitespace  \n"),
            "[Steering] padded whitespace"
        );
    }

    #[test]
    fn test_preview_text() {
        assert_eq!(preview_text("short text", 50), "short text");
        assert_eq!(
            preview_text("this is a very long text that will be truncated", 20),
            "this is a very lo..."
        );
    }

    #[test]
    fn test_calculate_compression_stats() {
        let (before, after, pct) = calculate_compression_stats(1000, 250);
        assert_eq!(before, 1000);
        assert_eq!(after, 250);
        assert!((pct - 75.0).abs() < f64::EPSILON);

        let (_before, _after, pct) = calculate_compression_stats(0, 0);
        assert_eq!(pct, 0.0);

        let (_before, _after, pct) = calculate_compression_stats(100, 150);
        assert_eq!(pct, 0.0);
    }

    #[test]
    fn test_fallback_summary_and_prompt_builder() {
        let history = vec![
            (
                "user".to_string(),
                "Hello, can you help me write a function?".to_string(),
            ),
            (
                "assistant".to_string(),
                "Sure! Here is the function:\nfn main() {}".to_string(),
            ),
        ];
        let prompt = build_compression_prompt(&history);
        assert!(prompt.contains("user: Hello, can you help me write a function?"));
        assert!(prompt.contains("assistant: Sure! Here is the function:"));

        let fallback = fallback_summary(&history);
        assert!(fallback.contains("- user: Hello, can you help me write a function?"));
        assert!(fallback.contains("- assistant: Sure! Here is the function:"));
    }

    #[tokio::test]
    async fn test_undo_last_exchange_empty_db() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let result = undo_last_exchange(db.pool(), "empty-session")
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_undo_last_exchange_full_cycle() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let session_key = "test-undo-session";

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json) VALUES (?, 'discord', 'c1', 'u1', '{}')",
        )
        .bind(session_key)
        .execute(db.pool())
        .await
        .unwrap();

        // Turn 1
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m1', ?, 'user', 'turn 1 question')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m2', ?, 'assistant', 'turn 1 answer')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();

        // Turn 2
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m3', ?, 'user', 'turn 2 question')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m4', ?, 'tool', 'tool result')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m5', ?, 'assistant', 'turn 2 answer')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();

        // Undo turn 2
        let undo = undo_last_exchange(db.pool(), session_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(undo.user_content, "turn 2 question");
        assert_eq!(undo.assistant_content.as_deref(), Some("turn 2 answer"));
        assert_eq!(undo.deleted_count, 3);

        // Verify only turn 1 remains
        let remaining: Vec<(String, String)> = sqlx::query_as(
            "SELECT role, content FROM messages WHERE session_key = ? ORDER BY sequence ASC",
        )
        .bind(session_key)
        .fetch_all(db.pool())
        .await
        .unwrap();

        assert_eq!(remaining.len(), 2);
        assert_eq!(
            remaining[0],
            ("user".to_string(), "turn 1 question".to_string())
        );
        assert_eq!(
            remaining[1],
            ("assistant".to_string(), "turn 1 answer".to_string())
        );

        // Undo turn 1
        let undo1 = undo_last_exchange(db.pool(), session_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(undo1.user_content, "turn 1 question");
        assert_eq!(undo1.deleted_count, 2);

        let remaining_after: Vec<(String,)> =
            sqlx::query_as("SELECT content FROM messages WHERE session_key = ?")
                .bind(session_key)
                .fetch_all(db.pool())
                .await
                .unwrap();
        assert!(remaining_after.is_empty());

        // Further undo returns None
        let undo_none = undo_last_exchange(db.pool(), session_key).await.unwrap();
        assert!(undo_none.is_none());
    }

    #[tokio::test]
    async fn test_retry_query_and_cleanup() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let session_key = "test-retry-session";

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json) VALUES (?, 'discord', 'c1', 'u1', '{}')",
        )
        .bind(session_key)
        .execute(db.pool())
        .await
        .unwrap();

        // No messages initially
        let user_row: Option<(i64, String)> = sqlx::query_as(
            "SELECT sequence, content FROM messages WHERE session_key = ? AND role = 'user' ORDER BY sequence DESC LIMIT 1",
        )
        .bind(session_key)
        .fetch_optional(db.pool())
        .await
        .unwrap();
        assert!(user_row.is_none());

        // Insert turn 1
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m1', ?, 'user', 'first prompt')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m2', ?, 'assistant', 'first response')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();

        // Insert turn 2 (failed/needs retry)
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m3', ?, 'user', 'retry this prompt')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES ('m4', ?, 'assistant', 'failed partial response')")
            .bind(session_key)
            .execute(db.pool())
            .await
            .unwrap();

        // Fetch last user message
        let (user_seq, user_content): (i64, String) = sqlx::query_as(
            "SELECT sequence, content FROM messages WHERE session_key = ? AND role = 'user' ORDER BY sequence DESC LIMIT 1",
        )
        .bind(session_key)
        .fetch_one(db.pool())
        .await
        .unwrap();

        assert_eq!(user_content, "retry this prompt");

        // Clean up sequence >= user_seq
        let res = sqlx::query("DELETE FROM messages WHERE session_key = ? AND sequence >= ?")
            .bind(session_key)
            .bind(user_seq)
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(res.rows_affected(), 2);

        // Verify only turn 1 remains
        let remaining: Vec<(String, String)> = sqlx::query_as(
            "SELECT role, content FROM messages WHERE session_key = ? ORDER BY sequence ASC",
        )
        .bind(session_key)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].1, "first prompt");
    }

    #[tokio::test]
    async fn test_compress_db_replacement() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let session_key = "test-compress-session";

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json) VALUES (?, 'discord', 'c1', 'u1', '{}')",
        )
        .bind(session_key)
        .execute(db.pool())
        .await
        .unwrap();

        for i in 0..5 {
            sqlx::query(
                "INSERT INTO messages (id, session_key, role, content) VALUES (?, ?, 'user', ?)",
            )
            .bind(format!("um{i}"))
            .bind(session_key)
            .bind(format!("User message {i} with some content"))
            .execute(db.pool())
            .await
            .unwrap();
            sqlx::query("INSERT INTO messages (id, session_key, role, content) VALUES (?, ?, 'assistant', ?)")
                .bind(format!("am{i}"))
                .bind(session_key)
                .bind(format!("Assistant response {i} with helpful details"))
                .execute(db.pool())
                .await
                .unwrap();
        }

        let rows: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT sequence, role, content FROM messages WHERE session_key = ? ORDER BY sequence ASC",
        )
        .bind(session_key)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(rows.len(), 10);

        let history: Vec<(String, String)> = rows.into_iter().map(|(_, r, c)| (r, c)).collect();
        let summary = fallback_summary(&history);
        let summary_content = format!("[Conversation Summary]\n{summary}");

        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query("DELETE FROM messages WHERE session_key = ?")
            .bind(session_key)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json, created_at)
             VALUES (?, ?, 'system', ?, '{\"compressed\": true}', CURRENT_TIMESTAMP)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(session_key)
        .bind(&summary_content)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let after_rows: Vec<(String, String)> =
            sqlx::query_as("SELECT role, content FROM messages WHERE session_key = ?")
                .bind(session_key)
                .fetch_all(db.pool())
                .await
                .unwrap();
        assert_eq!(after_rows.len(), 1);
        assert_eq!(after_rows[0].0, "system");
        assert!(after_rows[0].1.starts_with("[Conversation Summary]"));
    }

    #[test]
    fn test_is_thread_detection() {
        assert!(is_thread(serenity::ChannelType::PublicThread));
        assert!(is_thread(serenity::ChannelType::PrivateThread));
        assert!(is_thread(serenity::ChannelType::NewsThread));
        assert!(!is_thread(serenity::ChannelType::Text));
        assert!(!is_thread(serenity::ChannelType::Voice));
        assert!(!is_thread(serenity::ChannelType::Private));
    }
}
