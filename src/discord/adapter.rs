use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use poise::serenity_prelude as serenity;
use regex::Regex;
use serenity::all::{
    ChannelId, ChannelType, Color, CreateAllowedMentions, CreateAttachment, CreateEmbed,
    CreateForumPost, CreateInteractionResponse, CreateInteractionResponseMessage, CreateMessage,
    CreateThread, EditMessage, FullEvent, GatewayIntents, GetMessages, HttpBuilder, Interaction,
    Message, MessageFlags, MessageId, Typing,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::approval::{
    approval_buttons, is_approval_custom_id, parse_custom_id, ApprovalDecision, SmartApprovalGuard,
};
use super::commands::{self, is_user_authorized, CommandError, PoiseData};
use super::throttler::{
    chunk_markdown, DiscordMessageTransport, LiveEditThrottler, SerenityMessageTransport,
    DISCORD_MESSAGE_LIMIT,
};
use crate::{
    DeliveryLedgerService, InboundEvent, MessageAttachment, OmonError, OutboundAction,
    OutboundDispatcher, Result, SessionKey,
};

static MEDIA_DIRECTIVE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"[`"']?MEDIA:\s*[`"']?([^`"'\r\n]+?)[`"']?(?:$|\s)"#)
        .expect("valid media directive regex")
});

/// Extracts `MEDIA:<path>` directives from text, returning `(text_without_media, paths)`.
/// Directives like `[[audio_as_voice]]` and `[[as_document]]` are also stripped.
pub fn extract_media_directives(text: &str) -> (String, Vec<String>) {
    let mut paths = Vec::new();
    let mut cleaned_lines = Vec::new();

    let preprocessed = text
        .replace("[[audio_as_voice]]", "")
        .replace("[[as_document]]", "");

    for line in preprocessed.lines() {
        if !line.contains("MEDIA:") {
            cleaned_lines.push(line.to_string());
            continue;
        }

        let mut line_paths = Vec::new();
        for caps in MEDIA_DIRECTIVE_RE.captures_iter(line) {
            if let Some(matched) = caps.get(1) {
                let raw_path = matched.as_str().trim();
                let clean_path = raw_path
                    .trim_matches(|c| c == '"' || c == '\'' || c == '`')
                    .trim();
                if !clean_path.is_empty() {
                    line_paths.push(clean_path.to_string());
                }
            }
        }

        if !line_paths.is_empty() {
            paths.extend(line_paths);
            let stripped_line = MEDIA_DIRECTIVE_RE.replace_all(line, "");
            let trimmed = stripped_line.trim();
            if !trimmed.is_empty() {
                cleaned_lines.push(trimmed.to_string());
            }
        } else {
            cleaned_lines.push(line.to_string());
        }
    }

    let cleaned_text = cleaned_lines.join("\n").trim().to_string();
    (cleaned_text, paths)
}

/// Determines if a chunk at `chunk_index` should reference a triggering message.
pub fn should_chunk_reference(
    chunk_index: usize,
    reply_to: Option<MessageId>,
) -> Option<MessageId> {
    if chunk_index == 0 {
        reply_to
    } else {
        None
    }
}

/// Returns safe Discord `AllowedMentions` settings that permit user pings and
/// reply mentions, but deny server-wide `@everyone`/`@here` and role pings by default.
pub fn safe_allowed_mentions() -> CreateAllowedMentions {
    CreateAllowedMentions::new()
        .all_users(true)
        .everyone(false)
        .all_roles(false)
        .replied_user(true)
}

const SILENCE_SENTINELS: &[&str] = &["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY"];

static SILENCE_NARRATION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^[\s*_~`]*\(?\s*(silent|silence|no\s+response|no\s+reply)\s*\.?\)?[\\s*_~`]*$|^[\s*_~`]*[\x{1f507}\.\x{2026}]+[\s*_~`]*$",
    )
    .expect("valid silence narration regex")
});

fn strip_edge_silence_punctuation(text: &str) -> &str {
    let trimmed = text.trim();
    let start = trimmed
        .char_indices()
        .find(|&(_, c)| !c.is_ascii_punctuation() || c == '[' || c == ']')
        .map(|(idx, _)| idx)
        .unwrap_or(trimmed.len());
    let end = trimmed
        .char_indices()
        .rfind(|&(_, c)| !c.is_ascii_punctuation() || c == '[' || c == ']')
        .map(|(idx, c)| idx + c.len_utf8())
        .unwrap_or(0);
    if start >= end {
        ""
    } else {
        &trimmed[start..end]
    }
}

/// Returns `true` if `text` is an intentional silence response sentinel or anti-loop narration token.
pub fn is_silence_response(text: &str) -> bool {
    let stripped = text.trim();
    if stripped.is_empty() {
        return true;
    }
    if stripped.chars().count() > 64 {
        return false;
    }

    let normalized: String = stripped
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase();
    if SILENCE_SENTINELS.contains(&normalized.as_str()) {
        return true;
    }

    let edge_stripped = strip_edge_silence_punctuation(stripped);
    if !edge_stripped.is_empty() {
        let normalized_edge: String = edge_stripped
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase();
        if SILENCE_SENTINELS.contains(&normalized_edge.as_str()) {
            return true;
        }
    }

    SILENCE_NARRATION_RE.is_match(stripped)
}

/// Maximum character length for hydrated referenced message context.
pub const REFERENCED_CONTENT_CAP: usize = 500;

pub const DEFAULT_CHANNEL_CONTEXT_LIMIT: usize = 10;
pub const MAX_CHANNEL_CONTEXT_LIMIT: usize = 25;
pub const MAX_CONTEXT_LINE_CHARS: usize = 200;

/// Formats a list of recent channel messages into a compact conversational context block.
pub fn format_channel_context<A: AsRef<str>, C: AsRef<str>>(messages: &[(A, C)]) -> String {
    if messages.is_empty() {
        return String::new();
    }
    let mut lines = Vec::new();
    for (author, content) in messages {
        let author = crate::security::neutralize_untrusted_inline_text(author.as_ref(), 64);
        let truncated = crate::security::neutralize_untrusted_inline_text(
            content.as_ref(),
            MAX_CONTEXT_LINE_CHARS,
        );
        if author.is_empty() || truncated.is_empty() {
            continue;
        }
        lines.push(format!("{author}: {truncated}"));
    }
    if lines.is_empty() {
        return String::new();
    }
    format!("[Recent channel context]\n{}", lines.join("\n"))
}

/// Derives a clean thread name from a user message when auto-threading on mention.
pub fn derive_auto_thread_name(content: &str, bot_user_id: serenity::UserId) -> String {
    let stripped = strip_bot_mention(content, bot_user_id);
    let collapsed: String = stripped.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        "Conversation".to_string()
    } else if trimmed.chars().count() > 80 {
        let capped: String = trimmed.chars().take(77).collect();
        format!("{capped}...")
    } else {
        trimmed.to_string()
    }
}

/// Formats a compact runtime metadata footer line: `model · context% · cwd`.
/// Missing or empty fields are skipped silently.
pub fn format_runtime_footer(
    model: Option<&str>,
    context_percent: Option<u8>,
    cwd: Option<&Path>,
) -> String {
    let mut parts = Vec::new();

    if let Some(m) = model {
        let trimmed = m.trim();
        if !trimmed.is_empty() {
            let short_model = trimmed.rsplit('/').next().unwrap_or(trimmed);
            parts.push(short_model.to_string());
        }
    }

    if let Some(pct) = context_percent {
        let clamped = pct.min(100);
        parts.push(format!("{clamped}%"));
    }

    if let Some(path) = cwd {
        let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
        let path_str = if let Some(home_path) = home {
            if let Ok(rel) = path.strip_prefix(&home_path) {
                format!("~/{}", rel.display())
            } else {
                path.display().to_string()
            }
        } else {
            path.display().to_string()
        };
        let trimmed = path_str.trim();
        if !trimmed.is_empty() {
            parts.push(trimmed.to_string());
        }
    }

    if parts.is_empty() {
        String::new()
    } else {
        parts.join(" · ")
    }
}

/// Appends a runtime metadata footer to message content if footer is non-empty.
pub fn append_runtime_footer(
    content: &str,
    model: Option<&str>,
    context_percent: Option<u8>,
    cwd: Option<&Path>,
) -> String {
    let footer = format_runtime_footer(model, context_percent, cwd);
    if footer.is_empty() {
        return content.to_string();
    }
    if content.trim().is_empty() {
        footer
    } else {
        format!("{content}\n\n_{footer}_")
    }
}

static MENTION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<@!?[0-9]+>|<#[0-9]+>|<@&[0-9]+>").expect("valid mention regex"));

/// Derives a clean forum thread post title from the message or its first line.
///
/// Discord requires thread/post names to be 1 to 100 characters.
pub fn derive_forum_post_title(content: &str) -> String {
    let first_line = content
        .lines()
        .map(|l| l.trim())
        .find(|l| !l.is_empty())
        .unwrap_or("New Discussion");

    let no_mentions = MENTION_RE.replace_all(first_line, "");
    let stripped = no_mentions
        .trim_start_matches(|c: char| {
            c == '#'
                || c == '>'
                || c == '*'
                || c == '_'
                || c == '`'
                || c == '~'
                || c.is_whitespace()
        })
        .trim_end_matches(|c: char| {
            c == '*' || c == '_' || c == '`' || c == '~' || c.is_whitespace()
        });

    let collapsed: String = stripped.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        "New Discussion".to_string()
    } else if trimmed.chars().count() > 100 {
        let capped: String = trimmed.chars().take(97).collect();
        format!("{capped}...")
    } else if trimmed.chars().count() < 2 {
        format!("{trimmed} Discussion")
    } else {
        trimmed.to_string()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AllowBotsMode {
    #[default]
    None,
    Mentions,
    All,
}

impl AllowBotsMode {
    pub fn parse(s: Option<&str>) -> Self {
        match s.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("all") => Self::All,
            Some("mentions") | Some("mention") => Self::Mentions,
            _ => Self::None,
        }
    }
}

/// Configuration options for filtering and routing inbound Discord messages.
#[derive(Clone, Debug)]
pub struct InboundFilterConfig<'a> {
    pub free_response_channels: &'a [u64],
    pub allowed_users: &'a [u64],
    pub allowed_roles: &'a [u64],
    pub user_roles: &'a [u64],
    pub allow_all_users: bool,
    pub thread_sessions_per_user: bool,
    pub active_threads: &'a [u64],
    pub allowed_channels: &'a [u64],
    pub ignored_channels: &'a [u64],
    pub primary_bot_id: Option<u64>,
    pub thread_require_mention: bool,
    pub allow_bots: AllowBotsMode,
}

impl Default for InboundFilterConfig<'_> {
    fn default() -> Self {
        Self {
            free_response_channels: &[],
            allowed_users: &[],
            allowed_roles: &[],
            user_roles: &[],
            allow_all_users: false,
            thread_sessions_per_user: true,
            active_threads: &[],
            allowed_channels: &[],
            ignored_channels: &[],
            primary_bot_id: None,
            thread_require_mention: false,
            allow_bots: AllowBotsMode::None,
        }
    }
}

/// Composes reply context prefixing the user body with a quote block of the referenced message.
pub fn compose_reply_context(
    referenced_author: &str,
    referenced_content: &str,
    body: &str,
) -> String {
    let truncated_content = if referenced_content.chars().count() > REFERENCED_CONTENT_CAP {
        let capped: String = referenced_content
            .chars()
            .take(REFERENCED_CONTENT_CAP)
            .collect();
        format!("{capped}...")
    } else {
        referenced_content.to_string()
    };
    let clean_content = truncated_content.trim();
    if clean_content.is_empty() {
        if body.is_empty() {
            format!("> [Replying to @{referenced_author}]")
        } else {
            format!("> [Replying to @{referenced_author}]\n\n{body}")
        }
    } else if body.is_empty() {
        format!("> [Replying to @{referenced_author}]: {clean_content}")
    } else {
        format!("> [Replying to @{referenced_author}]: {clean_content}\n\n{body}")
    }
}

/// Default debounce window for coalescing rapid client-split messages (~600ms).
pub const DEFAULT_DEBOUNCE_DURATION: std::time::Duration = std::time::Duration::from_millis(600);

static GLOBAL_DEBOUNCER: std::sync::LazyLock<SplitMessageDebouncer> =
    std::sync::LazyLock::new(SplitMessageDebouncer::default);

pub fn global_debouncer() -> &'static SplitMessageDebouncer {
    &GLOBAL_DEBOUNCER
}

/// Pure testable helper to coalesce multiple buffered messages from a single
/// session into a single `InboundEvent`.
///
/// Contents are concatenated in arrival order (separated by newlines), attachments
/// are unioned with duplicate IDs removed, and the LAST message's platform id and
/// delivery id are used so that ledger deduplication semantics remain correct.
pub fn coalesce_inbound_events(events: Vec<InboundEvent>) -> Option<InboundEvent> {
    if events.is_empty() {
        return None;
    }
    if events.len() == 1 {
        return events.into_iter().next();
    }

    let first = &events[0];
    let last = events.last().unwrap();
    let session = first.session.clone();

    let contents: Vec<&str> = events
        .iter()
        .map(|e| e.content.as_str())
        .filter(|s| !s.trim().is_empty())
        .collect();
    let content = contents.join("\n");

    let mut attachments = Vec::new();
    let mut seen_ids = std::collections::HashSet::new();
    for event in &events {
        for attachment in &event.attachments {
            if seen_ids.insert(attachment.id.clone()) {
                attachments.push(attachment.clone());
            }
        }
    }

    let platform_message_id = last.platform_message_id.clone();
    let delivery_id = last
        .delivery_id
        .clone()
        .or_else(|| Some(format!("discord:{platform_message_id}")));

    let mut coalesced =
        InboundEvent::message(session, platform_message_id, content).with_attachments(attachments);
    coalesced.id = first.id;
    coalesced.received_at = first.received_at;
    coalesced.delivery_id = delivery_id;
    Some(coalesced)
}

struct DebounceBatch {
    events: Vec<InboundEvent>,
    generation: u64,
}

#[derive(Clone)]
pub struct SplitMessageDebouncer {
    duration: std::time::Duration,
    buffer: Arc<Mutex<HashMap<SessionKey, DebounceBatch>>>,
}

impl SplitMessageDebouncer {
    pub fn new(duration: std::time::Duration) -> Self {
        Self {
            duration,
            buffer: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn enqueue(&self, event: InboundEvent, data: PoiseData) {
        let session = event.session.clone();
        let (generation, delay) = {
            let mut lock = self.buffer.lock().await;
            let batch = lock
                .entry(session.clone())
                .or_insert_with(|| DebounceBatch {
                    events: Vec::new(),
                    generation: 0,
                });
            batch.events.push(event);
            batch.generation += 1;
            (batch.generation, self.duration)
        };

        let buffer = self.buffer.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let events_to_route = {
                let mut lock = buffer.lock().await;
                if let Some(batch) = lock.get(&session) {
                    if batch.generation == generation {
                        lock.remove(&session).map(|b| b.events)
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            if let Some(events) = events_to_route {
                if let Some(coalesced) = coalesce_inbound_events(events) {
                    tracing::info!(
                        session = %coalesced.session,
                        delivery_id = ?coalesced.delivery_id,
                        "Flushing debounced/coalesced Discord message"
                    );
                    if let Err(error) = route_claimed_event(&data, coalesced).await {
                        tracing::error!(session = %session, %error, "failed to route debounced Discord event");
                    }
                }
            }
        });
    }

    pub async fn cancel(&self, session: &SessionKey) -> Option<Vec<InboundEvent>> {
        let mut lock = self.buffer.lock().await;
        lock.remove(session).map(|b| b.events)
    }

    pub async fn is_empty(&self) -> bool {
        let lock = self.buffer.lock().await;
        lock.is_empty()
    }
}

impl Default for SplitMessageDebouncer {
    fn default() -> Self {
        Self::new(DEFAULT_DEBOUNCE_DURATION)
    }
}

#[derive(Clone)]
pub struct DiscordAdapter {
    data: PoiseData,
    approvals: SmartApprovalGuard,
}

impl DiscordAdapter {
    pub fn new(data: PoiseData) -> Self {
        Self {
            data,
            approvals: SmartApprovalGuard::new(),
        }
    }

    pub fn with_approval_guard(mut self, approvals: SmartApprovalGuard) -> Self {
        self.approvals = approvals;
        self
    }

    pub fn approval_guard(&self) -> &SmartApprovalGuard {
        &self.approvals
    }

    pub async fn client(&self, token: impl AsRef<str>) -> Result<serenity::Client> {
        let mut setup_data = self.data.clone();
        setup_data.approvals = self.approvals.clone();
        let framework = poise::Framework::builder()
            .options(poise::FrameworkOptions {
                commands: commands::all(),
                event_handler: |ctx, event, _framework, data| {
                    Box::pin(handle_event(ctx, event, data))
                },
                command_check: Some(|ctx| Box::pin(commands::command_check(ctx))),
                ..Default::default()
            })
            .setup(move |ctx, _ready, framework| {
                Box::pin(async move {
                    poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                    Ok(setup_data)
                })
            })
            .build();
        let intents = GatewayIntents::GUILDS
            | GatewayIntents::GUILD_MESSAGES
            | GatewayIntents::DIRECT_MESSAGES
            | GatewayIntents::MESSAGE_CONTENT;
        let http = HttpBuilder::new(token.as_ref())
            .default_allowed_mentions(safe_allowed_mentions())
            .build();
        Ok(serenity::ClientBuilder::new_with_http(http, intents)
            .framework(framework)
            .await?)
    }

    pub async fn start(&self, token: impl AsRef<str>) -> Result<()> {
        let mut client = self.client(token).await?;
        client.start().await?;
        Ok(())
    }

    pub async fn route_message(
        &self,
        message: &Message,
        bot_user_id: serenity::UserId,
        channel_type: Option<ChannelType>,
    ) -> Result<bool> {
        let active_threads: Vec<u64> = self
            .data
            .active_threads
            .read()
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        let config = InboundFilterConfig {
            free_response_channels: &self.data.free_response_channels,
            allowed_users: &self.data.allowed_users,
            allowed_roles: &self.data.allowed_roles,
            user_roles: &[],
            allow_all_users: self.data.allow_all_users,
            thread_sessions_per_user: self.data.thread_sessions_per_user,
            active_threads: &active_threads,
            allowed_channels: &self.data.allowed_channels,
            ignored_channels: &self.data.ignored_channels,
            primary_bot_id: self.data.primary_bot_id,
            thread_require_mention: self.data.thread_require_mention,
            allow_bots: self.data.allow_bots,
        };
        let Some(event) =
            message_to_inbound_with_config(message, bot_user_id, channel_type, &config)
        else {
            return Ok(false);
        };
        if channel_type.is_some_and(is_thread) {
            self.data.mark_thread_active(message.channel_id.get());
        }
        route_claimed_event(&self.data, event).await
    }
}

async fn handle_event(
    ctx: &serenity::Context,
    event: &FullEvent,
    data: &PoiseData,
) -> Result<(), CommandError> {
    match event {
        FullEvent::Message { new_message } => {
            tracing::info!(
                author = %new_message.author.name,
                author_id = %new_message.author.id,
                channel = %new_message.channel_id,
                guild = ?new_message.guild_id,
                content = %new_message.content,
                "Discord message event received"
            );
            let channel_type = if new_message.guild_id.is_some() {
                match new_message.channel_id.to_channel(ctx).await {
                    Ok(serenity::Channel::Guild(channel)) => Some(channel.kind),
                    _ => None,
                }
            } else {
                Some(ChannelType::Private)
            };
            let bot_user_id = ctx
                .http
                .get_current_user()
                .await
                .map(|u| u.id)
                .unwrap_or(ctx.cache.current_user().id);
            let is_guild_text = channel_type == Some(ChannelType::Text);
            let mentioned_bot_ids = new_message
                .mentions
                .iter()
                .filter(|user| user.bot)
                .map(|user| user.id)
                .collect::<Vec<_>>();
            let is_explicit_mention = mentioned_bot_ids.contains(&bot_user_id);
            let active_threads: Vec<u64> = data
                .active_threads
                .read()
                .map(|set| set.iter().copied().collect())
                .unwrap_or_default();
            let user_roles: Vec<u64> = if let Some(member) = &new_message.member {
                member.roles.iter().map(|r| r.get()).collect()
            } else if let Some(guild_id) = new_message.guild_id {
                match ctx.http.get_member(guild_id, new_message.author.id).await {
                    Ok(member) => member.roles.iter().map(|r| r.get()).collect(),
                    Err(_) => Vec::new(),
                }
            } else {
                Vec::new()
            };
            let config = InboundFilterConfig {
                free_response_channels: &data.free_response_channels,
                allowed_users: &data.allowed_users,
                allowed_roles: &data.allowed_roles,
                user_roles: &user_roles,
                allow_all_users: data.allow_all_users,
                thread_sessions_per_user: data.thread_sessions_per_user,
                active_threads: &active_threads,
                allowed_channels: &data.allowed_channels,
                ignored_channels: &data.ignored_channels,
                primary_bot_id: data.primary_bot_id,
                thread_require_mention: data.thread_require_mention,
                allow_bots: data.allow_bots,
            };
            if let Some(mut event) =
                message_to_inbound_with_config(new_message, bot_user_id, channel_type, &config)
            {
                let is_dm = channel_type == Some(ChannelType::Private);
                if data.channel_context
                    && !is_dm
                    && is_explicit_mention
                    && data.channel_context_limit > 0
                {
                    let limit = data.channel_context_limit.min(MAX_CHANNEL_CONTEXT_LIMIT) as u8;
                    let builder = GetMessages::new().before(new_message.id).limit(limit);
                    match new_message.channel_id.messages(&ctx.http, builder).await {
                        Ok(mut messages) => {
                            messages.reverse();
                            let history: Vec<(String, String)> = messages
                                .into_iter()
                                .filter(|m| {
                                    m.author.id != bot_user_id && !m.content.trim().is_empty()
                                })
                                .map(|m| (m.author.name, m.content))
                                .collect();
                            let context_block = format_channel_context(&history);
                            if !context_block.is_empty() {
                                if event.content.trim().is_empty() {
                                    event.content = context_block;
                                } else {
                                    event.content = format!("{context_block}\n\n{}", event.content);
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                channel = %new_message.channel_id,
                                "Failed to fetch recent channel context"
                            );
                        }
                    }
                }

                if data.processing_reactions {
                    if let Err(error) = new_message
                        .channel_id
                        .create_reaction(
                            &ctx.http,
                            new_message.id,
                            serenity::all::ReactionType::Unicode(
                                crate::models::PROCESSING_START_EMOJI.to_string(),
                            ),
                        )
                        .await
                    {
                        tracing::debug!(
                            %error,
                            message_id = %new_message.id,
                            channel = %new_message.channel_id,
                            "Failed to add start processing reaction to message"
                        );
                    }
                }

                if channel_type.is_some_and(is_thread) {
                    data.mark_thread_active(new_message.channel_id.get());
                } else if data.auto_thread && is_guild_text && is_explicit_mention {
                    let thread_name = derive_auto_thread_name(&new_message.content, bot_user_id);
                    let builder = CreateThread::new(thread_name);
                    match new_message
                        .channel_id
                        .create_thread_from_message(&ctx.http, new_message.id, builder)
                        .await
                    {
                        Ok(thread_channel) => {
                            let thread_id_num = thread_channel.id.get();
                            data.mark_thread_active(thread_id_num);
                            tracing::info!(
                                thread_id = %thread_id_num,
                                parent_channel = %new_message.channel_id,
                                "Auto-created thread on channel mention"
                            );
                            if !data.thread_sessions_per_user {
                                event.session.user_id = "shared".to_string();
                            }
                            event.session.thread_id = Some(thread_channel.id.to_string());
                            event.session.channel_id = thread_channel.id.to_string();
                        }
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                channel = %new_message.channel_id,
                                "Failed to auto-create thread from mention; falling back to in-channel reply"
                            );
                        }
                    }
                }
                if event.content.trim().eq_ignore_ascii_case("/stop") {
                    global_debouncer().cancel(&event.session).await;
                    tracing::info!(session = %event.session, "Routing Discord stop command immediately");
                    route_claimed_event(data, event).await?;
                } else {
                    tracing::info!(session = %event.session, bot_id = %bot_user_id, "Enqueueing inbound message to debounce buffer");
                    global_debouncer().enqueue(event, data.clone()).await;
                }
            } else {
                tracing::info!("Message ignored by filter (not DM, not thread, not mentioned, not in free channels)");
            }
        }
        FullEvent::InteractionCreate {
            interaction: Interaction::Component(component),
        } => {
            if is_approval_custom_id(&component.data.custom_id) {
                let component_roles: Vec<u64> = component
                    .member
                    .as_ref()
                    .map(|m| m.roles.iter().map(|r| r.get()).collect())
                    .unwrap_or_default();
                if !is_user_authorized(
                    component.user.id.get(),
                    &component_roles,
                    &data.allowed_users,
                    &data.allowed_roles,
                    data.allow_all_users,
                ) {
                    let refusal = CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .ephemeral(true)
                            .content("You are not authorized to approve commands."),
                    );
                    component.create_response(ctx, refusal).await?;
                    return Ok(());
                }

                let parsed = parse_custom_id(&component.data.custom_id);
                let response = if let Some((_, decision)) = parsed {
                    if data
                        .approvals
                        .resolve_custom_id(&component.data.custom_id)
                        .await
                    {
                        let decision_label = match decision {
                            ApprovalDecision::Once => "Approved (once)",
                            ApprovalDecision::Session => "Approved (session)",
                            ApprovalDecision::Always => "Approved (always)",
                            ApprovalDecision::Deny { .. } => "Denied",
                        };
                        let user_name = &component.user.name;
                        CreateInteractionResponse::UpdateMessage(
                            CreateInteractionResponseMessage::new()
                                .components(Vec::new())
                                .content(format!("{decision_label} by {user_name}")),
                        )
                    } else {
                        CreateInteractionResponse::UpdateMessage(
                            CreateInteractionResponseMessage::new()
                                .components(Vec::new())
                                .content(
                                    "⏱ Approval request no longer valid (expired or already resolved).",
                                ),
                        )
                    }
                } else {
                    CreateInteractionResponse::UpdateMessage(
                        CreateInteractionResponseMessage::new()
                            .components(Vec::new())
                            .content(
                                "이 승인 요청은 더 이상 유효하지 않습니다 (게이트웨이 재시작 또는 만료됨). 명령을 다시 실행해 주세요.",
                            ),
                    )
                };
                component.create_response(ctx, response).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

async fn route_claimed_event(data: &PoiseData, mut event: InboundEvent) -> Result<bool> {
    if event.content.trim().eq_ignore_ascii_case("/stop") {
        let interrupted = data.multiplexer.stop(&event.session).await?;
        tracing::info!(session = %event.session, interrupted, "processed Discord text stop command");
        return Ok(true);
    }

    let delivery_id = event
        .delivery_id
        .clone()
        .unwrap_or_else(|| format!("discord:{}", event.platform_message_id));
    let ledger = DeliveryLedgerService::new(data.pool.clone());
    if !ledger.record_incoming_as(&event, &delivery_id).await? {
        tracing::info!(delivery_id, "Ignoring duplicate Discord delivery");
        return Ok(false);
    }

    if let Some(downloader) = &data.attachment_downloader {
        for attachment in &mut event.attachments {
            if let Err(error) = downloader.hydrate(attachment).await {
                tracing::warn!(
                    attachment_id = %attachment.id,
                    filename = %attachment.filename,
                    %error,
                    "failed to download Discord attachment; routing remote metadata only"
                );
            }
        }
    }

    event.delivery_id = Some(delivery_id.clone());
    if let Err(error) = data.multiplexer.route(event).await {
        ledger.mark_failed(&delivery_id, error.to_string()).await?;
        return Err(error);
    }
    Ok(true)
}

pub fn message_to_inbound(
    message: &Message,
    bot_user_id: serenity::UserId,
    channel_type: Option<ChannelType>,
) -> Option<InboundEvent> {
    let config = InboundFilterConfig {
        primary_bot_id: Some(bot_user_id.get()),
        ..Default::default()
    };
    message_to_inbound_with_config(message, bot_user_id, channel_type, &config)
}

pub fn message_to_inbound_with_config(
    message: &Message,
    bot_user_id: serenity::UserId,
    channel_type: Option<ChannelType>,
    config: &InboundFilterConfig<'_>,
) -> Option<InboundEvent> {
    if message.webhook_id.is_some() {
        return None;
    }

    if message.author.id == bot_user_id {
        return None;
    }

    if message.author.bot {
        match config.allow_bots {
            AllowBotsMode::None => return None,
            AllowBotsMode::Mentions => {
                let mentioned_bot_ids = message
                    .mentions
                    .iter()
                    .filter(|user| user.bot)
                    .map(|user| user.id)
                    .collect::<Vec<_>>();
                if !mentioned_bot_ids.contains(&bot_user_id) {
                    return None;
                }
            }
            AllowBotsMode::All => {}
        }
    }

    // Only process standard text messages and replies. Ignore system messages (thread joins, removals, pins, etc.)
    if !matches!(
        message.kind,
        serenity::model::channel::MessageType::Regular
            | serenity::model::channel::MessageType::InlineReply
    ) {
        return None;
    }

    let is_dm = message.guild_id.is_none();
    let channel_id_u64 = message.channel_id.get();

    // Channel blacklist: if ignored_channels contains the channel id -> return None
    if config.ignored_channels.contains(&channel_id_u64) {
        return None;
    }

    // Channel whitelist: if allowed_channels is non-empty, guild channels must be in allowed_channels (DMs exempt)
    if !is_dm
        && !config.allowed_channels.is_empty()
        && !config.allowed_channels.contains(&channel_id_u64)
    {
        return None;
    }

    if !is_user_authorized(
        message.author.id.get(),
        config.user_roles,
        config.allowed_users,
        config.allowed_roles,
        config.allow_all_users,
    ) {
        return None;
    }

    let is_thread = channel_type.is_some_and(is_thread);
    let mentioned_bot_ids = message
        .mentions
        .iter()
        .filter(|user| user.bot)
        .map(|user| user.id)
        .collect::<Vec<_>>();
    let is_explicit_mention = mentioned_bot_ids.contains(&bot_user_id);
    let is_free_channel = config
        .free_response_channels
        .contains(&message.channel_id.get());

    if !mentioned_bot_ids.is_empty() {
        if !is_explicit_mention {
            return None;
        }
    } else {
        let is_active_thread = is_thread
            && !config.thread_require_mention
            && config.active_threads.contains(&message.channel_id.get());
        let is_implicit_response_channel = is_dm || is_active_thread || is_free_channel;
        if !is_implicit_response_channel {
            return None;
        }
        // Guild threads and free-response channels are visible to every bot, so only
        // the primary bot auto-responds there to avoid duplicate replies. DMs are 1:1
        // per bot, so each bot must always answer its own DMs.
        if !is_dm && config.primary_bot_id != Some(bot_user_id.get()) {
            return None;
        }
    }

    let raw_content = strip_bot_mention(&message.content, bot_user_id);
    if raw_content.trim().is_empty() && message.attachments.is_empty() {
        return None; // Do NOT auto-inject "Hello!" for empty/system messages
    }
    let content = if let Some(parent) = &message.referenced_message {
        let mut ref_content = parent.content.trim().to_string();
        if !parent.attachments.is_empty() {
            let att_summary = if parent.attachments.len() == 1 {
                format!("[Attachment: {}]", parent.attachments[0].filename)
            } else {
                let filenames: Vec<&str> = parent
                    .attachments
                    .iter()
                    .map(|att| att.filename.as_str())
                    .collect();
                format!("[Attachments: {}]", filenames.join(", "))
            };
            if ref_content.is_empty() {
                ref_content = att_summary;
            } else {
                ref_content = format!("{ref_content} {att_summary}");
            }
        }
        compose_reply_context(&parent.author.name, &ref_content, &raw_content)
    } else {
        raw_content
    };
    let user_id = if is_thread && !config.thread_sessions_per_user {
        "shared".to_string()
    } else {
        message.author.id.to_string()
    };
    let channel_id = message.channel_id.to_string();
    let session = SessionKey::new(
        "discord",
        message.guild_id.map(|id| id.to_string()),
        channel_id.clone(),
        is_thread.then_some(channel_id),
        user_id,
    )
    .with_bot_id(bot_user_id.to_string());
    let attachments = message
        .attachments
        .iter()
        .map(|attachment| MessageAttachment {
            id: attachment.id.to_string(),
            filename: attachment.filename.clone(),
            url: attachment.url.clone(),
            content_type: attachment.content_type.clone(),
            size_bytes: Some(u64::from(attachment.size)),
            local_path: None,
            text_content: None,
        })
        .collect();
    let mut event = InboundEvent::message(session, message.id.to_string(), content)
        .with_attachments(attachments);
    let delivery_id = if mentioned_bot_ids.len() > 1 {
        format!("discord:{}:{}", message.id, bot_user_id.get())
    } else {
        format!("discord:{}", message.id)
    };
    event.delivery_id = Some(delivery_id);
    Some(event)
}

fn is_thread(kind: ChannelType) -> bool {
    matches!(
        kind,
        ChannelType::NewsThread | ChannelType::PublicThread | ChannelType::PrivateThread
    )
}

fn strip_bot_mention(content: &str, bot_user_id: serenity::UserId) -> String {
    content
        .replace(&format!("<@{}>", bot_user_id.get()), "")
        .replace(&format!("<@!{}>", bot_user_id.get()), "")
        .trim()
        .to_owned()
}

#[async_trait]
pub trait DiscordFileUploader: Send + Sync {
    async fn upload(
        &self,
        http: Arc<serenity::Http>,
        channel: ChannelId,
        path: &Path,
    ) -> Result<()>;
}

pub const DISCORD_VOICE_MESSAGE_FLAG: u64 = 8192;

#[derive(Clone, Debug, PartialEq)]
pub struct VoiceMetadata {
    pub duration_secs: f64,
    pub waveform: String,
    pub flags: u64,
}

/// Builds Discord voice note metadata (flags 8192, duration in seconds, base64 sampled waveform).
pub fn build_voice_metadata(audio_bytes: &[u8], duration_hint: Option<f64>) -> VoiceMetadata {
    let waveform_samples: Vec<u8> = if audio_bytes.is_empty() {
        vec![0u8; 64]
    } else {
        let sample_count = 128.min(audio_bytes.len());
        let step = (audio_bytes.len() / sample_count).max(1);
        let mut samples = Vec::with_capacity(sample_count);
        for chunk in audio_bytes.chunks(step).take(sample_count) {
            let max_val = chunk.iter().copied().max().unwrap_or(0);
            samples.push(max_val);
        }
        if samples.is_empty() {
            vec![0u8; 64]
        } else {
            samples
        }
    };

    use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
    let waveform = BASE64_STANDARD.encode(&waveform_samples);

    let duration_secs = duration_hint.unwrap_or_else(|| {
        if audio_bytes.is_empty() {
            1.0
        } else {
            let est = audio_bytes.len() as f64 / 3500.0;
            (est.max(0.5) * 10.0).round() / 10.0
        }
    });

    VoiceMetadata {
        duration_secs,
        waveform,
        flags: DISCORD_VOICE_MESSAGE_FLAG,
    }
}

/// Determines whether a file or attachment represents a Discord voice message.
pub fn is_voice_audio_file(filename: &str, content_type: Option<&str>) -> bool {
    if let Some(ct) = content_type {
        let ct_lower = ct.to_ascii_lowercase();
        if ct_lower.starts_with("audio/ogg")
            || ct_lower.starts_with("audio/opus")
            || ct_lower.contains("voice")
        {
            return true;
        }
    }
    let lower = filename.to_ascii_lowercase();
    lower.ends_with(".ogg")
        || lower.ends_with(".opus")
        || lower.contains("voice-message")
        || lower.contains("voice_message")
}

#[derive(Clone, Default)]
pub struct SerenityFileUploader;

#[async_trait]
impl DiscordFileUploader for SerenityFileUploader {
    async fn upload(
        &self,
        http: Arc<serenity::Http>,
        channel: ChannelId,
        path: &Path,
    ) -> Result<()> {
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                OmonError::Config(format!(
                    "Discord upload path has no valid filename: {}",
                    path.display()
                ))
            })?
            .to_owned();
        let bytes = tokio::fs::read(path).await.map_err(|error| {
            OmonError::Config(format!(
                "failed to read Discord upload {}: {error}",
                path.display()
            ))
        })?;

        let is_voice = is_voice_audio_file(&filename, None);
        let is_forum = match channel.to_channel(&http).await {
            Ok(serenity::Channel::Guild(guild_channel)) => guild_channel.kind == ChannelType::Forum,
            _ => false,
        };

        if is_forum {
            let title = if is_voice {
                format!("Voice Note: {filename}")
            } else {
                format!("Upload: {filename}")
            };
            let attachment = CreateAttachment::bytes(bytes, filename);
            let mut create_msg = CreateMessage::new()
                .add_file(attachment)
                .allowed_mentions(safe_allowed_mentions());
            if is_voice {
                create_msg =
                    create_msg.flags(MessageFlags::from_bits_truncate(DISCORD_VOICE_MESSAGE_FLAG));
            }
            let builder = CreateForumPost::new(title, create_msg);
            channel.create_forum_post(&http, builder).await?;
            return Ok(());
        }

        let attachment = CreateAttachment::bytes(bytes, filename);
        let mut create_msg = CreateMessage::new().allowed_mentions(safe_allowed_mentions());
        if is_voice {
            create_msg =
                create_msg.flags(MessageFlags::from_bits_truncate(DISCORD_VOICE_MESSAGE_FLAG));
        }
        channel
            .send_files(&http, vec![attachment], create_msg)
            .await?;
        Ok(())
    }
}

struct ActiveDiscordStream {
    throttler: Arc<LiveEditThrottler<SerenityMessageTransport>>,
    last_sequence: Mutex<Option<u64>>,
}

type StreamKey = (String, Uuid);
type ApprovalMessageTarget = (SessionKey, ChannelId, MessageId);

#[derive(Clone)]
pub struct DiscordEgress {
    clients: Arc<HashMap<String, Arc<serenity::Http>>>,
    default_bot_id: String,
    streams: Arc<Mutex<HashMap<StreamKey, Arc<ActiveDiscordStream>>>>,
    typing: Arc<Mutex<HashMap<String, Typing>>>,
    file_uploader: Arc<dyn DiscordFileUploader>,
    approval_messages: Arc<Mutex<HashMap<Uuid, ApprovalMessageTarget>>>,
    allowed_users: Vec<u64>,
    approval_mentions: bool,
    dead_targets: Arc<DeadTargetRegistry>,
}

#[derive(Clone, Debug)]
pub struct DeadTargetEntry {
    pub channel_id: u64,
    pub reason: String,
    pub marked_at: chrono::DateTime<chrono::Utc>,
}

/// In-memory registry of confirmed-dead Discord channels (403 Forbidden / 404 Not Found).
///
/// Prevents repeated API errors, wasted delivery attempts, and rate-limit burn
/// when a channel is deleted or the bot is kicked/lacks permissions.
/// Self-healing: a successful send or explicit clear removes the channel from the registry.
#[derive(Clone, Debug)]
pub struct DeadTargetRegistry {
    inner: Arc<parking_lot::Mutex<HashMap<u64, DeadTargetEntry>>>,
    ttl: Option<std::time::Duration>,
}

impl Default for DeadTargetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DeadTargetRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            ttl: None,
        }
    }

    pub fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            inner: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            ttl: Some(ttl),
        }
    }

    pub fn is_dead(&self, channel_id: u64) -> bool {
        let mut map = self.inner.lock();
        if let Some(entry) = map.get(&channel_id) {
            if let Some(ttl) = self.ttl {
                let elapsed = (chrono::Utc::now() - entry.marked_at)
                    .to_std()
                    .unwrap_or(std::time::Duration::ZERO);
                if elapsed > ttl {
                    map.remove(&channel_id);
                    return false;
                }
            }
            return true;
        }
        false
    }

    pub fn mark_dead(&self, channel_id: u64, reason: impl Into<String>) -> bool {
        let mut map = self.inner.lock();
        let existed = map.contains_key(&channel_id);
        map.insert(
            channel_id,
            DeadTargetEntry {
                channel_id,
                reason: reason.into(),
                marked_at: chrono::Utc::now(),
            },
        );
        !existed
    }

    pub fn clear(&self, channel_id: u64) -> bool {
        let mut map = self.inner.lock();
        map.remove(&channel_id).is_some()
    }

    pub fn clear_all(&self) {
        let mut map = self.inner.lock();
        map.clear();
    }

    pub fn count(&self) -> usize {
        let map = self.inner.lock();
        map.len()
    }

    pub fn get(&self, channel_id: u64) -> Option<DeadTargetEntry> {
        let map = self.inner.lock();
        map.get(&channel_id).cloned()
    }
}

/// Identifies whether a Serenity HTTP error is a confirmed whole-target 403 Forbidden
/// or 404 Not Found error.
pub fn is_discord_dead_target_error(error: &serenity::Error) -> Option<(u16, String)> {
    if let serenity::Error::Http(serenity::all::HttpError::UnsuccessfulRequest(resp)) = error {
        let code = resp.status_code.as_u16();
        if code == 403 || code == 404 {
            return Some((code, resp.error.message.clone()));
        }
    }
    None
}

impl DiscordEgress {
    /// Creates a single-client egress. Sessions without a bot identity use this
    /// client. Identity-aware multi-bot deployments should use
    /// [`DiscordEgress::with_bot_clients`].
    pub fn new(http: Arc<serenity::Http>) -> Self {
        let default_bot_id = "default".to_owned();
        let clients = HashMap::from([(default_bot_id.clone(), http)]);
        Self {
            clients: Arc::new(clients),
            default_bot_id,
            streams: Arc::new(Mutex::new(HashMap::new())),
            typing: Arc::new(Mutex::new(HashMap::new())),
            file_uploader: Arc::new(SerenityFileUploader),
            approval_messages: Arc::new(Mutex::new(HashMap::new())),
            allowed_users: Vec::new(),
            approval_mentions: false,
            dead_targets: Arc::new(DeadTargetRegistry::new()),
        }
    }

    pub fn with_bot_clients(
        default_bot_id: impl Into<String>,
        clients: HashMap<String, Arc<serenity::Http>>,
    ) -> Result<Self> {
        let default_bot_id = default_bot_id.into();
        if !clients.contains_key(&default_bot_id) {
            return Err(OmonError::Config(format!(
                "default Discord bot identity {default_bot_id} has no HTTP client"
            )));
        }
        Ok(Self {
            clients: Arc::new(clients),
            default_bot_id,
            streams: Arc::new(Mutex::new(HashMap::new())),
            typing: Arc::new(Mutex::new(HashMap::new())),
            file_uploader: Arc::new(SerenityFileUploader),
            approval_messages: Arc::new(Mutex::new(HashMap::new())),
            allowed_users: Vec::new(),
            approval_mentions: false,
            dead_targets: Arc::new(DeadTargetRegistry::new()),
        })
    }

    pub fn dead_targets(&self) -> Arc<DeadTargetRegistry> {
        self.dead_targets.clone()
    }

    pub fn with_dead_targets(mut self, dead_targets: Arc<DeadTargetRegistry>) -> Self {
        self.dead_targets = dead_targets;
        self
    }

    pub fn with_approval_mentions(mut self, allowed_users: Vec<u64>, enabled: bool) -> Self {
        self.allowed_users = allowed_users;
        self.approval_mentions = enabled;
        self
    }

    pub async fn record_approval_message(
        &self,
        request_id: Uuid,
        session: SessionKey,
        channel_id: ChannelId,
        message_id: MessageId,
    ) {
        let mut map = self.approval_messages.lock().await;
        map.insert(request_id, (session, channel_id, message_id));
    }

    pub async fn get_approval_message(
        &self,
        request_id: &Uuid,
    ) -> Option<(SessionKey, ChannelId, MessageId)> {
        let map = self.approval_messages.lock().await;
        map.get(request_id).cloned()
    }

    pub async fn remove_approval_message(
        &self,
        request_id: &Uuid,
    ) -> Option<(SessionKey, ChannelId, MessageId)> {
        let mut map = self.approval_messages.lock().await;
        map.remove(request_id)
    }

    pub async fn approval_message_count(&self) -> usize {
        let map = self.approval_messages.lock().await;
        map.len()
    }

    pub fn with_file_uploader(mut self, uploader: Arc<dyn DiscordFileUploader>) -> Self {
        self.file_uploader = uploader;
        self
    }

    fn target(session: &SessionKey) -> Result<ChannelId> {
        let value = session.thread_id.as_ref().unwrap_or(&session.channel_id);
        value
            .parse::<u64>()
            .map(ChannelId::new)
            .map_err(|_| OmonError::Config(format!("invalid Discord channel ID: {value}")))
    }

    fn identity<'a>(&'a self, session: &'a SessionKey) -> &'a str {
        session.bot_id.as_deref().unwrap_or(&self.default_bot_id)
    }

    fn http_for(&self, session: &SessionKey) -> Result<Arc<serenity::Http>> {
        let identity = self.identity(session);
        self.clients.get(identity).cloned().ok_or_else(|| {
            OmonError::Config(format!(
                "no Discord HTTP client configured for bot identity {identity}"
            ))
        })
    }

    async fn stream(&self, session: SessionKey, chunk: crate::StreamChunk) -> Result<()> {
        let identity = self.identity(&session).to_owned();
        let key = (identity, chunk.stream_id);
        let channel = Self::target(&session)?;
        let http = self.http_for(&session)?;

        let active = {
            let mut streams = self.streams.lock().await;
            if let Some(active) = streams.get(&key) {
                active.clone()
            } else {
                let transport = Arc::new(SerenityMessageTransport::new(http));
                let message_id = transport
                    .send_message(channel, "\u{200b}".to_owned())
                    .await?;
                let active = Arc::new(ActiveDiscordStream {
                    throttler: Arc::new(LiveEditThrottler::new(transport, channel, message_id)),
                    last_sequence: Mutex::new(None),
                });
                streams.insert(key.clone(), active.clone());
                active
            }
        };

        let mut last_sequence = active.last_sequence.lock().await;
        if last_sequence.is_some_and(|sequence| chunk.sequence <= sequence) {
            return Ok(());
        }
        active
            .throttler
            .update(&chunk.content, chunk.is_final)
            .await?;
        *last_sequence = Some(chunk.sequence);
        drop(last_sequence);

        if chunk.is_final {
            let mut streams = self.streams.lock().await;
            if streams
                .get(&key)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &active))
            {
                streams.remove(&key);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl OutboundDispatcher for DiscordEgress {
    async fn dispatch(&self, action: OutboundAction) -> Result<()> {
        match action {
            OutboundAction::SendMessage {
                session,
                content,
                reply_to,
            } => {
                let http = self.http_for(&session)?;
                let channel = Self::target(&session)?;
                let channel_id = channel.get();

                if self.dead_targets.is_dead(channel_id) {
                    tracing::warn!(
                        channel_id,
                        "skipping message send to dead target (403/404 short-circuit)"
                    );
                    return Ok(());
                }

                let reply_id = reply_to
                    .as_deref()
                    .and_then(|id| id.parse::<u64>().ok())
                    .map(MessageId::new);

                let chunks = chunk_markdown(&content, DISCORD_MESSAGE_LIMIT);

                let is_forum = match channel.to_channel(&http).await {
                    Ok(serenity::Channel::Guild(guild_channel)) => {
                        guild_channel.kind == ChannelType::Forum
                    }
                    _ => false,
                };

                if is_forum {
                    let title = derive_forum_post_title(&content);
                    let first_chunk = chunks
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "\u{200b}".to_string());
                    let builder = CreateForumPost::new(
                        title,
                        CreateMessage::new()
                            .content(first_chunk)
                            .allowed_mentions(safe_allowed_mentions()),
                    );
                    match channel.create_forum_post(&http, builder).await {
                        Ok(post_channel) => {
                            self.dead_targets.clear(channel_id);
                            for chunk in chunks.into_iter().skip(1) {
                                if let Err(error) = post_channel
                                    .id
                                    .send_message(
                                        &http,
                                        CreateMessage::new()
                                            .content(chunk)
                                            .allowed_mentions(safe_allowed_mentions()),
                                    )
                                    .await
                                {
                                    if let Some((code, reason)) =
                                        is_discord_dead_target_error(&error)
                                    {
                                        self.dead_targets.mark_dead(
                                            channel_id,
                                            format!("HTTP {code}: {reason}"),
                                        );
                                    }
                                    return Err(error.into());
                                }
                            }
                            return Ok(());
                        }
                        Err(error) => {
                            if let Some((code, reason)) = is_discord_dead_target_error(&error) {
                                self.dead_targets
                                    .mark_dead(channel_id, format!("HTTP {code}: {reason}"));
                            }
                            return Err(error.into());
                        }
                    }
                }

                for (i, chunk) in chunks.into_iter().enumerate() {
                    let reference = should_chunk_reference(i, reply_id);
                    let send_result = if let Some(target_msg_id) = reference {
                        let builder = CreateMessage::new()
                            .content(chunk.clone())
                            .reference_message((channel, target_msg_id))
                            .allowed_mentions(safe_allowed_mentions());
                        match channel.send_message(&http, builder).await {
                            Ok(msg) => Ok(msg),
                            Err(error) => {
                                if let Some((code, reason)) = is_discord_dead_target_error(&error) {
                                    self.dead_targets
                                        .mark_dead(channel_id, format!("HTTP {code}: {reason}"));
                                    tracing::warn!(
                                        channel_id,
                                        code,
                                        %reason,
                                        "marked target channel dead due to 403/404"
                                    );
                                    return Err(error.into());
                                }
                                tracing::warn!(
                                    %error,
                                    target_msg_id = %target_msg_id,
                                    "Failed to send reply with message reference; retrying without reference"
                                );
                                channel
                                    .send_message(
                                        &http,
                                        CreateMessage::new()
                                            .content(chunk)
                                            .allowed_mentions(safe_allowed_mentions()),
                                    )
                                    .await
                            }
                        }
                    } else {
                        channel
                            .send_message(
                                &http,
                                CreateMessage::new()
                                    .content(chunk)
                                    .allowed_mentions(safe_allowed_mentions()),
                            )
                            .await
                    };

                    match send_result {
                        Ok(_) => {
                            self.dead_targets.clear(channel_id);
                        }
                        Err(error) => {
                            if let Some((code, reason)) = is_discord_dead_target_error(&error) {
                                self.dead_targets
                                    .mark_dead(channel_id, format!("HTTP {code}: {reason}"));
                                tracing::warn!(
                                    channel_id,
                                    code,
                                    %reason,
                                    "marked target channel dead due to 403/404"
                                );
                            }
                            return Err(error.into());
                        }
                    }
                }
            }
            OutboundAction::EditMessage {
                session,
                platform_message_id,
                content,
            } => {
                let http = self.http_for(&session)?;
                let channel = Self::target(&session)?;
                let channel_id = channel.get();

                if self.dead_targets.is_dead(channel_id) {
                    tracing::warn!(
                        channel_id,
                        "skipping message edit to dead target (403/404 short-circuit)"
                    );
                    return Ok(());
                }

                let message_id = platform_message_id
                    .parse::<u64>()
                    .map(MessageId::new)
                    .map_err(|_| {
                        OmonError::Config(format!(
                            "invalid Discord message ID: {platform_message_id}"
                        ))
                    })?;
                match channel
                    .edit_message(
                        &http,
                        message_id,
                        EditMessage::new()
                            .content(content)
                            .allowed_mentions(safe_allowed_mentions()),
                    )
                    .await
                {
                    Ok(_) => {
                        self.dead_targets.clear(channel_id);
                    }
                    Err(error) => {
                        if let Some((code, reason)) = is_discord_dead_target_error(&error) {
                            self.dead_targets
                                .mark_dead(channel_id, format!("HTTP {code}: {reason}"));
                            tracing::warn!(
                                channel_id,
                                code,
                                %reason,
                                "marked target channel dead due to 403/404"
                            );
                        }
                        return Err(error.into());
                    }
                }
            }
            OutboundAction::DeleteMessage {
                session,
                platform_message_id,
            } => {
                let http = self.http_for(&session)?;
                let channel = Self::target(&session)?;
                let channel_id = channel.get();

                if self.dead_targets.is_dead(channel_id) {
                    return Ok(());
                }

                let message_id = platform_message_id
                    .parse::<u64>()
                    .map(MessageId::new)
                    .map_err(|_| {
                        OmonError::Config(format!(
                            "invalid Discord message ID: {platform_message_id}"
                        ))
                    })?;
                match channel.delete_message(&http, message_id).await {
                    Ok(_) => {
                        self.dead_targets.clear(channel_id);
                    }
                    Err(error) => {
                        if let Some((code, reason)) = is_discord_dead_target_error(&error) {
                            self.dead_targets
                                .mark_dead(channel_id, format!("HTTP {code}: {reason}"));
                        }
                        return Err(error.into());
                    }
                }
            }
            OutboundAction::UploadFile { session, path } => {
                let http = self.http_for(&session)?;
                let channel = Self::target(&session)?;
                let channel_id = channel.get();

                if self.dead_targets.is_dead(channel_id) {
                    tracing::warn!(
                        channel_id,
                        "skipping file upload to dead target (403/404 short-circuit)"
                    );
                    return Ok(());
                }

                match self.file_uploader.upload(http, channel, &path).await {
                    Ok(()) => {
                        self.dead_targets.clear(channel_id);
                    }
                    Err(err) => {
                        if let OmonError::Discord(boxed) = &err {
                            if let Some((code, reason)) = is_discord_dead_target_error(boxed) {
                                self.dead_targets
                                    .mark_dead(channel_id, format!("HTTP {code}: {reason}"));
                                tracing::warn!(
                                    channel_id,
                                    code,
                                    %reason,
                                    "marked target channel dead during upload"
                                );
                            }
                        }
                        return Err(err);
                    }
                }
            }
            OutboundAction::Stream { session, chunk } => {
                self.stream(session, chunk).await?;
            }
            OutboundAction::Typing { session, active } => {
                let channel = Self::target(&session)?;
                let channel_id = channel.get();
                if self.dead_targets.is_dead(channel_id) {
                    return Ok(());
                }

                if active {
                    let http = self.http_for(&session)?;
                    let guard = http.start_typing(channel);
                    self.typing
                        .lock()
                        .await
                        .insert(session.storage_key(), guard);
                } else {
                    self.typing.lock().await.remove(&session.storage_key());
                }
            }
            OutboundAction::React {
                session,
                message_id,
                emoji,
                remove_others,
            } => {
                let channel = Self::target(&session)?;
                let channel_id = channel.get();
                if self.dead_targets.is_dead(channel_id) {
                    return Ok(());
                }

                let http = self.http_for(&session)?;
                let msg_id = message_id.parse::<u64>().map(MessageId::new).map_err(|_| {
                    OmonError::Config(format!("invalid Discord message ID: {message_id}"))
                })?;
                if remove_others {
                    if let Err(error) = channel
                        .delete_reaction(
                            &http,
                            msg_id,
                            None,
                            serenity::all::ReactionType::Unicode(
                                crate::models::PROCESSING_START_EMOJI.to_string(),
                            ),
                        )
                        .await
                    {
                        tracing::debug!(
                            %error,
                            %message_id,
                            "Failed to remove start processing reaction"
                        );
                    }
                }
                if let Err(error) = channel
                    .create_reaction(&http, msg_id, serenity::all::ReactionType::Unicode(emoji))
                    .await
                {
                    tracing::debug!(
                        %error,
                        %message_id,
                        "Failed to add reaction to message"
                    );
                }
            }
            OutboundAction::ApprovalRequest {
                session,
                request_id,
                command,
                reason,
            } => {
                let http = self.http_for(&session)?;
                let channel = Self::target(&session)?;
                let channel_id = channel.get();

                if self.dead_targets.is_dead(channel_id) {
                    tracing::warn!(
                        channel_id,
                        "skipping approval request to dead target (403/404 short-circuit)"
                    );
                    return Ok(());
                }

                let content = build_approval_content_with_mentions(
                    &command,
                    &reason,
                    &self.allowed_users,
                    self.approval_mentions,
                );
                let embed = build_approval_embed(&command, &reason);
                match channel
                    .send_message(
                        &http,
                        CreateMessage::new()
                            .content(content)
                            .embed(embed)
                            .components(approval_buttons(request_id))
                            .allowed_mentions(safe_allowed_mentions()),
                    )
                    .await
                {
                    Ok(msg) => {
                        self.dead_targets.clear(channel_id);
                        self.record_approval_message(request_id, session, channel, msg.id)
                            .await;
                    }
                    Err(error) => {
                        if let Some((code, reason)) = is_discord_dead_target_error(&error) {
                            self.dead_targets
                                .mark_dead(channel_id, format!("HTTP {code}: {reason}"));
                            tracing::warn!(
                                channel_id,
                                code,
                                %reason,
                                "marked target channel dead due to 403/404 on approval request"
                            );
                        }
                        return Err(error.into());
                    }
                }
            }
            OutboundAction::ExpireApproval { request_id } => {
                if let Some((session, channel, message_id)) =
                    self.remove_approval_message(&request_id).await
                {
                    let channel_id = channel.get();
                    if !self.dead_targets.is_dead(channel_id) {
                        if let Ok(http) = self.http_for(&session) {
                            let _ = channel
                                .edit_message(
                                    &http,
                                    message_id,
                                    EditMessage::new()
                                        .components(Vec::new())
                                        .content("⏱ Approval request expired")
                                        .allowed_mentions(safe_allowed_mentions()),
                                )
                                .await;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub const APPROVAL_REASON_BUDGET: usize = 300;
pub const APPROVAL_COMMAND_EMBED_LIMIT: usize = 4000;
pub const APPROVAL_CONTENT_LIMIT: usize = 2000;

pub fn truncate_approval_reason(reason: &str) -> String {
    let trimmed = reason.trim();
    if trimmed.chars().count() > APPROVAL_REASON_BUDGET {
        let prefix: String = trimmed
            .chars()
            .take(APPROVAL_REASON_BUDGET.saturating_sub(18))
            .collect();
        format!("{prefix}... [truncated]")
    } else if trimmed.is_empty() {
        "dangerous command".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn truncate_approval_command(command: &str, max_len: usize) -> String {
    let trimmed = command.trim();
    if trimmed.chars().count() > max_len {
        let prefix: String = trimmed.chars().take(max_len.saturating_sub(18)).collect();
        format!("{prefix}\n... [truncated]")
    } else {
        trimmed.to_string()
    }
}

pub fn build_approval_embed(command: &str, reason: &str) -> CreateEmbed {
    let reason_display = truncate_approval_reason(reason);
    let cmd_display = truncate_approval_command(command, APPROVAL_COMMAND_EMBED_LIMIT);
    CreateEmbed::new()
        .title("⚠️ Approval Required")
        .description(format!("```text\n{cmd_display}\n```"))
        .field("Reason", reason_display, false)
        .color(Color::from_rgb(243, 156, 18))
}

pub fn build_approval_content(command: &str, reason: &str) -> String {
    let reason_display = truncate_approval_reason(reason);
    let prefix = "⚠️ **Approval Required**\n\nCommand requested:\n```text\n";
    let suffix = format!("\n```\n**Reason:** {reason_display}");
    let budget = APPROVAL_CONTENT_LIMIT.saturating_sub(prefix.len() + suffix.len());
    let cmd_display = truncate_approval_command(command, budget);
    format!("{prefix}{cmd_display}{suffix}")
}

pub fn build_approval_mentions(allowed_users: &[u64], enabled: bool) -> Option<String> {
    if !enabled || allowed_users.is_empty() {
        return None;
    }
    let mut sorted_users = allowed_users.to_vec();
    sorted_users.sort_unstable();
    let mentions: Vec<String> = sorted_users
        .into_iter()
        .map(|uid| format!("<@{uid}>"))
        .collect();
    Some(mentions.join(" "))
}

pub fn build_approval_content_with_mentions(
    command: &str,
    reason: &str,
    allowed_users: &[u64],
    mentions_enabled: bool,
) -> String {
    let plain_content = build_approval_content(command, reason);
    if let Some(mentions) = build_approval_mentions(allowed_users, mentions_enabled) {
        format!("{mentions}\n\n{plain_content}")
    } else {
        plain_content
    }
}

pub fn is_authorized_clicker(user_id: u64, allowed: &[u64]) -> bool {
    allowed.is_empty() || allowed.contains(&user_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use std::time::Duration;

    fn test_session(name: &str) -> SessionKey {
        SessionKey::new("discord", Some("guild"), "channel", None::<String>, name)
    }

    #[test]
    fn test_format_runtime_footer_and_append() {
        let cwd = Path::new("/tmp/test_workspace");
        let footer = format_runtime_footer(Some("openai/gpt-4o"), Some(45), Some(cwd));
        assert_eq!(footer, "gpt-4o · 45% · /tmp/test_workspace");

        let app = append_runtime_footer("Answer text", Some("gpt-4o"), Some(45), Some(cwd));
        assert_eq!(app, "Answer text\n\n_gpt-4o · 45% · /tmp/test_workspace_");

        // Skips missing fields
        let partial = format_runtime_footer(Some("claude-3-5-sonnet"), None, None);
        assert_eq!(partial, "claude-3-5-sonnet");

        // Clamps percentage
        let clamped = format_runtime_footer(None, Some(150), None);
        assert_eq!(clamped, "100%");

        // Empty footer on no fields
        assert_eq!(format_runtime_footer(None, None, None), "");
        assert_eq!(append_runtime_footer("Plain", None, None, None), "Plain");
    }

    #[test]
    fn test_build_voice_metadata() {
        let dummy_audio = vec![10u8, 20, 50, 100, 200, 255, 128, 64];
        let meta = build_voice_metadata(&dummy_audio, Some(2.5));
        assert_eq!(meta.duration_secs, 2.5);
        assert_eq!(meta.flags, DISCORD_VOICE_MESSAGE_FLAG);
        assert!(!meta.waveform.is_empty());

        // Empty audio fallback
        let empty_meta = build_voice_metadata(&[], None);
        assert_eq!(empty_meta.duration_secs, 1.0);
        assert_eq!(empty_meta.flags, 8192);
        assert!(!empty_meta.waveform.is_empty());
    }

    #[test]
    fn test_is_voice_audio_file() {
        assert!(is_voice_audio_file("recording.ogg", None));
        assert!(is_voice_audio_file("speech.opus", None));
        assert!(is_voice_audio_file("voice-message.ogg", None));
        assert!(is_voice_audio_file("test.mp3", Some("audio/ogg")));
        assert!(is_voice_audio_file("audio.bin", Some("audio/opus")));
        assert!(is_voice_audio_file(
            "attachment",
            Some("audio/ogg; codecs=opus")
        ));

        assert!(!is_voice_audio_file("document.pdf", None));
        assert!(!is_voice_audio_file("image.png", Some("image/png")));
        assert!(!is_voice_audio_file("song.mp3", Some("audio/mpeg")));
    }

    #[test]
    fn test_derive_forum_post_title() {
        // Plain single line
        assert_eq!(
            derive_forum_post_title("How do we optimize SQLite queries?"),
            "How do we optimize SQLite queries?"
        );

        // Markdown header prefix
        assert_eq!(
            derive_forum_post_title("### Architecture Review for Q3\n\nSome body text here"),
            "Architecture Review for Q3"
        );

        // Mentions stripped
        assert_eq!(
            derive_forum_post_title("<@123456789> <@!987654321> Discussion about deployment"),
            "Discussion about deployment"
        );

        // Long title truncated to <= 100 chars
        let long_input = "a".repeat(150);
        let derived = derive_forum_post_title(&long_input);
        assert_eq!(derived.chars().count(), 100);
        assert!(derived.ends_with("..."));

        // Empty / whitespace
        assert_eq!(derive_forum_post_title(""), "New Discussion");
        assert_eq!(derive_forum_post_title("   \n\n  "), "New Discussion");

        // Single character padded to >= 2 chars
        assert_eq!(derive_forum_post_title("x"), "x Discussion");
    }

    #[test]
    fn coalesce_empty_returns_none() {
        assert!(coalesce_inbound_events(Vec::new()).is_none());
    }

    #[test]
    fn coalesce_single_message_preserves_event() {
        let session = test_session("user-1");
        let event = InboundEvent::message(session.clone(), "msg-1", "hello world");
        let coalesced = coalesce_inbound_events(vec![event.clone()]).unwrap();
        assert_eq!(coalesced.id, event.id);
        assert_eq!(coalesced.content, "hello world");
        assert_eq!(coalesced.platform_message_id, "msg-1");
        assert_eq!(coalesced.session, session);
    }

    #[test]
    fn coalesce_multiple_split_messages_concatenates_contents_and_unions_attachments() {
        let session = test_session("user-2");
        let mut event1 = InboundEvent::message(session.clone(), "chunk-1", "Part 1 of long text");
        event1.attachments = vec![
            MessageAttachment {
                id: "att-1".into(),
                filename: "file1.txt".into(),
                url: "https://example.com/1".into(),
                content_type: Some("text/plain".into()),
                size_bytes: Some(100),
                local_path: None,
                text_content: None,
            },
            MessageAttachment {
                id: "att-2".into(),
                filename: "file2.png".into(),
                url: "https://example.com/2".into(),
                content_type: Some("image/png".into()),
                size_bytes: Some(200),
                local_path: None,
                text_content: None,
            },
        ];

        let mut event2 = InboundEvent::message(session.clone(), "chunk-2", "Part 2 of long text");
        event2.delivery_id = Some("discord:chunk-2".into());
        // att-2 duplicate + att-3 new
        event2.attachments = vec![
            MessageAttachment {
                id: "att-2".into(),
                filename: "file2-duplicate.png".into(),
                url: "https://example.com/2".into(),
                content_type: Some("image/png".into()),
                size_bytes: Some(200),
                local_path: None,
                text_content: None,
            },
            MessageAttachment {
                id: "att-3".into(),
                filename: "file3.pdf".into(),
                url: "https://example.com/3".into(),
                content_type: Some("application/pdf".into()),
                size_bytes: Some(300),
                local_path: None,
                text_content: None,
            },
        ];

        let event3 = InboundEvent::message(session.clone(), "chunk-3", "Part 3 of long text");

        let coalesced = coalesce_inbound_events(vec![event1.clone(), event2, event3]).unwrap();

        assert_eq!(coalesced.id, event1.id);
        assert_eq!(
            coalesced.content,
            "Part 1 of long text\nPart 2 of long text\nPart 3 of long text"
        );
        assert_eq!(coalesced.platform_message_id, "chunk-3");
        assert_eq!(coalesced.delivery_id.as_deref(), Some("discord:chunk-3"));

        assert_eq!(coalesced.attachments.len(), 3);
        assert_eq!(coalesced.attachments[0].id, "att-1");
        assert_eq!(coalesced.attachments[1].id, "att-2");
        assert_eq!(coalesced.attachments[2].id, "att-3");
    }

    #[tokio::test]
    async fn debouncer_coalesces_rapid_events_and_routes_single_event() {
        use crate::{AgentRunner, MultiplexerConfig, SessionContext, SessionMultiplexer};

        struct CollectingRunner {
            events: Mutex<Vec<InboundEvent>>,
        }

        #[async_trait]
        impl AgentRunner for CollectingRunner {
            async fn run(&self, _session: &mut SessionContext, event: InboundEvent) -> Result<()> {
                self.events.lock().await.push(event);
                Ok(())
            }
        }

        let db = Database::connect("sqlite::memory:").await.unwrap();
        let runner = Arc::new(CollectingRunner {
            events: Mutex::new(Vec::new()),
        });
        let multiplexer = SessionMultiplexer::new(
            db.pool().clone(),
            runner.clone(),
            MultiplexerConfig::default(),
        );
        let data = PoiseData::new(multiplexer, db.pool().clone());

        let debouncer = SplitMessageDebouncer::new(Duration::from_millis(50));
        let session = test_session("debouncer-test-user");

        let msg1 = InboundEvent::message(session.clone(), "msg-1", "chunk 1");
        let msg2 = InboundEvent::message(session.clone(), "msg-2", "chunk 2");
        let msg3 = InboundEvent::message(session.clone(), "msg-3", "chunk 3");

        debouncer.enqueue(msg1, data.clone()).await;
        tokio::time::sleep(Duration::from_millis(15)).await;
        debouncer.enqueue(msg2, data.clone()).await;
        tokio::time::sleep(Duration::from_millis(15)).await;
        debouncer.enqueue(msg3, data.clone()).await;

        tokio::time::sleep(Duration::from_millis(120)).await;

        let runs = runner.events.lock().await;
        assert_eq!(runs.len(), 1, "expected exactly 1 coalesced turn");
        assert_eq!(runs[0].content, "chunk 1\nchunk 2\nchunk 3");
        assert_eq!(runs[0].platform_message_id, "msg-3");
    }

    #[tokio::test]
    async fn debouncer_cancel_discards_pending_chunks() {
        use crate::{AgentRunner, MultiplexerConfig, SessionContext, SessionMultiplexer};

        struct DummyRunner;
        #[async_trait]
        impl AgentRunner for DummyRunner {
            async fn run(&self, _session: &mut SessionContext, _event: InboundEvent) -> Result<()> {
                Ok(())
            }
        }

        let debouncer = SplitMessageDebouncer::new(Duration::from_millis(100));
        let session = test_session("cancel-test-user");
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let runner = Arc::new(DummyRunner);
        let multiplexer =
            SessionMultiplexer::new(db.pool().clone(), runner, MultiplexerConfig::default());
        let data = PoiseData::new(multiplexer, db.pool().clone());

        let msg1 = InboundEvent::message(session.clone(), "msg-1", "chunk 1");
        debouncer.enqueue(msg1, data).await;

        let cancelled: Option<Vec<InboundEvent>> = debouncer.cancel(&session).await;
        assert!(cancelled.is_some());
        assert_eq!(cancelled.unwrap().len(), 1);
        assert!(debouncer.is_empty().await);
    }

    #[test]
    fn compose_reply_context_standard() {
        let result = compose_reply_context("alice", "hello there", "general kenobi");
        assert_eq!(
            result,
            "> [Replying to @alice]: hello there\n\ngeneral kenobi"
        );
    }

    #[test]
    fn compose_reply_context_caps_at_500_chars() {
        let long_content = "x".repeat(600);
        let result = compose_reply_context("bob", &long_content, "my reply");
        let expected_quote = format!("> [Replying to @bob]: {}...\n\nmy reply", "x".repeat(500));
        assert_eq!(result, expected_quote);
    }

    #[test]
    fn compose_reply_context_empty_body() {
        let result = compose_reply_context("carol", "check this file", "");
        assert_eq!(result, "> [Replying to @carol]: check this file");
    }

    #[test]
    fn compose_reply_context_empty_referenced_content() {
        let result = compose_reply_context("dave", "", "my response");
        assert_eq!(result, "> [Replying to @dave]\n\nmy response");
    }

    #[test]
    fn test_format_channel_context_standard() {
        let history = vec![
            ("alice", "What's the weather today?"),
            ("bob", "Looks like rain in Seattle."),
        ];
        let formatted = format_channel_context(&history);
        assert_eq!(
            formatted,
            "[Recent channel context]\nalice: What's the weather today?\nbob: Looks like rain in Seattle."
        );
    }

    #[test]
    fn test_format_channel_context_empty_and_whitespace() {
        let history: Vec<(&str, &str)> = vec![];
        assert_eq!(format_channel_context(&history), "");

        let history = vec![("alice", "   "), ("", "hello"), ("bob", "actual message")];
        assert_eq!(
            format_channel_context(&history),
            "[Recent channel context]\nbob: actual message"
        );
    }

    #[test]
    fn test_format_channel_context_line_truncation() {
        let long_line = "a".repeat(300);
        let history = vec![("charlie", long_line.as_str())];
        let formatted = format_channel_context(&history);
        let expected = format!("[Recent channel context]\ncharlie: {}...", "a".repeat(197));
        assert_eq!(formatted, expected);
    }

    #[test]
    fn test_format_channel_context_collapses_multiline() {
        let multiline = "line 1\nline 2\n    line 3";
        let history = vec![("alice", multiline)];
        let formatted = format_channel_context(&history);
        assert_eq!(
            formatted,
            "[Recent channel context]\nalice: line 1 line 2 line 3"
        );
    }

    #[test]
    fn test_is_silence_response_positive_cases() {
        let positive = [
            "",
            "   \n\t ",
            "[SILENT]",
            " SILENT ",
            "NO_REPLY",
            "no reply",
            "NO REPLY",
            "no_reply",
            ".NO_REPLY",
            "*NO_REPLY*",
            " .NO_REPLY ",
            "*[SILENT]*",
            "NO_REPLY.",
            "[silent]",
            "*(silent)*",
            "*Silence.*",
            "🔇",
            ".",
            "…",
            "...",
            "(silent)",
            "_silent_",
            "silent",
            " *(silent)* ",
            "`silent`",
            "~silent~",
            "Silence",
            "no response",
            "No Reply.",
            &".".repeat(64),
        ];

        for text in positive {
            assert!(
                is_silence_response(text),
                "expected {text:?} to be detected as silence"
            );
        }
    }

    #[test]
    fn test_is_silence_response_negative_cases() {
        let negative = [
            "Use NO_REPLY when no answer is needed.",
            "The reply was [SILENT], intentionally.",
            "😄 NO_REPLY",
            "[SILENT",
            "Silence is golden — here is the plan...",
            "Silent install completed",
            "The deployment ran silently in the background",
            "ok",
            "👍",
            "Here is the result:\n\n- item one\n- item two",
            "I have nothing to add, but here is why: the build is green.",
            "silently",
            "no responses were collected from the survey",
            &("silent ".to_string() + &"x".repeat(70)),
            &".".repeat(65),
        ];

        for text in negative {
            assert!(
                !is_silence_response(text),
                "expected {text:?} NOT to be detected as silence"
            );
        }
    }

    #[test]
    fn test_extract_media_directives_single() {
        let text = "Here is the screenshot:\nMEDIA:/tmp/screenshot.png";
        let (cleaned, paths) = extract_media_directives(text);
        assert_eq!(cleaned, "Here is the screenshot:");
        assert_eq!(paths, vec!["/tmp/screenshot.png"]);
    }

    #[test]
    fn test_extract_media_directives_multiple_and_wrapped() {
        let text = "MEDIA: `/tmp/data.csv`\nMEDIA: \"/tmp/report.pdf\"\nMEDIA: '/tmp/notes.txt'";
        let (cleaned, paths) = extract_media_directives(text);
        assert_eq!(cleaned, "");
        assert_eq!(
            paths,
            vec!["/tmp/data.csv", "/tmp/report.pdf", "/tmp/notes.txt"]
        );
    }

    #[test]
    fn test_extract_media_directives_mixed_with_text() {
        let text =
            "Generated the document below:\nMEDIA:/tmp/doc.pdf\nPlease review and let me know.";
        let (cleaned, paths) = extract_media_directives(text);
        assert_eq!(
            cleaned,
            "Generated the document below:\nPlease review and let me know."
        );
        assert_eq!(paths, vec!["/tmp/doc.pdf"]);
    }

    #[test]
    fn test_extract_media_directives_nonexistent_paths_returned() {
        let text = "MEDIA:/does/not/exist/file.jpg";
        let (cleaned, paths) = extract_media_directives(text);
        assert_eq!(cleaned, "");
        assert_eq!(paths, vec!["/does/not/exist/file.jpg"]);
    }

    #[test]
    fn test_extract_media_directives_strips_audio_directives() {
        let text = "[[audio_as_voice]]\n[[as_document]]\nMEDIA:/tmp/voice.ogg";
        let (cleaned, paths) = extract_media_directives(text);
        assert_eq!(cleaned, "");
        assert_eq!(paths, vec!["/tmp/voice.ogg"]);
    }

    #[test]
    fn test_extract_media_directives_plain_text_unchanged() {
        let text = "Just normal conversational text without media.";
        let (cleaned, paths) = extract_media_directives(text);
        assert_eq!(cleaned, text);
        assert!(paths.is_empty());
    }

    #[test]
    fn test_should_chunk_reference() {
        let reply_id = MessageId::new(123456);
        assert_eq!(should_chunk_reference(0, Some(reply_id)), Some(reply_id));
        assert_eq!(should_chunk_reference(1, Some(reply_id)), None);
        assert_eq!(should_chunk_reference(2, Some(reply_id)), None);
        assert_eq!(should_chunk_reference(0, None), None);
        assert_eq!(should_chunk_reference(1, None), None);
    }

    #[test]
    fn test_approval_reason_and_command_truncation() {
        let short_reason = "recursive delete";
        assert_eq!(truncate_approval_reason(short_reason), "recursive delete");
        assert_eq!(truncate_approval_reason(""), "dangerous command");

        let long_reason = "a".repeat(500);
        let truncated_reason = truncate_approval_reason(&long_reason);
        assert!(truncated_reason.ends_with("... [truncated]"));
        assert!(truncated_reason.chars().count() <= APPROVAL_REASON_BUDGET);

        let short_cmd = "rm -rf /tmp/data";
        assert_eq!(
            truncate_approval_command(short_cmd, 100),
            "rm -rf /tmp/data"
        );

        let long_cmd = "echo ".to_string() + &"x".repeat(5000);
        let truncated_cmd = truncate_approval_command(&long_cmd, APPROVAL_COMMAND_EMBED_LIMIT);
        assert!(truncated_cmd.ends_with("\n... [truncated]"));
        assert!(truncated_cmd.chars().count() <= APPROVAL_COMMAND_EMBED_LIMIT);
    }

    #[test]
    fn test_build_approval_embed_and_content() {
        let cmd = "rm -rf /tmp/build";
        let reason = "recursive delete";
        let embed = build_approval_embed(cmd, reason);
        let content = build_approval_content(cmd, reason);

        let json = serde_json::to_value(&embed).unwrap();
        assert_eq!(json["title"], "⚠️ Approval Required");
        assert!(json["description"]
            .as_str()
            .unwrap()
            .contains("```text\nrm -rf /tmp/build\n```"));
        assert_eq!(json["fields"][0]["name"], "Reason");
        assert_eq!(json["fields"][0]["value"], "recursive delete");

        assert!(content.contains("⚠️ **Approval Required**"));
        assert!(content.contains("rm -rf /tmp/build"));
        assert!(content.contains("**Reason:** recursive delete"));
    }

    #[tokio::test]
    async fn test_egress_approval_message_tracking_map() {
        let http = Arc::new(serenity::Http::new("test-token"));
        let egress = DiscordEgress::new(http);

        let req_id = Uuid::new_v4();
        let session = test_session("user-1");
        let channel_id = ChannelId::new(12345);
        let msg_id = MessageId::new(67890);

        assert_eq!(egress.approval_message_count().await, 0);
        assert!(egress.get_approval_message(&req_id).await.is_none());

        egress
            .record_approval_message(req_id, session.clone(), channel_id, msg_id)
            .await;

        assert_eq!(egress.approval_message_count().await, 1);
        let recorded = egress.get_approval_message(&req_id).await.unwrap();
        assert_eq!(recorded.0, session);
        assert_eq!(recorded.1, channel_id);
        assert_eq!(recorded.2, msg_id);

        let removed = egress.remove_approval_message(&req_id).await.unwrap();
        assert_eq!(removed.0, session);
        assert_eq!(removed.1, channel_id);
        assert_eq!(removed.2, msg_id);

        assert_eq!(egress.approval_message_count().await, 0);
        assert!(egress.get_approval_message(&req_id).await.is_none());
    }

    #[test]
    fn test_build_approval_mentions() {
        // Disabled -> None
        assert_eq!(build_approval_mentions(&[12345, 67890], false), None);

        // Enabled but empty allowed users -> None
        assert_eq!(build_approval_mentions(&[], true), None);

        // Single user
        assert_eq!(
            build_approval_mentions(&[12345], true),
            Some("<@12345>".to_string())
        );

        // Multiple users sorted
        assert_eq!(
            build_approval_mentions(&[99999, 11111, 55555], true),
            Some("<@11111> <@55555> <@99999>".to_string())
        );

        // Content with mentions
        let content = build_approval_content_with_mentions(
            "rm -rf /tmp/test",
            "recursive delete",
            &[12345, 67890],
            true,
        );
        assert!(content.starts_with("<@12345> <@67890>\n\n"));
        assert!(content.contains("⚠️ **Approval Required**"));
        assert!(content.contains("rm -rf /tmp/test"));

        // Content without mentions
        let content_no_mentions = build_approval_content_with_mentions(
            "rm -rf /tmp/test",
            "recursive delete",
            &[12345, 67890],
            false,
        );
        assert!(!content_no_mentions.starts_with("<@"));
        assert!(content_no_mentions.starts_with("⚠️ **Approval Required**"));
    }

    #[test]
    fn test_dead_target_registry_lifecycle_and_ttl() {
        let registry = DeadTargetRegistry::new();

        assert!(!registry.is_dead(12345));
        assert_eq!(registry.count(), 0);

        // Mark channel 12345 dead
        let newly_added = registry.mark_dead(12345, "HTTP 404: Unknown Channel");
        assert!(newly_added);
        assert!(registry.is_dead(12345));
        assert_eq!(registry.count(), 1);

        // Second mark is not newly added
        assert!(!registry.mark_dead(12345, "HTTP 403: Forbidden"));
        assert_eq!(registry.count(), 1);

        let entry = registry.get(12345).unwrap();
        assert_eq!(entry.channel_id, 12345);
        assert_eq!(entry.reason, "HTTP 403: Forbidden");

        // Clear channel 12345
        assert!(registry.clear(12345));
        assert!(!registry.is_dead(12345));
        assert_eq!(registry.count(), 0);
        assert!(!registry.clear(12345));

        // Test with TTL
        let ttl_registry = DeadTargetRegistry::with_ttl(Duration::from_millis(50));
        ttl_registry.mark_dead(99999, "HTTP 403: Forbidden");
        assert!(ttl_registry.is_dead(99999));

        std::thread::sleep(Duration::from_millis(60));
        // Expired after TTL -> false
        assert!(!ttl_registry.is_dead(99999));
        assert_eq!(ttl_registry.count(), 0);
    }

    #[test]
    fn test_dead_target_registry_clear_all() {
        let registry = DeadTargetRegistry::new();
        registry.mark_dead(111, "HTTP 403");
        registry.mark_dead(222, "HTTP 404");
        registry.mark_dead(333, "HTTP 403");
        assert_eq!(registry.count(), 3);

        registry.clear_all();
        assert_eq!(registry.count(), 0);
        assert!(!registry.is_dead(111));
        assert!(!registry.is_dead(222));
        assert!(!registry.is_dead(333));
    }
}
