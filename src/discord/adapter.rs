use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use poise::serenity_prelude as serenity;
use serenity::all::{
    ChannelId, ChannelType, CreateAttachment, CreateInteractionResponse,
    CreateInteractionResponseMessage, CreateMessage, EditMessage, FullEvent, GatewayIntents,
    Interaction, Message, MessageId, Typing,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::approval::{approval_buttons, is_approval_custom_id, SmartApprovalGuard};
use super::commands::{self, CommandError, PoiseData};
use super::throttler::{
    chunk_markdown, DiscordMessageTransport, LiveEditThrottler, SerenityMessageTransport,
    DISCORD_MESSAGE_LIMIT,
};
use crate::{
    DeliveryLedgerService, InboundEvent, MessageAttachment, OmonError, OutboundAction,
    OutboundDispatcher, Result, SessionKey,
};

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
        Ok(serenity::ClientBuilder::new(token.as_ref(), intents)
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
        let Some(event) = message_to_inbound_with_config(
            message,
            bot_user_id,
            channel_type,
            &self.data.free_response_channels,
            &self.data.allowed_users,
            self.data.primary_bot_id,
        ) else {
            return Ok(false);
        };
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
            if let Some(event) = message_to_inbound_with_config(
                new_message,
                bot_user_id,
                channel_type,
                &data.free_response_channels,
                &data.allowed_users,
                data.primary_bot_id,
            ) {
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
                if !is_authorized_clicker(component.user.id.get(), &data.allowed_users) {
                    let refusal = CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .ephemeral(true)
                            .content("You are not authorized to approve commands."),
                    );
                    component.create_response(ctx, refusal).await?;
                    return Ok(());
                }

                let response = if data
                    .approvals
                    .resolve_custom_id(&component.data.custom_id)
                    .await
                {
                    CreateInteractionResponse::UpdateMessage(
                        CreateInteractionResponseMessage::new().components(Vec::new()),
                    )
                } else {
                    CreateInteractionResponse::UpdateMessage(
                        CreateInteractionResponseMessage::new()
                            .components(Vec::new())
                            .content("이 승인 요청은 더 이상 유효하지 않습니다 (게이트웨이 재시작 또는 만료됨). 명령을 다시 실행해 주세요."),
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
    message_to_inbound_with_config(
        message,
        bot_user_id,
        channel_type,
        &[],
        &[],
        Some(bot_user_id.get()),
    )
}

pub fn message_to_inbound_with_config(
    message: &Message,
    bot_user_id: serenity::UserId,
    channel_type: Option<ChannelType>,
    free_response_channels: &[u64],
    allowed_users: &[u64],
    primary_bot_id: Option<u64>,
) -> Option<InboundEvent> {
    if message.author.bot || message.webhook_id.is_some() {
        return None;
    }

    // Only process standard text messages and replies. Ignore system messages (thread joins, removals, pins, etc.)
    if !matches!(
        message.kind,
        serenity::model::channel::MessageType::Regular
            | serenity::model::channel::MessageType::InlineReply
    ) {
        return None;
    }

    if !allowed_users.is_empty() && !allowed_users.contains(&message.author.id.get()) {
        return None;
    }

    let is_dm = message.guild_id.is_none();
    let is_thread = channel_type.is_some_and(is_thread);
    let mentioned_bot_ids = message
        .mentions
        .iter()
        .filter(|user| user.bot)
        .map(|user| user.id)
        .collect::<Vec<_>>();
    let is_explicit_mention = mentioned_bot_ids.contains(&bot_user_id);
    let is_free_channel = free_response_channels.contains(&message.channel_id.get());

    if !mentioned_bot_ids.is_empty() {
        if !is_explicit_mention {
            return None;
        }
    } else {
        let is_implicit_response_channel = is_dm || is_thread || is_free_channel;
        if !is_implicit_response_channel {
            return None;
        }
        // Guild threads and free-response channels are visible to every bot, so only
        // the primary bot auto-responds there to avoid duplicate replies. DMs are 1:1
        // per bot, so each bot must always answer its own DMs.
        if !is_dm && primary_bot_id != Some(bot_user_id.get()) {
            return None;
        }
    }

    let content = strip_bot_mention(&message.content, bot_user_id);
    if content.trim().is_empty() && message.attachments.is_empty() {
        return None; // Do NOT auto-inject "Hello!" for empty/system messages
    }
    let channel_id = message.channel_id.to_string();
    let session = SessionKey::new(
        "discord",
        message.guild_id.map(|id| id.to_string()),
        channel_id.clone(),
        is_thread.then_some(channel_id),
        message.author.id.to_string(),
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
        let attachment = CreateAttachment::bytes(bytes, filename);
        channel
            .send_files(&http, vec![attachment], CreateMessage::new())
            .await?;
        Ok(())
    }
}

struct ActiveDiscordStream {
    throttler: Arc<LiveEditThrottler<SerenityMessageTransport>>,
    last_sequence: Mutex<Option<u64>>,
}

type StreamKey = (String, Uuid);

#[derive(Clone)]
pub struct DiscordEgress {
    clients: Arc<HashMap<String, Arc<serenity::Http>>>,
    default_bot_id: String,
    streams: Arc<Mutex<HashMap<StreamKey, Arc<ActiveDiscordStream>>>>,
    typing: Arc<Mutex<HashMap<String, Typing>>>,
    file_uploader: Arc<dyn DiscordFileUploader>,
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
        })
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
                session, content, ..
            } => {
                let http = self.http_for(&session)?;
                let channel = Self::target(&session)?;
                for chunk in chunk_markdown(&content, DISCORD_MESSAGE_LIMIT) {
                    channel
                        .send_message(&http, CreateMessage::new().content(chunk))
                        .await?;
                }
            }
            OutboundAction::EditMessage {
                session,
                platform_message_id,
                content,
            } => {
                let http = self.http_for(&session)?;
                let message_id = platform_message_id
                    .parse::<u64>()
                    .map(MessageId::new)
                    .map_err(|_| {
                        OmonError::Config(format!(
                            "invalid Discord message ID: {platform_message_id}"
                        ))
                    })?;
                Self::target(&session)?
                    .edit_message(&http, message_id, EditMessage::new().content(content))
                    .await?;
            }
            OutboundAction::DeleteMessage {
                session,
                platform_message_id,
            } => {
                let http = self.http_for(&session)?;
                let message_id = platform_message_id
                    .parse::<u64>()
                    .map(MessageId::new)
                    .map_err(|_| {
                        OmonError::Config(format!(
                            "invalid Discord message ID: {platform_message_id}"
                        ))
                    })?;
                Self::target(&session)?
                    .delete_message(&http, message_id)
                    .await?;
            }
            OutboundAction::UploadFile { session, path } => {
                let http = self.http_for(&session)?;
                let channel = Self::target(&session)?;
                self.file_uploader.upload(http, channel, &path).await?;
            }
            OutboundAction::Stream { session, chunk } => {
                self.stream(session, chunk).await?;
            }
            OutboundAction::Typing { session, active } => {
                if active {
                    let http = self.http_for(&session)?;
                    let channel = Self::target(&session)?;
                    let guard = http.start_typing(channel);
                    self.typing
                        .lock()
                        .await
                        .insert(session.storage_key(), guard);
                } else {
                    self.typing.lock().await.remove(&session.storage_key());
                }
            }
            OutboundAction::ApprovalRequest {
                session,
                request_id,
                command,
            } => {
                let http = self.http_for(&session)?;
                let channel = Self::target(&session)?;
                let content = format!(
                    "Approval required before running this command:\n```text\n{}\n```",
                    command
                );
                channel
                    .send_message(
                        &http,
                        CreateMessage::new()
                            .content(content)
                            .components(approval_buttons(request_id)),
                    )
                    .await?;
            }
        }
        Ok(())
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
            },
            MessageAttachment {
                id: "att-2".into(),
                filename: "file2.png".into(),
                url: "https://example.com/2".into(),
                content_type: Some("image/png".into()),
                size_bytes: Some(200),
                local_path: None,
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
            },
            MessageAttachment {
                id: "att-3".into(),
                filename: "file3.pdf".into(),
                url: "https://example.com/3".into(),
                content_type: Some("application/pdf".into()),
                size_bytes: Some(300),
                local_path: None,
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
}
