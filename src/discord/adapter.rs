use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use poise::serenity_prelude as serenity;
use serenity::all::{
    ChannelId, ChannelType, CreateInteractionResponse, CreateInteractionResponseMessage,
    CreateMessage, EditMessage, FullEvent, GatewayIntents, Interaction, Message, MessageId,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::approval::{approval_buttons, SmartApprovalGuard};
use super::commands::{self, CommandError, PoiseData};
use super::throttler::{
    chunk_markdown, DiscordMessageTransport, LiveEditThrottler, SerenityMessageTransport,
    DISCORD_MESSAGE_LIMIT,
};
use crate::{
    DeliveryLedgerService, InboundEvent, MessageAttachment, OmonError, OutboundAction,
    OutboundDispatcher, Result, SessionKey,
};

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
                tracing::info!(session = %event.session, bot_id = %bot_user_id, "Routing inbound message to multiplexer");
                route_claimed_event(data, event).await?;
            } else {
                tracing::info!("Message ignored by filter (not DM, not thread, not mentioned, not in free channels)");
            }
        }
        FullEvent::InteractionCreate {
            interaction: Interaction::Component(component),
        } => {
            if data
                .approvals
                .resolve_custom_id(&component.data.custom_id)
                .await
            {
                component
                    .create_response(
                        ctx,
                        CreateInteractionResponse::UpdateMessage(
                            CreateInteractionResponseMessage::new().components(Vec::new()),
                        ),
                    )
                    .await?;
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
        if !is_implicit_response_channel || primary_bot_id != Some(bot_user_id.get()) {
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
        })
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
            OutboundAction::Stream { session, chunk } => {
                self.stream(session, chunk).await?;
            }
            OutboundAction::Typing { session } => {
                let http = self.http_for(&session)?;
                http.broadcast_typing(Self::target(&session)?).await?;
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
