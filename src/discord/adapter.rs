use std::sync::Arc;

use async_trait::async_trait;
use poise::serenity_prelude as serenity;
use serenity::all::{
    ChannelId, ChannelType, CreateInteractionResponse, CreateInteractionResponseMessage,
    CreateMessage, EditMessage, FullEvent, GatewayIntents, Interaction, Message, MessageId,
};

use super::approval::SmartApprovalGuard;
use super::commands::{self, CommandError, PoiseData};
use super::throttler::{chunk_markdown, DISCORD_MESSAGE_LIMIT};
use crate::{
    InboundEvent, MessageAttachment, OmonError, OutboundAction, OutboundDispatcher, Result,
    SessionKey,
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
        let Some(event) = message_to_inbound(message, bot_user_id, channel_type) else {
            return Ok(false);
        };
        self.data.multiplexer.route(event).await?;
        Ok(true)
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
            ) {
                tracing::info!(session = %event.session, "Routing inbound message to multiplexer");
                data.multiplexer.route(event).await?;
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

pub fn message_to_inbound(
    message: &Message,
    bot_user_id: serenity::UserId,
    channel_type: Option<ChannelType>,
) -> Option<InboundEvent> {
    message_to_inbound_with_config(message, bot_user_id, channel_type, &[], &[])
}

pub fn message_to_inbound_with_config(
    message: &Message,
    bot_user_id: serenity::UserId,
    channel_type: Option<ChannelType>,
    free_response_channels: &[u64],
    allowed_users: &[u64],
) -> Option<InboundEvent> {
    if message.author.bot || message.webhook_id.is_some() {
        return None;
    }

    if !allowed_users.is_empty() && !allowed_users.contains(&message.author.id.get()) {
        return None;
    }

    let is_dm = message.guild_id.is_none();
    let is_thread = channel_type.is_some_and(is_thread);
    let is_mentioned = message.mentions.iter().any(|user| user.id == bot_user_id);
    let is_free_channel = free_response_channels.contains(&message.channel_id.get());
    if !is_dm && !is_thread && !is_mentioned && !is_free_channel {
        return None;
    }

    let mut content = strip_bot_mention(&message.content, bot_user_id);
    if content.trim().is_empty() && message.attachments.is_empty() {
        content = "Hello!".to_owned();
    }
    let channel_id = message.channel_id.to_string();
    let session = SessionKey::new(
        "discord",
        message.guild_id.map(|id| id.to_string()),
        channel_id.clone(),
        is_thread.then_some(channel_id),
        message.author.id.to_string(),
    );
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
    Some(
        InboundEvent::message(session, message.id.to_string(), content)
            .with_attachments(attachments),
    )
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

#[derive(Clone)]
pub struct DiscordEgress {
    http: Arc<serenity::Http>,
}

impl DiscordEgress {
    pub fn new(http: Arc<serenity::Http>) -> Self {
        Self { http }
    }

    fn target(session: &SessionKey) -> Result<ChannelId> {
        let value = session.thread_id.as_ref().unwrap_or(&session.channel_id);
        value
            .parse::<u64>()
            .map(ChannelId::new)
            .map_err(|_| OmonError::Config(format!("invalid Discord channel ID: {value}")))
    }
}

#[async_trait]
impl OutboundDispatcher for DiscordEgress {
    async fn dispatch(&self, action: OutboundAction) -> Result<()> {
        let http = &self.http;
        match action {
            OutboundAction::SendMessage {
                session, content, ..
            } => {
                let channel = Self::target(&session)?;
                for chunk in chunk_markdown(&content, DISCORD_MESSAGE_LIMIT) {
                    channel
                        .send_message(http, CreateMessage::new().content(chunk))
                        .await?;
                }
            }
            OutboundAction::EditMessage {
                session,
                platform_message_id,
                content,
            } => {
                let message_id = platform_message_id
                    .parse::<u64>()
                    .map(MessageId::new)
                    .map_err(|_| {
                        OmonError::Config(format!(
                            "invalid Discord message ID: {platform_message_id}"
                        ))
                    })?;
                Self::target(&session)?
                    .edit_message(http, message_id, EditMessage::new().content(content))
                    .await?;
            }
            OutboundAction::DeleteMessage {
                session,
                platform_message_id,
            } => {
                let message_id = platform_message_id
                    .parse::<u64>()
                    .map(MessageId::new)
                    .map_err(|_| {
                        OmonError::Config(format!(
                            "invalid Discord message ID: {platform_message_id}"
                        ))
                    })?;
                Self::target(&session)?
                    .delete_message(http, message_id)
                    .await?;
            }
            OutboundAction::Stream { session, chunk } => {
                Self::target(&session)?
                    .send_message(http, CreateMessage::new().content(chunk.content))
                    .await?;
            }
            OutboundAction::Typing { session } => {
                http.broadcast_typing(Self::target(&session)?).await?;
            }
        }
        Ok(())
    }
}
