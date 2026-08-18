use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use poise::serenity_prelude as serenity;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use serenity::all::{Channel, ChannelId, ChannelType, GetMessages, Message, MessageId};
use sqlx::SqlitePool;
use tokio::sync::OnceCell;

use super::Tool;
use crate::storage::{MessageSearchDocument, MessageSearchIndex, MessengerPolicyStore};
use crate::{neutralize_untrusted_inline_text, MessageContextPolicyMatrix, OmonError, SessionKey};

const DEFAULT_RESULT_LIMIT: usize = 20;
const HARD_MAX_RESULT_LIMIT: usize = 100;
const DEFAULT_SEARCH_SCAN_LIMIT: usize = 200;
const HARD_MAX_SEARCH_SCAN_LIMIT: usize = 2_000;
const MAX_MESSAGE_CONTENT_CHARS: usize = 2_000;
const MAX_AUTHOR_NAME_CHARS: usize = 128;
const MAX_ATTACHMENT_NAME_CHARS: usize = 256;
const MAX_SEARCH_QUERY_CHARS: usize = 256;
const DISCORD_PAGE_LIMIT: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageContextOperation {
    Recent,
    Search,
    GetMessage,
    Replies,
}

impl MessageContextOperation {
    fn parse(value: Option<&str>) -> Result<Self, OmonError> {
        match value
            .unwrap_or("recent")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "recent" => Ok(Self::Recent),
            "search" => Ok(Self::Search),
            "get_message" | "get" => Ok(Self::GetMessage),
            "replies" | "thread_replies" => Ok(Self::Replies),
            other => Err(OmonError::ToolExecution(format!(
                "unsupported message_context operation '{other}'; expected recent, search, get_message, or replies"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Recent => "recent",
            Self::Search => "search",
            Self::GetMessage => "get_message",
            Self::Replies => "replies",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageContextRequest {
    pub operation: MessageContextOperation,
    pub channel_id: Option<String>,
    pub message_id: Option<String>,
    pub query: Option<String>,
    pub limit: usize,
    pub scan_limit: usize,
    pub before_message_id: Option<String>,
}

impl MessageContextRequest {
    pub fn from_args(args: &Value) -> Result<Self, OmonError> {
        let operation =
            MessageContextOperation::parse(args.get("operation").and_then(Value::as_str))?;
        let channel_id = optional_non_empty_string(args, "channel_id")?;
        let message_id = optional_non_empty_string(args, "message_id")?;
        let before_message_id = optional_non_empty_string(args, "before_message_id")?;
        let limit = args
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_RESULT_LIMIT as u64)
            .clamp(1, HARD_MAX_RESULT_LIMIT as u64) as usize;
        let scan_limit =
            args.get("scan_limit")
                .and_then(Value::as_u64)
                .unwrap_or(DEFAULT_SEARCH_SCAN_LIMIT as u64)
                .clamp(limit as u64, HARD_MAX_SEARCH_SCAN_LIMIT as u64) as usize;

        let query = optional_non_empty_string(args, "query")?;
        if operation == MessageContextOperation::Search {
            let query_value = query.as_deref().ok_or_else(|| {
                OmonError::ToolExecution(
                    "message_context operation 'search' requires a non-empty 'query'".into(),
                )
            })?;
            if query_value.chars().count() > MAX_SEARCH_QUERY_CHARS {
                return Err(OmonError::ToolExecution(format!(
                    "message_context search query exceeds {MAX_SEARCH_QUERY_CHARS} characters"
                )));
            }
        }
        if matches!(
            operation,
            MessageContextOperation::GetMessage | MessageContextOperation::Replies
        ) && message_id.is_none()
        {
            return Err(OmonError::ToolExecution(format!(
                "message_context operation '{}' requires 'message_id'",
                operation.as_str()
            )));
        }

        Ok(Self {
            operation,
            channel_id,
            message_id,
            query,
            limit,
            scan_limit,
            before_message_id,
        })
    }

    fn apply_policy_limits(&mut self, policy: &MessageContextPolicyMatrix) {
        match self.operation {
            MessageContextOperation::Recent => self.limit = self.limit.min(policy.limits.recent),
            MessageContextOperation::Search => {
                self.limit = self.limit.min(policy.limits.search_results);
                self.scan_limit = self
                    .scan_limit
                    .min(policy.limits.search_scan)
                    .max(self.limit);
            }
            MessageContextOperation::GetMessage => self.limit = 1,
            MessageContextOperation::Replies => {
                self.limit = self.limit.min(policy.limits.replies);
                self.scan_limit = self
                    .scan_limit
                    .min(policy.limits.search_scan)
                    .max(self.limit);
            }
        }
    }
}

fn optional_non_empty_string(args: &Value, key: &str) -> Result<Option<String>, OmonError> {
    let Some(value) = args.get(key) else {
        return Ok(None);
    };
    let raw = value.as_str().ok_or_else(|| {
        OmonError::ToolExecution(format!("message_context '{key}' must be a string"))
    })?;
    let trimmed = raw.trim();
    Ok((!trimmed.is_empty()).then(|| trimmed.to_owned()))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageContextAttachment {
    pub id: String,
    pub filename: String,
    pub url: String,
    pub content_type: Option<String>,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageContextMessage {
    pub id: String,
    pub channel_id: String,
    pub author_id: String,
    pub author_name: String,
    pub author_is_bot: bool,
    pub content: String,
    pub timestamp: String,
    pub attachments: Vec<MessageContextAttachment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referenced_message_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageContextConversationMetadata {
    pub channel_id: String,
    pub channel_name: Option<String>,
    pub channel_kind: String,
    pub guild_id: Option<String>,
    pub topic: Option<String>,
    pub parent_channel_id: Option<String>,
    pub parent_channel_name: Option<String>,
    pub parent_channel_kind: Option<String>,
    pub parent_topic: Option<String>,
    pub is_thread: bool,
    pub is_forum_post: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct MessageContextResult {
    pub platform: String,
    pub operation: String,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    pub scanned: usize,
    pub indexed: usize,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_before_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<MessageContextConversationMetadata>,
    pub messages: Vec<MessageContextMessage>,
}

#[async_trait]
pub trait MessageContextProvider: Send + Sync {
    fn platform(&self) -> &str;

    async fn query(
        &self,
        session: &SessionKey,
        request: &MessageContextRequest,
    ) -> Result<MessageContextResult, OmonError>;
}

#[derive(Clone, Default)]
pub struct MessageContextTool {
    providers: HashMap<String, Arc<dyn MessageContextProvider>>,
}

impl MessageContextTool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_provider<P>(mut self, provider: P) -> Self
    where
        P: MessageContextProvider + 'static,
    {
        self.register_provider(provider);
        self
    }

    pub fn register_provider<P>(&mut self, provider: P) -> Option<Arc<dyn MessageContextProvider>>
    where
        P: MessageContextProvider + 'static,
    {
        self.providers
            .insert(provider.platform().to_ascii_lowercase(), Arc::new(provider))
    }

    pub fn from_environment(pool: SqlitePool) -> Option<Self> {
        let mut tool = Self::new();
        if let Some(discord) = DiscordMessageContextProvider::from_environment(pool) {
            tool.register_provider(discord);
        }
        (!tool.providers.is_empty()).then_some(tool)
    }
}

#[async_trait]
impl Tool for MessageContextTool {
    fn name(&self) -> &str {
        "message_context"
    }

    fn description(&self) -> &str {
        "Read or search authorized messenger context. Supports recent history, a single message, replies/thread messages, and SQLite FTS5 search refreshed from recent Discord bot-visible history."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "operation": {"type":"string","enum":["recent","search","get_message","replies"]},
                "channel_id": {"type":"string","description":"Optional target channel; defaults to the current session channel."},
                "message_id": {"type":"string","description":"Required for get_message and replies."},
                "query": {"type":"string","description":"Required for search; FTS5 token/prefix query."},
                "limit": {"type":"integer","minimum":1,"maximum":HARD_MAX_RESULT_LIMIT},
                "scan_limit": {"type":"integer","minimum":1,"maximum":HARD_MAX_SEARCH_SCAN_LIMIT},
                "before_message_id": {"type":"string","description":"Exclusive Discord pagination cursor."}
            }
        })
    }

    async fn execute(&self, _args: Value) -> Result<Value, OmonError> {
        Err(OmonError::ToolExecution(
            "message_context requires an active messenger session".into(),
        ))
    }

    async fn execute_with_context(
        &self,
        args: Value,
        session: Option<&SessionKey>,
    ) -> Result<Value, OmonError> {
        let session = session.ok_or_else(|| {
            OmonError::ToolExecution("message_context requires an active messenger session".into())
        })?;
        let request = MessageContextRequest::from_args(&args)?;
        let provider = self
            .providers
            .get(&session.platform.to_ascii_lowercase())
            .ok_or_else(|| {
                OmonError::ToolExecution(format!(
                    "message_context has no provider for platform '{}'",
                    session.platform
                ))
            })?;
        serde_json::to_value(provider.query(session, &request).await?).map_err(|error| {
            OmonError::ToolExecution(format!("failed to serialize message context: {error}"))
        })
    }
}

#[derive(Clone, Debug)]
pub struct MessageContextPolicy {
    allowed_cross_channels: HashSet<String>,
    ignored_channels: HashSet<String>,
    base_matrix: MessageContextPolicyMatrix,
}

impl Default for MessageContextPolicy {
    fn default() -> Self {
        Self::new(Vec::<String>::new(), Vec::<String>::new())
    }
}

impl MessageContextPolicy {
    pub fn new(
        allowed_cross_channels: impl IntoIterator<Item = String>,
        ignored_channels: impl IntoIterator<Item = String>,
    ) -> Self {
        Self::with_matrix(
            allowed_cross_channels,
            ignored_channels,
            MessageContextPolicyMatrix::default(),
        )
    }

    pub fn with_matrix(
        allowed_cross_channels: impl IntoIterator<Item = String>,
        ignored_channels: impl IntoIterator<Item = String>,
        base_matrix: MessageContextPolicyMatrix,
    ) -> Self {
        Self {
            allowed_cross_channels: allowed_cross_channels.into_iter().collect(),
            ignored_channels: ignored_channels.into_iter().collect(),
            base_matrix: base_matrix.normalized(),
        }
    }

    pub fn from_environment() -> Self {
        Self::with_matrix(
            discord_channel_set_from_environment("DISCORD_ALLOWED_CHANNELS"),
            discord_channel_set_from_environment("DISCORD_IGNORED_CHANNELS"),
            MessageContextPolicyMatrix::from_environment(),
        )
    }

    pub fn base_matrix(&self) -> &MessageContextPolicyMatrix {
        &self.base_matrix
    }

    pub fn allowed_cross_channels(&self) -> Vec<String> {
        sorted_set(&self.allowed_cross_channels)
    }

    pub fn ignored_channels(&self) -> Vec<String> {
        sorted_set(&self.ignored_channels)
    }

    pub fn authorize_target(
        &self,
        session: &SessionKey,
        requested_channel_id: Option<&str>,
        matrix: &MessageContextPolicyMatrix,
    ) -> Result<String, OmonError> {
        if !matrix.inherit_session_authorization {
            return Err(OmonError::ToolExecution(
                "message_context access denied: policy disables inherited session authorization"
                    .into(),
            ));
        }
        let target = requested_channel_id
            .unwrap_or(session.channel_id.as_str())
            .trim();
        validate_discord_id(target, "channel_id")?;
        if self.ignored_channels.contains(target) {
            return Err(OmonError::ToolExecution(format!(
                "message_context access denied: channel {target} is ignored by policy"
            )));
        }
        if target == session.channel_id {
            if !matrix.allow_current_conversation_reads {
                return Err(OmonError::ToolExecution(
                    "message_context access denied: current-conversation reads are disabled".into(),
                ));
            }
            if session.guild_id.is_none() && !matrix.allow_dm_reads {
                return Err(OmonError::ToolExecution(
                    "message_context access denied: direct-message reads are disabled".into(),
                ));
            }
            return Ok(target.to_owned());
        }
        if session.guild_id.is_none() {
            return Err(OmonError::ToolExecution(
                "message_context access denied: cross-channel reads are not allowed from direct-message sessions"
                    .into(),
            ));
        }
        if !matrix.allow_cross_channel_reads {
            return Err(OmonError::ToolExecution(
                "message_context access denied: cross-channel reads are disabled by policy".into(),
            ));
        }
        if !self.allowed_cross_channels.contains(target) {
            return Err(OmonError::ToolExecution(format!(
                "message_context access denied: cross-channel target {target} is not explicitly listed in DISCORD_ALLOWED_CHANNELS"
            )));
        }
        Ok(target.to_owned())
    }
}

fn sorted_set(values: &HashSet<String>) -> Vec<String> {
    let mut values = values.iter().cloned().collect::<Vec<_>>();
    values.sort();
    values
}

#[async_trait]
pub trait DiscordMessageContextApi: Send + Sync {
    async fn recent_messages(
        &self,
        session: &SessionKey,
        channel_id: &str,
        before_message_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MessageContextMessage>, OmonError>;

    async fn get_message(
        &self,
        session: &SessionKey,
        channel_id: &str,
        message_id: &str,
    ) -> Result<MessageContextMessage, OmonError>;

    async fn conversation_metadata(
        &self,
        session: &SessionKey,
        channel_id: &str,
    ) -> Result<Option<MessageContextConversationMetadata>, OmonError>;
}

struct DiscordClientSet {
    default_bot_id: String,
    by_bot_id: HashMap<String, Arc<serenity::http::Http>>,
}

pub struct SerenityDiscordMessageContextApi {
    tokens: Vec<String>,
    clients: OnceCell<DiscordClientSet>,
}

impl SerenityDiscordMessageContextApi {
    pub fn new(tokens: Vec<String>) -> Result<Self, OmonError> {
        if tokens.is_empty() {
            return Err(OmonError::Config(
                "Discord message context API requires at least one bot token".into(),
            ));
        }
        Ok(Self {
            tokens,
            clients: OnceCell::new(),
        })
    }

    async fn clients(&self) -> Result<&DiscordClientSet, OmonError> {
        self.clients
            .get_or_try_init(|| async {
                let mut by_bot_id = HashMap::new();
                let mut default_bot_id = None;
                for token in &self.tokens {
                    let http = Arc::new(serenity::http::Http::new(token));
                    let bot_id = http
                        .get_current_user()
                        .await
                        .map_err(OmonError::from)?
                        .id
                        .to_string();
                    if default_bot_id.is_none() {
                        default_bot_id = Some(bot_id.clone());
                    }
                    if by_bot_id.insert(bot_id.clone(), http).is_some() {
                        return Err(OmonError::Config(format!(
                            "multiple Discord tokens resolve to the same bot identity {bot_id}"
                        )));
                    }
                }
                Ok(DiscordClientSet {
                    default_bot_id: default_bot_id.ok_or_else(|| {
                        OmonError::Config(
                            "no Discord bot identity available for message_context".into(),
                        )
                    })?,
                    by_bot_id,
                })
            })
            .await
    }

    async fn http_for_session(
        &self,
        session: &SessionKey,
    ) -> Result<Arc<serenity::http::Http>, OmonError> {
        let clients = self.clients().await?;
        let bot_id = session
            .bot_id
            .as_deref()
            .unwrap_or(clients.default_bot_id.as_str());
        clients.by_bot_id.get(bot_id).cloned().ok_or_else(|| {
            OmonError::ToolExecution(format!(
                "message_context has no Discord HTTP client for session bot identity {bot_id}"
            ))
        })
    }
}

#[async_trait]
impl DiscordMessageContextApi for SerenityDiscordMessageContextApi {
    async fn recent_messages(
        &self,
        session: &SessionKey,
        channel_id: &str,
        before_message_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MessageContextMessage>, OmonError> {
        let http = self.http_for_session(session).await?;
        let channel_id = parse_channel_id(channel_id)?;
        let mut builder = GetMessages::new().limit(limit.min(DISCORD_PAGE_LIMIT) as u8);
        if let Some(before) = before_message_id {
            builder = builder.before(parse_message_id(before)?);
        }
        let messages = channel_id
            .messages(http.as_ref(), builder)
            .await
            .map_err(OmonError::from)?;
        Ok(messages.iter().map(normalize_discord_message).collect())
    }

    async fn get_message(
        &self,
        session: &SessionKey,
        channel_id: &str,
        message_id: &str,
    ) -> Result<MessageContextMessage, OmonError> {
        let http = self.http_for_session(session).await?;
        let message = parse_channel_id(channel_id)?
            .message(http.as_ref(), parse_message_id(message_id)?)
            .await
            .map_err(OmonError::from)?;
        Ok(normalize_discord_message(&message))
    }

    async fn conversation_metadata(
        &self,
        session: &SessionKey,
        channel_id: &str,
    ) -> Result<Option<MessageContextConversationMetadata>, OmonError> {
        let http = self.http_for_session(session).await?;
        let channel = match parse_channel_id(channel_id)?
            .to_channel(http.as_ref())
            .await
        {
            Ok(channel) => channel,
            Err(_) => return Ok(None),
        };
        match channel {
            Channel::Guild(guild_channel) => {
                let parent = if let Some(parent_id) = guild_channel.parent_id {
                    match parent_id.to_channel(http.as_ref()).await {
                        Ok(Channel::Guild(parent)) => Some(parent),
                        _ => None,
                    }
                } else {
                    None
                };
                let is_thread = is_thread_channel(guild_channel.kind);
                let is_forum_post = is_thread
                    && parent
                        .as_ref()
                        .is_some_and(|parent| matches!(parent.kind, ChannelType::Forum));
                Ok(Some(MessageContextConversationMetadata {
                    channel_id: guild_channel.id.to_string(),
                    channel_name: Some(neutralize_untrusted_inline_text(
                        &guild_channel.name,
                        MAX_AUTHOR_NAME_CHARS,
                    )),
                    channel_kind: format!("{:?}", guild_channel.kind),
                    guild_id: Some(guild_channel.guild_id.to_string()),
                    topic: guild_channel.topic.as_deref().map(|topic| {
                        neutralize_untrusted_inline_text(topic, MAX_MESSAGE_CONTENT_CHARS)
                    }),
                    parent_channel_id: guild_channel.parent_id.map(|id| id.to_string()),
                    parent_channel_name: parent.as_ref().map(|parent| {
                        neutralize_untrusted_inline_text(&parent.name, MAX_AUTHOR_NAME_CHARS)
                    }),
                    parent_channel_kind: parent.as_ref().map(|parent| format!("{:?}", parent.kind)),
                    parent_topic: parent.as_ref().and_then(|parent| {
                        parent.topic.as_deref().map(|topic| {
                            neutralize_untrusted_inline_text(topic, MAX_MESSAGE_CONTENT_CHARS)
                        })
                    }),
                    is_thread,
                    is_forum_post,
                }))
            }
            Channel::Private(_) => Ok(Some(MessageContextConversationMetadata {
                channel_id: channel_id.to_owned(),
                channel_name: None,
                channel_kind: "Private".into(),
                guild_id: None,
                topic: None,
                parent_channel_id: None,
                parent_channel_name: None,
                parent_channel_kind: None,
                parent_topic: None,
                is_thread: false,
                is_forum_post: false,
            })),
            _ => Ok(None),
        }
    }
}

pub struct DiscordMessageContextProvider {
    api: Arc<dyn DiscordMessageContextApi>,
    policy: MessageContextPolicy,
    policy_store: MessengerPolicyStore,
    search_index: MessageSearchIndex,
}

impl DiscordMessageContextProvider {
    pub fn new(
        tokens: Vec<String>,
        pool: SqlitePool,
        policy: MessageContextPolicy,
    ) -> Result<Self, OmonError> {
        Ok(Self::with_api(
            Arc::new(SerenityDiscordMessageContextApi::new(tokens)?),
            pool,
            policy,
        ))
    }

    pub fn with_api(
        api: Arc<dyn DiscordMessageContextApi>,
        pool: SqlitePool,
        policy: MessageContextPolicy,
    ) -> Self {
        Self {
            api,
            policy,
            policy_store: MessengerPolicyStore::new(pool.clone()),
            search_index: MessageSearchIndex::new(pool),
        }
    }

    pub fn from_environment(pool: SqlitePool) -> Option<Self> {
        let tokens = discord_tokens_from_environment();
        (!tokens.is_empty())
            .then(|| Self::new(tokens, pool, MessageContextPolicy::from_environment()).ok())
            .flatten()
    }

    pub fn policy(&self) -> &MessageContextPolicy {
        &self.policy
    }

    async fn effective_policy(&self) -> Result<MessageContextPolicyMatrix, OmonError> {
        self.policy_store
            .effective("discord", self.policy.base_matrix())
            .await
    }

    async fn authorize_and_metadata(
        &self,
        session: &SessionKey,
        requested_channel_id: Option<&str>,
        matrix: &MessageContextPolicyMatrix,
    ) -> Result<(String, Option<MessageContextConversationMetadata>), OmonError> {
        let target = self
            .policy
            .authorize_target(session, requested_channel_id, matrix)?;
        let metadata = self.api.conversation_metadata(session, &target).await?;
        if target != session.channel_id && matrix.same_workspace_only {
            let expected_guild = session.guild_id.as_deref().ok_or_else(|| {
                OmonError::ToolExecution(
                    "message_context access denied: cross-channel direct-message read".into(),
                )
            })?;
            if metadata
                .as_ref()
                .and_then(|metadata| metadata.guild_id.as_deref())
                != Some(expected_guild)
            {
                return Err(OmonError::ToolExecution(format!(
                    "message_context access denied: target channel {target} is not in the current workspace/guild"
                )));
            }
        }
        Ok((target, metadata))
    }

    async fn recent(
        &self,
        session: &SessionKey,
        target: &str,
        conversation: Option<MessageContextConversationMetadata>,
        request: &MessageContextRequest,
    ) -> Result<MessageContextResult, OmonError> {
        let messages = self
            .api
            .recent_messages(
                session,
                target,
                request.before_message_id.as_deref(),
                request.limit,
            )
            .await?;
        let scanned = messages.len();
        let indexed = self
            .index_messages(session, &messages, conversation.as_ref())
            .await?;
        Ok(MessageContextResult {
            platform: "discord".into(),
            operation: request.operation.as_str().into(),
            channel_id: target.to_owned(),
            query: None,
            scanned,
            indexed,
            truncated: scanned == request.limit,
            next_before_message_id: messages.last().map(|message| message.id.clone()),
            search_source: None,
            conversation,
            messages,
        })
    }

    async fn get_message_result(
        &self,
        session: &SessionKey,
        target: &str,
        conversation: Option<MessageContextConversationMetadata>,
        request: &MessageContextRequest,
    ) -> Result<MessageContextResult, OmonError> {
        let message = self
            .api
            .get_message(
                session,
                target,
                request.message_id.as_deref().ok_or_else(|| {
                    OmonError::ToolExecution(
                        "message_context get_message requires message_id".into(),
                    )
                })?,
            )
            .await?;
        let indexed = self
            .index_messages(
                session,
                std::slice::from_ref(&message),
                conversation.as_ref(),
            )
            .await?;
        Ok(MessageContextResult {
            platform: "discord".into(),
            operation: request.operation.as_str().into(),
            channel_id: target.to_owned(),
            query: None,
            scanned: 1,
            indexed,
            truncated: false,
            next_before_message_id: None,
            search_source: None,
            conversation,
            messages: vec![message],
        })
    }

    async fn replies(
        &self,
        session: &SessionKey,
        target: &str,
        conversation: Option<MessageContextConversationMetadata>,
        request: &MessageContextRequest,
    ) -> Result<MessageContextResult, OmonError> {
        let message_id = request.message_id.as_deref().ok_or_else(|| {
            OmonError::ToolExecution("message_context replies requires message_id".into())
        })?;
        self.api.get_message(session, target, message_id).await?;

        if let Some(thread_metadata) = self
            .api
            .conversation_metadata(session, message_id)
            .await?
            .filter(|metadata| metadata.is_thread)
        {
            if thread_metadata.parent_channel_id.as_deref() == Some(target) {
                let mut messages = self
                    .api
                    .recent_messages(
                        session,
                        message_id,
                        request.before_message_id.as_deref(),
                        request.limit.saturating_add(1).min(DISCORD_PAGE_LIMIT),
                    )
                    .await?;
                messages.retain(|message| message.id != message_id);
                messages.truncate(request.limit);
                let scanned = messages.len();
                let indexed = self
                    .index_messages(session, &messages, Some(&thread_metadata))
                    .await?;
                return Ok(MessageContextResult {
                    platform: "discord".into(),
                    operation: request.operation.as_str().into(),
                    channel_id: message_id.to_owned(),
                    query: None,
                    scanned,
                    indexed,
                    truncated: scanned == request.limit,
                    next_before_message_id: messages.last().map(|message| message.id.clone()),
                    search_source: None,
                    conversation: Some(thread_metadata),
                    messages,
                });
            }
        }

        let mut before = request.before_message_id.clone();
        let mut scanned = 0usize;
        let mut indexed = 0usize;
        let mut replies = Vec::new();
        let mut exhausted = false;
        while scanned < request.scan_limit && replies.len() < request.limit {
            let page_limit = (request.scan_limit - scanned).min(DISCORD_PAGE_LIMIT);
            let page = self
                .api
                .recent_messages(session, target, before.as_deref(), page_limit)
                .await?;
            if page.is_empty() {
                exhausted = true;
                break;
            }
            let page_len = page.len();
            scanned += page_len;
            indexed += self
                .index_messages(session, &page, conversation.as_ref())
                .await?;
            before = page.last().map(|message| message.id.clone());
            replies
                .extend(page.into_iter().filter(|message| {
                    message.referenced_message_id.as_deref() == Some(message_id)
                }));
            replies.truncate(request.limit);
            if page_len < page_limit {
                exhausted = true;
                break;
            }
        }
        Ok(MessageContextResult {
            platform: "discord".into(),
            operation: request.operation.as_str().into(),
            channel_id: target.to_owned(),
            query: None,
            scanned,
            indexed,
            truncated: !exhausted
                && (scanned >= request.scan_limit || replies.len() >= request.limit),
            next_before_message_id: before,
            search_source: None,
            conversation,
            messages: replies,
        })
    }

    async fn search(
        &self,
        session: &SessionKey,
        target: &str,
        conversation: Option<MessageContextConversationMetadata>,
        request: &MessageContextRequest,
    ) -> Result<MessageContextResult, OmonError> {
        let query = request.query.as_deref().ok_or_else(|| {
            OmonError::ToolExecution("message_context search requires a query".into())
        })?;
        let _ = self
            .search_index
            .search(
                "discord",
                target,
                query,
                request.before_message_id.as_deref(),
                request.limit,
            )
            .await?;

        let mut before = request.before_message_id.clone();
        let mut scanned = 0usize;
        let mut indexed = 0usize;
        let mut exhausted = false;
        while scanned < request.scan_limit {
            let page_limit = (request.scan_limit - scanned).min(DISCORD_PAGE_LIMIT);
            let page = self
                .api
                .recent_messages(session, target, before.as_deref(), page_limit)
                .await?;
            if page.is_empty() {
                exhausted = true;
                break;
            }
            let page_len = page.len();
            scanned += page_len;
            indexed += self
                .index_messages(session, &page, conversation.as_ref())
                .await?;
            before = page.last().map(|message| message.id.clone());
            if page_len < page_limit {
                exhausted = true;
                break;
            }
        }

        let messages = self
            .search_index
            .search(
                "discord",
                target,
                query,
                request.before_message_id.as_deref(),
                request.limit,
            )
            .await?
            .into_iter()
            .map(|hit| message_from_search_document(hit.document))
            .collect();
        Ok(MessageContextResult {
            platform: "discord".into(),
            operation: request.operation.as_str().into(),
            channel_id: target.to_owned(),
            query: Some(query.to_owned()),
            scanned,
            indexed,
            truncated: !exhausted && scanned >= request.scan_limit,
            next_before_message_id: before,
            search_source: Some("sqlite_fts5+discord_rest_backfill".into()),
            conversation,
            messages,
        })
    }

    async fn index_messages(
        &self,
        session: &SessionKey,
        messages: &[MessageContextMessage],
        conversation: Option<&MessageContextConversationMetadata>,
    ) -> Result<usize, OmonError> {
        let documents = messages
            .iter()
            .map(|message| search_document_from_message(session, message, conversation))
            .collect::<Vec<_>>();
        self.search_index.upsert_many(&documents).await
    }
}

#[async_trait]
impl MessageContextProvider for DiscordMessageContextProvider {
    fn platform(&self) -> &str {
        "discord"
    }

    async fn query(
        &self,
        session: &SessionKey,
        request: &MessageContextRequest,
    ) -> Result<MessageContextResult, OmonError> {
        if !session.platform.eq_ignore_ascii_case("discord") {
            return Err(OmonError::ToolExecution(format!(
                "Discord message context provider cannot serve platform '{}'",
                session.platform
            )));
        }
        let matrix = self.effective_policy().await?;
        let mut request = request.clone();
        request.apply_policy_limits(&matrix);
        let (target, conversation) = self
            .authorize_and_metadata(session, request.channel_id.as_deref(), &matrix)
            .await?;
        match request.operation {
            MessageContextOperation::Recent => {
                self.recent(session, &target, conversation, &request).await
            }
            MessageContextOperation::Search => {
                self.search(session, &target, conversation, &request).await
            }
            MessageContextOperation::GetMessage => {
                self.get_message_result(session, &target, conversation, &request)
                    .await
            }
            MessageContextOperation::Replies => {
                self.replies(session, &target, conversation, &request).await
            }
        }
    }
}

fn validate_discord_id(value: &str, field: &str) -> Result<u64, OmonError> {
    let parsed = value.parse::<u64>().map_err(|_| {
        OmonError::ToolExecution(format!(
            "message_context '{field}' must be a numeric Discord snowflake"
        ))
    })?;
    if parsed == 0 {
        return Err(OmonError::ToolExecution(format!(
            "message_context '{field}' must be greater than zero"
        )));
    }
    Ok(parsed)
}

fn parse_channel_id(value: &str) -> Result<ChannelId, OmonError> {
    validate_discord_id(value, "channel_id").map(ChannelId::new)
}

fn parse_message_id(value: &str) -> Result<MessageId, OmonError> {
    validate_discord_id(value, "message_id").map(MessageId::new)
}

fn is_thread_channel(kind: ChannelType) -> bool {
    matches!(
        kind,
        ChannelType::NewsThread | ChannelType::PublicThread | ChannelType::PrivateThread
    )
}

fn normalize_discord_message(message: &Message) -> MessageContextMessage {
    MessageContextMessage {
        id: message.id.to_string(),
        channel_id: message.channel_id.to_string(),
        author_id: message.author.id.to_string(),
        author_name: neutralize_untrusted_inline_text(&message.author.name, MAX_AUTHOR_NAME_CHARS),
        author_is_bot: message.author.bot,
        content: neutralize_untrusted_inline_text(&message.content, MAX_MESSAGE_CONTENT_CHARS),
        timestamp: message.timestamp.to_string(),
        attachments: message
            .attachments
            .iter()
            .map(|attachment| MessageContextAttachment {
                id: attachment.id.to_string(),
                filename: neutralize_untrusted_inline_text(
                    &attachment.filename,
                    MAX_ATTACHMENT_NAME_CHARS,
                ),
                url: attachment.url.clone(),
                content_type: attachment.content_type.clone(),
                size_bytes: u64::from(attachment.size),
            })
            .collect(),
        referenced_message_id: message
            .referenced_message
            .as_ref()
            .map(|referenced| referenced.id.to_string()),
    }
}

fn search_document_from_message(
    session: &SessionKey,
    message: &MessageContextMessage,
    conversation: Option<&MessageContextConversationMetadata>,
) -> MessageSearchDocument {
    MessageSearchDocument {
        platform: "discord".into(),
        guild_id: conversation
            .and_then(|metadata| metadata.guild_id.clone())
            .or_else(|| session.guild_id.clone()),
        channel_id: message.channel_id.clone(),
        thread_id: conversation
            .filter(|metadata| metadata.is_thread)
            .map(|metadata| metadata.channel_id.clone()),
        message_id: message.id.clone(),
        author_id: message.author_id.clone(),
        author_name: message.author_name.clone(),
        content: message.content.clone(),
        attachment_names: message
            .attachments
            .iter()
            .map(|attachment| attachment.filename.clone())
            .collect(),
        timestamp: DateTime::parse_from_rfc3339(&message.timestamp)
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        metadata: json!({
            "source": "discord_rest",
            "author_is_bot": message.author_is_bot,
            "referenced_message_id": message.referenced_message_id,
            "attachments": message.attachments,
            "conversation": conversation,
        }),
    }
}

fn message_from_search_document(document: MessageSearchDocument) -> MessageContextMessage {
    let attachments = document
        .metadata
        .get("attachments")
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<MessageContextAttachment>>(value).ok())
        .unwrap_or_else(|| {
            document
                .attachment_names
                .iter()
                .enumerate()
                .map(|(index, filename)| MessageContextAttachment {
                    id: format!("indexed-{index}"),
                    filename: filename.clone(),
                    url: String::new(),
                    content_type: None,
                    size_bytes: 0,
                })
                .collect()
        });
    MessageContextMessage {
        id: document.message_id,
        channel_id: document.channel_id,
        author_id: document.author_id,
        author_name: document.author_name,
        author_is_bot: document
            .metadata
            .get("author_is_bot")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        content: document.content,
        timestamp: document.timestamp.to_rfc3339(),
        attachments,
        referenced_message_id: document
            .metadata
            .get("referenced_message_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }
}

fn discord_tokens_from_environment() -> Vec<String> {
    let mut tokens = Vec::new();
    for name in ["DISCORD_BOT_TOKEN", "DISCORD_BOT_TOKENS"] {
        if let Ok(raw) = env::var(name) {
            for token in raw.split(',') {
                let trimmed = token.trim().trim_matches('"').trim_matches('\'');
                if !trimmed.is_empty() && !tokens.iter().any(|known| known == trimmed) {
                    tokens.push(trimmed.to_owned());
                }
            }
        }
    }
    tokens
}

fn discord_channel_set_from_environment(name: &str) -> Vec<String> {
    env::var(name)
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|value| {
                    let trimmed = value.trim();
                    validate_discord_id(trimmed, name)
                        .ok()
                        .map(|id| id.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}
