use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use omon_gateway::{
    Database, DiscordMessageContextApi, DiscordMessageContextProvider,
    MessageContextConversationMetadata, MessageContextMessage, MessageContextOperationLimits,
    MessageContextPolicy, MessageContextPolicyMatrix, MessageContextProvider,
    MessageContextRequest, MessageSearchIndex, MessengerPolicyStore, OmonError, SessionKey,
};
use serde_json::json;

#[derive(Clone, Default)]
struct MockDiscordApi {
    messages: Arc<Mutex<HashMap<String, Vec<MessageContextMessage>>>>,
    metadata: Arc<Mutex<HashMap<String, MessageContextConversationMetadata>>>,
}

impl MockDiscordApi {
    fn with_channel(
        self,
        metadata: MessageContextConversationMetadata,
        messages: Vec<MessageContextMessage>,
    ) -> Self {
        self.messages
            .lock()
            .unwrap()
            .insert(metadata.channel_id.clone(), messages);
        self.metadata
            .lock()
            .unwrap()
            .insert(metadata.channel_id.clone(), metadata);
        self
    }
}

#[async_trait]
impl DiscordMessageContextApi for MockDiscordApi {
    async fn recent_messages(
        &self,
        _session: &SessionKey,
        channel_id: &str,
        before_message_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MessageContextMessage>, OmonError> {
        let before = before_message_id.and_then(|value| value.parse::<u64>().ok());
        Ok(self
            .messages
            .lock()
            .unwrap()
            .get(channel_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|message| match before {
                Some(before) => message.id.parse::<u64>().ok().is_some_and(|id| id < before),
                None => true,
            })
            .take(limit)
            .collect())
    }

    async fn get_message(
        &self,
        _session: &SessionKey,
        channel_id: &str,
        message_id: &str,
    ) -> Result<MessageContextMessage, OmonError> {
        self.messages
            .lock()
            .unwrap()
            .get(channel_id)
            .and_then(|messages| messages.iter().find(|message| message.id == message_id))
            .cloned()
            .ok_or_else(|| OmonError::ToolExecution("mock message not found".into()))
    }

    async fn conversation_metadata(
        &self,
        _session: &SessionKey,
        channel_id: &str,
    ) -> Result<Option<MessageContextConversationMetadata>, OmonError> {
        Ok(self.metadata.lock().unwrap().get(channel_id).cloned())
    }
}

fn session(guild: Option<&str>, channel: &str) -> SessionKey {
    SessionKey::new("discord", guild, channel, None::<String>, "10")
}

fn message(id: &str, channel: &str, content: &str) -> MessageContextMessage {
    MessageContextMessage {
        id: id.into(),
        channel_id: channel.into(),
        author_id: "10".into(),
        author_name: "alice".into(),
        author_is_bot: false,
        content: content.into(),
        timestamp: "2026-08-18T08:00:00Z".into(),
        attachments: Vec::new(),
        referenced_message_id: None,
    }
}

fn metadata(channel: &str, guild: Option<&str>) -> MessageContextConversationMetadata {
    MessageContextConversationMetadata {
        channel_id: channel.into(),
        channel_name: Some(format!("channel-{channel}")),
        channel_kind: "Text".into(),
        guild_id: guild.map(str::to_owned),
        topic: None,
        parent_channel_id: None,
        parent_channel_name: None,
        parent_channel_kind: None,
        parent_topic: None,
        is_thread: false,
        is_forum_post: false,
    }
}

async fn provider(
    api: MockDiscordApi,
    policy: MessageContextPolicy,
) -> (Database, DiscordMessageContextProvider) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let provider =
        DiscordMessageContextProvider::with_api(Arc::new(api), db.pool().clone(), policy);
    (db, provider)
}

#[tokio::test]
async fn get_message_returns_single_message_and_conversation_metadata() {
    let meta = metadata("21", Some("11"));
    let api =
        MockDiscordApi::default().with_channel(meta.clone(), vec![message("200", "21", "target")]);
    let (_db, provider) = provider(api, MessageContextPolicy::default()).await;
    let result = provider
        .query(
            &session(Some("11"), "21"),
            &MessageContextRequest::from_args(
                &json!({"operation":"get_message","message_id":"200"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.messages.len(), 1);
    assert_eq!(result.messages[0].content, "target");
    assert_eq!(result.conversation, Some(meta));
    assert_eq!(result.indexed, 1);
}

#[tokio::test]
async fn replies_scans_ordinary_references_when_no_thread_exists() {
    let mut reply = message("190", "21", "reply");
    reply.referenced_message_id = Some("200".into());
    let api = MockDiscordApi::default().with_channel(
        metadata("21", Some("11")),
        vec![
            message("200", "21", "target"),
            reply.clone(),
            message("180", "21", "other"),
        ],
    );
    let (_db, provider) = provider(api, MessageContextPolicy::default()).await;
    let result = provider
        .query(
            &session(Some("11"), "21"),
            &MessageContextRequest::from_args(
                &json!({"operation":"replies","message_id":"200","scan_limit":10}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.messages, vec![reply]);
}

#[tokio::test]
async fn replies_prefers_native_thread_and_exposes_forum_post_metadata() {
    let parent = metadata("21", Some("11"));
    let mut thread = metadata("200", Some("11"));
    thread.channel_kind = "PublicThread".into();
    thread.parent_channel_id = Some("21".into());
    thread.parent_channel_name = Some("forum".into());
    thread.parent_channel_kind = Some("Forum".into());
    thread.parent_topic = Some("Support topics".into());
    thread.is_thread = true;
    thread.is_forum_post = true;
    let reply = message("199", "200", "thread reply");
    let api = MockDiscordApi::default()
        .with_channel(parent, vec![message("200", "21", "starter")])
        .with_channel(thread.clone(), vec![reply.clone()]);
    let (_db, provider) = provider(api, MessageContextPolicy::default()).await;
    let result = provider
        .query(
            &session(Some("11"), "21"),
            &MessageContextRequest::from_args(&json!({"operation":"replies","message_id":"200"}))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.channel_id, "200");
    assert_eq!(result.messages, vec![reply]);
    assert_eq!(result.conversation, Some(thread));
}

#[tokio::test]
async fn cross_channel_policy_checks_allowlist_and_same_guild_metadata() {
    let api = MockDiscordApi::default()
        .with_channel(metadata("21", Some("11")), vec![])
        .with_channel(metadata("33", Some("99")), vec![]);
    let (_db, provider) = provider(
        api,
        MessageContextPolicy::new(vec!["33".into()], Vec::new()),
    )
    .await;
    let error = provider
        .query(
            &session(Some("11"), "21"),
            &MessageContextRequest::from_args(&json!({"operation":"recent","channel_id":"33"}))
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("current workspace/guild"));
}

#[tokio::test]
async fn recent_pagination_and_fts_rest_backfill_are_integrated() {
    let api = MockDiscordApi::default().with_channel(
        metadata("21", Some("11")),
        vec![
            message("300", "21", "fresh release note"),
            message("200", "21", "older release note"),
            message("100", "21", "legacy architecture note"),
        ],
    );
    let (db, provider) = provider(api, MessageContextPolicy::default()).await;
    let current = session(Some("11"), "21");
    let recent = provider
        .query(
            &current,
            &MessageContextRequest::from_args(
                &json!({"operation":"recent","limit":1,"before_message_id":"300"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recent.messages[0].id, "200");

    let search = provider
        .query(
            &current,
            &MessageContextRequest::from_args(
                &json!({"operation":"search","query":"legacy","scan_limit":10}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        search.search_source.as_deref(),
        Some("sqlite_fts5+discord_rest_backfill")
    );
    assert_eq!(search.messages[0].id, "100");
    assert_eq!(
        MessageSearchIndex::new(db.pool().clone())
            .count("discord", "21")
            .await
            .unwrap(),
        3
    );
}

#[tokio::test]
async fn persisted_policy_override_applies_without_rebuilding_provider() {
    let api = MockDiscordApi::default()
        .with_channel(metadata("55", None), vec![message("100", "55", "private")]);
    let (db, provider) = provider(api, MessageContextPolicy::default()).await;
    let matrix = MessageContextPolicyMatrix {
        allow_dm_reads: false,
        ..Default::default()
    };
    MessengerPolicyStore::new(db.pool().clone())
        .set_override("discord", &matrix)
        .await
        .unwrap();

    let error = provider
        .query(
            &session(None, "55"),
            &MessageContextRequest::from_args(&json!({"operation":"recent"})).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("direct-message reads are disabled"));
}

#[tokio::test]
async fn persisted_policy_limits_are_enforced_per_operation() {
    let api = MockDiscordApi::default().with_channel(
        metadata("21", Some("11")),
        vec![
            message("300", "21", "one"),
            message("200", "21", "two"),
            message("100", "21", "three"),
        ],
    );
    let (db, provider) = provider(api, MessageContextPolicy::default()).await;
    let matrix = MessageContextPolicyMatrix {
        limits: MessageContextOperationLimits {
            recent: 1,
            search_results: 1,
            search_scan: 2,
            replies: 1,
        },
        ..Default::default()
    };
    MessengerPolicyStore::new(db.pool().clone())
        .set_override("discord", &matrix)
        .await
        .unwrap();

    let recent = provider
        .query(
            &session(Some("11"), "21"),
            &MessageContextRequest::from_args(&json!({"operation":"recent","limit":20})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recent.messages.len(), 1);

    let search = provider
        .query(
            &session(Some("11"), "21"),
            &MessageContextRequest::from_args(
                &json!({"operation":"search","query":"one","limit":20,"scan_limit":200}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(search.messages.len(), 1);
    assert_eq!(search.scanned, 2);
}

#[tokio::test]
async fn transcript_insert_trigger_populates_fts_index() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let key = session(Some("11"), "21");
    sqlx::query(
        "INSERT INTO sessions (
            session_key, platform, guild_id, channel_id, user_id, state_json
         ) VALUES (?, 'discord', '11', '21', '10', '{}')",
    )
    .bind(key.storage_key())
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO messages (
            id, session_key, role, content, metadata_json, platform_message_id
         ) VALUES ('local-id', ?, 'user', 'triggered searchable transcript', '{}', '400')",
    )
    .bind(key.storage_key())
    .execute(db.pool())
    .await
    .unwrap();

    let hits = MessageSearchIndex::new(db.pool().clone())
        .search("discord", "21", "triggered", None, 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].document.message_id, "400");
    assert_eq!(hits[0].document.metadata["source"], "transcript");
}
