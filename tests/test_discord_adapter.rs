use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omon_gateway::discord::adapter::message_to_inbound;
use omon_gateway::{
    chunk_markdown, ApprovalDecision, ApprovalError, DiscordMessageTransport, LiveEditThrottler,
    Result, SmartApprovalGuard,
};
use serenity::all::{ChannelId, ChannelType, Message, MessageId, UserId};
use tokio::sync::{mpsc, Mutex};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call {
    Typing,
    Edit(MessageId, String),
    Send(String),
    Delete(MessageId),
}

struct MockTransport {
    calls: Mutex<Vec<Call>>,
    typing: mpsc::UnboundedSender<()>,
}

#[async_trait]
impl DiscordMessageTransport for MockTransport {
    async fn start_typing(&self, _channel_id: ChannelId) -> Result<()> {
        self.calls.lock().await.push(Call::Typing);
        self.typing.send(()).unwrap();
        Ok(())
    }

    async fn edit_message(
        &self,
        _channel_id: ChannelId,
        message_id: MessageId,
        content: String,
    ) -> Result<()> {
        self.calls
            .lock()
            .await
            .push(Call::Edit(message_id, content));
        Ok(())
    }

    async fn send_message(&self, _channel_id: ChannelId, content: String) -> Result<MessageId> {
        self.calls.lock().await.push(Call::Send(content));
        Ok(MessageId::new(99))
    }

    async fn delete_message(&self, _channel_id: ChannelId, message_id: MessageId) -> Result<()> {
        self.calls.lock().await.push(Call::Delete(message_id));
        Ok(())
    }
}

#[test]
fn chunks_markdown_at_discord_limit_and_balances_code_fences() {
    let content = format!(
        "before\n```rust\n{}\n```\nafter",
        "let value = 1;\n".repeat(250)
    );
    let chunks = chunk_markdown(&content, 2_000);

    assert!(chunks.len() > 1);
    assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 2_000));
    assert!(chunks
        .iter()
        .all(|chunk| chunk.matches("```").count() % 2 == 0));
    assert!(chunks[0].ends_with("\n```"));
    assert!(chunks[1].starts_with("```rust\n"));
}

#[tokio::test(start_paused = true)]
async fn live_edits_use_typing_and_debounce_subsequent_updates() {
    let (typing_tx, _typing_rx) = mpsc::unbounded_channel();
    let transport = Arc::new(MockTransport {
        calls: Mutex::new(Vec::new()),
        typing: typing_tx,
    });
    let throttler = Arc::new(LiveEditThrottler::with_debounce(
        transport.clone(),
        ChannelId::new(7),
        MessageId::new(8),
        Duration::from_millis(800),
    ));

    throttler.update("first", false).await.unwrap();
    let update = {
        let throttler = throttler.clone();
        tokio::spawn(async move { throttler.update("second", false).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(transport.calls.lock().await.len(), 2);

    tokio::time::advance(Duration::from_millis(799)).await;
    tokio::task::yield_now().await;
    assert_eq!(transport.calls.lock().await.len(), 2);
    tokio::time::advance(Duration::from_millis(1)).await;
    update.await.unwrap().unwrap();

    assert_eq!(
        *transport.calls.lock().await,
        vec![
            Call::Typing,
            Call::Edit(MessageId::new(8), "first".into()),
            Call::Typing,
            Call::Edit(MessageId::new(8), "second".into()),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn final_live_edit_preempts_a_sleeping_intermediate_update() {
    let (typing_tx, _typing_rx) = mpsc::unbounded_channel();
    let transport = Arc::new(MockTransport {
        calls: Mutex::new(Vec::new()),
        typing: typing_tx,
    });
    let throttler = Arc::new(LiveEditThrottler::with_debounce(
        transport.clone(),
        ChannelId::new(7),
        MessageId::new(8),
        Duration::from_millis(800),
    ));

    throttler.update("first", false).await.unwrap();
    let stale = {
        let throttler = throttler.clone();
        tokio::spawn(async move { throttler.update("stale", false).await })
    };
    tokio::task::yield_now().await;

    tokio::time::timeout(Duration::from_millis(1), throttler.update("final", true))
        .await
        .expect("final update must not wait behind the debounce sleeper")
        .unwrap();
    assert_eq!(
        *transport.calls.lock().await,
        vec![
            Call::Typing,
            Call::Edit(MessageId::new(8), "first".into()),
            Call::Typing,
            Call::Edit(MessageId::new(8), "final".into()),
        ]
    );

    tokio::time::advance(Duration::from_millis(800)).await;
    stale.await.unwrap().unwrap();
    assert_eq!(transport.calls.lock().await.len(), 4);
}

#[tokio::test(start_paused = true)]
async fn final_live_edit_deletes_surplus_chunk_messages() {
    let (typing_tx, _typing_rx) = mpsc::unbounded_channel();
    let transport = Arc::new(MockTransport {
        calls: Mutex::new(Vec::new()),
        typing: typing_tx,
    });
    let throttler = LiveEditThrottler::with_debounce(
        transport.clone(),
        ChannelId::new(7),
        MessageId::new(8),
        Duration::from_millis(800),
    );

    throttler.update(&"x".repeat(2_100), false).await.unwrap();
    throttler.update("short", true).await.unwrap();

    let calls = transport.calls.lock().await;
    assert!(calls.iter().any(|call| matches!(call, Call::Send(_))));
    assert!(calls.contains(&Call::Delete(MessageId::new(99))));
}

#[tokio::test(start_paused = true)]
async fn approval_guard_resolves_both_buttons_and_times_out() {
    let guard = SmartApprovalGuard::new();

    let approved = guard.request().await;
    let approved_id = approved.request_id;
    assert!(
        guard
            .resolve_custom_id(&format!("omon:approval:{approved_id}:approve"))
            .await
    );
    assert_eq!(
        approved.wait(Duration::from_secs(60)).await,
        Ok(ApprovalDecision::Approved)
    );

    let rejected = guard.request().await;
    let rejected_id = rejected.request_id;
    assert!(
        guard
            .resolve_custom_id(&format!("omon:approval:{rejected_id}:reject"))
            .await
    );
    assert_eq!(
        rejected.wait(Duration::from_secs(60)).await,
        Ok(ApprovalDecision::Rejected)
    );

    let pending = guard.request().await;
    let wait = tokio::spawn(pending.wait(Duration::from_secs(60)));
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(wait.await.unwrap(), Err(ApprovalError::Timeout));
}

#[test]
fn converts_serenity_dm_mentions_threads_and_attachments() {
    let bot_id = UserId::new(42);
    let dm = message_fixture(None, "hello", Vec::new());
    let event = message_to_inbound(&dm, bot_id, Some(ChannelType::Private)).unwrap();
    assert_eq!(event.content, "hello");
    assert_eq!(event.session.guild_id, None);
    assert_eq!(event.attachments[0].filename, "main.rs");
    assert_eq!(
        event.attachments[0].content_type.as_deref(),
        Some("text/x-rust")
    );

    let ignored = message_fixture(Some(9), "ordinary channel message", Vec::new());
    assert!(message_to_inbound(&ignored, bot_id, Some(ChannelType::Text)).is_none());

    let mentioned = message_fixture(Some(9), "<@42> inspect this", vec![42]);
    let event = message_to_inbound(&mentioned, bot_id, Some(ChannelType::Text)).unwrap();
    assert_eq!(event.content, "inspect this");
    assert_eq!(event.session.thread_id, None);

    let thread = message_fixture(Some(9), "thread continuation", Vec::new());
    let event = message_to_inbound(&thread, bot_id, Some(ChannelType::PublicThread)).unwrap();
    assert_eq!(event.session.thread_id.as_deref(), Some("7"));
}

fn message_fixture(guild_id: Option<u64>, content: &str, mentions: Vec<u64>) -> Message {
    let mentions = mentions
        .into_iter()
        .map(|id| {
            serde_json::json!({
                "id": id.to_string(), "username": "omon", "discriminator": "0001",
                "avatar": null, "bot": true, "system": false, "mfa_enabled": false,
                "banner": null, "accent_color": null, "locale": null, "verified": false,
                "email": null, "flags": 0, "premium_type": 0, "public_flags": 0,
                "global_name": null, "avatar_decoration_data": null, "collectibles": null,
                "primary_guild": null
            })
        })
        .collect::<Vec<_>>();
    serde_json::from_value(serde_json::json!({
        "id": "8", "channel_id": "7", "guild_id": guild_id.map(|id| id.to_string()),
        "author": {
            "id": "10", "username": "alice", "discriminator": "0001", "avatar": null,
            "bot": false, "system": false, "mfa_enabled": false, "banner": null,
            "accent_color": null, "locale": null, "verified": false, "email": null,
            "flags": 0, "premium_type": 0, "public_flags": 0, "global_name": null,
            "avatar_decoration_data": null, "collectibles": null, "primary_guild": null
        },
        "content": content, "timestamp": "2026-08-14T00:00:00Z", "edited_timestamp": null,
        "tts": false, "mention_everyone": false, "mentions": mentions, "mention_roles": [],
        "mention_channels": [],
        "attachments": [{
            "id": "11", "filename": "main.rs", "description": null, "height": null,
            "width": null, "proxy_url": "https://cdn.example/main.rs", "size": 24,
            "url": "https://cdn.example/main.rs", "content_type": "text/x-rust",
            "ephemeral": false, "duration_secs": null, "waveform": null
        }],
        "embeds": [], "reactions": [], "nonce": null, "pinned": false, "webhook_id": null,
        "type": 0, "activity": null, "application": null, "application_id": null,
        "message_reference": null, "flags": null, "referenced_message": null,
        "message_snapshots": [], "interaction": null, "interaction_metadata": null,
        "thread": null, "components": [], "sticker_items": [], "position": null,
        "role_subscription_data": null, "member": null, "poll": null
    }))
    .unwrap()
}
