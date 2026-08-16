use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use omon_gateway::discord::adapter::{message_to_inbound, message_to_inbound_with_config};
use omon_gateway::discord::commands::is_user_allowed;
use omon_gateway::{
    chunk_markdown, render_user_prompt, ApprovalDecision, ApprovalError, AttachmentDownloader,
    ChatMessage, Database, DeliveryLedgerService, DiscordEgress, DiscordFileUploader,
    DiscordMessageTransport, InboundEvent, LiveEditThrottler, LlmClient, LlmConfig, LlmProvider,
    MessageAttachment, OutboundAction, OutboundDispatcher, Result, SessionKey, SmartApprovalGuard,
    DISCORD_ATTACHMENT_MAX_BYTES,
};
use serenity::all::{ChannelId, ChannelType, Message, MessageId, UserId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

#[derive(Default)]
struct MockFileUploader {
    calls: Mutex<Vec<(ChannelId, PathBuf)>>,
}

#[async_trait]
impl DiscordFileUploader for MockFileUploader {
    async fn upload(
        &self,
        _http: Arc<serenity::http::Http>,
        channel: ChannelId,
        path: &Path,
    ) -> Result<()> {
        self.calls.lock().await.push((channel, path.to_owned()));
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
        Ok(ApprovalDecision::Once)
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
        Ok(ApprovalDecision::Deny)
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
    assert_eq!(event.attachments[0].local_path, None);

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

#[test]
fn only_primary_bot_owns_unmentioned_threads_and_free_channels() {
    let primary = UserId::new(42);
    let secondary = UserId::new(84);
    let thread = message_fixture(Some(9), "continue", Vec::new());

    assert!(message_to_inbound_with_config(
        &thread,
        primary,
        Some(ChannelType::PublicThread),
        &[],
        &[],
        Some(primary.get()),
    )
    .is_some());
    assert!(message_to_inbound_with_config(
        &thread,
        secondary,
        Some(ChannelType::PublicThread),
        &[],
        &[],
        Some(primary.get()),
    )
    .is_none());

    let free = message_fixture(Some(9), "hello", Vec::new());
    assert!(message_to_inbound_with_config(
        &free,
        primary,
        Some(ChannelType::Text),
        &[7],
        &[],
        Some(primary.get()),
    )
    .is_some());
    assert!(message_to_inbound_with_config(
        &free,
        secondary,
        Some(ChannelType::Text),
        &[7],
        &[],
        Some(primary.get()),
    )
    .is_none());
}

#[test]
fn every_bot_answers_its_own_direct_messages_regardless_of_primary_bot() {
    let primary = UserId::new(42);
    let secondary = UserId::new(84);
    let dm = message_fixture(None, "hello", Vec::new());

    let non_primary_event = message_to_inbound_with_config(
        &dm,
        secondary,
        Some(ChannelType::Private),
        &[],
        &[],
        Some(primary.get()),
    );
    assert!(non_primary_event.is_some());
    assert_eq!(
        non_primary_event
            .as_ref()
            .unwrap()
            .session
            .bot_id
            .as_deref(),
        Some("84")
    );

    let primary_event = message_to_inbound_with_config(
        &dm,
        primary,
        Some(ChannelType::Private),
        &[],
        &[],
        Some(primary.get()),
    );
    assert!(primary_event.is_some());
    assert_eq!(
        primary_event.as_ref().unwrap().session.bot_id.as_deref(),
        Some("42")
    );
}

#[test]
fn every_explicitly_mentioned_bot_owns_exactly_its_target() {
    let message = message_fixture(Some(9), "<@42> <@84> compare", vec![42, 84]);

    assert!(message_to_inbound_with_config(
        &message,
        UserId::new(42),
        Some(ChannelType::Text),
        &[],
        &[],
        Some(42),
    )
    .is_some());
    assert!(message_to_inbound_with_config(
        &message,
        UserId::new(84),
        Some(ChannelType::Text),
        &[],
        &[],
        Some(42),
    )
    .is_some());
    assert!(message_to_inbound_with_config(
        &message,
        UserId::new(126),
        Some(ChannelType::Text),
        &[],
        &[],
        Some(42),
    )
    .is_none());
}

#[tokio::test]
async fn discord_delivery_claims_deduplicate_per_target_bot() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let ledger = DeliveryLedgerService::new(database.pool().clone());
    let session =
        SessionKey::new("discord", Some("9"), "7", None::<String>, "10").with_bot_id("42");
    let event = InboundEvent::message(session, "8", "hello");

    assert!(ledger
        .record_incoming_as(&event, "discord:8")
        .await
        .unwrap());
    assert!(!ledger
        .record_incoming_as(&event, "discord:8")
        .await
        .unwrap());
    assert!(ledger
        .record_incoming_as(&event, "discord:8:84")
        .await
        .unwrap());
}

#[test]
fn renders_complete_attachment_context_for_attachment_only_turns() {
    let event = InboundEvent::message(
        SessionKey::new("discord", Some("9"), "7", None::<String>, "10"),
        "8",
        "",
    )
    .with_attachments(vec![MessageAttachment {
        id: "11".into(),
        filename: "main.rs".into(),
        url: "https://cdn.example/main.rs".into(),
        content_type: Some("text/x-rust".into()),
        size_bytes: Some(24),
        local_path: None,
    }]);

    assert_eq!(
        render_user_prompt(&event),
        "[Attachment: main.rs (text/x-rust, 24 bytes) - https://cdn.example/main.rs]"
    );
}

#[test]
fn renders_downloaded_attachment_local_path() {
    let local_path = PathBuf::from("/tmp/omon/main.png");
    let event = InboundEvent::message(
        SessionKey::new("discord", Some("9"), "7", None::<String>, "10"),
        "8",
        "inspect",
    )
    .with_attachments(vec![MessageAttachment {
        id: "11".into(),
        filename: "main.png".into(),
        url: "https://cdn.example/main.png".into(),
        content_type: Some("image/png".into()),
        size_bytes: Some(24),
        local_path: Some(local_path.clone()),
    }]);

    let prompt = render_user_prompt(&event);
    assert!(prompt.starts_with("inspect\n\n[Attachment: main.png"));
    assert!(prompt.contains(&format!("local path: {}", local_path.display())));
}

#[tokio::test]
async fn downloads_discord_attachment_once_and_reuses_cache() {
    let workspace = test_workspace("download-cache");
    std::fs::create_dir_all(&workspace).unwrap();
    let body = b"cached-image-bytes".to_vec();
    let request_count = Arc::new(AtomicUsize::new(0));
    let (url, server) = spawn_single_response_server(body.clone(), request_count.clone()).await;
    let downloader = AttachmentDownloader::new(&workspace).unwrap();
    let attachment = MessageAttachment {
        id: "attachment/11".into(),
        filename: "../capture.png".into(),
        url,
        content_type: Some("image/png".into()),
        size_bytes: Some(body.len() as u64),
        local_path: None,
    };

    let first = downloader.download_attachment(&attachment).await.unwrap();
    server.await.unwrap();
    let second = downloader.download_attachment(&attachment).await.unwrap();

    assert_eq!(first, second);
    assert_eq!(std::fs::read(&first).unwrap(), body);
    assert_eq!(request_count.load(Ordering::SeqCst), 1);
    assert!(first.starts_with(std::fs::canonicalize(&workspace).unwrap()));
    assert_eq!(first.parent(), Some(downloader.attachment_root()));
    assert!(!first.to_string_lossy().contains(".."));

    let _ = std::fs::remove_dir_all(workspace);
}

#[tokio::test]
async fn rejects_discord_attachment_over_size_limit_before_network() {
    let workspace = test_workspace("size-limit");
    let downloader = AttachmentDownloader::new(&workspace).unwrap();
    let attachment = MessageAttachment {
        id: "11".into(),
        filename: "large.bin".into(),
        url: "http://127.0.0.1:1/never-requested".into(),
        content_type: Some("application/octet-stream".into()),
        size_bytes: Some(DISCORD_ATTACHMENT_MAX_BYTES + 1),
        local_path: None,
    };

    let error = downloader
        .download_attachment(&attachment)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("25 MB"));

    let _ = std::fs::remove_dir_all(workspace);
}

#[test]
fn encodes_supported_images_as_openai_and_anthropic_vision_blocks() {
    let workspace = test_workspace("vision");
    std::fs::create_dir_all(&workspace).unwrap();
    let formats = [
        ("png", "image/png", b"png-bytes".as_slice()),
        ("jpg", "image/jpeg", b"jpeg-bytes".as_slice()),
        ("webp", "image/webp", b"webp-bytes".as_slice()),
        ("gif", "image/gif", b"gif-bytes".as_slice()),
    ];
    let mut attachments = Vec::new();
    for (index, (extension, media_type, bytes)) in formats.iter().enumerate() {
        let path = workspace.join(format!("image-{index}.{extension}"));
        std::fs::write(&path, bytes).unwrap();
        attachments.push(MessageAttachment {
            id: index.to_string(),
            filename: path.file_name().unwrap().to_string_lossy().into_owned(),
            url: format!("https://cdn.example/image-{index}.{extension}"),
            content_type: Some((*media_type).into()),
            size_bytes: Some(bytes.len() as u64),
            local_path: Some(path),
        });
    }
    let message = ChatMessage::new("user", "inspect these").with_attachments(attachments.clone());

    let openai = LlmClient::new(LlmConfig::new(LlmProvider::OpenAi, "gpt-test")).unwrap();
    let openai_payload = openai.build_payload(std::slice::from_ref(&message), &[]);
    let openai_content = openai_payload["messages"][0]["content"].as_array().unwrap();
    assert_eq!(openai_content[0]["type"], "text");
    assert_eq!(openai_content.len(), formats.len() + 1);
    for (index, (_, media_type, bytes)) in formats.iter().enumerate() {
        assert_eq!(openai_content[index + 1]["type"], "image_url");
        assert_eq!(
            openai_content[index + 1]["image_url"]["url"],
            format!("data:{media_type};base64,{}", BASE64_STANDARD.encode(bytes))
        );
    }

    let anthropic = LlmClient::new(LlmConfig::new(LlmProvider::Anthropic, "claude-test")).unwrap();
    let anthropic_payload = anthropic.build_payload(&[message], &[]);
    let anthropic_content = anthropic_payload["messages"][0]["content"]
        .as_array()
        .unwrap();
    assert_eq!(anthropic_content.len(), formats.len() + 1);
    for (index, (_, media_type, bytes)) in formats.iter().enumerate() {
        assert_eq!(anthropic_content[index]["type"], "image");
        assert_eq!(anthropic_content[index]["source"]["type"], "base64");
        assert_eq!(
            anthropic_content[index]["source"]["media_type"],
            *media_type
        );
        assert_eq!(
            anthropic_content[index]["source"]["data"],
            BASE64_STANDARD.encode(bytes)
        );
    }
    assert_eq!(anthropic_content[formats.len()]["type"], "text");

    let _ = std::fs::remove_dir_all(workspace);
}

#[tokio::test]
async fn discord_egress_dispatches_upload_file_to_target_channel() {
    let workspace = test_workspace("upload");
    std::fs::create_dir_all(&workspace).unwrap();
    let path = workspace.join("report.txt");
    std::fs::write(&path, b"report").unwrap();
    let uploader = Arc::new(MockFileUploader::default());
    let egress = DiscordEgress::new(Arc::new(serenity::http::Http::new("test-token")))
        .with_file_uploader(uploader.clone());
    let session = SessionKey::new("discord", Some("9"), "7", None::<String>, "10");

    egress
        .dispatch(OutboundAction::UploadFile {
            session,
            path: path.clone(),
        })
        .await
        .unwrap();

    assert_eq!(
        *uploader.calls.lock().await,
        vec![(ChannelId::new(7), path)]
    );
    let _ = std::fs::remove_dir_all(workspace);
}

#[tokio::test]
async fn discord_egress_handles_typing_start_and_stop() {
    let egress = DiscordEgress::new(Arc::new(serenity::http::Http::new("test-token")));
    let session = SessionKey::new("discord", Some("9"), "7", None::<String>, "10");

    egress
        .dispatch(OutboundAction::Typing {
            session: session.clone(),
            active: true,
        })
        .await
        .unwrap();

    egress
        .dispatch(OutboundAction::Typing {
            session,
            active: false,
        })
        .await
        .unwrap();
}

#[test]
fn slash_authorization_defaults_open_and_enforces_allowlist() {
    assert!(is_user_allowed(&[], 10));
    assert!(is_user_allowed(&[10, 11], 10));
    assert!(!is_user_allowed(&[10, 11], 12));
}

fn test_workspace(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("omon-discord-{label}-{}", uuid::Uuid::new_v4()))
}

async fn spawn_single_response_server(
    body: Vec<u8>,
    request_count: Arc<AtomicUsize>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 2048];
        let _ = socket.read(&mut request).await.unwrap();
        request_count.fetch_add(1, Ordering::SeqCst);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    (format!("http://{address}/attachment"), handle)
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
