use std::path::PathBuf;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::SessionKey;

static TIMESTAMP_PREFIX_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:\[(?:[A-Z][a-z]{2} \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?: [A-Za-z0-9_+\-/: ]+)?|\d{4}-\d{2}-\d{2}T[^\]]+)\]\s*)+").unwrap()
});

pub fn format_message_timestamp(dt: DateTime<Utc>) -> String {
    format!("[{}]", dt.format("%a %Y-%m-%d %H:%M:%S UTC"))
}

pub fn strip_leading_message_timestamps(s: &str) -> String {
    if let Some(mat) = TIMESTAMP_PREFIX_RE.find(s) {
        if mat.start() == 0 {
            return s[mat.end()..].to_string();
        }
    }
    s.to_string()
}

pub fn message_timestamps_enabled() -> bool {
    std::env::var("DISCORD_MESSAGE_TIMESTAMPS")
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(true)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAttachment {
    pub id: String,
    pub filename: String,
    pub url: String,
    pub content_type: Option<String>,
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_content: Option<String>,
}

pub const PROCESSING_START_EMOJI: &str = "👀";
pub const PROCESSING_SUCCESS_EMOJI: &str = "✅";
pub const PROCESSING_FAILURE_EMOJI: &str = "❌";

pub fn reaction_emoji_for_outcome(success: bool) -> &'static str {
    if success {
        PROCESSING_SUCCESS_EMOJI
    } else {
        PROCESSING_FAILURE_EMOJI
    }
}

pub fn format_inlined_text(filename: &str, content: &str) -> String {
    format!("\n\n[Content of {filename}]:\n\n{content}")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboundEvent {
    pub id: Uuid,
    pub session: SessionKey,
    pub platform_message_id: String,
    /// Durable delivery-ledger claim associated with this event, when ingress
    /// deduplication is enabled for the transport.
    #[serde(default)]
    pub delivery_id: Option<String>,
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<MessageAttachment>,
    pub received_at: DateTime<Utc>,
}

pub fn render_user_prompt(event: &InboundEvent) -> String {
    let clean_content = strip_leading_message_timestamps(&event.content);
    let prompt_body = if message_timestamps_enabled() {
        let ts = format_message_timestamp(event.received_at);
        if clean_content.is_empty() {
            String::new()
        } else {
            format!("{ts} {clean_content}")
        }
    } else {
        clean_content
    };

    if event.attachments.is_empty() {
        return prompt_body;
    }

    let attachments = event
        .attachments
        .iter()
        .map(|attachment| {
            let is_voice = attachment.filename.ends_with(".ogg")
                || attachment.filename.ends_with(".opus")
                || attachment.filename.contains("voice-message")
                || attachment.filename.contains("voice_message")
                || attachment.content_type.as_deref().is_some_and(|ct| {
                    ct.starts_with("audio/ogg")
                        || ct.starts_with("audio/opus")
                        || ct.contains("voice")
                });
            let label = if is_voice {
                "Voice message"
            } else {
                "Attachment"
            };
            let local = attachment
                .local_path
                .as_ref()
                .map(|path| format!(" | local path: {}", path.display()))
                .unwrap_or_default();
            let mut formatted = format!(
                "[{}: {} ({}, {} bytes) - {}{}]",
                label,
                attachment.filename,
                attachment.content_type.as_deref().unwrap_or("unknown"),
                attachment
                    .size_bytes
                    .map_or_else(|| "unknown".to_owned(), |size| size.to_string()),
                attachment.url,
                local
            );
            if let Some(text) = &attachment.text_content {
                formatted.push_str(&format_inlined_text(&attachment.filename, text));
            }
            formatted
        })
        .collect::<Vec<_>>()
        .join("\n");
    if prompt_body.trim().is_empty() {
        attachments
    } else {
        format!("{prompt_body}\n\n{attachments}")
    }
}

impl InboundEvent {
    pub fn message(
        session: SessionKey,
        platform_message_id: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            session,
            platform_message_id: platform_message_id.into(),
            delivery_id: None,
            content: content.into(),
            attachments: Vec::new(),
            received_at: Utc::now(),
        }
    }

    pub fn with_attachments(mut self, attachments: Vec<MessageAttachment>) -> Self {
        self.attachments = attachments;
        self
    }

    pub fn with_delivery_id(mut self, delivery_id: impl Into<String>) -> Self {
        self.delivery_id = Some(delivery_id.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamChunk {
    pub stream_id: Uuid,
    pub sequence: u64,
    pub content: String,
    pub is_final: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutboundAction {
    SendMessage {
        session: SessionKey,
        content: String,
        reply_to: Option<String>,
    },
    EditMessage {
        session: SessionKey,
        platform_message_id: String,
        content: String,
    },
    DeleteMessage {
        session: SessionKey,
        platform_message_id: String,
    },
    UploadFile {
        session: SessionKey,
        path: PathBuf,
    },
    Stream {
        session: SessionKey,
        chunk: StreamChunk,
    },
    Typing {
        session: SessionKey,
        active: bool,
    },
    React {
        session: SessionKey,
        message_id: String,
        emoji: String,
        remove_others: bool,
    },
    ApprovalRequest {
        session: SessionKey,
        request_id: Uuid,
        command: String,
        reason: String,
    },
    ExpireApproval {
        request_id: Uuid,
    },
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::{
        format_inlined_text, format_message_timestamp, reaction_emoji_for_outcome,
        render_user_prompt, strip_leading_message_timestamps, InboundEvent, MessageAttachment,
        OutboundAction, StreamChunk, Utc, PROCESSING_FAILURE_EMOJI, PROCESSING_START_EMOJI,
        PROCESSING_SUCCESS_EMOJI,
    };
    use crate::models::SessionKey;

    fn session() -> SessionKey {
        SessionKey::new("discord", Some("guild"), "channel", None::<String>, "user")
    }

    #[test]
    fn constructs_inbound_message_with_attachment() {
        let attachment = MessageAttachment {
            id: "attachment-1".into(),
            filename: "notes.txt".into(),
            url: "https://cdn.example/notes.txt".into(),
            content_type: Some("text/plain".into()),
            size_bytes: Some(42),
            local_path: Some(PathBuf::from("/workspace/attachments/notes.txt")),
            text_content: None,
        };
        let event = InboundEvent::message(session(), "message-1", "hello")
            .with_attachments(vec![attachment.clone()]);

        assert_eq!(event.platform_message_id, "message-1");
        assert_eq!(event.content, "hello");
        assert_eq!(event.attachments, vec![attachment]);
        assert!(!event.id.is_nil());
    }

    #[test]
    fn serializes_tagged_outbound_action() {
        let action = OutboundAction::SendMessage {
            session: session(),
            content: "response".into(),
            reply_to: Some("message-1".into()),
        };

        let value = serde_json::to_value(&action).expect("outbound action should serialize");
        assert_eq!(value["type"], "send_message");
        assert_eq!(value["content"], "response");
        assert_eq!(value["reply_to"], "message-1");

        let typing_action = OutboundAction::Typing {
            session: session(),
            active: true,
        };
        let typing_value =
            serde_json::to_value(&typing_action).expect("typing action should serialize");
        assert_eq!(typing_value["type"], "typing");
        assert_eq!(typing_value["active"], true);

        let req_id = Uuid::new_v4();
        let expire_action = OutboundAction::ExpireApproval { request_id: req_id };
        let expire_value =
            serde_json::to_value(&expire_action).expect("expire action should serialize");
        assert_eq!(expire_value["type"], "expire_approval");
        assert_eq!(expire_value["request_id"], req_id.to_string());
    }

    #[test]
    fn serializes_and_deserializes_react_outbound_action() {
        let action = OutboundAction::React {
            session: session(),
            message_id: "msg-42".into(),
            emoji: "👀".into(),
            remove_others: true,
        };

        let value = serde_json::to_value(&action).expect("react action should serialize");
        assert_eq!(value["type"], "react");
        assert_eq!(value["message_id"], "msg-42");
        assert_eq!(value["emoji"], "👀");
        assert_eq!(value["remove_others"], true);

        let decoded: OutboundAction =
            serde_json::from_value(value).expect("react action should deserialize");
        assert_eq!(decoded, action);
    }

    #[test]
    fn emoji_by_outcome_selection() {
        assert_eq!(reaction_emoji_for_outcome(true), PROCESSING_SUCCESS_EMOJI);
        assert_eq!(reaction_emoji_for_outcome(false), PROCESSING_FAILURE_EMOJI);
        assert_eq!(PROCESSING_START_EMOJI, "👀");
        assert_eq!(PROCESSING_SUCCESS_EMOJI, "✅");
        assert_eq!(PROCESSING_FAILURE_EMOJI, "❌");
    }

    #[test]
    fn stream_chunk_round_trips_through_an_action() {
        let action = OutboundAction::Stream {
            session: session(),
            chunk: StreamChunk {
                stream_id: Uuid::new_v4(),
                sequence: 2,
                content: "partial response".into(),
                is_final: false,
            },
        };

        let json = serde_json::to_string(&action).expect("stream action should serialize");
        let decoded: OutboundAction =
            serde_json::from_str(&json).expect("stream action should deserialize");
        assert_eq!(decoded, action);
    }

    #[test]
    fn test_format_message_timestamp() {
        use chrono::TimeZone;
        let dt = Utc.with_ymd_and_hms(2026, 4, 28, 13, 40, 53).unwrap();
        assert_eq!(
            format_message_timestamp(dt),
            "[Tue 2026-04-28 13:40:53 UTC]"
        );
    }

    #[test]
    fn test_strip_leading_message_timestamps() {
        assert_eq!(
            strip_leading_message_timestamps("[Tue 2026-04-28 13:40:53 UTC] hello world"),
            "hello world"
        );
        assert_eq!(
            strip_leading_message_timestamps("[Tue 2026-04-28 13:40:53 CEST] hello world"),
            "hello world"
        );
        assert_eq!(
            strip_leading_message_timestamps("[2026-04-13T17:02:06+02:00] hello world"),
            "hello world"
        );
        assert_eq!(
            strip_leading_message_timestamps("[2026-04-13T17:02:06Z] hello world"),
            "hello world"
        );
        // Multiple stacked timestamps
        assert_eq!(
            strip_leading_message_timestamps(
                "[Tue 2026-04-28 13:40:53 UTC] [2026-04-13T17:02:06+0200]  actual message"
            ),
            "actual message"
        );
        // Plain text without timestamps
        assert_eq!(
            strip_leading_message_timestamps("hello [Tue 2026-04-28 13:40:53 UTC]"),
            "hello [Tue 2026-04-28 13:40:53 UTC]"
        );
        assert_eq!(strip_leading_message_timestamps(""), "");
    }

    #[test]
    fn test_timestamp_strip_format_idempotency() {
        use chrono::TimeZone;
        let dt = Utc.with_ymd_and_hms(2026, 4, 28, 13, 40, 53).unwrap();
        let formatted = format_message_timestamp(dt);
        let raw = "user query";
        let turn1 = format!("{formatted} {raw}");
        let clean1 = strip_leading_message_timestamps(&turn1);
        assert_eq!(clean1, raw);

        // Turn 2 re-injected
        let turn2 = format!("{formatted} {clean1}");
        let clean2 = strip_leading_message_timestamps(&turn2);
        assert_eq!(clean2, raw);
    }

    #[test]
    fn render_user_prompt_labels_voice_message() {
        let mut event = InboundEvent::message(
            SessionKey::new("discord", None::<String>, "1", None::<String>, "u1"),
            "msg-1",
            "Please listen to this",
        );
        event.attachments = vec![MessageAttachment {
            id: "att-voice".into(),
            filename: "voice-message.ogg".into(),
            url: "https://cdn.discordapp.com/voice.ogg".into(),
            content_type: Some("audio/ogg".into()),
            size_bytes: Some(12345),
            local_path: None,
            text_content: Some("[Voice message (audio downloaded)]".into()),
        }];

        let rendered = render_user_prompt(&event);
        assert!(rendered.contains("[Voice message: voice-message.ogg (audio/ogg, 12345 bytes)"));
        assert!(rendered.contains("[Voice message (audio downloaded)]"));
    }

    #[test]
    fn render_user_prompt_inlines_text_content() {
        let attachment = MessageAttachment {
            id: "attachment-1".into(),
            filename: "main.rs".into(),
            url: "https://cdn.example/main.rs".into(),
            content_type: Some("text/x-rust".into()),
            size_bytes: Some(21),
            local_path: Some(PathBuf::from("/workspace/main.rs")),
            text_content: Some("fn main() {\n}\n".into()),
        };
        let event = InboundEvent::message(session(), "message-1", "review this code")
            .with_attachments(vec![attachment]);

        let rendered = render_user_prompt(&event);
        assert!(rendered.contains("review this code\n\n[Attachment: main.rs (text/x-rust, 21 bytes) - https://cdn.example/main.rs | local path: /workspace/main.rs]\n\n[Content of main.rs]:\n\nfn main() {\n}\n"));
    }

    #[test]
    fn format_inlined_text_delimiters() {
        let formatted = format_inlined_text("config.toml", "key = \"value\"\n");
        assert_eq!(
            formatted,
            "\n\n[Content of config.toml]:\n\nkey = \"value\"\n"
        );
    }
}
