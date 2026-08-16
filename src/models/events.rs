use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::SessionKey;

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
    if event.attachments.is_empty() {
        return event.content.clone();
    }

    let attachments = event
        .attachments
        .iter()
        .map(|attachment| {
            let local = attachment
                .local_path
                .as_ref()
                .map(|path| format!(" | local path: {}", path.display()))
                .unwrap_or_default();
            let mut formatted = format!(
                "[Attachment: {} ({}, {} bytes) - {}{}]",
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
    if event.content.trim().is_empty() {
        attachments
    } else {
        format!("{}\n\n{attachments}", event.content)
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
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::{
        format_inlined_text, reaction_emoji_for_outcome, render_user_prompt, InboundEvent,
        MessageAttachment, OutboundAction, StreamChunk, PROCESSING_FAILURE_EMOJI,
        PROCESSING_START_EMOJI, PROCESSING_SUCCESS_EMOJI,
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
        assert_eq!(
            rendered,
            "review this code\n\n[Attachment: main.rs (text/x-rust, 21 bytes) - https://cdn.example/main.rs | local path: /workspace/main.rs]\n\n[Content of main.rs]:\n\nfn main() {\n}\n"
        );
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
