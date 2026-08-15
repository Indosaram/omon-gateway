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
            format!(
                "[Attachment: {} ({}, {} bytes) - {}]",
                attachment.filename,
                attachment.content_type.as_deref().unwrap_or("unknown"),
                attachment
                    .size_bytes
                    .map_or_else(|| "unknown".to_owned(), |size| size.to_string()),
                attachment.url
            )
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
    Stream {
        session: SessionKey,
        chunk: StreamChunk,
    },
    Typing {
        session: SessionKey,
    },
    ApprovalRequest {
        session: SessionKey,
        request_id: Uuid,
        command: String,
    },
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{InboundEvent, MessageAttachment, OutboundAction, StreamChunk};
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
}
