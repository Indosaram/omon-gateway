use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::SessionKey;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Pending,
    InProgress,
    Delivered,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryReceipt {
    pub delivery_id: Uuid,
    pub event_id: Uuid,
    pub session: SessionKey,
    pub status: DeliveryStatus,
    pub platform_message_id: Option<String>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl DeliveryReceipt {
    pub fn pending(event_id: Uuid, session: SessionKey) -> Self {
        let now = Utc::now();
        Self {
            delivery_id: Uuid::new_v4(),
            event_id,
            session,
            status: DeliveryStatus::Pending,
            platform_message_id: None,
            error: None,
            created_at: now,
            updated_at: now,
        }
    }
}
