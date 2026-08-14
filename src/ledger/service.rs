use chrono::{DateTime, Utc};
use sqlx::{FromRow, SqlitePool};
use uuid::Uuid;

use crate::{DeliveryStatus, InboundEvent, OmonError, Result, SessionKey};

#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct DeliveryLedgerEntry {
    pub message_id: String,
    pub session_key: String,
    pub status: String,
    pub received_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub processing_latency_ms: Option<i64>,
    pub platform_message_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DeliveryLedgerService {
    pool: SqlitePool,
}

impl DeliveryLedgerService {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn record_incoming(&self, event: &InboundEvent) -> Result<bool> {
        self.ensure_session(&event.session).await?;
        let result = sqlx::query(
            "INSERT INTO delivery_ledger (
                delivery_id, session_key, event_id, message_id, status,
                platform_message_id, created_at, updated_at, received_at
             ) VALUES (?, ?, ?, ?, 'in_progress', ?, ?, ?, ?)
             ON CONFLICT(message_id) DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(event.session.storage_key())
        .bind(event.id.to_string())
        .bind(&event.platform_message_id)
        .bind(&event.platform_message_id)
        .bind(event.received_at)
        .bind(event.received_at)
        .bind(event.received_at)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn is_duplicate(&self, message_id: &str) -> Result<bool> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM delivery_ledger WHERE message_id = ?)")
                .bind(message_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(exists)
    }

    pub async fn mark_delivered(&self, message_id: &str) -> Result<()> {
        self.complete(message_id, DeliveryStatus::Delivered, None)
            .await
    }

    pub async fn mark_failed(&self, message_id: &str, error: impl Into<String>) -> Result<()> {
        self.complete(message_id, DeliveryStatus::Failed, Some(error.into()))
            .await
    }

    pub async fn get(&self, message_id: &str) -> Result<Option<DeliveryLedgerEntry>> {
        Ok(sqlx::query_as::<_, DeliveryLedgerEntry>(
            "SELECT message_id, session_key, status, received_at, completed_at,
                    processing_latency_ms, platform_message_id, error
             FROM delivery_ledger WHERE message_id = ?",
        )
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn complete(
        &self,
        message_id: &str,
        status: DeliveryStatus,
        error: Option<String>,
    ) -> Result<()> {
        let completed_at = Utc::now();
        let status = status_name(&status);
        let result = sqlx::query(
            "UPDATE delivery_ledger
             SET status = ?, error = ?, completed_at = ?, updated_at = ?,
                 processing_latency_ms = MAX(0, CAST((julianday(?) - julianday(received_at)) * 86400000 AS INTEGER))
             WHERE message_id = ?",
        )
        .bind(status)
        .bind(error)
        .bind(completed_at)
        .bind(completed_at)
        .bind(completed_at)
        .bind(message_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(OmonError::Database(format!(
                "delivery message {message_id} does not exist"
            )));
        }
        Ok(())
    }

    async fn ensure_session(&self, session: &SessionKey) -> Result<()> {
        let state_json = serde_json::to_string(&crate::SessionState::default())
            .map_err(|error| OmonError::Database(error.to_string()))?;
        sqlx::query(
            "INSERT INTO sessions (
                session_key, platform, guild_id, channel_id, thread_id, user_id, state_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(session_key) DO NOTHING",
        )
        .bind(session.storage_key())
        .bind(&session.platform)
        .bind(&session.guild_id)
        .bind(&session.channel_id)
        .bind(&session.thread_id)
        .bind(&session.user_id)
        .bind(state_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn status_name(status: &DeliveryStatus) -> &'static str {
    match status {
        DeliveryStatus::Pending => "pending",
        DeliveryStatus::InProgress => "in_progress",
        DeliveryStatus::Delivered => "delivered",
        DeliveryStatus::Failed => "failed",
    }
}
