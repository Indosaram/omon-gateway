use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use uuid::Uuid;

use crate::{DeliveryStatus, InboundEvent, OmonError, Result, SessionKey};

pub const RECOVERED_REPLY_MARKER: &str =
    "♻️ Recovered reply — the gateway restarted during delivery, so this may be a duplicate:\n\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryObligationState {
    Pending,
    Attempting,
    Delivered,
    Failed,
    Abandoned,
}

impl DeliveryObligationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Attempting => "attempting",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct DeliveryObligation {
    pub id: String,
    pub session_key: String,
    pub channel_id: String,
    pub thread_id: Option<String>,
    pub content: String,
    pub state: String,
    pub attempts: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub owner_pid: Option<i64>,
    pub last_error: Option<String>,
}

/// Returns true if the process with the given PID is currently alive on this host.
pub fn is_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let res = unsafe { libc::kill(pid as i32, 0) };
    if res == 0 {
        true
    } else {
        let err = std::io::Error::last_os_error();
        err.raw_os_error() == Some(libc::EPERM)
    }
}

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
        self.record_incoming_as(event, &event.platform_message_id)
            .await
    }

    pub async fn record_incoming_as(
        &self,
        event: &InboundEvent,
        delivery_id: &str,
    ) -> Result<bool> {
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
        .bind(delivery_id)
        .bind(delivery_id)
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

    /// Records an outbound delivery obligation as 'pending'.
    pub async fn record_obligation(
        &self,
        id: &str,
        session: &SessionKey,
        content: &str,
    ) -> Result<()> {
        self.ensure_session(session).await?;
        let now = Utc::now();
        let pid = std::process::id() as i64;
        sqlx::query(
            "INSERT INTO delivery_obligations (
                id, session_key, channel_id, thread_id, content, state,
                attempts, created_at, updated_at, owner_pid, last_error
             ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, NULL)
             ON CONFLICT(id) DO UPDATE SET
                content = excluded.content,
                updated_at = excluded.updated_at,
                owner_pid = excluded.owner_pid",
        )
        .bind(id)
        .bind(session.storage_key())
        .bind(&session.channel_id)
        .bind(&session.thread_id)
        .bind(content)
        .bind(now)
        .bind(now)
        .bind(pid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Marks an obligation as 'attempting' immediately before dispatch.
    pub async fn mark_obligation_attempting(&self, id: &str) -> Result<()> {
        self.update_obligation_state(id, "attempting", None).await
    }

    /// Marks an obligation as 'delivered' once dispatch is confirmed.
    pub async fn mark_obligation_delivered(&self, id: &str) -> Result<()> {
        self.update_obligation_state(id, "delivered", None).await
    }

    /// Marks an obligation as 'failed' on definitive rejection or error.
    pub async fn mark_obligation_failed(&self, id: &str, error: &str) -> Result<()> {
        self.update_obligation_state(id, "failed", Some(error))
            .await
    }

    async fn update_obligation_state(
        &self,
        id: &str,
        state: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now();
        let result = sqlx::query(
            "UPDATE delivery_obligations
             SET state = ?, updated_at = ?, last_error = ?
             WHERE id = ?",
        )
        .bind(state)
        .bind(now)
        .bind(error)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(OmonError::Database(format!(
                "delivery obligation {id} not found"
            )));
        }
        Ok(())
    }

    /// Fetches a delivery obligation by ID.
    pub async fn get_obligation(&self, id: &str) -> Result<Option<DeliveryObligation>> {
        let row = sqlx::query_as::<_, DeliveryObligation>(
            "SELECT id, session_key, channel_id, thread_id, content, state,
                    attempts, created_at, updated_at, owner_pid, last_error
             FROM delivery_obligations
             WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Finds undelivered obligations owned by dead processes, increments attempts,
    /// re-stamps ownership to this process, and returns them for redelivery.
    ///
    /// Stale rows (older than `stale_after_secs`) and exhausted rows (`attempts >= max_attempts`)
    /// are transitioned to 'abandoned' and omitted from the return list.
    pub async fn sweep_recoverable(
        &self,
        max_attempts: i64,
        stale_after_secs: i64,
    ) -> Result<Vec<DeliveryObligation>> {
        let now = Utc::now();
        let current_pid = std::process::id() as i64;
        let rows = sqlx::query_as::<_, DeliveryObligation>(
            "SELECT id, session_key, channel_id, thread_id, content, state,
                    attempts, created_at, updated_at, owner_pid, last_error
             FROM delivery_obligations
             WHERE state IN ('pending', 'attempting', 'failed')
             ORDER BY created_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut claimed = Vec::new();
        for row in rows {
            if let Some(pid) = row.owner_pid {
                if pid > 0 && is_process_alive(pid as u32) {
                    continue; // A live gateway still owns this row
                }
            }

            let age_secs = now.signed_duration_since(row.created_at).num_seconds();
            if row.attempts >= max_attempts || age_secs > stale_after_secs {
                let _ = sqlx::query(
                    "UPDATE delivery_obligations SET state = 'abandoned', updated_at = ? WHERE id = ?",
                )
                .bind(now)
                .bind(&row.id)
                .execute(&self.pool)
                .await;
                continue;
            }

            let result = sqlx::query(
                "UPDATE delivery_obligations
                 SET owner_pid = ?, attempts = attempts + 1, updated_at = ?
                 WHERE id = ? AND (owner_pid IS NULL OR owner_pid = ? OR owner_pid = ?)",
            )
            .bind(current_pid)
            .bind(now)
            .bind(&row.id)
            .bind(row.owner_pid)
            .bind(current_pid)
            .execute(&self.pool)
            .await?;

            if result.rows_affected() > 0 {
                let mut updated_row = row;
                updated_row.attempts += 1;
                updated_row.owner_pid = Some(current_pid);
                claimed.push(updated_row);
            }
        }

        Ok(claimed)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;
    use chrono::Duration;

    fn test_session() -> SessionKey {
        SessionKey::new(
            "discord",
            Some("guild-1"),
            "chan-1",
            None::<String>,
            "user-1",
        )
    }

    #[tokio::test]
    async fn test_obligation_lifecycle_transitions() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let service = DeliveryLedgerService::new(db.pool().clone());
        let session = test_session();

        let obl_id = "test-obl-1";
        service
            .record_obligation(obl_id, &session, "Hello world response")
            .await
            .unwrap();

        let obl: DeliveryObligation = service.get_obligation(obl_id).await.unwrap().unwrap();
        assert_eq!(obl.id, obl_id);
        assert_eq!(obl.state, "pending");
        assert_eq!(obl.attempts, 0);
        assert_eq!(obl.content, "Hello world response");
        assert_eq!(obl.channel_id, "chan-1");
        assert_eq!(obl.last_error, None);

        // Transition to attempting
        service.mark_obligation_attempting(obl_id).await.unwrap();
        let obl: DeliveryObligation = service.get_obligation(obl_id).await.unwrap().unwrap();
        assert_eq!(obl.state, "attempting");

        // Transition to delivered
        service.mark_obligation_delivered(obl_id).await.unwrap();
        let obl: DeliveryObligation = service.get_obligation(obl_id).await.unwrap().unwrap();
        assert_eq!(obl.state, "delivered");

        // Transition to failed
        service
            .mark_obligation_failed(obl_id, "Network timeout 504")
            .await
            .unwrap();
        let obl: DeliveryObligation = service.get_obligation(obl_id).await.unwrap().unwrap();
        assert_eq!(obl.state, "failed");
        assert_eq!(obl.last_error.as_deref(), Some("Network timeout 504"));
    }

    #[tokio::test]
    async fn test_sweep_recoverable_selection_and_pruning() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let service = DeliveryLedgerService::new(db.pool().clone());
        let session = test_session();
        let current_pid = std::process::id() as i64;
        let dead_pid = 999_999_i64; // Guaranteed dead process pid
        let now = Utc::now();

        // 1. Pending obligation from dead process -> recoverable
        service
            .record_obligation("obl-pending-dead", &session, "content 1")
            .await
            .unwrap();
        sqlx::query("UPDATE delivery_obligations SET owner_pid = ? WHERE id = 'obl-pending-dead'")
            .bind(dead_pid)
            .execute(db.pool())
            .await
            .unwrap();

        // 2. Attempting obligation from dead process -> recoverable
        service
            .record_obligation("obl-attempting-dead", &session, "content 2")
            .await
            .unwrap();
        sqlx::query("UPDATE delivery_obligations SET state = 'attempting', owner_pid = ? WHERE id = 'obl-attempting-dead'")
            .bind(dead_pid)
            .execute(db.pool())
            .await
            .unwrap();

        // 3. Failed obligation from dead process -> recoverable
        service
            .record_obligation("obl-failed-dead", &session, "content 3")
            .await
            .unwrap();
        sqlx::query("UPDATE delivery_obligations SET state = 'failed', owner_pid = ? WHERE id = 'obl-failed-dead'")
            .bind(dead_pid)
            .execute(db.pool())
            .await
            .unwrap();

        // 4. Delivered obligation from dead process -> NOT recoverable
        service
            .record_obligation("obl-delivered-dead", &session, "content 4")
            .await
            .unwrap();
        sqlx::query("UPDATE delivery_obligations SET state = 'delivered', owner_pid = ? WHERE id = 'obl-delivered-dead'")
            .bind(dead_pid)
            .execute(db.pool())
            .await
            .unwrap();

        // 5. Pending obligation from CURRENT LIVE process -> NOT recoverable (live process owns it)
        service
            .record_obligation("obl-live-proc", &session, "content 5")
            .await
            .unwrap();
        sqlx::query("UPDATE delivery_obligations SET owner_pid = ? WHERE id = 'obl-live-proc'")
            .bind(current_pid)
            .execute(db.pool())
            .await
            .unwrap();

        // 6. Obligation with attempts >= 3 -> abandoned
        service
            .record_obligation("obl-max-attempts", &session, "content 6")
            .await
            .unwrap();
        sqlx::query("UPDATE delivery_obligations SET attempts = 3, owner_pid = ? WHERE id = 'obl-max-attempts'")
            .bind(dead_pid)
            .execute(db.pool())
            .await
            .unwrap();

        // 7. Obligation older than stale cutoff (2 days old) -> abandoned
        let stale_time = now - Duration::days(2);
        service
            .record_obligation("obl-stale", &session, "content 7")
            .await
            .unwrap();
        sqlx::query(
            "UPDATE delivery_obligations SET created_at = ?, owner_pid = ? WHERE id = 'obl-stale'",
        )
        .bind(stale_time)
        .bind(dead_pid)
        .execute(db.pool())
        .await
        .unwrap();

        let claimed: Vec<DeliveryObligation> = service.sweep_recoverable(3, 86400).await.unwrap();
        let claimed_ids: Vec<String> = claimed.into_iter().map(|o| o.id).collect();

        assert_eq!(claimed_ids.len(), 3);
        assert!(claimed_ids.contains(&"obl-pending-dead".to_string()));
        assert!(claimed_ids.contains(&"obl-attempting-dead".to_string()));
        assert!(claimed_ids.contains(&"obl-failed-dead".to_string()));

        // Verify that max-attempts and stale rows are now 'abandoned'
        let obl_max: DeliveryObligation = service
            .get_obligation("obl-max-attempts")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(obl_max.state, "abandoned");

        let obl_stale: DeliveryObligation =
            service.get_obligation("obl-stale").await.unwrap().unwrap();
        assert_eq!(obl_stale.state, "abandoned");

        // Verify claimed rows now have owner_pid set to current_pid and attempts incremented
        let obl_p: DeliveryObligation = service
            .get_obligation("obl-pending-dead")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(obl_p.owner_pid, Some(current_pid));
        assert_eq!(obl_p.attempts, 1);
    }
}
