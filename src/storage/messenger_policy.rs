use sqlx::SqlitePool;

use crate::{MessageContextPolicyMatrix, OmonError, Result};

#[derive(Clone)]
pub struct MessengerPolicyStore {
    pool: SqlitePool,
}

impl MessengerPolicyStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn get_override(&self, platform: &str) -> Result<Option<MessageContextPolicyMatrix>> {
        let raw = sqlx::query_scalar::<_, String>(
            "SELECT policy_json FROM messenger_policy_overrides WHERE platform = ?",
        )
        .bind(platform)
        .fetch_optional(&self.pool)
        .await?;
        raw.map(|value| {
            serde_json::from_str::<MessageContextPolicyMatrix>(&value)
                .map(MessageContextPolicyMatrix::normalized)
                .map_err(|error| {
                    OmonError::Database(format!("invalid messenger policy JSON: {error}"))
                })
        })
        .transpose()
    }

    pub async fn effective(
        &self,
        platform: &str,
        base: &MessageContextPolicyMatrix,
    ) -> Result<MessageContextPolicyMatrix> {
        Ok(self
            .get_override(platform)
            .await?
            .unwrap_or_else(|| base.clone())
            .normalized())
    }

    pub async fn set_override(
        &self,
        platform: &str,
        policy: &MessageContextPolicyMatrix,
    ) -> Result<MessageContextPolicyMatrix> {
        let normalized = policy.clone().normalized();
        let payload = serde_json::to_string(&normalized)
            .map_err(|error| OmonError::Database(error.to_string()))?;
        sqlx::query(
            "INSERT INTO messenger_policy_overrides (platform, policy_json, updated_at)
             VALUES (?, ?, CURRENT_TIMESTAMP)
             ON CONFLICT(platform) DO UPDATE SET
                policy_json = excluded.policy_json,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(platform)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(normalized)
    }

    pub async fn clear_override(&self, platform: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM messenger_policy_overrides WHERE platform = ?")
            .bind(platform)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[tokio::test]
    async fn override_round_trip_and_clear() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let store = MessengerPolicyStore::new(db.pool().clone());
        let base = MessageContextPolicyMatrix::default();
        assert_eq!(store.effective("discord", &base).await.unwrap(), base);

        let mut override_policy = base.clone();
        override_policy.allow_dm_reads = false;
        override_policy.limits.search_scan = 9999;
        let saved = store
            .set_override("discord", &override_policy)
            .await
            .unwrap();
        assert!(!saved.allow_dm_reads);
        assert_eq!(saved.limits.search_scan, 2_000);
        assert_eq!(store.effective("discord", &base).await.unwrap(), saved);
        assert!(store.clear_override("discord").await.unwrap());
        assert_eq!(store.effective("discord", &base).await.unwrap(), base);
    }
}
