use sqlx::SqlitePool;

use crate::Result;

/// Clears cached `omo_thread_id` bindings from all session records in SQLite.
///
/// When per-agent workspace isolation is enabled, existing sessions must not
/// reuse legacy global-workspace thread IDs. Wiping the metadata entry forces
/// the backend to start a fresh thread in the agent's dedicated workspace root.
///
/// # Errors
/// Returns [`crate::OmonError`] if executing the SQLite update statement fails.
pub async fn wipe_omo_thread_bindings(pool: &SqlitePool) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE sessions SET state_json = json_remove(state_json, '$.metadata.omo_thread_id') WHERE json_extract(state_json, '$.metadata.omo_thread_id') IS NOT NULL",
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::wipe_omo_thread_bindings;
    use crate::models::SessionState;
    use crate::storage::Database;
    use serde_json::json;
    use sqlx::Row;

    #[tokio::test]
    async fn wipe_omo_thread_bindings_clears_only_omo_thread_id_and_preserves_other_metadata() {
        // Given: an initialized in-memory database with sessions containing varied metadata
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");
        let pool = database.pool();

        let state_1 = json!({
            "active_model": "gpt-5",
            "system_prompt": "prompt 1",
            "metadata": {
                "omo_thread_id": "thread-111",
                "hermes_cron_job_id": "cron-job-aaa"
            }
        });
        let state_2 = json!({
            "metadata": {
                "omo_thread_id": "thread-222",
                "custom_key": "custom_value"
            }
        });
        let state_3 = json!({
            "metadata": {
                "hermes_cron_job_id": "cron-job-bbb"
            }
        });

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json)
             VALUES ('sess-1', 'discord', 'c1', 'u1', ?),
                    ('sess-2', 'discord', 'c2', 'u2', ?),
                    ('sess-3', 'discord', 'c3', 'u3', ?)",
        )
        .bind(serde_json::to_string(&state_1).unwrap())
        .bind(serde_json::to_string(&state_2).unwrap())
        .bind(serde_json::to_string(&state_3).unwrap())
        .execute(pool)
        .await
        .unwrap();

        // When: wipe_omo_thread_bindings is executed
        let wiped_count = wipe_omo_thread_bindings(pool)
            .await
            .expect("wipe should succeed");

        // Then: exactly 2 sessions had omo_thread_id stripped, and other metadata remains intact
        assert_eq!(wiped_count, 2);

        let row1 = sqlx::query("SELECT state_json FROM sessions WHERE session_key = 'sess-1'")
            .fetch_one(pool)
            .await
            .unwrap();
        let state_json_1: String = row1.get("state_json");
        let parsed_1: SessionState = serde_json::from_str(&state_json_1).unwrap();
        assert!(!parsed_1.metadata.contains_key("omo_thread_id"));
        assert_eq!(
            parsed_1.metadata.get("hermes_cron_job_id").unwrap(),
            "cron-job-aaa"
        );
        assert_eq!(parsed_1.active_model.as_deref(), Some("gpt-5"));
        assert_eq!(parsed_1.system_prompt.as_deref(), Some("prompt 1"));

        let row2 = sqlx::query("SELECT state_json FROM sessions WHERE session_key = 'sess-2'")
            .fetch_one(pool)
            .await
            .unwrap();
        let state_json_2: String = row2.get("state_json");
        let parsed_2: SessionState = serde_json::from_str(&state_json_2).unwrap();
        assert!(!parsed_2.metadata.contains_key("omo_thread_id"));
        assert_eq!(parsed_2.metadata.get("custom_key").unwrap(), "custom_value");

        let row3 = sqlx::query("SELECT state_json FROM sessions WHERE session_key = 'sess-3'")
            .fetch_one(pool)
            .await
            .unwrap();
        let state_json_3: String = row3.get("state_json");
        let parsed_3: SessionState = serde_json::from_str(&state_json_3).unwrap();
        assert!(!parsed_3.metadata.contains_key("omo_thread_id"));
        assert_eq!(
            parsed_3.metadata.get("hermes_cron_job_id").unwrap(),
            "cron-job-bbb"
        );

        // Subsequent wipe should be a no-op (idempotent)
        let second_wipe_count = wipe_omo_thread_bindings(pool)
            .await
            .expect("second wipe should succeed");
        assert_eq!(second_wipe_count, 0);
    }
}
