use serde_json::json;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::error::Result;

/// Appends an out-of-band delivery record to the target session transcript with role preservation.
///
/// `role` should typically be "assistant" for agent out-of-band responses,
/// or "user" for system/external notifications (to maintain LLM provider alternation).
pub async fn mirror_to_session(
    pool: &SqlitePool,
    session_key: &str,
    role: &str,
    content: &str,
    source_label: Option<&str>,
) -> Result<bool> {
    let text = content.trim();
    if text.is_empty() {
        return Ok(false);
    }

    let exists: Option<(String,)> =
        sqlx::query_as("SELECT session_key FROM sessions WHERE session_key = ? LIMIT 1")
            .bind(session_key)
            .fetch_optional(pool)
            .await?;

    let Some((target_session_key,)) = exists else {
        return Ok(false);
    };

    let message_id = Uuid::new_v4().to_string();
    let metadata = json!({
        "mirror": true,
        "mirror_source": source_label.unwrap_or("out_of_band"),
    });

    sqlx::query(
        "INSERT INTO messages (id, session_key, role, content, metadata_json, created_at)
         VALUES (?, ?, ?, ?, ?, (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))",
    )
    .bind(&message_id)
    .bind(&target_session_key)
    .bind(role)
    .bind(text)
    .bind(metadata.to_string())
    .execute(pool)
    .await?;

    let _ = sqlx::query(
        "UPDATE sessions SET updated_at = (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) WHERE session_key = ?",
    )
    .bind(&target_session_key)
    .execute(pool)
    .await;

    Ok(true)
}

/// Finds the most relevant active session key for a platform and origin coordinates.
pub async fn find_session_by_origin(
    pool: &SqlitePool,
    platform: &str,
    chat_id: &str,
    thread_id: Option<&str>,
    user_id: Option<&str>,
) -> Result<Option<String>> {
    let platform = platform.to_ascii_lowercase();

    // 1. Thread-specific match if thread_id is given
    if let Some(tid) = thread_id {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT session_key FROM sessions
             WHERE lower(platform) = ? AND channel_id = ? AND thread_id = ?
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(&platform)
        .bind(chat_id)
        .bind(tid)
        .fetch_optional(pool)
        .await?;

        if let Some((k,)) = row {
            return Ok(Some(k));
        }
    }

    // 2. User-specific match in channel if user_id is given
    if let Some(uid) = user_id {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT session_key FROM sessions
             WHERE lower(platform) = ? AND channel_id = ? AND user_id = ?
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(&platform)
        .bind(chat_id)
        .bind(uid)
        .fetch_optional(pool)
        .await?;

        if let Some((k,)) = row {
            return Ok(Some(k));
        }
    }

    // 3. Fallback to most recently active session for this platform and channel
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT session_key FROM sessions
         WHERE lower(platform) = ? AND channel_id = ?
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(&platform)
    .bind(chat_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(k,)| k))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mirror_to_session_role_preservation() {
        let pool = crate::storage::init_pool("sqlite::memory:").await.unwrap();

        let session_key = "test-session-key";
        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json)
             VALUES (?, 'discord', 'c1', 'u1', '{}')",
        )
        .bind(session_key)
        .execute(&pool)
        .await
        .unwrap();

        // 1. Mirror assistant role
        let mirrored = mirror_to_session(
            &pool,
            session_key,
            "assistant",
            "Job completed: ok",
            Some("cron"),
        )
        .await
        .unwrap();
        assert!(mirrored);

        // 2. Mirror user role
        let mirrored_user = mirror_to_session(
            &pool,
            session_key,
            "user",
            "System alert: high memory",
            Some("system"),
        )
        .await
        .unwrap();
        assert!(mirrored_user);

        // 3. Empty content should return false
        let mirrored_empty = mirror_to_session(&pool, session_key, "assistant", "   ", None)
            .await
            .unwrap();
        assert!(!mirrored_empty);

        // 4. Non-existent session should return false
        let mirrored_missing =
            mirror_to_session(&pool, "non-existent-key", "assistant", "content", None)
                .await
                .unwrap();
        assert!(!mirrored_missing);

        // Verify rows in messages table
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT role, content, metadata_json FROM messages WHERE session_key = ? ORDER BY sequence ASC",
        )
        .bind(session_key)
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "assistant");
        assert_eq!(rows[0].1, "Job completed: ok");
        assert!(rows[0].2.contains("\"mirror\":true"));
        assert!(rows[0].2.contains("\"mirror_source\":\"cron\""));

        assert_eq!(rows[1].0, "user");
        assert_eq!(rows[1].1, "System alert: high memory");
        assert!(rows[1].2.contains("\"mirror_source\":\"system\""));
    }

    #[tokio::test]
    async fn test_find_session_by_origin_precedence() {
        let pool = crate::storage::init_pool("sqlite::memory:").await.unwrap();

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, thread_id, user_id, state_json)
             VALUES ('sess-chan', 'discord', 'c1', NULL, 'u1', '{}'),
                    ('sess-thread', 'discord', 'c1', 't1', 'u1', '{}'),
                    ('sess-user2', 'discord', 'c1', NULL, 'u2', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();

        // 1. Finding with thread_id returns thread session
        let found = find_session_by_origin(&pool, "discord", "c1", Some("t1"), None)
            .await
            .unwrap();
        assert_eq!(found, Some("sess-thread".to_string()));

        // 2. Finding with user_id returns user-specific session
        let found_user = find_session_by_origin(&pool, "discord", "c1", None, Some("u2"))
            .await
            .unwrap();
        assert_eq!(found_user, Some("sess-user2".to_string()));

        // 3. Finding with channel only returns a matching channel session
        let found_chan = find_session_by_origin(&pool, "discord", "c1", None, None)
            .await
            .unwrap();
        assert!(found_chan.is_some());
    }
}
