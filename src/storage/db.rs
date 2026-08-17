use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::error::Result;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone, Debug)]
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    /// Opens a SQLite pool, configures durable file databases for WAL mode,
    /// and applies every embedded migration before returning.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let in_memory = database_url == "sqlite::memory:"
            || database_url.starts_with("sqlite::memory:?")
            || database_url.contains("mode=memory");

        let mut options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));

        if !in_memory {
            options = options
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
                .pragma("wal_autocheckpoint", "1000");
        }

        // SQLite permits only one writer at a time. A single warm connection
        // queues all access in sqlx instead of letting pooled write transactions
        // collide at SQLite and fail with SQLITE_BUSY. It also keeps a plain
        // `sqlite::memory:` database coherent because each connection would
        // otherwise receive a private database.
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .connect_with(options)
            .await?;

        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn close(self) {
        self.pool.close().await;
    }
}

/// Marks a session as having an in-flight or queued turn pending restart recovery.
pub async fn mark_session_resume_pending(pool: &SqlitePool, session_key: &str) -> Result<()> {
    sqlx::query(
        "UPDATE sessions SET resume_pending = 1, updated_at = (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) WHERE session_key = ?",
    )
    .bind(session_key)
    .execute(pool)
    .await?;
    Ok(())
}

/// Clears the resume_pending marker for a session. Returns `true` if the flag was set.
pub async fn clear_session_resume_pending(pool: &SqlitePool, session_key: &str) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE sessions SET resume_pending = 0, updated_at = (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) WHERE session_key = ? AND resume_pending = 1",
    )
    .bind(session_key)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Marks a session as suspended (or unsuspended) in SQLite state_json.
pub async fn mark_session_suspended(
    pool: &SqlitePool,
    session_key: &str,
    suspended: bool,
) -> Result<()> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT state_json FROM sessions WHERE session_key = ?")
            .bind(session_key)
            .fetch_optional(pool)
            .await?;
    if let Some((state_json,)) = row {
        let mut state: crate::SessionState = serde_json::from_str(&state_json).unwrap_or_default();
        state.suspended = suspended;
        let new_json = serde_json::to_string(&state).unwrap_or_else(|_| "{}".to_string());
        sqlx::query(
            "UPDATE sessions SET state_json = ?, updated_at = (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) WHERE session_key = ?",
        )
        .bind(new_json)
        .bind(session_key)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Checks if a session is currently marked suspended.
pub async fn is_session_suspended(pool: &SqlitePool, session_key: &str) -> Result<bool> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT state_json FROM sessions WHERE session_key = ?")
            .bind(session_key)
            .fetch_optional(pool)
            .await?;
    if let Some((state_json,)) = row {
        let state: crate::SessionState = serde_json::from_str(&state_json).unwrap_or_default();
        return Ok(state.suspended);
    }
    Ok(false)
}

/// Counts the number of sessions currently marked resume_pending.
pub async fn count_resume_pending_sessions(pool: &SqlitePool) -> Result<i64> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sessions WHERE resume_pending = 1")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

#[derive(sqlx::FromRow)]
struct ResumePendingSessionRow {
    #[allow(dead_code)]
    session_key: String,
    platform: String,
    guild_id: Option<String>,
    channel_id: String,
    thread_id: Option<String>,
    user_id: String,
}

/// Queries all session keys that currently have resume_pending = 1.
pub async fn fetch_resume_pending_session_keys(
    pool: &SqlitePool,
) -> Result<Vec<crate::SessionKey>> {
    let rows: Vec<ResumePendingSessionRow> = sqlx::query_as(
        "SELECT session_key, platform, guild_id, channel_id, thread_id, user_id FROM sessions WHERE resume_pending = 1 ORDER BY updated_at ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            crate::SessionKey::new(
                row.platform,
                row.guild_id,
                row.channel_id,
                row.thread_id,
                row.user_id,
            )
        })
        .collect())
}

#[derive(Clone, Debug)]
pub struct UnfinishedTurn {
    pub message_id: String,
    pub content: String,
    pub metadata_json: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub platform_message_id: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LastMessageRow {
    id: String,
    role: String,
    content: String,
    metadata_json: String,
    created_at: chrono::DateTime<chrono::Utc>,
    platform_message_id: Option<String>,
}

/// Finds the last unfinished user turn for a session if the most recent transcript row is a user message.
pub async fn find_last_unfinished_user_turn(
    pool: &SqlitePool,
    session_key: &str,
) -> Result<Option<UnfinishedTurn>> {
    let row: Option<LastMessageRow> = sqlx::query_as(
        "SELECT id, role, content, metadata_json, created_at, platform_message_id
         FROM messages
         WHERE session_key = ?
         ORDER BY sequence DESC
         LIMIT 1",
    )
    .bind(session_key)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = row {
        if row.role == "user" {
            return Ok(Some(UnfinishedTurn {
                message_id: row.id,
                content: row.content,
                metadata_json: row.metadata_json,
                created_at: row.created_at,
                platform_message_id: row.platform_message_id,
            }));
        }
    }
    Ok(None)
}

/// Checks if a message with the given `platform_message_id` already exists in the transcript for `session_key`.
pub async fn has_platform_message_id(
    pool: &SqlitePool,
    session_key: &str,
    platform_message_id: &str,
) -> Result<bool> {
    let exists: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM messages WHERE session_key = ? AND platform_message_id = ? LIMIT 1",
    )
    .bind(session_key)
    .bind(platform_message_id)
    .fetch_optional(pool)
    .await?;
    Ok(exists.is_some())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct PendingWrite {
    pub id: String,
    pub kind: String,
    pub payload: String,
    pub created_at: DateTime<Utc>,
}

pub fn write_approval_enabled() -> bool {
    std::env::var("WRITE_APPROVAL")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

pub async fn stage_pending_write(pool: &SqlitePool, kind: &str, payload: &str) -> Result<String> {
    let raw_uuid = Uuid::new_v4().to_string();
    let short_id = raw_uuid.replace('-', "")[..8].to_string();
    let now = Utc::now();
    sqlx::query("INSERT INTO pending_writes (id, kind, payload, created_at) VALUES (?, ?, ?, ?)")
        .bind(&short_id)
        .bind(kind)
        .bind(payload)
        .bind(now)
        .execute(pool)
        .await?;
    Ok(short_id)
}

pub async fn list_pending_writes(
    pool: &SqlitePool,
    kind: Option<&str>,
) -> Result<Vec<PendingWrite>> {
    if let Some(kind) = kind {
        let rows = sqlx::query_as::<_, PendingWrite>(
            "SELECT id, kind, payload, created_at FROM pending_writes WHERE kind = ? ORDER BY created_at ASC",
        )
        .bind(kind)
        .fetch_all(pool)
        .await?;
        Ok(rows)
    } else {
        let rows = sqlx::query_as::<_, PendingWrite>(
            "SELECT id, kind, payload, created_at FROM pending_writes ORDER BY created_at ASC",
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }
}

pub async fn get_pending_write(pool: &SqlitePool, id: &str) -> Result<Option<PendingWrite>> {
    let row = sqlx::query_as::<_, PendingWrite>(
        "SELECT id, kind, payload, created_at FROM pending_writes WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub async fn delete_pending_write(pool: &SqlitePool, id: &str) -> Result<bool> {
    let res = sqlx::query("DELETE FROM pending_writes WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn approve_pending_write(
    pool: &SqlitePool,
    id: &str,
    target_skills_dir: Option<&Path>,
) -> Result<Option<String>> {
    let Some(item) = get_pending_write(pool, id).await? else {
        return Ok(None);
    };

    match item.kind.as_str() {
        "memory" => {
            let val: serde_json::Value = serde_json::from_str(&item.payload)
                .map_err(|e| crate::OmonError::Database(format!("invalid memory payload: {e}")))?;
            let session_key_str = val
                .get("session_key")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let content = val.get("content").and_then(|v| v.as_str()).unwrap_or("");
            let metadata = val
                .get("metadata")
                .cloned()
                .unwrap_or(serde_json::json!({}));
            let memory_id = Uuid::new_v4().to_string();
            let metadata_json = serde_json::to_string(&metadata)
                .map_err(|e| crate::OmonError::Database(e.to_string()))?;

            // Ensure dummy session row exists for foreign key constraints if needed
            let _ = sqlx::query(
                "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json) VALUES (?, 'discord', 'unknown', 'unknown', '{}') ON CONFLICT(session_key) DO NOTHING"
            )
            .bind(session_key_str)
            .execute(pool)
            .await;

            sqlx::query(
                "INSERT INTO memories (id, session_key, content, metadata_json) VALUES (?, ?, ?, ?)",
            )
            .bind(&memory_id)
            .bind(session_key_str)
            .bind(content)
            .bind(metadata_json)
            .execute(pool)
            .await?;

            delete_pending_write(pool, id).await?;
            Ok(Some(format!("Approved memory write [{id}]: \"{content}\"")))
        }
        "skill" => {
            let val: serde_json::Value = serde_json::from_str(&item.payload)
                .map_err(|e| crate::OmonError::Database(format!("invalid skill payload: {e}")))?;
            let name = val.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let content = val.get("content").and_then(|v| v.as_str()).unwrap_or("");
            if name.is_empty() {
                return Err(crate::OmonError::ToolExecution(
                    "empty skill name in payload".into(),
                ));
            }

            let base_dir = if let Some(dir) = target_skills_dir {
                dir.to_path_buf()
            } else if let Ok(home) = std::env::var("HOME") {
                PathBuf::from(&home).join(".omon").join("skills")
            } else {
                PathBuf::from(".omon").join("skills")
            };

            let skill_dir = base_dir.join(name);
            std::fs::create_dir_all(&skill_dir).map_err(|e| {
                crate::OmonError::ToolExecution(format!("failed to create skill dir: {e}"))
            })?;
            let skill_file = skill_dir.join("SKILL.md");
            std::fs::write(&skill_file, content).map_err(|e| {
                crate::OmonError::ToolExecution(format!("failed to write SKILL.md: {e}"))
            })?;

            delete_pending_write(pool, id).await?;
            Ok(Some(format!(
                "Approved skill write [{id}]: skill '{name}' written to {}",
                skill_file.display()
            )))
        }
        _ => {
            delete_pending_write(pool, id).await?;
            Ok(Some(format!(
                "Discarded unknown pending write kind: {}",
                item.kind
            )))
        }
    }
}

pub async fn reject_pending_write(pool: &SqlitePool, id: &str) -> Result<bool> {
    delete_pending_write(pool, id).await
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use sqlx::Row;
    use tokio::sync::Barrier;

    use super::Database;

    #[tokio::test]
    async fn applies_all_migrations_to_an_in_memory_database() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )
        .fetch_all(database.pool())
        .await
        .expect("schema should be queryable");
        let tables: HashSet<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();

        for expected in [
            "sessions",
            "messages",
            "delivery_ledger",
            "cron_jobs",
            "memories",
            "cron_runs",
            "delivery_obligations",
            "approval_allowlist",
            "pending_writes",
        ] {
            assert!(tables.contains(expected), "missing table {expected}");
        }
        assert!(tables.contains("_sqlx_migrations"));
    }

    #[tokio::test]
    async fn migration_creates_pending_writes_table_and_indexes() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'pending_writes'",
        )
        .fetch_all(database.pool())
        .await
        .expect("indexes should be queryable");

        let indexes: HashSet<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();

        assert!(indexes.contains("idx_pending_writes_kind_created"));
    }

    #[tokio::test]
    async fn test_pending_writes_store_round_trip() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");
        let pool = database.pool();

        // 1. Stage memory write
        let mem_payload = serde_json::json!({
            "session_key": "sess:test",
            "content": "User prefers dark mode",
            "metadata": {"source": "test"}
        });
        let mem_id = super::stage_pending_write(pool, "memory", &mem_payload.to_string())
            .await
            .unwrap();

        // List pending writes
        let pending = super::list_pending_writes(pool, Some("memory"))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, mem_id);
        assert_eq!(pending[0].kind, "memory");

        // Approve memory write
        let approve_res = super::approve_pending_write(pool, &mem_id, None)
            .await
            .unwrap();
        assert!(approve_res.is_some());
        assert!(approve_res.unwrap().contains("User prefers dark mode"));

        // Verify removed from pending
        let pending_after = super::list_pending_writes(pool, Some("memory"))
            .await
            .unwrap();
        assert!(pending_after.is_empty());

        // Verify inserted into memories
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM memories WHERE session_key = 'sess:test'")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(count, 1);

        // 2. Stage and Reject memory write
        let mem_id2 = super::stage_pending_write(pool, "memory", &mem_payload.to_string())
            .await
            .unwrap();
        let rejected = super::reject_pending_write(pool, &mem_id2).await.unwrap();
        assert!(rejected);
        let pending_after2 = super::list_pending_writes(pool, Some("memory"))
            .await
            .unwrap();
        assert!(pending_after2.is_empty());

        // 3. Stage and Approve skill write
        let skill_dir = tempfile::tempdir().unwrap();
        let skill_payload = serde_json::json!({
            "name": "super-agent",
            "content": "# Super Agent Skill\n\nInstructions here."
        });
        let skill_id = super::stage_pending_write(pool, "skill", &skill_payload.to_string())
            .await
            .unwrap();

        let pending_skills = super::list_pending_writes(pool, Some("skill"))
            .await
            .unwrap();
        assert_eq!(pending_skills.len(), 1);
        assert_eq!(pending_skills[0].id, skill_id);

        let approve_skill = super::approve_pending_write(pool, &skill_id, Some(skill_dir.path()))
            .await
            .unwrap();
        assert!(approve_skill.is_some());
        assert!(skill_dir
            .path()
            .join("super-agent")
            .join("SKILL.md")
            .exists());
        let written_content =
            std::fs::read_to_string(skill_dir.path().join("super-agent").join("SKILL.md")).unwrap();
        assert_eq!(written_content, "# Super Agent Skill\n\nInstructions here.");

        // 4. Stage and Reject skill write
        let skill_id2 = super::stage_pending_write(pool, "skill", &skill_payload.to_string())
            .await
            .unwrap();
        let rejected_skill = super::reject_pending_write(pool, &skill_id2).await.unwrap();
        assert!(rejected_skill);
        let pending_skills2 = super::list_pending_writes(pool, Some("skill"))
            .await
            .unwrap();
        assert!(pending_skills2.is_empty());
    }

    #[tokio::test]
    async fn migration_creates_cron_runs_indexes() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'cron_runs'",
        )
        .fetch_all(database.pool())
        .await
        .expect("indexes should be queryable");

        let indexes: HashSet<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();

        assert!(indexes.contains("idx_cron_runs_job_status"));
        assert!(indexes.contains("idx_cron_runs_job_attempt"));
        assert!(indexes.contains("idx_cron_runs_job_started"));
        assert!(indexes.contains("idx_cron_runs_active_lease"));
    }

    #[tokio::test]
    async fn migration_creates_cron_runs_owner_pid_column() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let row = sqlx::query("SELECT owner_pid FROM cron_runs LIMIT 0")
            .fetch_optional(database.pool())
            .await;
        assert!(row.is_ok(), "owner_pid column should exist on cron_runs");
    }

    #[tokio::test]
    async fn file_database_serializes_concurrent_writers_without_lock_errors() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("writers.db");
        let database = Database::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("file database should initialize");
        assert_eq!(database.pool().options().get_max_connections(), 1);
        assert_eq!(database.pool().options().get_min_connections(), 1);

        sqlx::query("CREATE TABLE concurrent_writes (value INTEGER NOT NULL)")
            .execute(database.pool())
            .await
            .expect("test table should be created");

        // The former 10-connection file pool made this workload flaky because
        // independently acquired writers could collide and return SQLITE_BUSY.
        const WRITERS: usize = 32;
        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut tasks = Vec::with_capacity(WRITERS);
        for value in 0..WRITERS {
            let pool = database.pool().clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                sqlx::query("INSERT INTO concurrent_writes (value) VALUES (?)")
                    .bind(value as i64)
                    .execute(&pool)
                    .await
            }));
        }

        for task in tasks {
            task.await
                .expect("writer task should not panic")
                .expect("serialized writer should not return SQLITE_BUSY");
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM concurrent_writes")
            .fetch_one(database.pool())
            .await
            .expect("written rows should be queryable");
        assert_eq!(count, WRITERS as i64);

        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(database.pool())
            .await
            .expect("journal mode should be queryable");
        let autocheckpoint: i64 = sqlx::query_scalar("PRAGMA wal_autocheckpoint")
            .fetch_one(database.pool())
            .await
            .expect("WAL autocheckpoint should be queryable");
        assert_eq!(journal_mode, "wal");
        assert_eq!(autocheckpoint, 1000);
    }

    #[tokio::test]
    async fn enforces_foreign_keys_after_migration() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let result = sqlx::query(
            "INSERT INTO messages (id, session_key, role, content) VALUES (?, ?, ?, ?)",
        )
        .bind("message-1")
        .bind("missing-session")
        .bind("user")
        .bind("hello")
        .execute(database.pool())
        .await;

        assert!(result.is_err(), "orphan messages must be rejected");
    }

    #[tokio::test]
    async fn migrations_are_idempotent_for_repeated_connections() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let count_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(database.pool())
            .await
            .expect("migration ledger should be queryable");

        super::MIGRATOR
            .run(database.pool())
            .await
            .expect("reapplying migrations should be safe");

        let count_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(database.pool())
            .await
            .expect("migration ledger should be queryable");
        assert_eq!(count_after, count_before);
    }

    #[tokio::test]
    async fn message_sequence_preserves_causality_and_recent_history_window() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");
        sqlx::query(
            "INSERT INTO sessions (
                session_key, platform, channel_id, user_id, state_json
             ) VALUES ('session-order', 'test', 'channel', 'user', '{}')",
        )
        .execute(database.pool())
        .await
        .unwrap();

        // Give every row the same timestamp and IDs whose lexical order is the
        // reverse of insertion order. Neither field may be used as causality.
        for index in 0..105 {
            sqlx::query(
                "INSERT INTO messages (id, session_key, role, content, created_at)
                 VALUES (?, 'session-order', 'user', ?, '2026-08-15T00:00:00.000Z')",
            )
            .bind(format!("message-{:03}", 104 - index))
            .bind(index.to_string())
            .execute(database.pool())
            .await
            .unwrap();
        }

        let sequence: Vec<(i64, String)> = sqlx::query_as(
            "SELECT sequence, content FROM messages
             WHERE session_key = 'session-order' ORDER BY sequence",
        )
        .fetch_all(database.pool())
        .await
        .unwrap();
        assert_eq!(sequence.first(), Some(&(1, "0".into())));
        assert_eq!(sequence.last(), Some(&(105, "104".into())));

        let recent: Vec<(String,)> = sqlx::query_as(
            "SELECT content FROM (
                SELECT sequence, content FROM messages
                WHERE session_key = 'session-order'
                ORDER BY sequence DESC LIMIT 100
             ) ORDER BY sequence ASC",
        )
        .fetch_all(database.pool())
        .await
        .unwrap();
        assert_eq!(recent.len(), 100);
        assert_eq!(recent.first().map(|row| row.0.as_str()), Some("5"));
        assert_eq!(recent.last().map(|row| row.0.as_str()), Some("104"));
    }

    #[tokio::test]
    async fn migration_creates_discord_channel_cursors_table() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let row = sqlx::query(
            "SELECT channel_id, last_message_id, updated_at FROM discord_channel_cursors LIMIT 0",
        )
        .fetch_optional(database.pool())
        .await;
        assert!(
            row.is_ok(),
            "discord_channel_cursors table should exist with correct columns"
        );
    }

    #[tokio::test]
    async fn migration_creates_pairing_tables_and_indexes() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let row_codes = sqlx::query(
            "SELECT code, user_id, created_at, expires_at, attempts FROM pairing_codes LIMIT 0",
        )
        .fetch_optional(database.pool())
        .await;
        assert!(
            row_codes.is_ok(),
            "pairing_codes table should exist with correct columns"
        );

        let row_paired = sqlx::query("SELECT user_id, paired_at FROM paired_users LIMIT 0")
            .fetch_optional(database.pool())
            .await;
        assert!(
            row_paired.is_ok(),
            "paired_users table should exist with correct columns"
        );

        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'pairing_codes'",
        )
        .fetch_all(database.pool())
        .await
        .expect("indexes should be queryable");

        let indexes: HashSet<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();

        assert!(indexes.contains("idx_pairing_codes_user_id"));
    }

    #[tokio::test]
    async fn migration_creates_resume_pending_column_and_index() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let row = sqlx::query("SELECT resume_pending FROM sessions LIMIT 0")
            .fetch_optional(database.pool())
            .await;
        assert!(row.is_ok(), "resume_pending column should exist");

        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'sessions'",
        )
        .fetch_all(database.pool())
        .await
        .expect("indexes should be queryable");

        let indexes: HashSet<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();

        assert!(indexes.contains("idx_sessions_resume_pending"));
    }

    #[tokio::test]
    async fn test_resume_pending_flag_lifecycle_and_queries() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");
        let pool = database.pool();

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json)
             VALUES ('sess-1', 'discord', 'c1', 'u1', '{}'),
                    ('sess-2', 'discord', 'c2', 'u2', '{}')",
        )
        .execute(pool)
        .await
        .unwrap();

        // Initially none are resume_pending
        let pending = super::fetch_resume_pending_session_keys(pool)
            .await
            .unwrap();
        assert!(pending.is_empty());
        assert_eq!(super::count_resume_pending_sessions(pool).await.unwrap(), 0);

        // Mark sess-1 as resume_pending
        super::mark_session_resume_pending(pool, "sess-1")
            .await
            .unwrap();
        let pending = super::fetch_resume_pending_session_keys(pool)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].channel_id, "c1");
        assert_eq!(super::count_resume_pending_sessions(pool).await.unwrap(), 1);

        // Clear sess-1
        let cleared = super::clear_session_resume_pending(pool, "sess-1")
            .await
            .unwrap();
        assert!(cleared);
        let cleared_again = super::clear_session_resume_pending(pool, "sess-1")
            .await
            .unwrap();
        assert!(!cleared_again); // already cleared
        assert_eq!(super::count_resume_pending_sessions(pool).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_find_last_unfinished_user_turn() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");
        let pool = database.pool();

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json)
             VALUES ('sess-turn', 'discord', 'c1', 'u1', '{}')",
        )
        .execute(pool)
        .await
        .unwrap();

        // No messages -> None
        let turn = super::find_last_unfinished_user_turn(pool, "sess-turn")
            .await
            .unwrap();
        assert!(turn.is_none());

        // User message -> Some
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json)
             VALUES ('msg-u1', 'sess-turn', 'user', 'hello agent', '[]')",
        )
        .execute(pool)
        .await
        .unwrap();

        let turn = super::find_last_unfinished_user_turn(pool, "sess-turn")
            .await
            .unwrap()
            .expect("should find turn");
        assert_eq!(turn.content, "hello agent");
        assert_eq!(turn.message_id, "msg-u1");

        // Assistant message completes it -> None
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json)
             VALUES ('msg-a1', 'sess-turn', 'assistant', 'hello user', '{}')",
        )
        .execute(pool)
        .await
        .unwrap();

        let turn = super::find_last_unfinished_user_turn(pool, "sess-turn")
            .await
            .unwrap();
        assert!(turn.is_none());
    }

    #[tokio::test]
    async fn migration_creates_messages_platform_id_column_and_index() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");

        let row = sqlx::query("SELECT platform_message_id FROM messages LIMIT 0")
            .fetch_optional(database.pool())
            .await;
        assert!(row.is_ok(), "platform_message_id column should exist");

        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'messages'",
        )
        .fetch_all(database.pool())
        .await
        .expect("indexes should be queryable");

        let indexes: HashSet<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();

        assert!(indexes.contains("idx_messages_session_platform_id"));
    }

    #[tokio::test]
    async fn test_has_platform_message_id_dedup_query() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("database should initialize");
        let pool = database.pool();

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json)
             VALUES ('sess-dedup', 'discord', 'c1', 'u1', '{}')",
        )
        .execute(pool)
        .await
        .unwrap();

        // Empty transcript -> false
        assert!(
            !super::has_platform_message_id(pool, "sess-dedup", "plat-msg-123")
                .await
                .unwrap()
        );

        // Insert message with platform_message_id
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, platform_message_id)
             VALUES ('m-dedup-1', 'sess-dedup', 'user', 'hello', 'plat-msg-123')",
        )
        .execute(pool)
        .await
        .unwrap();

        // Existing platform_message_id on same session -> true
        assert!(
            super::has_platform_message_id(pool, "sess-dedup", "plat-msg-123")
                .await
                .unwrap()
        );

        // Different platform_message_id -> false
        assert!(
            !super::has_platform_message_id(pool, "sess-dedup", "plat-msg-456")
                .await
                .unwrap()
        );

        // Same platform_message_id on different session -> false
        assert!(
            !super::has_platform_message_id(pool, "sess-other", "plat-msg-123")
                .await
                .unwrap()
        );
    }
}
