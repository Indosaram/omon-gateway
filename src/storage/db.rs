use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;

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
                .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);
        }

        // A plain `sqlite::memory:` database is private to each connection.
        // Keeping its pool at one connection gives tests and callers one
        // coherent database while preserving normal pooling for file URLs.
        let max_connections = if in_memory { 1 } else { 10 };
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use sqlx::Row;

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
        ] {
            assert!(tables.contains(expected), "missing table {expected}");
        }
        assert!(tables.contains("_sqlx_migrations"));
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
}
