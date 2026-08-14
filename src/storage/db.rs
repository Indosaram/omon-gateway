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
}
