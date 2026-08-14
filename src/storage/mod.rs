mod db;

use sqlx::SqlitePool;

use crate::Result;

pub use db::Database;

pub async fn init_pool(database_url: &str) -> Result<SqlitePool> {
    Ok(Database::connect(database_url).await?.pool().clone())
}
