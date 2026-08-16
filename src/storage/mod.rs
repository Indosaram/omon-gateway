mod db;

use sqlx::SqlitePool;

use crate::Result;

pub use db::{
    clear_session_resume_pending, count_resume_pending_sessions, fetch_resume_pending_session_keys,
    find_last_unfinished_user_turn, mark_session_resume_pending, Database, UnfinishedTurn,
};

pub async fn init_pool(database_url: &str) -> Result<SqlitePool> {
    Ok(Database::connect(database_url).await?.pool().clone())
}
