mod db;
mod message_search;
mod messenger_policy;

use sqlx::SqlitePool;

use crate::Result;
pub use db::{
    approve_pending_write, clear_session_resume_pending, count_resume_pending_sessions,
    delete_pending_write, fetch_resume_pending_session_keys, find_last_unfinished_user_turn,
    get_pending_write, has_platform_message_id, is_session_suspended, list_pending_writes,
    mark_session_resume_pending, mark_session_suspended, reject_pending_write, stage_pending_write,
    write_approval_enabled, Database, PendingWrite, UnfinishedTurn,
};
pub use message_search::{MessageSearchDocument, MessageSearchHit, MessageSearchIndex};
pub use messenger_policy::MessengerPolicyStore;

pub async fn init_pool(database_url: &str) -> Result<SqlitePool> {
    Ok(Database::connect(database_url).await?.pool().clone())
}
