use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::RwLock;

use crate::error::{OmonError, Result};

pub const PAIRING_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
pub const CODE_LENGTH: usize = 8;
pub const CODE_TTL_SECONDS: i64 = 3600; // 1 hour
pub const RATE_LIMIT_SECONDS: i64 = 600; // 10 minutes
pub const MAX_FAILED_ATTEMPTS: i64 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PairingOutcome {
    Success { user_id: u64 },
    InvalidCode,
    Expired,
    LockedOut,
}

#[derive(sqlx::FromRow)]
struct PairingCodeRow {
    code: String,
    user_id: String,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    attempts: i64,
}

#[derive(Clone)]
pub struct PairingStore {
    pool: SqlitePool,
    paired_cache: Arc<RwLock<HashSet<u64>>>,
}

impl PairingStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            paired_cache: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Initializes the in-memory cache of paired users from SQLite.
    pub async fn init_cache(&self) -> Result<()> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT user_id FROM paired_users")
            .fetch_all(&self.pool)
            .await?;
        let mut set = self.paired_cache.write().await;
        set.clear();
        for (uid_str,) in rows {
            if let Ok(uid) = uid_str.parse::<u64>() {
                set.insert(uid);
            }
        }
        Ok(())
    }

    /// Fast in-memory check if a user is paired.
    pub async fn is_user_paired(&self, user_id: u64) -> bool {
        self.paired_cache.read().await.contains(&user_id)
    }

    /// Synchronous read check against the paired cache.
    pub fn is_user_paired_sync(&self, user_id: u64) -> bool {
        self.paired_cache
            .try_read()
            .map(|set| set.contains(&user_id))
            .unwrap_or(false)
    }

    /// Returns a list of all currently paired user IDs (async).
    pub async fn get_paired_user_ids(&self) -> Vec<u64> {
        self.paired_cache.read().await.iter().copied().collect()
    }

    /// Returns a list of all currently paired user IDs (sync/non-blocking).
    pub fn get_paired_user_ids_sync(&self) -> Vec<u64> {
        self.paired_cache
            .try_read()
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Normalizes code formatting (removes dashes/spaces, uppercase).
    pub fn normalize_code(raw: &str) -> String {
        raw.chars()
            .filter(|c| c.is_alphanumeric())
            .flat_map(char::to_uppercase)
            .collect()
    }

    /// Formats an 8-char code as `XXXX-XXXX`.
    pub fn format_code(raw: &str) -> String {
        let normalized = Self::normalize_code(raw);
        if normalized.len() == 8 {
            format!("{}-{}", &normalized[..4], &normalized[4..])
        } else {
            normalized
        }
    }

    /// Generates a secure random 8-character code from the unambiguous alphabet.
    pub fn generate_raw_code() -> String {
        use uuid::Uuid;
        let bytes = Uuid::new_v4().into_bytes();
        let mut code = String::with_capacity(CODE_LENGTH);
        for byte in bytes.iter().take(CODE_LENGTH) {
            let idx = (*byte as usize) % PAIRING_ALPHABET.len();
            code.push(PAIRING_ALPHABET[idx] as char);
        }
        code
    }

    /// Requests a one-time pairing code for an unauthorized user.
    /// Reuses an unexpired active code if within the rate-limit window.
    pub async fn request_pairing_code(&self, user_id: u64) -> Result<String> {
        let now = Utc::now();
        let user_id_str = user_id.to_string();

        // Check if there is already an active, unexpired code for this user
        let existing: Option<(String, DateTime<Utc>, DateTime<Utc>, i64)> = sqlx::query_as(
            "SELECT code, created_at, expires_at, attempts FROM pairing_codes WHERE user_id = ? ORDER BY created_at DESC LIMIT 1"
        )
        .bind(&user_id_str)
        .fetch_optional(&self.pool)
        .await?;

        if let Some((code, created_at, expires_at, attempts)) = existing {
            if attempts < MAX_FAILED_ATTEMPTS && expires_at > now {
                // If created recently, reuse it (rate limit compliance)
                if (now - created_at).num_seconds() < RATE_LIMIT_SECONDS {
                    return Ok(Self::format_code(&code));
                }
            }
        }

        // Delete any expired or previous codes for this user
        let _ = sqlx::query("DELETE FROM pairing_codes WHERE user_id = ?")
            .bind(&user_id_str)
            .execute(&self.pool)
            .await;

        let raw_code = Self::generate_raw_code();
        let expires_at = now + chrono::Duration::seconds(CODE_TTL_SECONDS);

        sqlx::query(
            "INSERT INTO pairing_codes (code, user_id, created_at, expires_at, attempts) VALUES (?, ?, ?, ?, 0)"
        )
        .bind(&raw_code)
        .bind(&user_id_str)
        .bind(now)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| OmonError::Database(format!("failed to store pairing code: {e}")))?;

        Ok(Self::format_code(&raw_code))
    }

    /// Approves a pairing code entered by an operator.
    pub async fn approve_code(
        &self,
        input_code: &str,
        _operator_id: u64,
    ) -> Result<PairingOutcome> {
        let normalized = Self::normalize_code(input_code);
        if normalized.is_empty() {
            return Ok(PairingOutcome::InvalidCode);
        }

        let now = Utc::now();
        let record: Option<PairingCodeRow> = sqlx::query_as(
            "SELECT code, user_id, created_at, expires_at, attempts FROM pairing_codes WHERE code = ?",
        )
        .bind(&normalized)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = record else {
            // Increment attempts on any partial matches or record attempt
            let _ =
                sqlx::query("UPDATE pairing_codes SET attempts = attempts + 1 WHERE attempts < ?")
                    .bind(MAX_FAILED_ATTEMPTS)
                    .execute(&self.pool)
                    .await;
            return Ok(PairingOutcome::InvalidCode);
        };

        if row.attempts >= MAX_FAILED_ATTEMPTS {
            return Ok(PairingOutcome::LockedOut);
        }

        if now > row.expires_at {
            // Delete expired code
            let _ = sqlx::query("DELETE FROM pairing_codes WHERE code = ?")
                .bind(&row.code)
                .execute(&self.pool)
                .await;
            return Ok(PairingOutcome::Expired);
        }

        let Ok(user_id) = row.user_id.parse::<u64>() else {
            return Ok(PairingOutcome::InvalidCode);
        };

        // Insert into paired_users
        sqlx::query(
            "INSERT INTO paired_users (user_id, paired_at) VALUES (?, ?) ON CONFLICT(user_id) DO NOTHING",
        )
        .bind(&row.user_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| OmonError::Database(format!("failed to record paired user: {e}")))?;

        // Remove the consumed code
        let _ = sqlx::query("DELETE FROM pairing_codes WHERE code = ?")
            .bind(&row.code)
            .execute(&self.pool)
            .await;

        // Update in-memory cache
        self.paired_cache.write().await.insert(user_id);

        Ok(PairingOutcome::Success { user_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_code_format_and_normalization() {
        let code = "ABCD2345";
        let formatted = PairingStore::format_code(code);
        assert_eq!(formatted, "ABCD-2345");

        let normalized = PairingStore::normalize_code("abcd-2345");
        assert_eq!(normalized, "ABCD2345");
    }

    #[test]
    fn test_generate_raw_code_alphabet_and_length() {
        for _ in 0..100 {
            let code = PairingStore::generate_raw_code();
            assert_eq!(code.len(), CODE_LENGTH);
            for byte in code.bytes() {
                assert!(PAIRING_ALPHABET.contains(&byte), "byte must be in alphabet");
                // Confirm no 0, O, 1, I
                assert_ne!(byte, b'0');
                assert_ne!(byte, b'O');
                assert_ne!(byte, b'1');
                assert_ne!(byte, b'I');
            }
        }
    }

    #[tokio::test]
    async fn test_pairing_lifecycle_and_approval() {
        let pool = crate::storage::init_pool("sqlite::memory:").await.unwrap();
        let store = PairingStore::new(pool);
        store.init_cache().await.unwrap();

        let user_id = 987654321_u64;
        assert!(!store.is_user_paired(user_id).await);

        // Request code
        let code = store.request_pairing_code(user_id).await.unwrap();
        assert_eq!(code.len(), 9); // XXXX-XXXX

        // Approve with valid code
        let outcome = store.approve_code(&code, 11111).await.unwrap();
        assert_eq!(outcome, PairingOutcome::Success { user_id });

        // Now user should be paired
        assert!(store.is_user_paired(user_id).await);
        assert!(store.is_user_paired_sync(user_id));

        // Approving again with same code should return InvalidCode (consumed)
        let second_try = store.approve_code(&code, 11111).await.unwrap();
        assert_eq!(second_try, PairingOutcome::InvalidCode);
    }

    #[tokio::test]
    async fn test_pairing_expiry_and_lockout() {
        let pool = crate::storage::init_pool("sqlite::memory:").await.unwrap();
        let store = PairingStore::new(pool.clone());
        store.init_cache().await.unwrap();

        // Test expired code
        let now = Utc::now();
        let expired_time = now - chrono::Duration::hours(2);
        sqlx::query(
            "INSERT INTO pairing_codes (code, user_id, created_at, expires_at, attempts) VALUES ('EXPIRED1', '123456789', ?, ?, 0)"
        )
        .bind(expired_time - chrono::Duration::hours(1))
        .bind(expired_time)
        .execute(&pool)
        .await
        .unwrap();

        let outcome = store.approve_code("EXPIRED1", 11111).await.unwrap();
        assert_eq!(outcome, PairingOutcome::Expired);

        // Test lockout after 5 attempts
        sqlx::query(
            "INSERT INTO pairing_codes (code, user_id, created_at, expires_at, attempts) VALUES ('LOCKOUT1', '123456789', ?, ?, 5)"
        )
        .bind(now)
        .bind(now + chrono::Duration::hours(1))
        .execute(&pool)
        .await
        .unwrap();

        let outcome = store.approve_code("LOCKOUT1", 11111).await.unwrap();
        assert_eq!(outcome, PairingOutcome::LockedOut);
    }
}
