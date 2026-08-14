use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, SqlitePool};
use uuid::Uuid;

use crate::{OmonError, Result, SessionKey, SessionState};

#[derive(Clone, Debug, PartialEq)]
pub struct Memory {
    pub id: String,
    pub session_key: String,
    pub content: String,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub score: f64,
}

#[derive(Clone, Debug)]
pub struct MemoryStore {
    pool: SqlitePool,
}

impl MemoryStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn remember(
        &self,
        session: &SessionKey,
        content: impl Into<String>,
        metadata: Value,
    ) -> Result<Memory> {
        ensure_session(&self.pool, session).await?;
        let id = Uuid::new_v4().to_string();
        let content = content.into();
        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|error| OmonError::Database(error.to_string()))?;
        sqlx::query(
            "INSERT INTO memories (id, session_key, content, metadata_json) VALUES (?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(session.storage_key())
        .bind(&content)
        .bind(metadata_json)
        .execute(&self.pool)
        .await?;
        self.get(&id)
            .await?
            .ok_or_else(|| OmonError::Database("inserted memory could not be read".into()))
    }

    pub async fn get(&self, id: &str) -> Result<Option<Memory>> {
        let row = sqlx::query_as::<_, MemoryRow>(
            "SELECT id, session_key, content, metadata_json, created_at, updated_at
             FROM memories WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| row.into_memory(0.0)).transpose()
    }

    pub async fn delete(&self, id: &str) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM memories WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }

    pub async fn search(
        &self,
        session: &SessionKey,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Memory>> {
        if limit == 0 || query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, MemoryRow>(
            "SELECT id, session_key, content, metadata_json, created_at, updated_at
             FROM memories WHERE session_key = ? ORDER BY updated_at DESC",
        )
        .bind(session.storage_key())
        .fetch_all(&self.pool)
        .await?;
        let query_terms = term_counts(query);
        let query_set: HashSet<_> = query_terms.keys().cloned().collect();
        let query_lower = query.to_lowercase();
        let mut memories = rows
            .into_iter()
            .filter_map(|row| {
                let terms = term_counts(&row.content);
                let overlap = query_set
                    .iter()
                    .filter(|term| terms.contains_key(*term))
                    .count();
                let cosine = cosine_similarity(&query_terms, &terms);
                let phrase = row.content.to_lowercase().contains(&query_lower);
                let score = cosine + overlap as f64 * 0.25 + if phrase { 1.0 } else { 0.0 };
                (score > 0.0).then_some((row, score))
            })
            .collect::<Vec<_>>();
        memories.sort_by(|(left_row, left_score), (right_row, right_score)| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| right_row.updated_at.cmp(&left_row.updated_at))
        });
        memories
            .into_iter()
            .take(limit)
            .map(|(row, score)| row.into_memory(score))
            .collect()
    }

    pub async fn keyword_search(
        &self,
        session: &SessionKey,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Memory>> {
        self.search(session, query, limit).await
    }

    pub async fn similarity_search(
        &self,
        session: &SessionKey,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Memory>> {
        self.search(session, query, limit).await
    }
}

#[derive(FromRow)]
struct MemoryRow {
    id: String,
    session_key: String,
    content: String,
    metadata_json: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl MemoryRow {
    fn into_memory(self, score: f64) -> Result<Memory> {
        Ok(Memory {
            id: self.id,
            session_key: self.session_key,
            content: self.content,
            metadata: serde_json::from_str(&self.metadata_json)
                .map_err(|error| OmonError::Database(error.to_string()))?,
            created_at: self.created_at,
            updated_at: self.updated_at,
            score,
        })
    }
}

async fn ensure_session(pool: &SqlitePool, session: &SessionKey) -> Result<()> {
    sqlx::query(
        "INSERT INTO sessions (session_key, platform, guild_id, channel_id, thread_id, user_id, state_json)
         VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_key) DO NOTHING",
    )
    .bind(session.storage_key())
    .bind(&session.platform)
    .bind(&session.guild_id)
    .bind(&session.channel_id)
    .bind(&session.thread_id)
    .bind(&session.user_id)
    .bind(serde_json::to_string(&SessionState::default()).map_err(|error| OmonError::Database(error.to_string()))?)
    .execute(pool)
    .await?;
    Ok(())
}

fn term_counts(value: &str) -> HashMap<String, usize> {
    let mut terms = HashMap::new();
    for term in value
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
    {
        *terms.entry(term.to_lowercase()).or_default() += 1;
    }
    terms
}

fn cosine_similarity(left: &HashMap<String, usize>, right: &HashMap<String, usize>) -> f64 {
    let dot = left
        .iter()
        .map(|(term, count)| *count as f64 * right.get(term).copied().unwrap_or(0) as f64)
        .sum::<f64>();
    let left_norm = left
        .values()
        .map(|count| (*count as f64).powi(2))
        .sum::<f64>()
        .sqrt();
    let right_norm = right
        .values()
        .map(|count| (*count as f64).powi(2))
        .sum::<f64>()
        .sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot / (left_norm * right_norm)
    }
}
