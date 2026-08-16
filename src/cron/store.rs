use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;

use crate::{next_run, OmonError, Result};

const SOURCE_KEY: &str = "_omon_hermes_source";
pub const DEFAULT_CRON_RUNS_RETENTION_DAYS: i64 = 14;

pub fn cron_runs_retention_days_from_environment() -> Result<i64> {
    let Some(value) = std::env::var_os("CRON_RUNS_RETENTION_DAYS") else {
        return Ok(DEFAULT_CRON_RUNS_RETENTION_DAYS);
    };
    let value = value.to_string_lossy();
    let days = value.parse::<i64>().map_err(|_| {
        OmonError::Config(format!(
            "CRON_RUNS_RETENTION_DAYS must be a non-negative integer, got `{value}`"
        ))
    })?;
    if days < 0 {
        return Err(OmonError::Config(format!(
            "CRON_RUNS_RETENTION_DAYS must be a non-negative integer, got `{value}`"
        )));
    }
    Ok(days)
}

pub async fn prune_terminal_cron_runs(
    pool: &SqlitePool,
    retention_days: i64,
    now: DateTime<Utc>,
) -> Result<u64> {
    if retention_days < 0 {
        return Err(OmonError::Config(
            "cron run retention days must be non-negative".into(),
        ));
    }
    let cutoff = now - chrono::TimeDelta::days(retention_days);
    let result = sqlx::query(
        "DELETE FROM cron_runs
         WHERE status IN ('succeeded', 'failed')
           AND COALESCE(completed_at, started_at) < ?",
    )
    .bind(cutoff)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HermesSchedule {
    pub kind: String,
    #[serde(default)]
    pub expr: Option<String>,
    #[serde(default)]
    pub minutes: Option<u64>,
    #[serde(default)]
    pub run_at: Option<String>,
    #[serde(default)]
    pub display: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HermesOrigin {
    pub platform: String,
    pub chat_id: String,
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub chat_name: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HermesRepeat {
    #[serde(default)]
    pub times: Option<u64>,
    #[serde(default)]
    pub completed: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HermesJob {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub skill: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub script: Option<String>,
    #[serde(default)]
    pub no_agent: bool,
    #[serde(default)]
    pub context_from: Option<Value>,
    pub schedule: HermesSchedule,
    #[serde(default)]
    pub schedule_display: String,
    #[serde(default)]
    pub repeat: HermesRepeat,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub next_run_at: Option<String>,
    #[serde(default)]
    pub last_run_at: Option<String>,
    #[serde(default)]
    pub last_status: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_delivery_error: Option<String>,
    #[serde(default)]
    pub deliver: Option<String>,
    #[serde(default)]
    pub origin: Option<HermesOrigin>,
    #[serde(default)]
    pub enabled_toolsets: Option<Vec<String>>,
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    #[serde(default)]
    pub attach_to_session: Option<bool>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

fn default_true() -> bool {
    true
}

impl HermesJob {
    pub fn expression(&self) -> Result<String> {
        match self.schedule.kind.as_str() {
            "cron" => self
                .schedule
                .expr
                .clone()
                .filter(|value| !value.trim().is_empty()),
            "interval" => self
                .schedule
                .minutes
                .map(|minutes| format!("interval:{}m", minutes)),
            "once" => self
                .schedule
                .run_at
                .clone()
                .map(|value| format!("once:{value}")),
            kind => {
                return Err(OmonError::Config(format!(
                    "unsupported Hermes schedule kind `{kind}` for job {}",
                    self.id
                )))
            }
        }
        .ok_or_else(|| {
            OmonError::Config(format!("Hermes job {} has an incomplete schedule", self.id))
        })
    }

    pub fn discord_destination(&self) -> Result<Option<HermesOrigin>> {
        let deliver = self.deliver.as_deref().unwrap_or("origin").trim();
        if deliver == "local" || deliver.is_empty() {
            return Ok(None);
        }
        if deliver.contains(',') || deliver == "all" || deliver == "discord" {
            return Err(OmonError::Config(format!(
                "Hermes job {} uses delivery mode `{deliver}` which requires a channel directory/fan-out dispatcher",
                self.id
            )));
        }
        let target = if deliver == "origin" {
            self.origin.clone()
        } else if let Some(channel) = deliver.strip_prefix("discord:") {
            Some(HermesOrigin {
                platform: "discord".into(),
                chat_id: channel.trim_start_matches('#').to_owned(),
                ..HermesOrigin::default()
            })
        } else {
            return Err(OmonError::Config(format!(
                "Hermes job {} uses unsupported delivery target `{deliver}`",
                self.id
            )));
        };
        match target {
            Some(origin)
                if origin.platform.eq_ignore_ascii_case("discord")
                    && !origin.chat_id.is_empty() =>
            {
                Ok(Some(origin))
            }
            Some(origin) => Err(OmonError::Config(format!(
                "Hermes job {} cannot be delivered by the Discord gateway to {}:{}",
                self.id, origin.platform, origin.chat_id
            ))),
            None => Err(OmonError::Config(format!(
                "Hermes job {} has deliver=origin but no origin",
                self.id
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HermesStore {
    profile: String,
    home: PathBuf,
}

impl HermesStore {
    pub fn new(profile: impl Into<String>, home: impl Into<PathBuf>) -> Self {
        Self {
            profile: profile.into(),
            home: home.into(),
        }
    }

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn jobs_path(&self) -> PathBuf {
        self.home.join("cron").join("jobs.json")
    }

    pub async fn load(&self) -> Result<Vec<HermesJob>> {
        let path = self.jobs_path();
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(OmonError::Config(format!(
                    "failed to read {}: {error}",
                    path.display()
                )))
            }
        };
        #[derive(Deserialize)]
        struct Document {
            #[serde(default)]
            jobs: Vec<HermesJob>,
        }
        serde_json::from_slice::<Document>(&bytes)
            .map(|document| document.jobs)
            .map_err(|error| {
                OmonError::Config(format!(
                    "invalid Hermes cron store {}: {error}",
                    path.display()
                ))
            })
    }
}

#[derive(Clone)]
pub struct HermesStoreSynchronizer {
    pool: SqlitePool,
    stores: Vec<HermesStore>,
}

impl HermesStoreSynchronizer {
    pub fn new(pool: SqlitePool, stores: Vec<HermesStore>) -> Self {
        Self { pool, stores }
    }

    pub fn from_environment(pool: SqlitePool) -> Result<Self> {
        let root = std::env::var_os("HERMES_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".hermes")))
            .ok_or_else(|| {
                OmonError::Config(
                    "HOME or HERMES_HOME is required for Hermes cron synchronization".into(),
                )
            })?;
        let profiles = std::env::var("OMON_HERMES_PROFILES")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                let mut profiles = vec!["default".to_owned()];
                if let Ok(entries) = std::fs::read_dir(root.join("profiles")) {
                    profiles.extend(
                        entries
                            .flatten()
                            .filter(|entry| entry.path().is_dir())
                            .map(|entry| entry.file_name().to_string_lossy().into_owned()),
                    );
                }
                profiles
            });
        let stores = profiles
            .into_iter()
            .map(|profile| {
                let home = if profile == "default" {
                    root.clone()
                } else {
                    root.join("profiles").join(&profile)
                };
                HermesStore::new(profile, home)
            })
            .collect();
        Ok(Self::new(pool, stores))
    }

    pub async fn sync(&self) -> Result<usize> {
        let mut imported = 0;
        for store in &self.stores {
            let jobs = store.load().await?;
            let source = store.jobs_path().to_string_lossy().into_owned();
            let mut live = HashSet::new();
            for job in jobs {
                if job.id.trim().is_empty() {
                    return Err(OmonError::Config(format!(
                        "Hermes store {source} contains a job without an id"
                    )));
                }
                let expression = job.expression()?;
                let id = format!("hermes:{}:{}", store.profile(), job.id);
                live.insert(id.clone());
                let mut payload = serde_json::to_value(&job)
                    .map_err(|error| OmonError::Config(error.to_string()))?;
                payload[SOURCE_KEY] = Value::String(source.clone());
                payload["_omon_hermes_profile"] = Value::String(store.profile().to_owned());
                payload["_omon_hermes_home"] =
                    Value::String(store.home().to_string_lossy().into_owned());
                let payload_json = serde_json::to_string(&payload)
                    .map_err(|error| OmonError::Config(error.to_string()))?;
                let next_run_at = job
                    .next_run_at
                    .as_deref()
                    .map(parse_timestamp)
                    .transpose()?
                    .or_else(|| next_run(&expression, Utc::now()).ok());
                let created = job
                    .created_at
                    .as_deref()
                    .map(parse_timestamp)
                    .transpose()?
                    .unwrap_or_else(Utc::now);
                let now = Utc::now();
                sqlx::query(
                    "INSERT INTO cron_jobs (id, expression, payload_json, enabled, next_run_at, created_at, updated_at)\n                     VALUES (?, ?, ?, ?, ?, ?, ?)\n                     ON CONFLICT(id) DO UPDATE SET\n                     expression=excluded.expression,\n                     payload_json=CASE\n                         WHEN json_extract(cron_jobs.payload_json, '$.repeat.completed') IS NOT NULL\n                              AND CAST(json_extract(cron_jobs.payload_json, '$.repeat.completed') AS INTEGER) > CAST(COALESCE(json_extract(excluded.payload_json, '$.repeat.completed'), 0) AS INTEGER)\n                         THEN json_set(excluded.payload_json, '$.repeat.completed', CAST(json_extract(cron_jobs.payload_json, '$.repeat.completed') AS INTEGER))\n                         ELSE excluded.payload_json\n                     END,\n                     enabled=CASE\n                         WHEN cron_jobs.expression LIKE 'once:%' AND cron_jobs.next_run_at IS NULL\n                         THEN cron_jobs.enabled\n                         WHEN json_extract(cron_jobs.payload_json, '$.repeat.times') IS NOT NULL\n                              AND CAST(json_extract(cron_jobs.payload_json, '$.repeat.times') AS INTEGER) > 0\n                              AND CAST(json_extract(cron_jobs.payload_json, '$.repeat.completed') AS INTEGER) >= CAST(json_extract(cron_jobs.payload_json, '$.repeat.times') AS INTEGER)\n                              AND cron_jobs.next_run_at IS NULL\n                         THEN cron_jobs.enabled\n                         ELSE excluded.enabled\n                     END,\n                     next_run_at=CASE\n                         WHEN cron_jobs.expression LIKE 'once:%' AND cron_jobs.next_run_at IS NULL\n                         THEN NULL\n                         WHEN json_extract(cron_jobs.payload_json, '$.repeat.times') IS NOT NULL\n                              AND CAST(json_extract(cron_jobs.payload_json, '$.repeat.times') AS INTEGER) > 0\n                              AND CAST(json_extract(cron_jobs.payload_json, '$.repeat.completed') AS INTEGER) >= CAST(json_extract(cron_jobs.payload_json, '$.repeat.times') AS INTEGER)\n                              AND cron_jobs.next_run_at IS NULL\n                         THEN NULL\n                         WHEN cron_jobs.expression <> excluded.expression\n                           OR json_remove(cron_jobs.payload_json, '$.repeat.completed') <> json_remove(excluded.payload_json, '$.repeat.completed')\n                           OR cron_jobs.enabled <> excluded.enabled\n                         THEN excluded.next_run_at\n                         ELSE cron_jobs.next_run_at\n                     END,\n                     updated_at=excluded.updated_at"
                )
                .bind(&id).bind(expression).bind(payload_json).bind(job.enabled)
                .bind(next_run_at).bind(created).bind(now).execute(&self.pool).await?;
                imported += 1;
            }
            let rows: Vec<(String,)> = sqlx::query_as("SELECT id FROM cron_jobs WHERE json_extract(payload_json, '$._omon_hermes_source') = ?")
                .bind(&source).fetch_all(&self.pool).await?;
            for (id,) in rows {
                if !live.contains(&id) {
                    sqlx::query("DELETE FROM cron_jobs WHERE id = ?")
                        .bind(id)
                        .execute(&self.pool)
                        .await?;
                }
            }
        }
        Ok(imported)
    }
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| OmonError::Config(format!("invalid Hermes timestamp `{value}`: {error}")))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::Database;

    fn job(value: Value) -> HermesJob {
        serde_json::from_value(value).expect("valid Hermes job")
    }

    #[test]
    fn parses_full_job_and_resolves_origin_delivery() {
        let job = job(json!({
            "id": "brief",
            "prompt": "summarize",
            "schedule": {"kind": "cron", "expr": "0 9 * * *"},
            "deliver": "origin",
            "origin": {"platform": "discord", "chat_id": "42", "thread_id": "43"},
            "enabled_toolsets": ["web", "file"]
        }));
        assert_eq!(job.expression().unwrap(), "0 9 * * *");
        let target = job.discord_destination().unwrap().unwrap();
        assert_eq!(target.chat_id, "42");
        assert_eq!(target.thread_id.as_deref(), Some("43"));
        assert_eq!(job.enabled_toolsets.unwrap(), vec!["web", "file"]);
    }

    #[test]
    fn supports_interval_and_one_shot_schedules() {
        let interval = job(json!({"id":"a", "schedule":{"kind":"interval", "minutes":5}}));
        let once = job(
            json!({"id":"b", "schedule":{"kind":"once", "run_at":"2026-08-15T09:00:00+09:00"}}),
        );
        assert_eq!(interval.expression().unwrap(), "interval:5m");
        assert_eq!(once.expression().unwrap(), "once:2026-08-15T09:00:00+09:00");
    }

    #[tokio::test]
    async fn prunes_only_old_terminal_cron_runs() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO cron_jobs
             (id, expression, payload_json, enabled, next_run_at, created_at, updated_at)
             VALUES ('retention-job', 'interval:1h', '{}', 1, NULL, ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(database.pool())
        .await
        .unwrap();

        for (run_id, status, started_at, completed_at) in [
            (
                "old-succeeded",
                "succeeded",
                now - chrono::TimeDelta::days(31),
                Some(now - chrono::TimeDelta::days(30)),
            ),
            (
                "old-failed",
                "failed",
                now - chrono::TimeDelta::days(30),
                None,
            ),
            (
                "recent-succeeded",
                "succeeded",
                now - chrono::TimeDelta::days(2),
                Some(now - chrono::TimeDelta::days(1)),
            ),
            (
                "old-running",
                "running",
                now - chrono::TimeDelta::days(30),
                None,
            ),
        ] {
            sqlx::query(
                "INSERT INTO cron_runs
                 (run_id, job_id, claim_token, lease_expires_at, started_at, completed_at, status, attempt, error)
                 VALUES (?, 'retention-job', ?, ?, ?, ?, ?, 1, NULL)",
            )
            .bind(run_id)
            .bind(format!("token-{run_id}"))
            .bind(now + chrono::TimeDelta::hours(1))
            .bind(started_at)
            .bind(completed_at)
            .bind(status)
            .execute(database.pool())
            .await
            .unwrap();
        }

        let deleted = prune_terminal_cron_runs(database.pool(), 14, now)
            .await
            .unwrap();
        assert_eq!(deleted, 2);
        let remaining: HashSet<String> =
            sqlx::query_scalar("SELECT run_id FROM cron_runs ORDER BY run_id")
                .fetch_all(database.pool())
                .await
                .unwrap()
                .into_iter()
                .collect();
        assert_eq!(
            remaining,
            HashSet::from(["old-running".to_owned(), "recent-succeeded".to_owned()])
        );
    }

    #[tokio::test]
    async fn synchronization_does_not_rearm_completed_one_shot() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        let root = std::env::temp_dir().join(format!("omon-hermes-once-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(root.join("cron")).await.unwrap();
        tokio::fs::write(
            root.join("cron/jobs.json"),
            serde_json::to_vec(&json!({"jobs": [{
                "id": "once", "prompt": "run", "enabled": true,
                "next_run_at": "2026-08-15T09:00:00+09:00",
                "schedule": {"kind": "once", "run_at": "2026-08-15T09:00:00+09:00"},
                "deliver": "local"
            }]}))
            .unwrap(),
        )
        .await
        .unwrap();
        let sync = HermesStoreSynchronizer::new(
            database.pool().clone(),
            vec![HermesStore::new("default", &root)],
        );
        sync.sync().await.unwrap();
        sqlx::query(
            "UPDATE cron_jobs SET enabled = 0, next_run_at = NULL WHERE id = 'hermes:default:once'",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sync.sync().await.unwrap();
        let state: (bool, Option<DateTime<Utc>>) = sqlx::query_as(
            "SELECT enabled, next_run_at FROM cron_jobs WHERE id = 'hermes:default:once'",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(state, (false, None));
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn synchronization_preserves_scheduler_advanced_next_run() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        let root = std::env::temp_dir().join(format!("omon-hermes-store-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(root.join("cron")).await.unwrap();
        tokio::fs::write(
            root.join("cron/jobs.json"),
            serde_json::to_vec(&json!({
                "jobs": [{
                    "id": "brief", "prompt": "summarize", "enabled": true,
                    "created_at": "2026-08-01T09:00:00+09:00",
                    "next_run_at": "2026-08-15T09:00:00+09:00",
                    "schedule": {"kind": "cron", "expr": "0 9 * * *"},
                    "deliver": "discord:42"
                }]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        let sync = HermesStoreSynchronizer::new(
            database.pool().clone(),
            vec![HermesStore::new("default", &root)],
        );
        sync.sync().await.unwrap();
        let advanced = Utc::now() + chrono::TimeDelta::days(30);
        sqlx::query("UPDATE cron_jobs SET next_run_at = ? WHERE id = 'hermes:default:brief'")
            .bind(advanced)
            .execute(database.pool())
            .await
            .unwrap();
        sync.sync().await.unwrap();
        let actual: DateTime<Utc> = sqlx::query_scalar(
            "SELECT next_run_at FROM cron_jobs WHERE id = 'hermes:default:brief'",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(actual, advanced);
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn synchronization_does_not_rearm_completed_repeat_times() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        let root =
            std::env::temp_dir().join(format!("omon-hermes-repeat-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(root.join("cron")).await.unwrap();
        tokio::fs::write(
            root.join("cron/jobs.json"),
            serde_json::to_vec(&json!({"jobs": [{
                "id": "repeat_job", "prompt": "run", "enabled": true,
                "schedule": {"kind": "interval", "minutes": 5},
                "repeat": {"times": 2, "completed": 0},
                "deliver": "local"
            }]}))
            .unwrap(),
        )
        .await
        .unwrap();
        let sync = HermesStoreSynchronizer::new(
            database.pool().clone(),
            vec![HermesStore::new("default", &root)],
        );
        sync.sync().await.unwrap();

        // Simulate scheduler completing 2 runs and disabling job
        sqlx::query(
            "UPDATE cron_jobs SET enabled = 0, next_run_at = NULL, payload_json = json_set(payload_json, '$.repeat.completed', 2) WHERE id = 'hermes:default:repeat_job'",
        )
        .execute(database.pool())
        .await
        .unwrap();

        // Sync again from jobs.json (which still has completed: 0)
        sync.sync().await.unwrap();

        let state: (bool, Option<DateTime<Utc>>, String) = sqlx::query_as(
            "SELECT enabled, next_run_at, payload_json FROM cron_jobs WHERE id = 'hermes:default:repeat_job'",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(
            !state.0,
            "Job must remain disabled after repeat.times limit was reached"
        );
        assert_eq!(state.1, None, "Disabled job must not have next_run_at");
        let payload: Value = serde_json::from_str(&state.2).unwrap();
        assert_eq!(payload["repeat"]["completed"], 2);

        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
