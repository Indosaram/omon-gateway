use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, SqlitePool};
use tokio::sync::{broadcast, watch, Mutex, Notify};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::{
    HermesJob, HermesStoreSynchronizer, OmonError, OutboundAction, OutboundDispatcher, Result,
    SessionKey,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CronJobSpec {
    pub expression: String,
    #[serde(default)]
    pub payload: Value,
    pub session_key: Option<String>,
}

impl CronJobSpec {
    pub fn new(expression: impl Into<String>, payload: Value) -> Self {
        Self {
            expression: expression.into(),
            payload,
            session_key: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, FromRow)]
pub struct CronJob {
    pub id: String,
    pub session_key: Option<String>,
    pub expression: String,
    #[sqlx(rename = "payload_json")]
    pub payload_json: String,
    pub enabled: bool,
    pub next_run_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CronJob {
    pub fn payload(&self) -> Result<Value> {
        serde_json::from_str(&self.payload_json)
            .map_err(|error| OmonError::Config(format!("invalid cron payload: {error}")))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CronNotification {
    pub job_id: String,
    pub channel_id: u64,
    pub content: String,
    pub triggered_at: DateTime<Utc>,
}

#[async_trait]
pub trait CronTaskExecutor: Send + Sync + 'static {
    /// Executes a job and optionally returns completion text. If no text is
    /// returned, the scheduler uses `notification` or `content` from payload.
    async fn execute(&self, job: &CronJob) -> Result<Option<String>>;
}

#[derive(Default)]
pub struct ShellAndPayloadTaskExecutor;

#[async_trait]
impl CronTaskExecutor for ShellAndPayloadTaskExecutor {
    async fn execute(&self, job: &CronJob) -> Result<Option<String>> {
        let payload = job.payload()?;

        // 1. If payload contains "command" / "script", execute shell process
        if let Some(cmd) = payload
            .get("command")
            .or_else(|| payload.get("script"))
            .and_then(Value::as_str)
        {
            tracing::info!(job_id = %job.id, command = %cmd, "Executing cron shell command");
            let output = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(cmd)
                .output()
                .await
                .map_err(|e| {
                    OmonError::ToolExecution(format!("failed to execute cron command: {e}"))
                })?;

            let status_msg = if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                tracing::info!(job_id = %job.id, "Cron command completed successfully");
                if stdout.trim().is_empty() {
                    format!("Cron job `{}` completed successfully", job.id)
                } else {
                    format!("Cron job `{}` output:\n```\n{}\n```", job.id, stdout.trim())
                }
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::error!(job_id = %job.id, stderr = %stderr, "Cron command failed");
                format!(
                    "Cron job `{}` failed (exit code: {:?}):\n```\n{}\n```",
                    job.id,
                    output.status.code(),
                    stderr.trim()
                )
            };
            return Ok(Some(status_msg));
        }

        // 2. Otherwise return notification/content
        Ok(payload
            .get("notification")
            .or_else(|| payload.get("content"))
            .and_then(Value::as_str)
            .map(str::to_owned))
    }
}

#[derive(Default)]
pub struct PayloadTaskExecutor;

#[async_trait]
impl CronTaskExecutor for PayloadTaskExecutor {
    async fn execute(&self, job: &CronJob) -> Result<Option<String>> {
        ShellAndPayloadTaskExecutor.execute(job).await
    }
}

struct SchedulerState {
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct CronScheduler {
    pool: SqlitePool,
    executor: Arc<dyn CronTaskExecutor>,
    dispatcher: Option<Arc<dyn OutboundDispatcher>>,
    hermes_sync: Option<Arc<HermesStoreSynchronizer>>,
    notifications: broadcast::Sender<CronNotification>,
    wake: Arc<Notify>,
    poll_interval: Duration,
    state: Arc<SchedulerState>,
}

impl CronScheduler {
    pub fn new(pool: SqlitePool, executor: Arc<dyn CronTaskExecutor>) -> Self {
        Self::with_options(pool, executor, None, Duration::from_secs(1))
    }

    pub fn with_dispatcher(
        pool: SqlitePool,
        executor: Arc<dyn CronTaskExecutor>,
        dispatcher: Arc<dyn OutboundDispatcher>,
    ) -> Self {
        Self::with_options(pool, executor, Some(dispatcher), Duration::from_secs(1))
    }

    pub fn with_poll_interval(
        pool: SqlitePool,
        executor: Arc<dyn CronTaskExecutor>,
        poll_interval: Duration,
    ) -> Self {
        Self::with_options(pool, executor, None, poll_interval)
    }

    fn with_options(
        pool: SqlitePool,
        executor: Arc<dyn CronTaskExecutor>,
        dispatcher: Option<Arc<dyn OutboundDispatcher>>,
        poll_interval: Duration,
    ) -> Self {
        let (notifications, _) = broadcast::channel(128);
        let (shutdown, _) = watch::channel(false);
        Self {
            pool,
            executor,
            dispatcher,
            hermes_sync: None,
            notifications,
            wake: Arc::new(Notify::new()),
            poll_interval,
            state: Arc::new(SchedulerState {
                shutdown,
                task: Mutex::new(None),
            }),
        }
    }

    pub fn with_hermes_sync(mut self, synchronizer: HermesStoreSynchronizer) -> Self {
        self.hermes_sync = Some(Arc::new(synchronizer));
        self
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CronNotification> {
        self.notifications.subscribe()
    }

    pub async fn start(&self) {
        let mut task = self.state.task.lock().await;
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        let scheduler = self.clone();
        let mut shutdown = self.state.shutdown.subscribe();
        *task = Some(tokio::spawn(async move {
            loop {
                if *shutdown.borrow() {
                    break;
                }
                if let Some(synchronizer) = &scheduler.hermes_sync {
                    if let Err(error) = synchronizer.sync().await {
                        tracing::error!(%error, "Hermes cron store synchronization failed");
                    }
                }
                if let Err(error) = scheduler.run_due_jobs().await {
                    tracing::error!(%error, "cron scheduler poll failed");
                }
                tokio::select! {
                    _ = tokio::time::sleep(scheduler.poll_interval) => {},
                    _ = scheduler.wake.notified() => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }));
    }

    pub async fn shutdown(&self) {
        let _ = self.state.shutdown.send(true);
        self.wake.notify_waiters();
        if let Some(task) = self.state.task.lock().await.take() {
            let _ = task.await;
        }
    }

    pub async fn register_with_id(
        &self,
        id: impl Into<String>,
        spec: CronJobSpec,
    ) -> Result<CronJob> {
        let id = id.into();
        let now = Utc::now();
        let next_run_at = next_run(&spec.expression, now)?;
        let payload_json = serde_json::to_string(&spec.payload)
            .map_err(|error| OmonError::Config(error.to_string()))?;
        sqlx::query(
            "INSERT INTO cron_jobs
             (id, session_key, expression, payload_json, enabled, next_run_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, 1, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
             expression = excluded.expression,
             payload_json = excluded.payload_json,
             next_run_at = excluded.next_run_at,
             updated_at = excluded.updated_at",
        )
        .bind(&id)
        .bind(&spec.session_key)
        .bind(&spec.expression)
        .bind(payload_json)
        .bind(next_run_at)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.wake.notify_one();
        self.get(&id)
            .await?
            .ok_or_else(|| OmonError::Database("registered cron job disappeared".into()))
    }

    pub async fn register(&self, spec: CronJobSpec) -> Result<CronJob> {
        let now = Utc::now();
        let next_run_at = next_run(&spec.expression, now)?;
        let id = Uuid::new_v4().to_string();
        let payload_json = serde_json::to_string(&spec.payload)
            .map_err(|error| OmonError::Config(error.to_string()))?;
        sqlx::query(
            "INSERT INTO cron_jobs
             (id, session_key, expression, payload_json, enabled, next_run_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, 1, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&spec.session_key)
        .bind(&spec.expression)
        .bind(payload_json)
        .bind(next_run_at)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.wake.notify_one();
        self.get(&id)
            .await?
            .ok_or_else(|| OmonError::Database("registered cron job disappeared".into()))
    }

    pub async fn register_job(
        &self,
        expression: impl Into<String>,
        payload: Value,
    ) -> Result<CronJob> {
        self.register(CronJobSpec::new(expression, payload)).await
    }

    pub async fn get(&self, id: &str) -> Result<Option<CronJob>> {
        Ok(
            sqlx::query_as::<_, CronJob>("SELECT * FROM cron_jobs WHERE id = ?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    pub async fn list_active(&self) -> Result<Vec<CronJob>> {
        Ok(sqlx::query_as::<_, CronJob>(
            "SELECT * FROM cron_jobs WHERE enabled = 1 ORDER BY next_run_at, id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn pause(&self, id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE cron_jobs SET enabled = 0, next_run_at = NULL, updated_at = ? WHERE id = ?",
        )
        .bind(Utc::now())
        .bind(id)
        .execute(&self.pool)
        .await?;
        self.wake.notify_one();
        Ok(result.rows_affected() != 0)
    }

    pub async fn pause_job(&self, id: &str) -> Result<bool> {
        self.pause(id).await
    }

    pub async fn resume(&self, id: &str) -> Result<bool> {
        let Some(job) = self.get(id).await? else {
            return Ok(false);
        };
        let now = Utc::now();
        let next = next_run(&job.expression, now)?;
        sqlx::query(
            "UPDATE cron_jobs SET enabled = 1, next_run_at = ?, updated_at = ? WHERE id = ?",
        )
        .bind(next)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        self.wake.notify_one();
        Ok(true)
    }

    pub async fn resume_job(&self, id: &str) -> Result<bool> {
        self.resume(id).await
    }

    pub async fn delete(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM cron_jobs WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        self.wake.notify_one();
        Ok(result.rows_affected() != 0)
    }

    pub async fn delete_job(&self, id: &str) -> Result<bool> {
        self.delete(id).await
    }

    /// Executes a job immediately without changing its enabled state or schedule.
    pub async fn trigger(&self, id: &str) -> Result<bool> {
        let Some(job) = self.get(id).await? else {
            return Ok(false);
        };
        self.execute_job(job).await?;
        Ok(true)
    }

    pub async fn trigger_job(&self, id: &str) -> Result<bool> {
        self.trigger(id).await
    }

    /// Claims and executes every due job. Public for deterministic integration
    /// tests and for deployments which drive polling externally.
    pub async fn run_due_jobs(&self) -> Result<usize> {
        let now = Utc::now();
        let jobs = sqlx::query_as::<_, CronJob>(
            "SELECT * FROM cron_jobs
             WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?
             ORDER BY next_run_at, id",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        let mut executed = 0;
        for mut job in jobs {
            let one_shot = job.expression.starts_with("once:");
            let next = if one_shot {
                None
            } else {
                Some(next_run(&job.expression, now)?)
            };
            let claimed = sqlx::query(
                "UPDATE cron_jobs SET next_run_at = ?, enabled = ?, updated_at = ?
                 WHERE id = ? AND enabled = 1 AND next_run_at = ?",
            )
            .bind(next)
            .bind(!one_shot)
            .bind(now)
            .bind(&job.id)
            .bind(job.next_run_at)
            .execute(&self.pool)
            .await?;
            if claimed.rows_affected() == 0 {
                continue;
            }
            job.next_run_at = next;
            self.execute_job(job).await?;
            executed += 1;
        }
        Ok(executed)
    }

    async fn execute_job(&self, job: CronJob) -> Result<()> {
        let result = self.executor.execute(&job).await;
        let payload = job.payload()?;
        let destination = delivery_destination(&payload)?;
        if let Some(destination) = destination {
            let channel_id = destination.chat_id.parse::<u64>().map_err(|_| {
                OmonError::Config(format!(
                    "invalid Discord channel ID: {}",
                    destination.chat_id
                ))
            })?;
            let content = match result {
                Ok(content) => content
                    .or_else(|| {
                        payload
                            .get("notification")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .or_else(|| {
                        payload
                            .get("content")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .unwrap_or_else(|| format!("Cron job {} completed", job.id)),
                Err(error) => format!("Cron job {} failed: {error}", job.id),
            };
            let notification = CronNotification {
                job_id: job.id.clone(),
                channel_id,
                content: content.clone(),
                triggered_at: Utc::now(),
            };
            let _ = self.notifications.send(notification);
            if let Some(dispatcher) = &self.dispatcher {
                dispatcher
                    .dispatch(OutboundAction::SendMessage {
                        session: SessionKey::new(
                            "discord",
                            None::<String>,
                            destination.chat_id,
                            destination.thread_id,
                            destination.user_id.unwrap_or_else(|| "cron".into()),
                        ),
                        content,
                        reply_to: None,
                    })
                    .await?;
            }
        } else {
            result?;
        }
        Ok(())
    }
}

fn delivery_destination(payload: &Value) -> Result<Option<crate::HermesOrigin>> {
    if payload.get("schedule").is_some() {
        let job: HermesJob = serde_json::from_value(payload.clone())
            .map_err(|error| OmonError::Config(format!("invalid Hermes cron payload: {error}")))?;
        return job.discord_destination();
    }
    Ok(payload
        .get("channel_id")
        .and_then(|value| {
            value
                .as_u64()
                .map(|value| value.to_string())
                .or_else(|| value.as_str().map(str::to_owned))
        })
        .or_else(|| {
            payload
                .get("deliver")
                .and_then(Value::as_str)
                .and_then(|value| value.strip_prefix("discord:"))
                .map(str::to_owned)
        })
        .map(|chat_id| crate::HermesOrigin {
            platform: "discord".into(),
            chat_id,
            ..crate::HermesOrigin::default()
        }))
}

pub fn next_run(expression: &str, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
    if let Some(timestamp) = expression.strip_prefix("once:") {
        return DateTime::parse_from_rfc3339(timestamp)
            .map(|value| value.with_timezone(&Utc))
            .map_err(|error| {
                OmonError::Config(format!("invalid one-shot timestamp `{timestamp}`: {error}"))
            });
    }
    if let Some(interval) = parse_interval(expression)? {
        let delta = TimeDelta::from_std(interval)
            .map_err(|_| OmonError::Config("cron interval is too large".into()))?;
        return Ok(after + delta);
    }
    let normalized = normalize_cron_expression(expression);
    let schedule = Schedule::from_str(&normalized).map_err(|error| {
        OmonError::Config(format!("invalid cron expression `{expression}`: {error}"))
    })?;
    schedule
        .after(&after)
        .next()
        .ok_or_else(|| OmonError::Config(format!("cron expression `{expression}` has no next run")))
}

fn normalize_cron_expression(expression: &str) -> String {
    let parts: Vec<&str> = expression.split_whitespace().collect();
    if parts.len() == 5 {
        format!("0 {expression}")
    } else {
        expression.to_string()
    }
}

fn parse_interval(expression: &str) -> Result<Option<Duration>> {
    let value = expression
        .strip_prefix("interval:")
        .or_else(|| expression.strip_prefix("@every "))
        .or_else(|| expression.strip_prefix("every "));
    let Some(value) = value.map(str::trim) else {
        return Ok(None);
    };
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let amount: u64 = number
        .parse()
        .map_err(|_| OmonError::Config(format!("invalid interval expression `{expression}`")))?;
    if amount == 0 {
        return Err(OmonError::Config(
            "cron interval must be greater than zero".into(),
        ));
    }
    let duration = match unit.trim() {
        "ms" => Duration::from_millis(amount),
        "s" | "sec" | "secs" => Duration::from_secs(amount),
        "m" | "min" | "mins" => Duration::from_secs(amount.saturating_mul(60)),
        "h" | "hr" | "hrs" => Duration::from_secs(amount.saturating_mul(3_600)),
        "d" | "day" | "days" => Duration::from_secs(amount.saturating_mul(86_400)),
        _ => {
            return Err(OmonError::Config(format!(
                "invalid interval unit in `{expression}`"
            )))
        }
    };
    Ok(Some(duration))
}
