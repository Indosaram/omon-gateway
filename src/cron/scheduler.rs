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

const LEASE_DURATION: TimeDelta = TimeDelta::minutes(30);
const LEASE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

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

            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                tracing::info!(job_id = %job.id, "Cron command completed successfully");
                return Ok(Some(if stdout.trim().is_empty() {
                    format!("Cron job `{}` completed successfully", job.id)
                } else {
                    format!("Cron job `{}` output:\n```\n{}\n```", job.id, stdout.trim())
                }));
            }

            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(OmonError::ToolExecution(format!(
                "cron job `{}` failed with {:?}: {}",
                job.id,
                output.status.code(),
                stderr.trim()
            )));
        }

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
    executions: Mutex<Vec<JoinHandle<()>>>,
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

#[derive(Clone)]
struct CronClaim {
    run_id: String,
    claim_token: String,
    job: CronJob,
    advance_schedule: bool,
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
                executions: Mutex::new(Vec::new()),
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
        let executions = std::mem::take(&mut *self.state.executions.lock().await);
        for execution in executions {
            let _ = execution.await;
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

    /// Claims a job through the same lease pipeline used by scheduled runs and
    /// executes it asynchronously. A manual run does not consume or advance the
    /// persisted schedule.
    pub async fn trigger(&self, id: &str) -> Result<bool> {
        if self.get(id).await?.is_none() {
            return Ok(false);
        }
        let Some(claim) = self.claim_job(id, false, false).await? else {
            return Ok(false);
        };
        self.spawn_claim(claim).await;
        Ok(true)
    }

    pub async fn trigger_job(&self, id: &str) -> Result<bool> {
        self.trigger(id).await
    }

    /// Claims every due job and starts each execution in its own task. Claims
    /// are protected by durable lease rows, so concurrent scheduler instances
    /// cannot execute the same job while a live lease exists.
    pub async fn run_due_jobs(&self) -> Result<usize> {
        let now = Utc::now();
        let job_ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM cron_jobs
             WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?
             ORDER BY next_run_at, id",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        let mut claimed = 0;
        for id in job_ids {
            if let Some(claim) = self.claim_job(&id, true, true).await? {
                self.spawn_claim(claim).await;
                claimed += 1;
            }
        }
        Ok(claimed)
    }

    async fn claim_job(
        &self,
        id: &str,
        require_due: bool,
        advance_schedule: bool,
    ) -> Result<Option<CronClaim>> {
        let now = Utc::now();
        let lease_expires_at = now + LEASE_DURATION;
        let run_id = Uuid::new_v4().to_string();
        let claim_token = Uuid::new_v4().to_string();

        sqlx::query(
            "UPDATE cron_runs
             SET status = 'failed', completed_at = ?, error = COALESCE(error, 'lease expired before completion')
             WHERE job_id = ? AND status = 'running' AND lease_expires_at <= ?",
        )
        .bind(now)
        .bind(id)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let due_clause = if require_due {
            "AND enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?"
        } else {
            ""
        };
        let sql = format!(
            "INSERT INTO cron_runs
             (run_id, job_id, claim_token, lease_expires_at, started_at, completed_at, status, attempt, error)
             SELECT ?, id, ?, ?, ?, NULL, 'running',
                    COALESCE((SELECT MAX(attempt) + 1 FROM cron_runs WHERE job_id = ?), 1), NULL
             FROM cron_jobs
             WHERE id = ? {due_clause}
               AND NOT EXISTS (
                   SELECT 1 FROM cron_runs
                   WHERE job_id = ? AND status = 'running' AND lease_expires_at > ?
               )"
        );
        let mut query = sqlx::query(&sql)
            .bind(&run_id)
            .bind(&claim_token)
            .bind(lease_expires_at)
            .bind(now)
            .bind(id)
            .bind(id);
        if require_due {
            query = query.bind(now);
        }
        let inserted = query.bind(id).bind(now).execute(&self.pool).await?;
        if inserted.rows_affected() == 0 {
            return Ok(None);
        }

        let job = self
            .get(id)
            .await?
            .ok_or_else(|| OmonError::Database(format!("claimed cron job {id} disappeared")))?;
        Ok(Some(CronClaim {
            run_id,
            claim_token,
            job,
            advance_schedule,
        }))
    }

    async fn spawn_claim(&self, claim: CronClaim) {
        let scheduler = self.clone();
        let handle = tokio::spawn(async move {
            scheduler.execute_claim(claim).await;
        });
        let mut executions = self.state.executions.lock().await;
        executions.retain(|execution| !execution.is_finished());
        executions.push(handle);
    }

    async fn execute_claim(&self, claim: CronClaim) {
        let heartbeat_scheduler = self.clone();
        let heartbeat_token = claim.claim_token.clone();
        let (heartbeat_stop, mut heartbeat_stopped) = watch::channel(false);
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(LEASE_REFRESH_INTERVAL) => {
                        if let Err(error) = heartbeat_scheduler.refresh_lease(&heartbeat_token).await {
                            tracing::error!(%error, claim_token = %heartbeat_token, "cron lease refresh failed");
                        }
                    }
                    changed = heartbeat_stopped.changed() => {
                        if changed.is_err() || *heartbeat_stopped.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        let result = self.execute_job(&claim.job).await;
        let _ = heartbeat_stop.send(true);
        let _ = heartbeat.await;

        match result {
            Ok(()) => {
                if let Err(error) = self.complete_success(&claim).await {
                    tracing::error!(%error, job_id = %claim.job.id, run_id = %claim.run_id, "failed to commit successful cron run");
                }
            }
            Err(error) => {
                if let Err(record_error) = self.complete_failure(&claim, &error).await {
                    tracing::error!(%record_error, job_id = %claim.job.id, run_id = %claim.run_id, "failed to record cron failure");
                }
                tracing::error!(%error, job_id = %claim.job.id, run_id = %claim.run_id, "cron job execution failed");
            }
        }
    }

    async fn refresh_lease(&self, claim_token: &str) -> Result<()> {
        let lease_expires_at = Utc::now() + LEASE_DURATION;
        sqlx::query(
            "UPDATE cron_runs SET lease_expires_at = ?
             WHERE claim_token = ? AND status = 'running'",
        )
        .bind(lease_expires_at)
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn complete_success(&self, claim: &CronClaim) -> Result<()> {
        let now = Utc::now();
        let mut transaction = self.pool.begin().await?;
        let completed = sqlx::query(
            "UPDATE cron_runs
             SET status = 'succeeded', completed_at = ?, lease_expires_at = ?, error = NULL
             WHERE run_id = ? AND claim_token = ? AND status = 'running'",
        )
        .bind(now)
        .bind(now)
        .bind(&claim.run_id)
        .bind(&claim.claim_token)
        .execute(&mut *transaction)
        .await?;
        if completed.rows_affected() == 0 {
            transaction.rollback().await?;
            return Err(OmonError::Database(format!(
                "cron claim {} is no longer active",
                claim.claim_token
            )));
        }

        if claim.advance_schedule {
            if claim.job.expression.starts_with("once:") {
                sqlx::query(
                    "UPDATE cron_jobs
                     SET enabled = 0, next_run_at = NULL, updated_at = ?
                     WHERE id = ? AND expression = ? AND payload_json = ?",
                )
                .bind(now)
                .bind(&claim.job.id)
                .bind(&claim.job.expression)
                .bind(&claim.job.payload_json)
                .execute(&mut *transaction)
                .await?;
            } else {
                let next = next_run(&claim.job.expression, now)?;
                sqlx::query(
                    "UPDATE cron_jobs
                     SET next_run_at = ?, updated_at = ?
                     WHERE id = ? AND enabled = 1 AND expression = ? AND payload_json = ?",
                )
                .bind(next)
                .bind(now)
                .bind(&claim.job.id)
                .bind(&claim.job.expression)
                .bind(&claim.job.payload_json)
                .execute(&mut *transaction)
                .await?;
            }
        }
        transaction.commit().await?;
        self.wake.notify_one();
        Ok(())
    }

    async fn complete_failure(&self, claim: &CronClaim, error: &OmonError) -> Result<()> {
        let now = Utc::now();
        sqlx::query(
            "UPDATE cron_runs
             SET status = 'failed', completed_at = ?, lease_expires_at = ?, error = ?
             WHERE run_id = ? AND claim_token = ? AND status = 'running'",
        )
        .bind(now)
        .bind(now)
        .bind(error.to_string())
        .bind(&claim.run_id)
        .bind(&claim.claim_token)
        .execute(&self.pool)
        .await?;
        self.wake.notify_one();
        Ok(())
    }

    async fn execute_job(&self, job: &CronJob) -> Result<()> {
        let payload = job.payload()?;
        let destination = delivery_destination(&payload)?;
        let execution = self.executor.execute(job).await;

        match execution {
            Ok(result_content) => {
                if let Some(destination) = destination {
                    let content = result_content
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
                        .unwrap_or_else(|| format!("Cron job {} completed", job.id));
                    self.deliver(job, destination, content).await?;
                }
                Ok(())
            }
            Err(error) => {
                if let Some(destination) = destination {
                    let content = format!("Cron job {} failed: {error}", job.id);
                    if let Err(delivery_error) = self.deliver(job, destination, content).await {
                        tracing::error!(%delivery_error, job_id = %job.id, "failed to deliver cron failure notification");
                    }
                }
                Err(error)
            }
        }
    }

    async fn deliver(
        &self,
        job: &CronJob,
        destination: crate::HermesOrigin,
        content: String,
    ) -> Result<()> {
        let channel_id = destination.chat_id.parse::<u64>().map_err(|_| {
            OmonError::Config(format!(
                "invalid Discord channel ID: {}",
                destination.chat_id
            ))
        })?;
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
        .map_err(|_| OmonError::Config(format!("invalid interval expression `{expression}")))?;
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
