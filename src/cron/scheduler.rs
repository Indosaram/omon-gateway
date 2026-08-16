use std::path::{Path, PathBuf};
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

pub const MAX_CONTEXT_CHARS: usize = 8000;

pub fn is_valid_context_job_id(job_id: &str) -> bool {
    !job_id.is_empty() && !job_id.contains("..") && !job_id.contains('/') && !job_id.contains('\\')
}

pub fn parse_context_from_ids(context_from: Option<&Value>) -> Vec<String> {
    let Some(value) = context_from else {
        return Vec::new();
    };
    match value {
        Value::String(s) => {
            let s = s.trim();
            if is_valid_context_job_id(s) {
                vec![s.to_string()]
            } else {
                Vec::new()
            }
        }
        Value::Array(arr) => arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|s| is_valid_context_job_id(s))
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

pub fn truncate_context_output(output: &str, max_chars: usize) -> String {
    let count = output.chars().count();
    if count > max_chars {
        let prefix: String = output.chars().take(max_chars).collect();
        format!("{prefix}\n\n[... output truncated ...]")
    } else {
        output.to_string()
    }
}

pub fn format_context_from_block(job_id: &str, output: &str) -> String {
    format!(
        "## Output from job '{job_id}'\n\
        The following is the most recent output from a preceding cron job. Use it as context for your analysis.\n\n\
        ```\n{output}\n```"
    )
}

pub async fn resolve_predecessor_output(
    pool: &SqlitePool,
    hermes_home: Option<&Path>,
    job_id: &str,
) -> Option<String> {
    if !is_valid_context_job_id(job_id) {
        return None;
    }

    // 1. Check messages DB for recent assistant output from this cron job
    let db_result: Option<(String,)> = sqlx::query_as(
        "SELECT content FROM messages \
         WHERE (session_key LIKE ? OR session_key LIKE ?) \
           AND role = 'assistant' AND TRIM(content) != '' \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(format!("%cron:{job_id}"))
    .bind(format!("%:{job_id}"))
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    if let Some((content,)) = db_result {
        if !content.trim().is_empty() {
            return Some(content);
        }
    }

    // 2. Fallback: Check ~/.hermes/cron/output/<job_id>/*.md or <home>/cron/output/<job_id>/*.md
    let output_dirs = {
        let mut dirs = Vec::new();
        if let Some(home) = hermes_home {
            dirs.push(home.join("cron").join("output").join(job_id));
        }
        if let Some(env_home) = std::env::var_os("HERMES_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".hermes")))
        {
            dirs.push(env_home.join("cron").join("output").join(job_id));
        }
        dirs
    };

    for output_dir in output_dirs {
        if output_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&output_dir) {
                let mut files: Vec<(PathBuf, std::time::SystemTime)> = entries
                    .filter_map(std::result::Result::ok)
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|ext| ext == "md"))
                    .filter_map(|p| {
                        let mtime = std::fs::metadata(&p).ok()?.modified().ok()?;
                        Some((p, mtime))
                    })
                    .collect();
                files.sort_by(|a, b| b.1.cmp(&a.1));
                if let Some((latest_file, _)) = files.first() {
                    if let Ok(content) = std::fs::read_to_string(latest_file) {
                        if !content.trim().is_empty() {
                            return Some(content);
                        }
                    }
                }
            }
        }
    }

    None
}

pub fn parse_wake_gate(stdout: &str) -> bool {
    let stripped_lines: Vec<&str> = stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    let Some(&last_line) = stripped_lines.last() else {
        return true;
    };

    // 1. Try parsing JSON
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(last_line) {
        if let Some(obj) = value.as_object() {
            if let Some(wake) = obj.get("wakeAgent").or_else(|| obj.get("wake_agent")) {
                if let Some(b) = wake.as_bool() {
                    return b;
                }
            }
        }
    }

    // 2. Try parsing plain-text / YAML pattern (e.g. "wakeAgent: false")
    let lower = last_line.to_ascii_lowercase();
    if lower == "wakeagent: false"
        || lower == "wake_agent: false"
        || lower == "{\"wakeagent\": false}"
        || lower == "{\"wakeagent\":false}"
    {
        return false;
    }

    true
}

const CRON_SILENCE_TOKENS: &[&str] = &["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY"];

fn is_silence_token_line(line: &str) -> bool {
    let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");
    let upper = normalized.to_uppercase();
    CRON_SILENCE_TOKENS.iter().any(|&token| token == upper)
}

pub fn is_cron_silence_response(text: &str) -> bool {
    let stripped = text.trim();
    if stripped.is_empty() {
        return true;
    }

    if is_silence_token_line(stripped) {
        return true;
    }

    let lines: Vec<&str> = stripped
        .lines()
        .map(str::trim)
        .filter(|ln| !ln.is_empty())
        .collect();
    if let Some(&first) = lines.first() {
        if is_silence_token_line(first) {
            return true;
        }
    }
    if let Some(&last) = lines.last() {
        if is_silence_token_line(last) {
            return true;
        }
    }

    let upper = stripped.to_uppercase();
    if upper.starts_with("[SILENT]") {
        return true;
    }

    false
}

pub fn should_disable_after(completed: u64, times: Option<u64>) -> bool {
    match times {
        Some(limit) if limit > 0 => completed >= limit,
        _ => false,
    }
}

pub fn extract_repeat_info(payload: &Value) -> (Option<u64>, u64) {
    if let Some(repeat) = payload.get("repeat") {
        let times = repeat.get("times").and_then(Value::as_u64);
        let completed = repeat.get("completed").and_then(Value::as_u64).unwrap_or(0);
        return (times, completed);
    }
    let times = payload.get("times").and_then(Value::as_u64);
    let completed = payload
        .get("completed")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (times, completed)
}

pub fn increment_repeat_completed(payload: &mut Value) -> (Option<u64>, u64) {
    if let Some(repeat) = payload.get_mut("repeat") {
        if let Some(obj) = repeat.as_object_mut() {
            let times = obj.get("times").and_then(Value::as_u64);
            let completed = obj.get("completed").and_then(Value::as_u64).unwrap_or(0) + 1;
            obj.insert("completed".to_string(), Value::from(completed));
            return (times, completed);
        }
    }
    let times = payload.get("times").and_then(Value::as_u64);
    let completed = payload
        .get("completed")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        + 1;
    if let Some(obj) = payload.as_object_mut() {
        if times.is_some() {
            obj.insert("completed".to_string(), Value::from(completed));
        } else {
            let mut repeat_obj = serde_json::Map::new();
            repeat_obj.insert("completed".to_string(), Value::from(completed));
            obj.insert("repeat".to_string(), Value::Object(repeat_obj));
        }
    }
    (times, completed)
}

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
            let mut command = tokio::process::Command::new("sh");
            command.arg("-c").arg(cmd);
            let augmented_path = crate::tools::augmented_path_from_environment();
            if !augmented_path.is_empty() {
                command.env("PATH", augmented_path);
            }
            let output = command.output().await.map_err(|e| {
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

        let mut payload: Value =
            serde_json::from_str(&claim.job.payload_json).unwrap_or_else(|_| serde_json::json!({}));
        let (times, completed_count) = increment_repeat_completed(&mut payload);
        let updated_payload_json =
            serde_json::to_string(&payload).unwrap_or_else(|_| claim.job.payload_json.clone());
        let limit_reached = should_disable_after(completed_count, times);

        if claim.advance_schedule {
            if claim.job.expression.starts_with("once:") || limit_reached {
                sqlx::query(
                    "UPDATE cron_jobs
                     SET enabled = 0, next_run_at = NULL, payload_json = ?, updated_at = ?
                     WHERE id = ? AND expression = ? AND payload_json = ?",
                )
                .bind(&updated_payload_json)
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
                     SET next_run_at = ?, payload_json = ?, updated_at = ?
                     WHERE id = ? AND enabled = 1 AND expression = ? AND payload_json = ?",
                )
                .bind(next)
                .bind(&updated_payload_json)
                .bind(now)
                .bind(&claim.job.id)
                .bind(&claim.job.expression)
                .bind(&claim.job.payload_json)
                .execute(&mut *transaction)
                .await?;
            }
        } else {
            sqlx::query(
                "UPDATE cron_jobs
                 SET payload_json = ?, updated_at = ?
                 WHERE id = ? AND expression = ? AND payload_json = ?",
            )
            .bind(&updated_payload_json)
            .bind(now)
            .bind(&claim.job.id)
            .bind(&claim.job.expression)
            .bind(&claim.job.payload_json)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        self.wake.notify_one();
        Ok(())
    }

    async fn complete_failure(&self, claim: &CronClaim, error: &OmonError) -> Result<()> {
        let now = Utc::now();
        let mut transaction = self.pool.begin().await?;
        let completed = sqlx::query(
            "UPDATE cron_runs
             SET status = 'failed', completed_at = ?, lease_expires_at = ?, error = ?
             WHERE run_id = ? AND claim_token = ? AND status = 'running'",
        )
        .bind(now)
        .bind(now)
        .bind(error.to_string())
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
                let recent_statuses: Vec<String> = sqlx::query_scalar(
                    "SELECT status FROM cron_runs
                     WHERE job_id = ?
                     ORDER BY started_at DESC, run_id DESC
                     LIMIT 50",
                )
                .bind(&claim.job.id)
                .fetch_all(&mut *transaction)
                .await?;

                let consecutive_failures = recent_statuses
                    .iter()
                    .take_while(|status| status.as_str() == "failed")
                    .count() as u32;

                let next_result =
                    next_run_after_failure(&claim.job.expression, now, consecutive_failures);
                match next_result {
                    Ok(Some(next)) => {
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
                    Ok(None) => {
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
                    }
                    Err(calc_err) => {
                        tracing::error!(
                            %calc_err,
                            job_id = %claim.job.id,
                            "failed to compute next run after failure, disabling cron job"
                        );
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
                    }
                }
            }
        }
        transaction.commit().await?;
        self.wake.notify_one();
        Ok(())
    }

    async fn execute_job(&self, job: &CronJob) -> Result<()> {
        let payload = job.payload()?;
        let destinations = delivery_destination(&payload)?;
        let execution = self.executor.execute(job).await;

        match execution {
            Ok(result_content) => {
                if !destinations.is_empty() {
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
                        });
                    if let Some(content) = content {
                        if !content.trim().is_empty() && !is_cron_silence_response(&content) {
                            for destination in &destinations {
                                self.deliver(job, destination, &content).await?;
                            }
                        } else {
                            tracing::info!(
                                job_id = %job.id,
                                "cron job returned empty or silence response, suppressing delivery"
                            );
                        }
                    }
                }
                Ok(())
            }
            Err(error) => {
                if !destinations.is_empty() {
                    let content = format!("Cron job {} failed: {error}", job.id);
                    for destination in &destinations {
                        if let Err(delivery_error) = self.deliver(job, destination, &content).await
                        {
                            tracing::error!(%delivery_error, job_id = %job.id, "failed to deliver cron failure notification");
                        }
                    }
                }
                Err(error)
            }
        }
    }

    async fn deliver(
        &self,
        job: &CronJob,
        destination: &crate::HermesOrigin,
        content: &str,
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
            content: content.to_string(),
            triggered_at: Utc::now(),
        };
        let _ = self.notifications.send(notification);
        if let Some(dispatcher) = &self.dispatcher {
            dispatcher
                .dispatch(OutboundAction::SendMessage {
                    session: SessionKey::new(
                        "discord",
                        None::<String>,
                        destination.chat_id.clone(),
                        destination.thread_id.clone(),
                        destination.user_id.clone().unwrap_or_else(|| "cron".into()),
                    ),
                    content: content.to_string(),
                    reply_to: None,
                })
                .await?;
        }
        Ok(())
    }
}

pub fn delivery_destinations(payload: &Value) -> Result<Vec<crate::HermesOrigin>> {
    if payload.get("schedule").is_some() {
        let job: HermesJob = serde_json::from_value(payload.clone())
            .map_err(|error| OmonError::Config(format!("invalid Hermes cron payload: {error}")))?;
        return job.discord_destinations();
    }

    let mut destinations = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if let Some(chat_id) = payload.get("channel_id").and_then(|value| {
        value
            .as_u64()
            .map(|value| value.to_string())
            .or_else(|| value.as_str().map(str::to_owned))
    }) {
        let chat_id = chat_id.trim().to_string();
        if !chat_id.is_empty() {
            let key = ("discord".to_string(), chat_id.clone(), None);
            seen.insert(key);
            destinations.push(crate::HermesOrigin {
                platform: "discord".into(),
                chat_id,
                ..crate::HermesOrigin::default()
            });
        }
    }

    let deliver_str = if let Some(s) = payload.get("deliver").and_then(Value::as_str) {
        Some(s.to_string())
    } else if let Some(arr) = payload.get("deliver").and_then(Value::as_array) {
        let parts: Vec<&str> = arr.iter().filter_map(Value::as_str).collect();
        Some(parts.join(","))
    } else {
        None
    };

    if let Some(deliver) = deliver_str {
        for part in deliver.split(',') {
            let part = part.trim();
            if part.is_empty() || part == "local" {
                continue;
            }
            if part == "origin" || part == "all" || part == "discord" {
                if let Some(origin_val) = payload.get("origin") {
                    if let Ok(origin) =
                        serde_json::from_value::<crate::HermesOrigin>(origin_val.clone())
                    {
                        if origin.platform.eq_ignore_ascii_case("discord")
                            && !origin.chat_id.is_empty()
                        {
                            let key = (
                                origin.platform.to_lowercase(),
                                origin.chat_id.clone(),
                                origin.thread_id.clone(),
                            );
                            if seen.insert(key) {
                                destinations.push(origin);
                            }
                        }
                    }
                }
            } else if let Some(channel) = part.strip_prefix("discord:") {
                let chat_id = channel.trim_start_matches('#').trim().to_string();
                if !chat_id.is_empty() {
                    let key = ("discord".to_string(), chat_id.clone(), None);
                    if seen.insert(key) {
                        destinations.push(crate::HermesOrigin {
                            platform: "discord".into(),
                            chat_id,
                            ..crate::HermesOrigin::default()
                        });
                    }
                }
            } else if part.contains(':') {
                // Unknown platform prefix (e.g. telegram:123) - skip gracefully
                continue;
            } else {
                let chat_id = part.trim_start_matches('#').trim().to_string();
                if !chat_id.is_empty() {
                    let key = ("discord".to_string(), chat_id.clone(), None);
                    if seen.insert(key) {
                        destinations.push(crate::HermesOrigin {
                            platform: "discord".into(),
                            chat_id,
                            ..crate::HermesOrigin::default()
                        });
                    }
                }
            }
        }
    }

    Ok(destinations)
}

pub fn delivery_destination(payload: &Value) -> Result<Vec<crate::HermesOrigin>> {
    delivery_destinations(payload)
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

pub const MIN_RETRY_INTERVAL: Duration = Duration::from_secs(10);
pub const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(3600);

pub fn failure_backoff_duration(consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return Duration::ZERO;
    }
    let exponent = (consecutive_failures - 1).min(10);
    let multiplier = 1u64.checked_shl(exponent).unwrap_or(1024);
    let seconds = (10u64.saturating_mul(multiplier)).clamp(10, 3600);
    Duration::from_secs(seconds)
}

pub fn next_run_after_failure(
    expression: &str,
    now: DateTime<Utc>,
    consecutive_failures: u32,
) -> Result<Option<DateTime<Utc>>> {
    if expression.starts_with("once:") {
        return Ok(None);
    }
    let scheduled_next = next_run(expression, now)?;
    let backoff = failure_backoff_duration(consecutive_failures);
    let backoff_delta = TimeDelta::from_std(backoff)
        .map_err(|_| OmonError::Config("backoff duration out of range".into()))?;
    let backoff_next = now + backoff_delta;
    Ok(Some(std::cmp::max(scheduled_next, backoff_next)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_backoff_scales_exponentially_and_clamps() {
        assert_eq!(failure_backoff_duration(0), Duration::ZERO);
        assert_eq!(failure_backoff_duration(1), Duration::from_secs(10));
        assert_eq!(failure_backoff_duration(2), Duration::from_secs(20));
        assert_eq!(failure_backoff_duration(3), Duration::from_secs(40));
        assert_eq!(failure_backoff_duration(4), Duration::from_secs(80));
        assert_eq!(failure_backoff_duration(5), Duration::from_secs(160));
        assert_eq!(failure_backoff_duration(6), Duration::from_secs(320));
        assert_eq!(failure_backoff_duration(7), Duration::from_secs(640));
        assert_eq!(failure_backoff_duration(8), Duration::from_secs(1280));
        assert_eq!(failure_backoff_duration(9), Duration::from_secs(2560));
        assert_eq!(failure_backoff_duration(10), Duration::from_secs(3600));
        assert_eq!(failure_backoff_duration(100), Duration::from_secs(3600));
    }

    #[test]
    fn next_run_after_failure_advances_interval_and_applies_backoff() {
        let now = DateTime::parse_from_rfc3339("2026-08-16T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // interval:1s with 1 failure -> backoff of 10s wins over 1s schedule
        let next1 = next_run_after_failure("interval:1s", now, 1)
            .unwrap()
            .unwrap();
        assert_eq!(next1, now + TimeDelta::seconds(10));

        // interval:1s with 2 failures -> backoff of 20s wins over 1s schedule
        let next2 = next_run_after_failure("interval:1s", now, 2)
            .unwrap()
            .unwrap();
        assert_eq!(next2, now + TimeDelta::seconds(20));

        // interval:1h with 1 failure -> schedule (1h) wins over backoff (10s)
        let next_1h = next_run_after_failure("interval:1h", now, 1)
            .unwrap()
            .unwrap();
        assert_eq!(next_1h, now + TimeDelta::hours(1));

        // once: expression returns None on failure (job should finish)
        let next_once = next_run_after_failure("once:2026-08-16T12:00:00Z", now, 1).unwrap();
        assert!(next_once.is_none());
    }

    #[test]
    fn next_run_after_failure_cron_expression_respects_schedule_and_backoff() {
        let now = DateTime::parse_from_rfc3339("2026-08-16T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Cron running every minute (next schedule = 12:01:00 = 60s)
        // Failure 1 backoff is 10s, so schedule (60s) wins
        let next1 = next_run_after_failure("* * * * *", now, 1)
            .unwrap()
            .unwrap();
        assert_eq!(next1, now + TimeDelta::seconds(60));

        // Failure 5 backoff is 160s, so backoff (160s) wins over schedule (60s)
        let next5 = next_run_after_failure("* * * * *", now, 5)
            .unwrap()
            .unwrap();
        assert_eq!(next5, now + TimeDelta::seconds(160));
    }

    #[test]
    fn test_is_cron_silence_response_matches_sentinels_and_variants() {
        assert!(is_cron_silence_response(""));
        assert!(is_cron_silence_response("   \n  \t "));
        assert!(is_cron_silence_response("[SILENT]"));
        assert!(is_cron_silence_response("[silent]"));
        assert!(is_cron_silence_response("SILENT"));
        assert!(is_cron_silence_response("silent"));
        assert!(is_cron_silence_response("NO_REPLY"));
        assert!(is_cron_silence_response("no_reply"));
        assert!(is_cron_silence_response("NO REPLY"));
        assert!(is_cron_silence_response("no reply"));
        assert!(is_cron_silence_response("[SILENT] No updates to report"));
        assert!(is_cron_silence_response("[silent] no new issues found"));
        assert!(is_cron_silence_response("Everything checked\n\n[SILENT]"));
        assert!(is_cron_silence_response(
            "[SILENT]\nChecked 5 repos, all green"
        ));
        assert!(is_cron_silence_response("NO_REPLY\nProcessed batch"));
    }

    #[test]
    fn test_is_cron_silence_response_preserves_mid_sentence_content() {
        assert!(!is_cron_silence_response(
            "I considered staying [SILENT] but here is the summary of issues."
        ));
        assert!(!is_cron_silence_response("Silent retry succeeded"));
        assert!(!is_cron_silence_response("Found 3 new vulnerabilities"));
    }

    struct SilentExecutor(Option<String>);

    #[async_trait]
    impl CronTaskExecutor for SilentExecutor {
        async fn execute(&self, _job: &CronJob) -> Result<Option<String>> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn test_execute_job_suppresses_delivery_for_silent_and_empty_output() {
        let database = crate::Database::connect("sqlite::memory:").await.unwrap();

        // 1. Executor returning None suppresses delivery
        let scheduler = CronScheduler::new(database.pool().clone(), Arc::new(SilentExecutor(None)));
        let mut notifications = scheduler.subscribe();
        let job = scheduler
            .register_job(
                "interval:1m",
                serde_json::json!({
                    "channel_id": 123456,
                }),
            )
            .await
            .unwrap();

        scheduler.execute_job(&job).await.unwrap();
        // Notification channel should have no message
        assert!(notifications.try_recv().is_err());

        // 2. Executor returning [SILENT] suppresses delivery
        let silent_scheduler = CronScheduler::new(
            database.pool().clone(),
            Arc::new(SilentExecutor(Some("[SILENT]".into()))),
        );
        let mut silent_notifications = silent_scheduler.subscribe();
        silent_scheduler.execute_job(&job).await.unwrap();
        assert!(silent_notifications.try_recv().is_err());

        // 3. Executor returning non-empty report delivers
        let active_scheduler = CronScheduler::new(
            database.pool().clone(),
            Arc::new(SilentExecutor(Some("Report content".into()))),
        );
        let mut active_notifications = active_scheduler.subscribe();
        active_scheduler.execute_job(&job).await.unwrap();
        let note = active_notifications.try_recv().unwrap();
        assert_eq!(note.content, "Report content");
    }

    #[test]
    fn test_should_disable_after() {
        assert!(!should_disable_after(0, Some(2)));
        assert!(!should_disable_after(1, Some(2)));
        assert!(should_disable_after(2, Some(2)));
        assert!(should_disable_after(3, Some(2)));
        assert!(!should_disable_after(5, None));
        assert!(!should_disable_after(5, Some(0)));
    }

    #[test]
    fn test_increment_repeat_completed() {
        let mut payload = serde_json::json!({
            "repeat": {
                "times": 3,
                "completed": 1
            }
        });
        let (times, completed) = increment_repeat_completed(&mut payload);
        assert_eq!(times, Some(3));
        assert_eq!(completed, 2);
        assert_eq!(payload["repeat"]["completed"], 2);

        let mut payload_flat = serde_json::json!({
            "times": 2
        });
        let (times, completed) = increment_repeat_completed(&mut payload_flat);
        assert_eq!(times, Some(2));
        assert_eq!(completed, 1);
        assert_eq!(payload_flat["completed"], 1);
    }

    #[tokio::test]
    async fn test_repeat_times_disables_job_after_limit_reached() {
        let database = crate::Database::connect("sqlite::memory:").await.unwrap();
        let scheduler = CronScheduler::new(
            database.pool().clone(),
            Arc::new(SilentExecutor(Some("Run ok".into()))),
        );

        let job = scheduler
            .register(CronJobSpec::new(
                "interval:1m",
                serde_json::json!({
                    "channel_id": "123",
                    "repeat": {
                        "times": 2,
                        "completed": 0
                    }
                }),
            ))
            .await
            .unwrap();

        // 1. Run 1: claim, complete_success
        let claim1 = scheduler
            .claim_job(&job.id, false, true)
            .await
            .unwrap()
            .unwrap();
        scheduler.complete_success(&claim1).await.unwrap();

        let job_after_1 = scheduler.get(&job.id).await.unwrap().unwrap();
        assert!(job_after_1.enabled);
        assert!(job_after_1.next_run_at.is_some());
        let (times1, comp1) = extract_repeat_info(&job_after_1.payload().unwrap());
        assert_eq!(times1, Some(2));
        assert_eq!(comp1, 1);

        // 2. Run 2 (reaches limit of 2): claim, complete_success
        let claim2 = scheduler
            .claim_job(&job.id, false, true)
            .await
            .unwrap()
            .unwrap();
        scheduler.complete_success(&claim2).await.unwrap();

        let job_after_2 = scheduler.get(&job.id).await.unwrap().unwrap();
        assert!(
            !job_after_2.enabled,
            "Job must be disabled after hitting repeat.times limit"
        );
        assert!(
            job_after_2.next_run_at.is_none(),
            "Disabled job must clear next_run_at"
        );
        let (times2, comp2) = extract_repeat_info(&job_after_2.payload().unwrap());
        assert_eq!(times2, Some(2));
        assert_eq!(comp2, 2);
    }

    #[test]
    fn test_parse_context_from_ids() {
        assert_eq!(parse_context_from_ids(None), Vec::<String>::new());
        assert_eq!(
            parse_context_from_ids(Some(&serde_json::json!("job1"))),
            vec!["job1".to_string()]
        );
        assert_eq!(
            parse_context_from_ids(Some(&serde_json::json!(["job1", "job2", "invalid/../id"]))),
            vec!["job1".to_string(), "job2".to_string()]
        );
        assert_eq!(
            parse_context_from_ids(Some(&serde_json::json!({"not": "a list"}))),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_truncate_context_output() {
        let short = "Hello world";
        assert_eq!(truncate_context_output(short, 8000), "Hello world");

        let long = "a".repeat(8005);
        let truncated = truncate_context_output(&long, 8000);
        assert_eq!(
            truncated.len(),
            8000 + "\n\n[... output truncated ...]".len()
        );
        assert!(truncated.starts_with(&"a".repeat(8000)));
        assert!(truncated.ends_with("\n\n[... output truncated ...]"));
    }

    #[test]
    fn test_format_context_from_block() {
        let formatted = format_context_from_block("weather_job", "Temperature: 20C");
        let expected = "## Output from job 'weather_job'\nThe following is the most recent output from a preceding cron job. Use it as context for your analysis.\n\n```\nTemperature: 20C\n```";
        assert_eq!(formatted, expected);
    }

    #[tokio::test]
    async fn test_resolve_predecessor_output_from_db_and_disk() {
        let database = crate::Database::connect("sqlite::memory:").await.unwrap();

        // 1. Resolve from DB messages
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO sessions (session_key, platform, channel_id, user_id, state_json, created_at, updated_at) \
             VALUES ('discord:123:cron:job_alpha', 'discord', '123', 'cron:job_alpha', '{}', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(database.pool())
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json, created_at) \
             VALUES ('msg1', 'discord:123:cron:job_alpha', 'assistant', 'Output from alpha', '{}', ?)",
        )
        .bind(now)
        .execute(database.pool())
        .await
        .unwrap();

        let resolved_db = resolve_predecessor_output(database.pool(), None, "job_alpha").await;
        assert_eq!(resolved_db.as_deref(), Some("Output from alpha"));

        // 2. Resolve fallback from disk
        let temp_dir =
            std::env::temp_dir().join(format!("omon-test-hermes-{}", uuid::Uuid::new_v4()));
        let job_out_dir = temp_dir.join("cron").join("output").join("job_beta");
        tokio::fs::create_dir_all(&job_out_dir).await.unwrap();
        tokio::fs::write(
            job_out_dir.join("2026-08-16T10-00-00.md"),
            "Disk output from beta",
        )
        .await
        .unwrap();

        let resolved_disk =
            resolve_predecessor_output(database.pool(), Some(&temp_dir), "job_beta").await;
        assert_eq!(resolved_disk.as_deref(), Some("Disk output from beta"));

        let _ = tokio::fs::remove_dir_all(temp_dir).await;
    }

    #[test]
    fn test_parse_wake_gate() {
        assert!(parse_wake_gate(""));
        assert!(parse_wake_gate("All systems normal\nChecked 10 records"));
        assert!(parse_wake_gate("{\"wakeAgent\": true}"));
        assert!(parse_wake_gate("wakeAgent: true"));
        assert!(parse_wake_gate("Checked logs\n{\"wakeAgent\": true}"));

        assert!(!parse_wake_gate("{\"wakeAgent\": false}"));
        assert!(!parse_wake_gate("{\"wake_agent\": false}"));
        assert!(!parse_wake_gate("wakeAgent: false"));
        assert!(!parse_wake_gate("wake_agent: false"));
        assert!(!parse_wake_gate("{\"wakeAgent\":false}"));
        assert!(!parse_wake_gate(
            "Running daily health check...\nFound no pending alerts.\n{\"wakeAgent\": false}"
        ));
        assert!(!parse_wake_gate(
            "Running daily health check...\nFound no pending alerts.\nwakeAgent: false"
        ));
    }

    #[test]
    fn test_delivery_fan_out_parsing() {
        // 1. Comma-separated discord channels
        let payload = serde_json::json!({
            "deliver": "discord:123,discord:456"
        });
        let targets = delivery_destination(&payload).unwrap();
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].chat_id, "123");
        assert_eq!(targets[1].chat_id, "456");

        // 2. Comma-separated with unknown platforms (e.g. telegram)
        let payload = serde_json::json!({
            "deliver": "discord:123,telegram:456,discord:789"
        });
        let targets = delivery_destination(&payload).unwrap();
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].chat_id, "123");
        assert_eq!(targets[1].chat_id, "789");

        // 3. origin and all with discord origin
        let payload = serde_json::json!({
            "id": "job1",
            "schedule": {"kind": "cron", "expr": "0 9 * * *"},
            "deliver": "origin,all",
            "origin": {"platform": "discord", "chat_id": "999", "thread_id": "888"}
        });
        let targets = delivery_destination(&payload).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].chat_id, "999");
        assert_eq!(targets[0].thread_id.as_deref(), Some("888"));

        // 4. all with non-discord origin -> gracefully skipped
        let payload = serde_json::json!({
            "id": "job2",
            "schedule": {"kind": "cron", "expr": "0 9 * * *"},
            "deliver": "all",
            "origin": {"platform": "telegram", "chat_id": "999"}
        });
        let targets = delivery_destination(&payload).unwrap();
        assert!(targets.is_empty());

        // 5. local delivery -> empty
        let payload = serde_json::json!({
            "deliver": "local"
        });
        let targets = delivery_destination(&payload).unwrap();
        assert!(targets.is_empty());

        // 6. Deduplication of explicit channel and origin
        let payload = serde_json::json!({
            "deliver": "discord:123,discord:123,123"
        });
        let targets = delivery_destination(&payload).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].chat_id, "123");
    }
}
