use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omon_gateway::{
    AudioFrame, AudioFrameBuffer, CronJob, CronJobSpec, CronScheduler, CronTaskExecutor, Database,
    OmonError,
};
use serde_json::json;
use tokio::sync::{mpsc, Notify};

struct RecordingExecutor(mpsc::UnboundedSender<String>);

#[async_trait]
impl CronTaskExecutor for RecordingExecutor {
    async fn execute(&self, job: &CronJob) -> Result<Option<String>, OmonError> {
        self.0.send(job.id.clone()).unwrap();
        Ok(Some("finished".into()))
    }
}

struct FailingExecutor;

#[async_trait]
impl CronTaskExecutor for FailingExecutor {
    async fn execute(&self, _job: &CronJob) -> Result<Option<String>, OmonError> {
        Err(OmonError::ToolExecution("intentional failure".into()))
    }
}

struct BlockingExecutor {
    started: mpsc::UnboundedSender<String>,
    release: Arc<Notify>,
}

#[async_trait]
impl CronTaskExecutor for BlockingExecutor {
    async fn execute(&self, job: &CronJob) -> Result<Option<String>, OmonError> {
        self.started.send(job.id.clone()).unwrap();
        self.release.notified().await;
        Ok(Some("finished".into()))
    }
}

#[tokio::test]
async fn cron_scheduler_registers_triggers_and_manages_jobs() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let scheduler = CronScheduler::new(database.pool().clone(), Arc::new(RecordingExecutor(tx)));
    let job = scheduler
        .register(CronJobSpec::new(
            "interval:1s",
            json!({"channel_id": "42", "content": "run"}),
        ))
        .await
        .unwrap();
    let scheduled = job.next_run_at;

    assert!(job.enabled);
    assert_eq!(scheduler.list_active().await.unwrap().len(), 1);
    assert!(scheduler.trigger(&job.id).await.unwrap());
    assert_eq!(rx.recv().await.unwrap(), job.id);
    scheduler.shutdown().await;
    assert_eq!(
        scheduler.get(&job.id).await.unwrap().unwrap().next_run_at,
        scheduled
    );
    assert!(scheduler.pause(&job.id).await.unwrap());
    assert!(scheduler.list_active().await.unwrap().is_empty());
    assert!(scheduler.resume(&job.id).await.unwrap());
    assert!(scheduler.delete(&job.id).await.unwrap());
    assert!(scheduler.get(&job.id).await.unwrap().is_none());
}

#[tokio::test]
async fn background_scheduler_executes_due_interval_and_notifies_channel() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (tx, mut executions) = mpsc::unbounded_channel();
    let scheduler = CronScheduler::with_poll_interval(
        database.pool().clone(),
        Arc::new(RecordingExecutor(tx)),
        Duration::from_secs(60),
    );
    let mut notifications = scheduler.subscribe();
    let job = scheduler
        .register_job(
            "interval:1ms",
            json!({"channel_id": 123456, "notification": "done"}),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE cron_jobs SET next_run_at = ? WHERE id = ?")
        .bind(chrono::Utc::now() - chrono::TimeDelta::seconds(1))
        .bind(&job.id)
        .execute(database.pool())
        .await
        .unwrap();

    scheduler.start().await;
    scheduler.run_due_jobs().await.unwrap();
    let executed = tokio::time::timeout(Duration::from_secs(2), executions.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(executed, job.id);
    let event = tokio::time::timeout(Duration::from_secs(2), notifications.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.channel_id, 123456);
    assert_eq!(event.content, "finished");
    scheduler.shutdown().await;
}

#[tokio::test]
async fn failed_one_shot_due_run_disables_job_and_records_failed_lease() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let scheduler = CronScheduler::new(database.pool().clone(), Arc::new(FailingExecutor));
    let job = scheduler
        .register(CronJobSpec::new(
            "once:2999-01-01T00:00:00Z",
            json!({"content": "run"}),
        ))
        .await
        .unwrap();
    let due = chrono::Utc::now() - chrono::TimeDelta::seconds(1);
    sqlx::query("UPDATE cron_jobs SET next_run_at = ? WHERE id = ?")
        .bind(due)
        .bind(&job.id)
        .execute(database.pool())
        .await
        .unwrap();

    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 1);
    scheduler.shutdown().await;

    let updated = scheduler.get(&job.id).await.unwrap().unwrap();
    assert!(!updated.enabled);
    assert!(updated.next_run_at.is_none());
    let (status, error): (String, Option<String>) = sqlx::query_as(
        "SELECT status, error FROM cron_runs WHERE job_id = ? ORDER BY started_at DESC LIMIT 1",
    )
    .bind(&job.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(status, "failed");
    assert!(error.is_some_and(|value| value.contains("intentional failure")));
}

#[tokio::test]
async fn failed_interval_advances_next_run_and_applies_backoff() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let scheduler = CronScheduler::new(database.pool().clone(), Arc::new(FailingExecutor));
    let job = scheduler
        .register(CronJobSpec::new("interval:1m", json!({"content": "run"})))
        .await
        .unwrap();

    let before_run = chrono::Utc::now();
    let due = before_run - chrono::TimeDelta::seconds(1);
    sqlx::query("UPDATE cron_jobs SET next_run_at = ? WHERE id = ?")
        .bind(due)
        .bind(&job.id)
        .execute(database.pool())
        .await
        .unwrap();

    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 1);
    scheduler.shutdown().await;

    let updated = scheduler.get(&job.id).await.unwrap().unwrap();
    assert!(updated.enabled);
    let next_run = updated.next_run_at.expect("next_run_at should be set");
    // Next run must be strictly in the future (at least 60s from execution time)
    assert!(next_run >= before_run + chrono::TimeDelta::seconds(55));

    // Calling run_due_jobs immediately claims nothing (no tight loop!)
    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 0);
}

#[tokio::test]
async fn repeated_failures_scale_backoff_deterministically() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let scheduler = CronScheduler::new(database.pool().clone(), Arc::new(FailingExecutor));
    let job = scheduler
        .register(CronJobSpec::new("interval:1s", json!({"content": "run"})))
        .await
        .unwrap();

    let wait_for_failure = |attempt: i64| {
        let pool = database.pool().clone();
        let job_id = job.id.clone();
        async move {
            for _ in 0..200 {
                let status: Option<String> = sqlx::query_scalar(
                    "SELECT status FROM cron_runs WHERE job_id = ? AND attempt = ? AND status = 'failed'",
                )
                .bind(&job_id)
                .bind(attempt)
                .fetch_optional(&pool)
                .await
                .unwrap();
                if status.is_some() {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!("timed out waiting for attempt {attempt} to record failure");
        }
    };

    // 1st failure: backoff of 10s (exceeds 1s interval)
    let t1 = chrono::Utc::now();
    sqlx::query("UPDATE cron_jobs SET next_run_at = ? WHERE id = ?")
        .bind(t1 - chrono::TimeDelta::seconds(1))
        .bind(&job.id)
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 1);
    wait_for_failure(1).await;

    let job_after_1 = scheduler.get(&job.id).await.unwrap().unwrap();
    let next1 = job_after_1.next_run_at.unwrap();
    assert!(next1 >= t1 + chrono::TimeDelta::seconds(9));
    assert!(next1 <= t1 + chrono::TimeDelta::seconds(15));

    // 2nd failure: backoff of 20s
    let t2 = chrono::Utc::now();
    sqlx::query("UPDATE cron_jobs SET next_run_at = ? WHERE id = ?")
        .bind(t2 - chrono::TimeDelta::seconds(1))
        .bind(&job.id)
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 1);
    wait_for_failure(2).await;

    let job_after_2 = scheduler.get(&job.id).await.unwrap().unwrap();
    let next2 = job_after_2.next_run_at.unwrap();
    assert!(next2 >= t2 + chrono::TimeDelta::seconds(19));
    assert!(next2 <= t2 + chrono::TimeDelta::seconds(25));

    // 3rd failure: backoff of 40s
    let t3 = chrono::Utc::now();
    sqlx::query("UPDATE cron_jobs SET next_run_at = ? WHERE id = ?")
        .bind(t3 - chrono::TimeDelta::seconds(1))
        .bind(&job.id)
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 1);
    wait_for_failure(3).await;

    let job_after_3 = scheduler.get(&job.id).await.unwrap().unwrap();
    let next3 = job_after_3.next_run_at.unwrap();
    assert!(next3 >= t3 + chrono::TimeDelta::seconds(39));
    assert!(next3 <= t3 + chrono::TimeDelta::seconds(45));

    // Immediate run_due_jobs claims 0 because next_run_at is far in the future
    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 0);

    scheduler.shutdown().await;
}

#[tokio::test]
async fn active_lease_blocks_duplicate_claims_until_success_commits_one_shot() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let scheduler = CronScheduler::new(
        database.pool().clone(),
        Arc::new(BlockingExecutor {
            started: started_tx,
            release: release.clone(),
        }),
    );
    let job = scheduler
        .register(CronJobSpec::new(
            "once:2999-01-01T00:00:00Z",
            json!({"content": "run"}),
        ))
        .await
        .unwrap();
    let due = chrono::Utc::now() - chrono::TimeDelta::seconds(1);
    sqlx::query("UPDATE cron_jobs SET next_run_at = ? WHERE id = ?")
        .bind(due)
        .bind(&job.id)
        .execute(database.pool())
        .await
        .unwrap();

    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 1);
    assert_eq!(started_rx.recv().await.unwrap(), job.id);
    assert_eq!(scheduler.run_due_jobs().await.unwrap(), 0);
    assert!(!scheduler.trigger(&job.id).await.unwrap());

    let during = scheduler.get(&job.id).await.unwrap().unwrap();
    assert!(during.enabled);
    assert!(during.next_run_at.is_some());
    let running: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cron_runs WHERE job_id = ? AND status = 'running'",
    )
    .bind(&job.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(running, 1);

    release.notify_waiters();
    scheduler.shutdown().await;

    let completed = scheduler.get(&job.id).await.unwrap().unwrap();
    assert!(!completed.enabled);
    assert!(completed.next_run_at.is_none());
    let (status, completed_at): (String, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT status, completed_at FROM cron_runs WHERE job_id = ? ORDER BY started_at DESC LIMIT 1",
    )
    .bind(&job.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(status, "succeeded");
    assert!(completed_at.is_some());
}

#[test]
fn audio_frame_buffer_serializes_without_losing_pcm_samples() {
    let first = AudioFrame::pcm(99, Some(7), 1, vec![-32_768, -1, 0, 1, 32_767]);
    let second = AudioFrame::pcm(99, Some(8), 2, vec![10, 20]);
    let mut buffer = AudioFrameBuffer::new(2);
    assert!(buffer.push(first.clone()).is_none());
    assert!(buffer.push(second.clone()).is_none());

    let encoded = buffer.serialized().unwrap();
    let mut decoded = AudioFrameBuffer::deserialize(&encoded, 2).unwrap();
    assert_eq!(decoded.pop(), Some(first));
    assert_eq!(decoded.pop(), Some(second));
    assert!(decoded.is_empty());
}
