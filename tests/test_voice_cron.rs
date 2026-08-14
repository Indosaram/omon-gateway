use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omon_gateway::{
    AudioFrame, AudioFrameBuffer, CronJob, CronJobSpec, CronScheduler, CronTaskExecutor, Database,
    OmonError,
};
use serde_json::json;
use tokio::sync::mpsc;

struct RecordingExecutor(mpsc::UnboundedSender<String>);

#[async_trait]
impl CronTaskExecutor for RecordingExecutor {
    async fn execute(&self, job: &CronJob) -> Result<Option<String>, OmonError> {
        self.0.send(job.id.clone()).unwrap();
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

    assert!(job.enabled);
    assert_eq!(scheduler.list_active().await.unwrap().len(), 1);
    assert!(scheduler.trigger(&job.id).await.unwrap());
    assert_eq!(rx.recv().await.unwrap(), job.id);
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
