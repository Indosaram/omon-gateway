use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omon_gateway::{
    AgentRunner, Database, DeliveryLedgerService, InboundEvent, MultiplexerConfig, OmonError,
    SessionContext, SessionKey, SessionMultiplexer,
};
use tokio::sync::{mpsc, oneshot, Barrier, Mutex};

fn session(user: &str) -> SessionKey {
    SessionKey::new("discord", Some("guild"), "channel", None::<String>, user)
}

#[tokio::test]
async fn routes_multiple_sessions_in_parallel() {
    struct ParallelRunner {
        barrier: Barrier,
        completed: mpsc::UnboundedSender<String>,
    }

    #[async_trait]
    impl AgentRunner for ParallelRunner {
        async fn run(
            &self,
            _session: &mut SessionContext,
            event: InboundEvent,
        ) -> Result<(), OmonError> {
            self.barrier.wait().await;
            self.completed
                .send(event.content)
                .expect("test receiver should remain open");
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let runner = Arc::new(ParallelRunner {
        barrier: Barrier::new(8),
        completed: completed_tx,
    });
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        runner,
        MultiplexerConfig::default(),
    );

    let mut routes = Vec::new();
    for index in 0..8 {
        let multiplexer = multiplexer.clone();
        routes.push(tokio::spawn(async move {
            multiplexer
                .route(InboundEvent::message(
                    session(&format!("user-{index}")),
                    format!("message-{index}"),
                    format!("event-{index}"),
                ))
                .await
                .unwrap();
        }));
    }
    for route in routes {
        route.await.unwrap();
    }

    let mut received = Vec::new();
    while received.len() < 8 {
        received.push(
            tokio::time::timeout(Duration::from_secs(2), completed_rx.recv())
                .await
                .expect("parallel actors should all complete")
                .unwrap(),
        );
    }
    assert_eq!(multiplexer.active_sessions(), 8);
}

#[tokio::test]
async fn handles_events_sequentially_within_one_session() {
    struct SequentialRunner {
        active: Mutex<HashMap<String, usize>>,
        completed: mpsc::UnboundedSender<String>,
    }

    #[async_trait]
    impl AgentRunner for SequentialRunner {
        async fn run(
            &self,
            session: &mut SessionContext,
            event: InboundEvent,
        ) -> Result<(), OmonError> {
            let mut active = self.active.lock().await;
            let count = active.entry(session.key.storage_key()).or_default();
            *count += 1;
            assert_eq!(*count, 1, "same-session executions overlapped");
            drop(active);
            tokio::task::yield_now().await;
            let mut active = self.active.lock().await;
            *active.get_mut(&session.key.storage_key()).unwrap() -= 1;
            drop(active);
            self.completed
                .send(event.content)
                .expect("test receiver should remain open");
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        Arc::new(SequentialRunner {
            active: Mutex::new(HashMap::new()),
            completed: completed_tx,
        }),
        MultiplexerConfig::default(),
    );
    let key = session("one-user");

    let mut observed = Vec::new();
    for index in 0..20 {
        multiplexer
            .route(InboundEvent::message(
                key.clone(),
                format!("message-{index}"),
                index.to_string(),
            ))
            .await
            .unwrap();
        observed.push(
            tokio::time::timeout(Duration::from_secs(2), completed_rx.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(
        observed,
        (0..20).map(|index| index.to_string()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn events_arriving_during_running_turn_are_queued_and_processed_in_order() {
    struct QueuedRunner {
        started: mpsc::UnboundedSender<String>,
        first_release: Mutex<Option<oneshot::Receiver<()>>>,
        completed: mpsc::UnboundedSender<String>,
    }

    #[async_trait]
    impl AgentRunner for QueuedRunner {
        async fn run(
            &self,
            session: &mut SessionContext,
            event: InboundEvent,
        ) -> Result<(), OmonError> {
            self.started.send(event.content.clone()).unwrap();
            if event.content == "first" {
                let release = self.first_release.lock().await.take().unwrap();
                let _ = release.await;
                session.state.metadata.insert("turn_1".into(), true.into());
            } else if event.content == "second" {
                session.state.metadata.insert("turn_2".into(), true.into());
            } else if event.content == "third" {
                session.state.metadata.insert("turn_3".into(), true.into());
            }
            self.completed.send(event.content).unwrap();
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let key = session("queue-user");
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let (release_tx, release_rx) = oneshot::channel();
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        Arc::new(QueuedRunner {
            started: started_tx,
            first_release: Mutex::new(Some(release_rx)),
            completed: completed_tx,
        }),
        MultiplexerConfig::default(),
    );

    multiplexer
        .route(InboundEvent::message(key.clone(), "queue-1", "first"))
        .await
        .unwrap();
    assert_eq!(started_rx.recv().await.as_deref(), Some("first"));

    // Route second and third events while the first turn is still in flight
    multiplexer
        .route(InboundEvent::message(key.clone(), "queue-2", "second"))
        .await
        .unwrap();
    multiplexer
        .route(InboundEvent::message(key.clone(), "queue-3", "third"))
        .await
        .unwrap();

    // Release turn 1
    release_tx.send(()).unwrap();

    assert_eq!(completed_rx.recv().await.as_deref(), Some("first"));
    assert_eq!(started_rx.recv().await.as_deref(), Some("second"));
    assert_eq!(completed_rx.recv().await.as_deref(), Some("second"));
    assert_eq!(started_rx.recv().await.as_deref(), Some("third"));
    assert_eq!(completed_rx.recv().await.as_deref(), Some("third"));

    let messages: Vec<String> =
        sqlx::query_scalar("SELECT content FROM messages WHERE session_key = ? ORDER BY sequence")
            .bind(key.storage_key())
            .fetch_all(database.pool())
            .await
            .unwrap();
    assert_eq!(messages, vec!["first", "second", "third"]);
}

#[tokio::test]
async fn stop_immediately_cancels_the_active_turn() {
    struct StoppableRunner {
        started: mpsc::UnboundedSender<()>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
        dropped: Arc<AtomicBool>,
        completed: Arc<AtomicBool>,
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl AgentRunner for StoppableRunner {
        async fn run(
            &self,
            _session: &mut SessionContext,
            _event: InboundEvent,
        ) -> Result<(), OmonError> {
            let _drop_signal = DropSignal(self.dropped.clone());
            self.started.send(()).unwrap();
            let release = self.release.lock().await.take().unwrap();
            let _ = release.await;
            self.completed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let key = session("stop-user");
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (_release_tx, release_rx) = oneshot::channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        Arc::new(StoppableRunner {
            started: started_tx,
            release: Mutex::new(Some(release_rx)),
            dropped: dropped.clone(),
            completed: completed.clone(),
        }),
        MultiplexerConfig::default(),
    );

    multiplexer
        .route(InboundEvent::message(key.clone(), "stop-1", "long turn"))
        .await
        .unwrap();
    started_rx.recv().await.unwrap();

    assert!(multiplexer.stop(&key).await.unwrap());
    assert!(dropped.load(Ordering::SeqCst));
    assert!(!completed.load(Ordering::SeqCst));
    assert!(!multiplexer.stop(&key).await.unwrap());
}

#[tokio::test]
async fn stop_cancels_active_turn_and_clears_queued_events() {
    struct QueuedStoppableRunner {
        started: mpsc::UnboundedSender<String>,
        first_release: Mutex<Option<oneshot::Receiver<()>>>,
        completed: mpsc::UnboundedSender<String>,
    }

    #[async_trait]
    impl AgentRunner for QueuedStoppableRunner {
        async fn run(
            &self,
            _session: &mut SessionContext,
            event: InboundEvent,
        ) -> Result<(), OmonError> {
            self.started.send(event.content.clone()).unwrap();
            if event.content == "turn-1" {
                let release = self.first_release.lock().await.take().unwrap();
                let _ = release.await;
            }
            self.completed.send(event.content).unwrap();
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let key = session("stop-queue-user");
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let (_release_tx, release_rx) = oneshot::channel();
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        Arc::new(QueuedStoppableRunner {
            started: started_tx,
            first_release: Mutex::new(Some(release_rx)),
            completed: completed_tx,
        }),
        MultiplexerConfig::default(),
    );

    multiplexer
        .route(InboundEvent::message(key.clone(), "turn-1", "turn-1"))
        .await
        .unwrap();
    assert_eq!(started_rx.recv().await.as_deref(), Some("turn-1"));

    // Queue turn 2 and turn 3 behind turn 1
    multiplexer
        .route(InboundEvent::message(key.clone(), "turn-2", "turn-2"))
        .await
        .unwrap();
    multiplexer
        .route(InboundEvent::message(key.clone(), "turn-3", "turn-3"))
        .await
        .unwrap();

    // Stop session: cancels active turn 1 and drains queued turns
    assert!(multiplexer.stop(&key).await.unwrap());

    // Neither turn-1, turn-2, nor turn-3 should have completed successfully
    assert!(completed_rx.try_recv().is_err());
    // Turn 2 and 3 should not have started
    assert!(started_rx.try_recv().is_err());
}

#[tokio::test]
async fn scale_to_zero_evicts_and_flushes_idle_sessions() {
    struct StatefulRunner {
        completed: mpsc::UnboundedSender<()>,
    }

    #[async_trait]
    impl AgentRunner for StatefulRunner {
        async fn run(
            &self,
            session: &mut SessionContext,
            _event: InboundEvent,
        ) -> Result<(), OmonError> {
            session.state.metadata.insert("flushed".into(), true.into());
            self.completed.send(()).unwrap();
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let key = session("idle-user");
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        Arc::new(StatefulRunner {
            completed: completed_tx,
        }),
        MultiplexerConfig {
            idle_timeout: Duration::ZERO,
            gc_interval: Duration::from_secs(60),
        },
    );
    multiplexer
        .route(InboundEvent::message(key.clone(), "idle-message", "hello"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), completed_rx.recv())
        .await
        .unwrap()
        .unwrap();

    // With a zero idle timeout the session becomes collectable as soon as the
    // actor finishes the turn and returns to idle. Poll deterministically until
    // it is evicted (bounded) instead of relying on a paused-clock/real-clock
    // interplay, which raced under full-suite load.
    let mut evicted = 0;
    for _ in 0..200 {
        evicted = multiplexer.collect_garbage().await.unwrap();
        if evicted == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(evicted, 1, "idle session should be garbage-collected");
    assert!(!multiplexer.contains_session(&key));
    let state: String = sqlx::query_scalar("SELECT state_json FROM sessions WHERE session_key = ?")
        .bind(key.storage_key())
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&state).unwrap()["metadata"]["flushed"],
        true
    );
}

#[tokio::test]
async fn route_reports_actor_startup_failure_instead_of_acknowledging_a_lost_event() {
    struct NeverRunner(AtomicBool);

    #[async_trait]
    impl AgentRunner for NeverRunner {
        async fn run(
            &self,
            _session: &mut SessionContext,
            _event: InboundEvent,
        ) -> Result<(), OmonError> {
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let key = session("corrupt-state-user");
    sqlx::query(
        "INSERT INTO sessions (
            session_key, platform, guild_id, channel_id, thread_id, user_id,
            state_json, created_at, updated_at
         ) VALUES (?, 'discord', 'guild', 'channel', NULL, 'corrupt-state-user',
                   '{not-json', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(key.storage_key())
    .execute(database.pool())
    .await
    .unwrap();

    let runner = Arc::new(NeverRunner(AtomicBool::new(false)));
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        runner.clone(),
        MultiplexerConfig::default(),
    );
    let result = multiplexer
        .route(InboundEvent::message(
            key,
            "message-corrupt",
            "must not vanish",
        ))
        .await;

    assert!(result.is_err(), "route must surface actor load failure");
    assert!(!runner.0.load(Ordering::SeqCst));
    let persisted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(
        persisted, 0,
        "failed startup must not falsely persist/execute"
    );
}

#[tokio::test]
async fn dropping_multiplexer_releases_actor_cycle_and_flushes_dirty_state() {
    struct ReclaimRunner {
        completed: mpsc::UnboundedSender<()>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ReclaimRunner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl AgentRunner for ReclaimRunner {
        async fn run(
            &self,
            session: &mut SessionContext,
            _event: InboundEvent,
        ) -> Result<(), OmonError> {
            session
                .state
                .metadata
                .insert("drop_flushed".into(), true.into());
            self.completed.send(()).unwrap();
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let key = session("drop-user");
    let dropped = Arc::new(AtomicBool::new(false));
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let runner = Arc::new(ReclaimRunner {
        completed: completed_tx,
        dropped: dropped.clone(),
    });
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        runner.clone(),
        MultiplexerConfig::default(),
    );

    multiplexer
        .route(InboundEvent::message(key.clone(), "drop-message", "hello"))
        .await
        .unwrap();
    completed_rx.recv().await.unwrap();
    drop(runner);
    drop(multiplexer);

    tokio::time::timeout(Duration::from_secs(2), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor-owned runner Arc should be released when multiplexer drops");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state: String =
                sqlx::query_scalar("SELECT state_json FROM sessions WHERE session_key = ?")
                    .bind(key.storage_key())
                    .fetch_one(database.pool())
                    .await
                    .unwrap();
            if serde_json::from_str::<serde_json::Value>(&state).unwrap()["metadata"]
                ["drop_flushed"]
                == true
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dirty session state should flush during graceful actor shutdown");
}

#[tokio::test]
async fn gc_does_not_evict_an_actor_with_an_active_turn() {
    struct BlockingRunner {
        entered: mpsc::UnboundedSender<()>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
        completed: mpsc::UnboundedSender<()>,
    }

    #[async_trait]
    impl AgentRunner for BlockingRunner {
        async fn run(
            &self,
            _session: &mut SessionContext,
            _event: InboundEvent,
        ) -> Result<(), OmonError> {
            self.entered.send(()).unwrap();
            let release = self.release.lock().await.take().unwrap();
            let _ = release.await;
            self.completed.send(()).unwrap();
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let key = session("gc-race-user");
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let (release_tx, release_rx) = oneshot::channel();
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        Arc::new(BlockingRunner {
            entered: entered_tx,
            release: Mutex::new(Some(release_rx)),
            completed: completed_tx,
        }),
        MultiplexerConfig {
            idle_timeout: Duration::ZERO,
            gc_interval: Duration::from_secs(60),
        },
    );

    multiplexer
        .route(InboundEvent::message(key.clone(), "race-1", "first"))
        .await
        .unwrap();
    entered_rx.recv().await.unwrap();

    assert_eq!(multiplexer.collect_garbage().await.unwrap(), 0);
    assert!(multiplexer.contains_session(&key));

    release_tx.send(()).unwrap();
    completed_rx.recv().await.unwrap();
}

#[tokio::test]
async fn delivery_ledger_deduplicates_concurrent_claims_and_records_latency() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let ledger = DeliveryLedgerService::new(database.pool().clone());
    let event = InboundEvent::message(session("ledger-user"), "platform-message-1", "hello");

    let (first, second) = tokio::join!(
        ledger.record_incoming(&event),
        ledger.record_incoming(&event)
    );
    assert_ne!(first.unwrap(), second.unwrap());
    assert!(ledger.is_duplicate("platform-message-1").await.unwrap());

    ledger.mark_delivered("platform-message-1").await.unwrap();
    let entry = ledger.get("platform-message-1").await.unwrap().unwrap();
    assert_eq!(entry.status, "delivered");
    assert!(entry.completed_at.is_some());
    assert!(entry.processing_latency_ms.unwrap() >= 0);
}

#[tokio::test]
async fn transcript_level_inbound_dedup_skips_duplicate_platform_message_id() {
    struct CountingRunner {
        runs: std::sync::atomic::AtomicUsize,
        ran_tx: tokio::sync::mpsc::UnboundedSender<()>,
    }

    #[async_trait]
    impl AgentRunner for CountingRunner {
        async fn run(
            &self,
            _session: &mut SessionContext,
            _event: InboundEvent,
        ) -> Result<(), OmonError> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            let _ = self.ran_tx.send(());
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (ran_tx, mut ran_rx) = tokio::sync::mpsc::unbounded_channel();
    let runner = Arc::new(CountingRunner {
        runs: std::sync::atomic::AtomicUsize::new(0),
        ran_tx,
    });
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        runner.clone(),
        MultiplexerConfig::default(),
    );

    let key = session("dedup-user");
    let event1 = InboundEvent::message(key.clone(), "plat-msg-unique-1", "first prompt");
    multiplexer.route(event1).await.unwrap();

    // Wait for the first turn to actually run (bounded), instead of a fixed sleep.
    tokio::time::timeout(Duration::from_secs(5), ran_rx.recv())
        .await
        .expect("first event should run within timeout")
        .expect("runner channel closed");
    assert_eq!(runner.runs.load(Ordering::SeqCst), 1);

    // Send another event with the exact same platform_message_id (simulating a
    // replayed webhook/gateway event after ledger eviction).
    let event2 = InboundEvent::message(key.clone(), "plat-msg-unique-1", "duplicate prompt");
    multiplexer.route(event2).await.unwrap();

    // The duplicate must NOT run: assert no run signal arrives within a bounded window.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), ran_rx.recv())
            .await
            .is_err(),
        "duplicate platform_message_id must not trigger a second run"
    );
    assert_eq!(runner.runs.load(Ordering::SeqCst), 1);

    // Send a new event with a different platform_message_id.
    let event3 = InboundEvent::message(key.clone(), "plat-msg-unique-2", "second prompt");
    multiplexer.route(event3).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), ran_rx.recv())
        .await
        .expect("new event should run within timeout")
        .expect("runner channel closed");
    assert_eq!(runner.runs.load(Ordering::SeqCst), 2);
}
