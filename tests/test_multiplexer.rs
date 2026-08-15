use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omon_gateway::{
    AgentRunner, Database, DeliveryLedgerService, InboundEvent, MultiplexerConfig, OmonError,
    SessionContext, SessionKey, SessionMultiplexer,
};
use tokio::sync::{mpsc, Barrier, Mutex, Semaphore};

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

    for index in 0..20 {
        multiplexer
            .route(InboundEvent::message(
                key.clone(),
                format!("message-{index}"),
                index.to_string(),
            ))
            .await
            .unwrap();
    }

    let mut observed = Vec::new();
    while observed.len() < 20 {
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
            idle_timeout: Duration::from_millis(1),
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
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(2)).await;

    assert_eq!(multiplexer.collect_garbage().await.unwrap(), 1);
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
async fn route_waits_while_gc_retires_an_actor_instead_of_enqueueing_behind_eviction() {
    struct BlockingRunner {
        entered: mpsc::UnboundedSender<String>,
        completed: mpsc::UnboundedSender<String>,
        release_first: Arc<Semaphore>,
    }

    #[async_trait]
    impl AgentRunner for BlockingRunner {
        async fn run(
            &self,
            _session: &mut SessionContext,
            event: InboundEvent,
        ) -> Result<(), OmonError> {
            self.entered.send(event.content.clone()).unwrap();
            if event.content == "first" {
                self.release_first.acquire().await.unwrap().forget();
            }
            self.completed.send(event.content).unwrap();
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let key = session("gc-race-user");
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let release_first = Arc::new(Semaphore::new(0));
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        Arc::new(BlockingRunner {
            entered: entered_tx,
            completed: completed_tx,
            release_first: release_first.clone(),
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
    assert_eq!(entered_rx.recv().await.as_deref(), Some("first"));

    let gc = {
        let multiplexer = multiplexer.clone();
        tokio::spawn(async move { multiplexer.collect_garbage().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut second_route = {
        let multiplexer = multiplexer.clone();
        let key = key.clone();
        tokio::spawn(async move {
            multiplexer
                .route(InboundEvent::message(key, "race-2", "second"))
                .await
        })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut second_route)
            .await
            .is_err(),
        "route must not acknowledge an event while its actor is retiring"
    );

    release_first.add_permits(1);
    assert_eq!(completed_rx.recv().await.as_deref(), Some("first"));
    assert_eq!(gc.await.unwrap().unwrap(), 1);
    second_route.await.unwrap().unwrap();
    assert_eq!(entered_rx.recv().await.as_deref(), Some("second"));
    assert_eq!(completed_rx.recv().await.as_deref(), Some("second"));
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
