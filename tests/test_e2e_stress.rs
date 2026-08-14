use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omon_gateway::{
    AgentRunner, Database, DeliveryLedgerService, InboundEvent, MultiplexerConfig, OmonError,
    SessionContext, SessionKey, SessionMultiplexer,
};
use tokio::sync::{mpsc, Barrier, Mutex};

const SESSION_COUNT: usize = 20;
const MESSAGES_PER_SESSION: usize = 6;
const MESSAGE_COUNT: usize = SESSION_COUNT * MESSAGES_PER_SESSION;

fn discord_session(index: usize) -> SessionKey {
    match index % 3 {
        0 => SessionKey::new(
            "discord",
            None::<String>,
            format!("dm-{index}"),
            None::<String>,
            format!("user-{index}"),
        ),
        1 => SessionKey::new(
            "discord",
            Some(format!("guild-{index}")),
            format!("channel-{index}"),
            None::<String>,
            format!("user-{index}"),
        ),
        _ => SessionKey::new(
            "discord",
            Some(format!("guild-{index}")),
            format!("channel-{index}"),
            Some(format!("thread-{index}")),
            format!("user-{index}"),
        ),
    }
}

fn update_max(maximum: &AtomicUsize, candidate: usize) {
    let mut current = maximum.load(Ordering::SeqCst);
    while candidate > current {
        match maximum.compare_exchange(current, candidate, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

#[tokio::test]
async fn routes_120_messages_in_order_per_session_and_in_parallel_across_sessions() {
    struct StressRunner {
        first_message_rendezvous: Barrier,
        started: mpsc::UnboundedSender<SessionKey>,
        completed: mpsc::UnboundedSender<(SessionKey, usize)>,
        active_sessions: Mutex<HashSet<SessionKey>>,
        global_active: AtomicUsize,
        maximum_global_active: AtomicUsize,
    }

    #[async_trait]
    impl AgentRunner for StressRunner {
        async fn run(
            &self,
            session: &mut SessionContext,
            event: InboundEvent,
        ) -> Result<(), OmonError> {
            let sequence = event
                .content
                .parse::<usize>()
                .expect("stress event content should contain its sequence number");

            {
                let mut active = self.active_sessions.lock().await;
                assert!(
                    active.insert(session.key.clone()),
                    "same-session executions overlapped for {}",
                    session.key
                );
            }
            let active = self.global_active.fetch_add(1, Ordering::SeqCst) + 1;
            update_max(&self.maximum_global_active, active);

            if sequence == 0 {
                self.started
                    .send(session.key.clone())
                    .expect("start observer should remain open");
                self.first_message_rendezvous.wait().await;
            }

            assert!(
                self.active_sessions.lock().await.remove(&session.key),
                "active session should be registered"
            );
            self.global_active.fetch_sub(1, Ordering::SeqCst);
            self.completed
                .send((session.key.clone(), sequence))
                .expect("completion observer should remain open");
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let runner = Arc::new(StressRunner {
        first_message_rendezvous: Barrier::new(SESSION_COUNT + 1),
        started: started_tx,
        completed: completed_tx,
        active_sessions: Mutex::new(HashSet::new()),
        global_active: AtomicUsize::new(0),
        maximum_global_active: AtomicUsize::new(0),
    });
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        runner.clone(),
        MultiplexerConfig::default(),
    );
    let route_rendezvous = Arc::new(Barrier::new(SESSION_COUNT + 1));

    let mut producers = Vec::with_capacity(SESSION_COUNT);
    for session_index in 0..SESSION_COUNT {
        let multiplexer = multiplexer.clone();
        let route_rendezvous = route_rendezvous.clone();
        producers.push(tokio::spawn(async move {
            let key = discord_session(session_index);
            route_rendezvous.wait().await;
            for sequence in 0..MESSAGES_PER_SESSION {
                multiplexer
                    .route(InboundEvent::message(
                        key.clone(),
                        format!("message-{session_index}-{sequence}"),
                        sequence.to_string(),
                    ))
                    .await
                    .unwrap();
            }
        }));
    }
    route_rendezvous.wait().await;
    for producer in producers {
        producer.await.unwrap();
    }

    let started = tokio::time::timeout(Duration::from_secs(5), async {
        let mut sessions = HashSet::with_capacity(SESSION_COUNT);
        while sessions.len() < SESSION_COUNT {
            sessions.insert(
                started_rx
                    .recv()
                    .await
                    .expect("runner should report starts"),
            );
        }
        sessions
    })
    .await
    .expect("all distinct sessions should enter the runner concurrently");
    assert_eq!(started.len(), SESSION_COUNT);
    assert_eq!(
        runner.maximum_global_active.load(Ordering::SeqCst),
        SESSION_COUNT,
        "every distinct session should be active before the rendezvous releases"
    );
    runner.first_message_rendezvous.wait().await;

    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        let mut by_session: HashMap<SessionKey, Vec<usize>> = HashMap::new();
        for _ in 0..MESSAGE_COUNT {
            let (key, sequence) = completed_rx
                .recv()
                .await
                .expect("runner should report every completion");
            by_session.entry(key).or_default().push(sequence);
        }
        by_session
    })
    .await
    .expect("all stress messages should complete");

    assert_eq!(observed.len(), SESSION_COUNT);
    let expected: Vec<_> = (0..MESSAGES_PER_SESSION).collect();
    for key in (0..SESSION_COUNT).map(discord_session) {
        assert_eq!(
            observed.get(&key),
            Some(&expected),
            "messages must remain sequential and in-order for {key}"
        );
    }
    assert_eq!(multiplexer.active_sessions(), SESSION_COUNT);
}

#[tokio::test]
async fn delivery_ledger_is_idempotent_during_a_128_way_duplicate_storm() {
    const DUPLICATES: usize = 128;

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let ledger = DeliveryLedgerService::new(database.pool().clone());
    let event = InboundEvent::message(
        discord_session(0),
        "duplicate-platform-message",
        "duplicate payload",
    );
    let rendezvous = Arc::new(Barrier::new(DUPLICATES + 1));
    let mut claims = Vec::with_capacity(DUPLICATES);

    for _ in 0..DUPLICATES {
        let ledger = ledger.clone();
        let event = event.clone();
        let rendezvous = rendezvous.clone();
        claims.push(tokio::spawn(async move {
            rendezvous.wait().await;
            ledger.record_incoming(&event).await.unwrap()
        }));
    }
    rendezvous.wait().await;

    let mut accepted = 0;
    for claim in claims {
        accepted += usize::from(claim.await.unwrap());
    }
    assert_eq!(accepted, 1, "exactly one duplicate claim must win");
    assert!(ledger
        .is_duplicate("duplicate-platform-message")
        .await
        .unwrap());

    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM delivery_ledger WHERE message_id = 'duplicate-platform-message'",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(rows, 1, "the ledger must contain one durable delivery row");
}

#[tokio::test]
async fn scale_to_zero_evicts_all_sessions_during_high_turnover() {
    const SESSIONS_PER_WAVE: usize = 80;
    const WAVES: usize = 2;

    struct TurnoverRunner {
        completed: mpsc::UnboundedSender<SessionKey>,
    }

    #[async_trait]
    impl AgentRunner for TurnoverRunner {
        async fn run(
            &self,
            session: &mut SessionContext,
            _event: InboundEvent,
        ) -> Result<(), OmonError> {
            session
                .state
                .metadata
                .insert("flushed".into(), serde_json::Value::Bool(true));
            self.completed
                .send(session.key.clone())
                .expect("completion observer should remain open");
            Ok(())
        }
    }

    let database = Database::connect("sqlite::memory:").await.unwrap();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let multiplexer = SessionMultiplexer::new(
        database.pool().clone(),
        Arc::new(TurnoverRunner {
            completed: completed_tx,
        }),
        MultiplexerConfig {
            idle_timeout: Duration::ZERO,
            gc_interval: Duration::from_secs(60 * 60),
        },
    );

    for wave in 0..WAVES {
        let mut routes = Vec::with_capacity(SESSIONS_PER_WAVE);
        for index in 0..SESSIONS_PER_WAVE {
            let multiplexer = multiplexer.clone();
            routes.push(tokio::spawn(async move {
                let unique_index = wave * SESSIONS_PER_WAVE + index;
                multiplexer
                    .route(InboundEvent::message(
                        discord_session(unique_index),
                        format!("turnover-{wave}-{index}"),
                        "turnover",
                    ))
                    .await
                    .unwrap();
            }));
        }
        for route in routes {
            route.await.unwrap();
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..SESSIONS_PER_WAVE {
                completed_rx
                    .recv()
                    .await
                    .expect("every turnover session should complete");
            }
        })
        .await
        .expect("turnover wave should finish");
        assert_eq!(multiplexer.active_sessions(), SESSIONS_PER_WAVE);

        assert_eq!(
            multiplexer.collect_garbage().await.unwrap(),
            SESSIONS_PER_WAVE,
            "every idle session in wave {wave} should be evicted"
        );
        assert_eq!(multiplexer.active_sessions(), 0);
    }

    let persisted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sessions WHERE json_extract(state_json, '$.metadata.flushed') = 1",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(persisted, (SESSIONS_PER_WAVE * WAVES) as i64);
}
