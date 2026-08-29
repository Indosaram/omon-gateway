use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use omon_gateway::{
    AgentBackend, InboundEvent, OmoBackend, OmoBackendConfig, OutboundAction, OutboundDispatcher,
    SessionContext, SessionKey, StreamChunk,
};
use parking_lot::Mutex as ParkingMutex;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

struct CapturingDispatcher {
    actions: ParkingMutex<Vec<OutboundAction>>,
}

impl CapturingDispatcher {
    fn new() -> Self {
        Self {
            actions: ParkingMutex::new(Vec::new()),
        }
    }

    fn stream_chunks(&self) -> Vec<StreamChunk> {
        self.actions
            .lock()
            .iter()
            .filter_map(|action| match action {
                OutboundAction::Stream { chunk, .. } => Some(chunk.clone()),
                _ => None,
            })
            .collect()
    }
}

#[async_trait]
impl OutboundDispatcher for CapturingDispatcher {
    async fn dispatch(&self, action: OutboundAction) -> omon_gateway::Result<()> {
        self.actions.lock().push(action);
        Ok(())
    }
}

struct FakeAppServer {
    pub port: u16,
    pub thread_start_count: Arc<AtomicUsize>,
    pub received_developer_instructions: Arc<ParkingMutex<Option<String>>>,
    pub received_model: Arc<ParkingMutex<Option<String>>>,
    pub turn_threads: Arc<ParkingMutex<Vec<String>>>,
    pub approval_responses: Arc<ParkingMutex<Vec<Value>>>,
    pub conn_count: Arc<AtomicUsize>,
    pub interrupts: Arc<ParkingMutex<Vec<Value>>>,
}

impl FakeAppServer {
    async fn spawn() -> Self {
        Self::spawn_inner(false, false, false).await
    }

    async fn spawn_with_activity() -> Self {
        Self::spawn_inner(true, false, false).await
    }

    /// Drop the client's first connection abruptly: exercises the
    /// one-shot transport retry.
    async fn spawn_with_first_connection_drop() -> Self {
        Self::spawn_inner(false, true, false).await
    }

    /// Stream deltas forever (well, 5s) without turn/completed: exercises
    /// the whole-turn deadline + turn/interrupt.
    async fn spawn_with_looping_deltas() -> Self {
        Self::spawn_inner(false, false, true).await
    }

    async fn spawn_inner(
        emit_activity: bool,
        drop_first_connection: bool,
        loop_deltas: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind free port");
        let port = listener.local_addr().expect("local addr").port();

        let thread_start_count = Arc::new(AtomicUsize::new(0));
        let received_developer_instructions = Arc::new(ParkingMutex::new(None));
        let received_model = Arc::new(ParkingMutex::new(None));
        let turn_threads = Arc::new(ParkingMutex::new(Vec::new()));
        let approval_responses = Arc::new(ParkingMutex::new(Vec::new()));
        let emit_activity_flag = Arc::new(AtomicBool::new(emit_activity));
        let conn_count = Arc::new(AtomicUsize::new(0));
        let drop_flag = drop_first_connection;
        let interrupts = Arc::new(ParkingMutex::new(Vec::new()));

        let tsc = thread_start_count.clone();
        let rdi = received_developer_instructions.clone();
        let rm = received_model.clone();
        let tt = turn_threads.clone();
        let ar = approval_responses.clone();
        let ea = emit_activity_flag.clone();
        let cc = conn_count.clone();
        let drop_first = drop_flag;
        let it = interrupts.clone();
        let looping = loop_deltas;

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let conn_num = cc.fetch_add(1, Ordering::SeqCst) + 1;
                if drop_first && conn_num == 1 {
                    // Abruptly drop the first connection (no WS close frame):
                    // the client must see a transport error and retry once.
                    drop(stream);
                    continue;
                }
                let tsc = tsc.clone();
                let rdi = rdi.clone();
                let rm = rm.clone();
                let tt = tt.clone();
                let ar = ar.clone();
                let ea = ea.clone();
                let it = it.clone();

                tokio::spawn(async move {
                    let ws = match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(_) => return,
                    };
                    let (mut ws_sink, mut ws_stream) = ws.split();

                    let (outgoing_tx, mut outgoing_rx) = tokio::sync::mpsc::channel::<Message>(32);

                    tokio::spawn(async move {
                        while let Some(msg) = outgoing_rx.recv().await {
                            if ws_sink.send(msg).await.is_err() {
                                break;
                            }
                        }
                    });

                    while let Some(Ok(msg)) = ws_stream.next().await {
                        if let Message::Text(text) = msg {
                            let parsed: Value = match serde_json::from_str(text.as_str()) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };

                            // Check if client is responding to a server request (e.g. approval denial)
                            if parsed.get("method").is_none() && parsed.get("id").is_some() {
                                ar.lock().push(parsed.clone());
                                continue;
                            }

                            let method = parsed.get("method").and_then(Value::as_str).unwrap_or("");
                            let id = parsed.get("id").and_then(Value::as_u64).unwrap_or(0);

                            match method {
                                "turn/interrupt" => {
                                    it.lock().push(parsed.clone());
                                }
                                "initialize" => {
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "userAgent": "fake-app-server/1.0",
                                            "codexHome": "/tmp/codex"
                                        }
                                    });
                                    let _ = outgoing_tx.send(Message::text(resp.to_string())).await;
                                }
                                "thread/start" => {
                                    tsc.fetch_add(1, Ordering::SeqCst);
                                    let params =
                                        parsed.get("params").cloned().unwrap_or(Value::Null);
                                    if let Some(dev_inst) =
                                        params.get("developerInstructions").and_then(Value::as_str)
                                    {
                                        *rdi.lock() = Some(dev_inst.to_string());
                                    }
                                    if let Some(m) = params.get("model").and_then(Value::as_str) {
                                        *rm.lock() = Some(m.to_string());
                                    }

                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "thread": {
                                                "id": "server-assigned-thread-uuid-1234",
                                                "sessionId": "fake-session-001"
                                            },
                                            "model": "claude-3-5-sonnet"
                                        }
                                    });
                                    let _ = outgoing_tx.send(Message::text(resp.to_string())).await;
                                }
                                "turn/start" => {
                                    let params =
                                        parsed.get("params").cloned().unwrap_or(Value::Null);
                                    let thread_id = params
                                        .get("threadId")
                                        .and_then(Value::as_str)
                                        .unwrap_or("")
                                        .to_string();
                                    tt.lock().push(thread_id.clone());

                                    // Send turn/start response
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "turn": {
                                                "id": "turn-001",
                                                "status": "inProgress"
                                            }
                                        }
                                    });
                                    let _ = outgoing_tx.send(Message::text(resp.to_string())).await;

                                    // Send server request requiring client denial response: execCommandApproval
                                    let approval_req = json!({
                                        "jsonrpc": "2.0",
                                        "id": 999,
                                        "method": "execCommandApproval",
                                        "params": {
                                            "command": "rm -rf /",
                                            "reason": "destructive"
                                        }
                                    });
                                    let _ = outgoing_tx
                                        .send(Message::text(approval_req.to_string()))
                                        .await;

                                    // Small delay to allow client to process approval request before ending turn
                                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                                    // Stream item/agentMessage/delta chunks
                                    let emit_activity = ea.load(Ordering::SeqCst);
                                    if emit_activity {
                                        let reasoning_done = json!({
                                            "jsonrpc": "2.0",
                                            "method": "item/completed",
                                            "params": {
                                                "threadId": thread_id,
                                                "turnId": "turn-001",
                                                "item": {
                                                    "type": "reasoning",
                                                    "text": "hmm let me think about this carefully"
                                                }
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(reasoning_done.to_string()))
                                            .await;

                                        let tool_started = json!({
                                            "jsonrpc": "2.0",
                                            "method": "item/started",
                                            "params": {
                                                "threadId": thread_id,
                                                "turnId": "turn-001",
                                                "item": {
                                                    "type": "commandExecution",
                                                    "id": "call-1",
                                                    "command": "echo OMOITEMPROBE-OK",
                                                    "status": "inProgress"
                                                }
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(tool_started.to_string()))
                                            .await;

                                        let output_delta = json!({
                                            "jsonrpc": "2.0",
                                            "method": "item/commandExecution/outputDelta",
                                            "params": {
                                                "threadId": thread_id,
                                                "turnId": "turn-001",
                                                "itemId": "call-1",
                                                "delta": "OMOITEMPROBE-OK"
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(output_delta.to_string()))
                                            .await;

                                        let tool_completed = json!({
                                            "jsonrpc": "2.0",
                                            "method": "item/completed",
                                            "params": {
                                                "threadId": thread_id,
                                                "turnId": "turn-001",
                                                "item": {
                                                    "type": "commandExecution",
                                                    "id": "call-1",
                                                    "command": "echo OMOITEMPROBE-OK",
                                                    "status": "completed",
                                                    "aggregatedOutput": "OMOITEMPROBE-OK"
                                                }
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(tool_completed.to_string()))
                                            .await;
                                    }

                                    if looping {
                                        // Emulate an agent that never finishes:
                                        // deltas forever, no turn/completed.
                                        let t_id = thread_id.clone();
                                        let sink = outgoing_tx.clone();
                                        tokio::spawn(async move {
                                            let started = json!({
                                                "jsonrpc": "2.0",
                                                "method": "turn/started",
                                                "params": {
                                                    "threadId": t_id,
                                                    "turnId": "turn-loop"
                                                }
                                            });
                                            let _ =
                                                sink.send(Message::text(started.to_string())).await;
                                            let mut i: u32 = 0;
                                            while i < 60 {
                                                i += 1;
                                                let d = json!({
                                                    "jsonrpc": "2.0",
                                                    "method": "item/agentMessage/delta",
                                                    "params": {
                                                        "threadId": t_id,
                                                        "turnId": "turn-loop",
                                                        "itemId": "loop-msg",
                                                        "delta": format!("loop {} ", i)
                                                    }
                                                });
                                                if sink
                                                    .send(Message::text(d.to_string()))
                                                    .await
                                                    .is_err()
                                                {
                                                    break;
                                                }
                                                tokio::time::sleep(
                                                    std::time::Duration::from_millis(150),
                                                )
                                                .await;
                                            }
                                            let done = json!({
                                                "jsonrpc": "2.0",
                                                "method": "turn/completed",
                                                "params": {
                                                    "threadId": t_id,
                                                    "turn": {"id": "turn-loop", "status": "completed"}
                                                }
                                            });
                                            let _ =
                                                sink.send(Message::text(done.to_string())).await;
                                        });
                                        // NOTE: the reader task must keep running
                                        // so it can observe the client's
                                        // turn/interrupt frame.
                                    }

                                    if !looping {
                                        let delta1 = json!({
                                            "jsonrpc": "2.0",
                                            "method": "item/agentMessage/delta",
                                            "params": {
                                                "threadId": thread_id,
                                                "turnId": "turn-001",
                                                "itemId": "msg-001",
                                                "delta": if emit_activity { "OMO" } else { "Hello " }
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(delta1.to_string()))
                                            .await;

                                        let delta2 = json!({
                                            "jsonrpc": "2.0",
                                            "method": "item/agentMessage/delta",
                                            "params": {
                                                "threadId": thread_id,
                                                "turnId": "turn-001",
                                                "itemId": "msg-001",
                                                "delta": if emit_activity { "ACT-OK" } else { "World!" }
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(delta2.to_string()))
                                            .await;

                                        // Send item/completed
                                        let item_completed = json!({
                                            "jsonrpc": "2.0",
                                            "method": "item/completed",
                                            "params": {
                                                "threadId": thread_id,
                                                "turnId": "turn-001",
                                                "item": {
                                                    "type": "agentMessage",
                                                    "text": if emit_activity { "OMOACT-OK" } else { "Hello World!" }
                                                }
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(item_completed.to_string()))
                                            .await;

                                        // Send turn/completed
                                        let turn_completed = json!({
                                            "jsonrpc": "2.0",
                                            "method": "turn/completed",
                                            "params": {
                                                "threadId": thread_id,
                                                "turn": {
                                                    "id": "turn-001",
                                                    "status": "completed"
                                                }
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(turn_completed.to_string()))
                                            .await;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                });
            }
        });

        Self {
            port,
            thread_start_count,
            received_developer_instructions,
            received_model,
            turn_threads,
            approval_responses,
            conn_count,
            interrupts,
        }
    }
}

#[tokio::test]
async fn test_omo_backend_e2e_thread_lifecycle_and_streaming() {
    // Given: Fake app-server running on 127.0.0.1 with ephemeral port
    let server = FakeAppServer::spawn().await;
    let url = format!("ws://127.0.0.1:{}", server.port);

    let config = OmoBackendConfig::new(&url).with_default_model(Some("claude-3-5-sonnet"));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());

    let session_key = SessionKey::new(
        "discord",
        Some("guild-1"),
        "chan-1",
        None::<String>,
        "user-1",
    );
    let mut session = SessionContext::new(session_key.clone());
    session.state.system_prompt = Some("You are a helpful test persona.".to_string());
    session.state.active_model = Some("claude-3-5-sonnet".to_string());

    // When: First turn executes
    let event1 = InboundEvent::message(session_key.clone(), "msg-1", "Say hello");
    let result1 = backend.run(&mut session, event1).await;

    // Then: First turn succeeds
    assert!(result1.is_ok(), "First turn failed: {:?}", result1.err());
    assert_eq!(server.thread_start_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        server.received_developer_instructions.lock().as_deref(),
        Some("You are a helpful test persona.")
    );
    assert_eq!(
        server.received_model.lock().as_deref(),
        Some("claude-3-5-sonnet")
    );

    // Thread ID stored in session state metadata
    let stored_thread_id = session
        .state
        .metadata
        .get("omo_thread_id")
        .and_then(Value::as_str)
        .map(String::from);
    assert_eq!(
        stored_thread_id.as_deref(),
        Some("server-assigned-thread-uuid-1234"),
        "threadId must be stored in session state metadata"
    );

    // StreamChunks yielded in order
    let chunks = dispatcher.stream_chunks();
    assert!(!chunks.is_empty(), "Must yield stream chunks");
    let combined_content = chunks.last().map(|c| c.content.clone()).unwrap_or_default();
    assert_eq!(combined_content, "Hello World!");

    // Server approval request was answered with denial
    let approvals = server.approval_responses.lock().clone();
    assert!(!approvals.is_empty(), "Approval response must be sent");
    let first_approval = &approvals[0];
    assert_eq!(first_approval.get("id").and_then(Value::as_u64), Some(999));
    let allow = first_approval
        .get("result")
        .and_then(|r| r.get("allow"))
        .and_then(Value::as_bool);
    let decision = first_approval
        .get("result")
        .and_then(|r| r.get("decision"))
        .and_then(Value::as_str);
    assert_eq!(allow, Some(false));
    assert!(matches!(decision, Some("decline") | Some("deny")));

    // When: Second turn executes for the same session
    let event2 = InboundEvent::message(session_key.clone(), "msg-2", "Say hello again");
    let result2 = backend.run(&mut session, event2).await;

    // Then: Second turn succeeds and REUSES the threadId without starting a new thread
    assert!(result2.is_ok(), "Second turn failed: {:?}", result2.err());
    assert_eq!(
        server.thread_start_count.load(Ordering::SeqCst),
        1,
        "thread/start must NOT be called again on second turn with same session"
    );
    let turn_threads = server.turn_threads.lock().clone();
    assert_eq!(turn_threads.len(), 2);
    assert_eq!(turn_threads[0], "server-assigned-thread-uuid-1234");
    assert_eq!(turn_threads[1], "server-assigned-thread-uuid-1234");
}

#[tokio::test]
async fn test_omo_backend_unreachable_daemon_error() {
    // Port 1 is not listening
    let config = OmoBackendConfig::new("ws://127.0.0.1:1").with_default_model(Some("test-model"));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher);

    let session_key = SessionKey::new(
        "discord",
        Some("guild-1"),
        "chan-1",
        None::<String>,
        "user-1",
    );
    let mut session = SessionContext::new(session_key.clone());
    let event = InboundEvent::message(session_key, "msg-1", "Hello");

    let result = backend.run(&mut session, event).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_omo_backend_emits_hermes_activity_lines() {
    // Given: fake server emitting reasoning + commandExecution activity items
    let server = FakeAppServer::spawn_with_activity().await;
    let url = format!("ws://127.0.0.1:{}", server.port);

    let config = OmoBackendConfig::new(&url).with_default_model(Some("claude-3-5-sonnet"));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());

    let session_key = SessionKey::new(
        "discord",
        Some("guild-1"),
        "chan-1",
        None::<String>,
        "user-1",
    );
    let mut session = SessionContext::new(session_key.clone());
    session.state.system_prompt = Some("You are a helpful test persona.".to_string());

    // When: a turn runs through tool + reasoning activity
    let event = InboundEvent::message(session_key.clone(), "msg-1", "Run the probe command");
    let result = backend.run(&mut session, event).await;
    assert!(result.is_ok(), "turn failed: {:?}", result.err());

    // Then: activity lines appear in the stream, interleaved before the reply
    let chunks = dispatcher.stream_chunks();
    assert!(!chunks.is_empty(), "no chunks emitted");

    let combined: String = chunks
        .iter()
        .map(|c| c.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        combined.contains("⚙️ Running `echo OMOITEMPROBE-OK`…"),
        "tool activity line missing: {combined}"
    );
    assert!(
        combined.contains("> 💭 hmm let me think about this carefully…"),
        "thinking block missing: {combined}"
    );
    assert!(
        combined.contains("⤷ OMOITEMPROBE-OK"),
        "output excerpt missing: {combined}"
    );

    // And: the final visible message carries activity block + reply
    let last = chunks.last().map(|c| c.content.clone()).unwrap_or_default();
    let expected = "> 💭 hmm let me think about this carefully…\n⚙️ Running `echo OMOITEMPROBE-OK`…\n⤷ OMOITEMPROBE-OK\n\nOMOACT-OK";
    assert_eq!(last, expected, "final chunk layout mismatch: {last:?}");
}

#[tokio::test]
async fn test_omo_backend_persists_without_preexisting_session_row() {
    // Regression: cron sessions are created implicitly by backend.run —
    // persisting the assistant message must not violate the messages->
    // sessions foreign key (code 787).
    let server = FakeAppServer::spawn().await;
    let url = format!("ws://127.0.0.1:{}", server.port);

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .expect("in-memory pool");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migrations");

    let config = OmoBackendConfig::new(&url).with_default_model(Some("claude-3-5-sonnet"));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone()).with_pool(pool.clone());

    let session_key = SessionKey::new(
        "local",
        None::<String>,
        "test-cron-job",
        None::<String>,
        "cron:test-cron-job",
    );
    let mut session = SessionContext::new(session_key.clone());
    session.state.system_prompt = Some("You are a helpful test persona.".to_string());

    let event = InboundEvent::message(session_key.clone(), "msg-1", "Say hello");
    let result = backend.run(&mut session, event).await;
    assert!(
        result.is_ok(),
        "turn must succeed without a pre-existing session row: {:?}",
        result.err()
    );

    let session_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sessions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(session_rows >= 1, "session row must be ensured");

    let assistant: Option<String> =
        sqlx::query_scalar("SELECT content FROM messages WHERE role='assistant'")
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert_eq!(assistant.as_deref(), Some("Hello World!"));

    let fk_enabled: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        fk_enabled, 1,
        "foreign keys must be enforced for this test to be meaningful"
    );
}

#[tokio::test]
async fn test_omo_backend_retries_once_after_connection_drop() {
    // Given: the fake server drops the client's first connection abruptly
    let server = FakeAppServer::spawn_with_first_connection_drop().await;
    let url = format!("ws://127.0.0.1:{}", server.port);

    let config = OmoBackendConfig::new(&url).with_default_model(Some("claude-3-5-sonnet"));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());

    let session_key = SessionKey::new(
        "discord",
        Some("guild-1"),
        "chan-1",
        None::<String>,
        "user-1",
    );
    let mut session = SessionContext::new(session_key.clone());

    // When: the turn survives exactly one transport drop
    let event = InboundEvent::message(session_key.clone(), "msg-1", "Say hello");
    let result = backend.run(&mut session, event).await;
    assert!(
        result.is_ok(),
        "turn must succeed via the one-shot transport retry: {:?}",
        result.err()
    );

    // Then: the retry reconnected and served the full flow
    assert_eq!(server.conn_count.load(Ordering::SeqCst), 2);
    let chunks = dispatcher.stream_chunks();
    let last = chunks.last().map(|c| c.content.clone()).unwrap_or_default();
    assert_eq!(last, "Hello World!");
}

#[tokio::test]
async fn test_omo_backend_deadline_interrupts_looping_turn() {
    // Given: a daemon whose agent loops forever streaming deltas
    let server = FakeAppServer::spawn_with_looping_deltas().await;
    let url = format!("ws://127.0.0.1:{}", server.port);

    let config = OmoBackendConfig::new(&url)
        .with_default_model(Some("claude-3-5-sonnet"))
        .with_total_timeout(std::time::Duration::from_millis(600));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());

    let session_key = SessionKey::new(
        "discord",
        Some("guild-1"),
        "chan-1",
        None::<String>,
        "user-1",
    );
    let mut session = SessionContext::new(session_key.clone());

    // When: the turn runs past the whole-turn deadline
    let event = InboundEvent::message(session_key.clone(), "msg-1", "Loop forever");
    let started = std::time::Instant::now();
    let result = backend.run(&mut session, event).await;
    let elapsed = started.elapsed();

    // Then: the turn fails fast instead of hanging, and the daemon is
    // asked to interrupt the turn (freeing the thread).
    assert!(
        result.is_err(),
        "looping turn must hit the total deadline, got {:?}",
        result.ok()
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "deadline must fire promptly, took {:?}",
        elapsed
    );
    assert!(
        !server.interrupts.lock().is_empty(),
        "turn/interrupt must be sent to the daemon"
    );
}
