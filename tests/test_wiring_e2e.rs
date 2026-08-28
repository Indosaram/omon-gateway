use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use omon_gateway::storage::init_pool;
use omon_gateway::{
    validate_agent_backend_value, InboundEvent, MultiplexerConfig, OmoBackend, OmoBackendConfig,
    OutboundAction, OutboundDispatcher, ProfileRoute, ProfileRouter, SessionKey,
    SessionMultiplexer, StreamChunk,
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

#[allow(dead_code)]
struct FakeAppServer {
    pub port: u16,
    pub thread_start_count: Arc<AtomicUsize>,
    pub received_developer_instructions: Arc<ParkingMutex<Option<String>>>,
    pub received_model: Arc<ParkingMutex<Option<String>>>,
    pub turn_threads: Arc<ParkingMutex<Vec<String>>>,
    pub approval_responses: Arc<ParkingMutex<Vec<Value>>>,
}

impl FakeAppServer {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind free port");
        let port = listener.local_addr().expect("local addr").port();

        let thread_start_count = Arc::new(AtomicUsize::new(0));
        let received_developer_instructions = Arc::new(ParkingMutex::new(None));
        let received_model = Arc::new(ParkingMutex::new(None));
        let turn_threads = Arc::new(ParkingMutex::new(Vec::new()));
        let approval_responses = Arc::new(ParkingMutex::new(Vec::new()));

        let tsc = thread_start_count.clone();
        let rdi = received_developer_instructions.clone();
        let rm = received_model.clone();
        let tt = turn_threads.clone();
        let ar = approval_responses.clone();

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tsc = tsc.clone();
                let rdi = rdi.clone();
                let rm = rm.clone();
                let tt = tt.clone();
                let ar = ar.clone();

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

                            if parsed.get("method").is_none() && parsed.get("id").is_some() {
                                ar.lock().push(parsed.clone());
                                continue;
                            }

                            let method = parsed.get("method").and_then(Value::as_str).unwrap_or("");
                            let id = parsed.get("id").and_then(Value::as_u64).unwrap_or(0);

                            match method {
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
                                                "id": "server-assigned-thread-e2e-9999",
                                                "sessionId": "e2e-session-001"
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

                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "turn": {
                                                "id": "turn-e2e-001",
                                                "status": "inProgress"
                                            }
                                        }
                                    });
                                    let _ = outgoing_tx.send(Message::text(resp.to_string())).await;

                                    // Stream delta chunks
                                    let delta = json!({
                                        "jsonrpc": "2.0",
                                        "method": "item/agentMessage/delta",
                                        "params": {
                                            "threadId": thread_id,
                                            "turnId": "turn-e2e-001",
                                            "itemId": "msg-001",
                                            "delta": "Hello from OMO app-server via actor!"
                                        }
                                    });
                                    let _ =
                                        outgoing_tx.send(Message::text(delta.to_string())).await;

                                    let turn_completed = json!({
                                        "jsonrpc": "2.0",
                                        "method": "turn/completed",
                                        "params": {
                                            "threadId": thread_id,
                                            "turn": {
                                                "id": "turn-e2e-001",
                                                "status": "completed"
                                            }
                                        }
                                    });
                                    let _ = outgoing_tx
                                        .send(Message::text(turn_completed.to_string()))
                                        .await;
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
        }
    }
}

#[tokio::test]
async fn test_e2e_actor_omo_backend_persists_assistant_message() {
    // Given: In-process fake app-server on ephemeral port
    let server = FakeAppServer::spawn().await;
    let url = format!("ws://127.0.0.1:{}", server.port);

    // In-memory sqlite pool
    let pool = init_pool("sqlite::memory:").await.unwrap();

    let config = OmoBackendConfig::new(&url).with_default_model(Some("default-test-model"));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = Arc::new(OmoBackend::new(config, dispatcher.clone()).with_pool(pool.clone()));

    let profile_route = ProfileRoute {
        name: Some("test-persona".into()),
        guild: Some(100),
        channel: Some(200),
        thread: None,
        enabled: true,
        model: Some("persona-model-override".into()),
        system_prompt: Some("You are a specialized profile persona.".into()),
        enabled_toolsets: None,
    };
    let profile_router = ProfileRouter::new(vec![profile_route]);

    let multiplexer = SessionMultiplexer::with_profile_router(
        pool.clone(),
        backend,
        Some(dispatcher.clone()),
        MultiplexerConfig::default(),
        profile_router,
    );

    let session_key = SessionKey::new("discord", Some("100"), "200", None::<String>, "user-e2e-1");

    let event = InboundEvent::message(
        session_key.clone(),
        "platform-msg-e2e-1",
        "Perform task with OMO backend",
    );

    // When: An inbound event is routed through the multiplexer actor
    multiplexer
        .route(event)
        .await
        .expect("route event to actor");

    // Allow actor task to finish processing and persist messages
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Then:
    // 1. Fake app-server received developer instructions from profile route
    assert_eq!(
        server.received_developer_instructions.lock().as_deref(),
        Some("You are a specialized profile persona.")
    );
    assert_eq!(
        server.received_model.lock().as_deref(),
        Some("persona-model-override")
    );

    // 2. Stream chunks were emitted to dispatcher
    let chunks = dispatcher.stream_chunks();
    assert!(!chunks.is_empty(), "Must emit stream chunks to dispatcher");
    let final_chunk = chunks.last().unwrap();
    assert!(final_chunk.is_final);
    assert_eq!(final_chunk.content, "Hello from OMO app-server via actor!");

    // 3. User message is persisted in SQLite messages table
    let user_msgs: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT role, content, platform_message_id FROM messages WHERE session_key = ? AND role = 'user'",
    )
    .bind(session_key.storage_key())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(user_msgs.len(), 1);
    assert_eq!(user_msgs[0].1, "Perform task with OMO backend");
    assert_eq!(user_msgs[0].2.as_deref(), Some("platform-msg-e2e-1"));

    // 4. Assistant message is persisted in SQLite messages table
    let assistant_msgs: Vec<(String, String)> = sqlx::query_as(
        "SELECT role, content FROM messages WHERE session_key = ? AND role = 'assistant'",
    )
    .bind(session_key.storage_key())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        assistant_msgs.len(),
        1,
        "Assistant message produced by OmoBackend must be persisted in SQLite"
    );
    assert_eq!(assistant_msgs[0].1, "Hello from OMO app-server via actor!");

    // 5. Session state has omo_thread_id in metadata
    let state_row: (String,) =
        sqlx::query_as("SELECT state_json FROM sessions WHERE session_key = ?")
            .bind(session_key.storage_key())
            .fetch_one(&pool)
            .await
            .unwrap();
    let state_val: Value = serde_json::from_str(&state_row.0).unwrap();
    assert_eq!(
        state_val
            .pointer("/metadata/omo_thread_id")
            .and_then(Value::as_str),
        Some("server-assigned-thread-e2e-9999")
    );
}

#[test]
fn test_boot_time_backend_selection_parsing() {
    // 1. Absent or empty defaults to Omo (succeeds)
    assert!(validate_agent_backend_value(None).is_ok());
    assert!(validate_agent_backend_value(Some("")).is_ok());

    // 2. omo or appserver resolves to Omo (succeeds)
    assert!(validate_agent_backend_value(Some("omo")).is_ok());
    assert!(validate_agent_backend_value(Some("omo-appserver")).is_ok());
    assert!(validate_agent_backend_value(Some("appserver")).is_ok());

    // 3. Removed direct-LLM aliases fail with descriptive error
    let err_llm = validate_agent_backend_value(Some("llm")).unwrap_err();
    let err_llm_str = err_llm.to_string();
    assert!(
        err_llm_str.contains("direct LLM backend has been removed"),
        "Error message '{err_llm_str}' must state direct LLM backend removed"
    );

    // 4. Unknown value fails with an error explicitly naming OMON_AGENT_BACKEND
    let err = validate_agent_backend_value(Some("unknown_backend_engine")).unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("OMON_AGENT_BACKEND"),
        "Error message '{err_str}' must mention OMON_AGENT_BACKEND"
    );
}
