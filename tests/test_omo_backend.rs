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
    pub thread_resume_count: Arc<AtomicUsize>,
    pub received_developer_instructions: Arc<ParkingMutex<Option<String>>>,
    pub received_model: Arc<ParkingMutex<Option<String>>>,
    pub received_cwd: Arc<ParkingMutex<Option<String>>>,
    pub received_roots: Arc<ParkingMutex<Vec<String>>>,
    pub turn_threads: Arc<ParkingMutex<Vec<String>>>,
    pub approval_responses: Arc<ParkingMutex<Vec<Value>>>,
    pub conn_count: Arc<AtomicUsize>,
    pub interrupts: Arc<ParkingMutex<Vec<Value>>>,
}

impl FakeAppServer {
    async fn spawn() -> Self {
        Self::spawn_inner(false, false, false, false, 0).await
    }

    async fn spawn_with_activity() -> Self {
        Self::spawn_inner(true, false, false, false, 0).await
    }

    async fn spawn_without_turn_completed() -> Self {
        Self::spawn_inner(false, false, false, true, 0).await
    }

    async fn spawn_with_delayed_turn_completed(delay_ms: u64) -> Self {
        Self::spawn_inner(false, false, false, false, delay_ms).await
    }

    /// Drop the client's first connection abruptly: exercises the
    /// one-shot transport retry.
    async fn spawn_with_first_connection_drop() -> Self {
        Self::spawn_inner(false, true, false, false, 0).await
    }

    /// Stream deltas forever (well, 5s) without turn/completed: exercises
    /// the whole-turn deadline + turn/interrupt.
    async fn spawn_with_looping_deltas() -> Self {
        Self::spawn_inner(false, false, true, false, 0).await
    }

    async fn spawn_inner(
        emit_activity: bool,
        drop_first_connection: bool,
        loop_deltas: bool,
        omit_turn_completed: bool,
        turn_completed_delay_ms: u64,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind free port");
        let port = listener.local_addr().expect("local addr").port();

        let thread_start_count = Arc::new(AtomicUsize::new(0));
        let thread_resume_count = Arc::new(AtomicUsize::new(0));
        let received_developer_instructions = Arc::new(ParkingMutex::new(None));
        let received_model = Arc::new(ParkingMutex::new(None));
        let received_cwd = Arc::new(ParkingMutex::new(None));
        let received_roots = Arc::new(ParkingMutex::new(Vec::new()));
        let turn_threads = Arc::new(ParkingMutex::new(Vec::new()));
        let approval_responses = Arc::new(ParkingMutex::new(Vec::new()));
        let emit_activity_flag = Arc::new(AtomicBool::new(emit_activity));
        let conn_count = Arc::new(AtomicUsize::new(0));
        let drop_flag = drop_first_connection;
        let interrupts = Arc::new(ParkingMutex::new(Vec::new()));

        let tsc = thread_start_count.clone();
        let trc = thread_resume_count.clone();
        let rdi = received_developer_instructions.clone();
        let rm = received_model.clone();
        let rcwd = received_cwd.clone();
        let rroots = received_roots.clone();
        let tt = turn_threads.clone();
        let ar = approval_responses.clone();
        let ea = emit_activity_flag.clone();
        let cc = conn_count.clone();
        let drop_first = drop_flag;
        let it = interrupts.clone();
        let looping = loop_deltas;
        let omit_terminal = omit_turn_completed;
        let terminal_delay_ms = turn_completed_delay_ms;

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
                let trc = trc.clone();
                let rdi = rdi.clone();
                let rm = rm.clone();
                let rcwd = rcwd.clone();
                let rroots = rroots.clone();
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
                                    if let Some(cwd) = params.get("cwd").and_then(Value::as_str) {
                                        *rcwd.lock() = Some(cwd.to_string());
                                    }
                                    if let Some(roots) = params
                                        .get("runtimeWorkspaceRoots")
                                        .and_then(Value::as_array)
                                    {
                                        *rroots.lock() = roots
                                            .iter()
                                            .filter_map(Value::as_str)
                                            .map(String::from)
                                            .collect();
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
                                "thread/resume" => {
                                    trc.fetch_add(1, Ordering::SeqCst);
                                    let thread_id = parsed
                                        .pointer("/params/threadId")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default();
                                    if thread_id == "stale-thread" {
                                        let response = json!({
                                            "jsonrpc": "2.0",
                                            "id": id,
                                            "error": {
                                                "code": -32603,
                                                "message": "no rollout found for thread id stale-thread"
                                            }
                                        });
                                        let _ = outgoing_tx
                                            .send(Message::text(response.to_string()))
                                            .await;
                                        continue;
                                    }
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "thread": {
                                                "id": "server-assigned-thread-uuid-1234",
                                                "sessionId": "fake-session-001"
                                            }
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
                                        if !omit_terminal {
                                            if terminal_delay_ms > 0 {
                                                tokio::time::sleep(
                                                    std::time::Duration::from_millis(
                                                        terminal_delay_ms,
                                                    ),
                                                )
                                                .await;
                                            }
                                            let _ = outgoing_tx
                                                .send(Message::text(turn_completed.to_string()))
                                                .await;
                                        } else {
                                            let idle = json!({
                                                "jsonrpc": "2.0",
                                                "method": "thread/status/changed",
                                                "params": {
                                                    "threadId": thread_id,
                                                    "status": {"type": "idle"}
                                                }
                                            });
                                            let _ = outgoing_tx
                                                .send(Message::text(idle.to_string()))
                                                .await;
                                        }
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
            thread_resume_count,
            received_developer_instructions,
            received_model,
            received_cwd,
            received_roots,
            turn_threads,
            approval_responses,
            conn_count,
            interrupts,
        }
    }
}

async fn spawn_turn_start_error_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind free port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
            return;
        };
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            let Ok(request) = serde_json::from_str::<Value>(text.as_str()) else {
                continue;
            };
            let id = request
                .get("id")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let response = match request.get("method").and_then(Value::as_str) {
                Some("initialize") => json!({"jsonrpc":"2.0","id":id,"result":{}}),
                Some("thread/start") => json!({
                    "jsonrpc":"2.0","id":id,
                    "result":{"thread":{"id":"thread-1"}}
                }),
                Some("turn/start") => json!({
                    "jsonrpc":"2.0","id":id,
                    "error":{"code":-32603,"message":"turn rejected"}
                }),
                _ => continue,
            };
            let _ = ws.send(Message::text(response.to_string())).await;
        }
    });
    port
}

async fn spawn_tool_after_agent_message_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind free port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
            return;
        };
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            let Ok(request) = serde_json::from_str::<Value>(text.as_str()) else {
                continue;
            };
            let id = request
                .get("id")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            match request.get("method").and_then(Value::as_str) {
                Some("initialize") => {
                    let response = json!({"jsonrpc":"2.0","id":id,"result":{}});
                    let _ = ws.send(Message::text(response.to_string())).await;
                }
                Some("thread/start") => {
                    let response = json!({
                        "jsonrpc":"2.0","id":id,
                        "result":{"thread":{"id":"thread-tool-sequence"}}
                    });
                    let _ = ws.send(Message::text(response.to_string())).await;
                }
                Some("turn/start") => {
                    let response = json!({
                        "jsonrpc":"2.0","id":id,
                        "result":{"turn":{"id":"turn-tool-sequence","status":"inProgress"}}
                    });
                    let _ = ws.send(Message::text(response.to_string())).await;
                    let intent = json!({
                        "jsonrpc":"2.0","method":"item/completed",
                        "params":{"item":{"type":"agentMessage","id":"intent","text":"I read this as the digest task."}}
                    });
                    let _ = ws.send(Message::text(intent.to_string())).await;
                    let tool_started = json!({
                        "jsonrpc":"2.0","method":"item/started",
                        "params":{"item":{"type":"commandExecution","id":"tool-1","command":"write digest","status":"inProgress"}}
                    });
                    let _ = ws.send(Message::text(tool_started.to_string())).await;

                    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

                    let tool_completed = json!({
                        "jsonrpc":"2.0","method":"item/completed",
                        "params":{"item":{"type":"commandExecution","id":"tool-1","command":"write digest","status":"completed"}}
                    });
                    let _ = ws.send(Message::text(tool_completed.to_string())).await;
                    let digest = json!({
                        "jsonrpc":"2.0","method":"item/completed",
                        "params":{"item":{"type":"agentMessage","id":"digest","text":"## Actual digest body"}}
                    });
                    let _ = ws.send(Message::text(digest.to_string())).await;
                    let idle = json!({
                        "jsonrpc":"2.0","method":"thread/status/changed",
                        "params":{"threadId":"thread-tool-sequence","status":{"type":"idle"}}
                    });
                    let _ = ws.send(Message::text(idle.to_string())).await;
                }
                _ => {}
            }
        }
    });
    port
}

#[tokio::test]
async fn test_omo_backend_surfaces_turn_start_rpc_error_immediately() {
    // Given: app-server rejects turn/start before any turn notifications.
    let port = spawn_turn_start_error_server().await;
    let config = OmoBackendConfig::new(format!("ws://127.0.0.1:{port}"))
        .with_total_timeout(std::time::Duration::from_secs(5));
    let backend = OmoBackend::new(config, Arc::new(CapturingDispatcher::new()));
    let session_key = SessionKey::new("discord", None::<String>, "chan", None::<String>, "user");
    let mut session = SessionContext::new(session_key.clone());

    // When: a turn is rejected at the RPC boundary.
    let started = std::time::Instant::now();
    let error = backend
        .run(
            &mut session,
            InboundEvent::message(session_key, "msg", "hello"),
        )
        .await
        .expect_err("turn/start RPC error must fail the run");

    // Then: the concrete server error is surfaced without waiting for the deadline.
    assert!(error.to_string().contains("turn rejected"));
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn test_omo_backend_completes_from_final_agent_message_when_turn_terminal_is_missing() {
    let server = FakeAppServer::spawn_without_turn_completed().await;
    let config = OmoBackendConfig::new(format!("ws://127.0.0.1:{}", server.port))
        .with_request_timeout(std::time::Duration::from_millis(800));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());
    let session_key = SessionKey::new("discord", None::<String>, "chan", None::<String>, "cron");
    let mut session = SessionContext::new(session_key.clone());

    let result = backend
        .run(
            &mut session,
            InboundEvent::message(session_key, "msg", "digest"),
        )
        .await;

    assert!(
        result.is_ok(),
        "missing terminal fallback failed: {result:?}"
    );
    let chunks = dispatcher.stream_chunks();
    let final_chunk = chunks.last().expect("final fallback chunk");
    assert!(final_chunk.is_final);
    assert_eq!(final_chunk.content, "Hello World!");
}

#[tokio::test]
async fn test_omo_backend_waits_for_final_message_after_tool_activity() {
    let port = spawn_tool_after_agent_message_server().await;
    let config = OmoBackendConfig::new(format!("ws://127.0.0.1:{port}"))
        .with_request_timeout(std::time::Duration::from_secs(3));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());
    let session_key = SessionKey::new("discord", None::<String>, "chan", None::<String>, "cron");
    let mut session = SessionContext::new(session_key.clone());

    let result = backend
        .run(
            &mut session,
            InboundEvent::message(session_key, "msg", "digest"),
        )
        .await;

    assert!(result.is_ok(), "tool sequence failed: {result:?}");
    let final_chunk = dispatcher
        .stream_chunks()
        .into_iter()
        .find(|chunk| chunk.is_final)
        .expect("final digest chunk");
    assert!(final_chunk.content.contains("## Actual digest body"));
    assert!(!final_chunk
        .content
        .contains("I read this as the digest task"));
}

#[tokio::test]
async fn test_omo_backend_accepts_received_completion_at_deadline_edge() {
    let server = FakeAppServer::spawn_with_delayed_turn_completed(80).await;
    let config = OmoBackendConfig::new(format!("ws://127.0.0.1:{}", server.port))
        .with_request_timeout(std::time::Duration::from_secs(2))
        .with_total_timeout(std::time::Duration::from_millis(50));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());
    let session_key = SessionKey::new("discord", None::<String>, "chan", None::<String>, "cron");
    let mut session = SessionContext::new(session_key.clone());

    let result = backend
        .run(
            &mut session,
            InboundEvent::message(session_key, "msg", "digest"),
        )
        .await;

    assert!(
        result.is_ok(),
        "deadline-edge completion failed: {result:?}"
    );
    assert!(dispatcher
        .stream_chunks()
        .last()
        .is_some_and(|chunk| chunk.is_final));
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

    // Then: Second turn resumes the existing thread on its new WebSocket and
    // reuses the threadId without starting another thread.
    assert!(result2.is_ok(), "Second turn failed: {:?}", result2.err());
    assert_eq!(
        server.thread_start_count.load(Ordering::SeqCst),
        1,
        "thread/start must NOT be called again on second turn with same session"
    );
    assert_eq!(
        server.thread_resume_count.load(Ordering::SeqCst),
        1,
        "thread/resume must subscribe the new WebSocket before the second turn"
    );
    let turn_threads = server.turn_threads.lock().clone();
    assert_eq!(turn_threads.len(), 2);
    assert_eq!(turn_threads[0], "server-assigned-thread-uuid-1234");
    assert_eq!(turn_threads[1], "server-assigned-thread-uuid-1234");
}

#[tokio::test]
async fn test_omo_backend_replaces_stale_cached_thread() {
    // Given: a session carrying a thread ID that the app-server has unloaded.
    let server = FakeAppServer::spawn().await;
    let config = OmoBackendConfig::new(format!("ws://127.0.0.1:{}", server.port));
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher);
    let session_key = SessionKey::new(
        "discord",
        Some("guild-1"),
        "chan-1",
        None::<String>,
        "cron:stale-test",
    );
    let mut session = SessionContext::new(session_key.clone());
    session
        .state
        .metadata
        .insert("omo_thread_id".into(), json!("stale-thread"));

    // When: a turn starts after the remote thread has been unloaded.
    let result = backend
        .run(
            &mut session,
            InboundEvent::message(session_key, "msg-1", "Hello"),
        )
        .await;

    // Then: the stale ID is evicted and a new thread starts transparently.
    assert!(result.is_ok(), "stale thread recovery failed: {result:?}");
    assert_eq!(server.thread_resume_count.load(Ordering::SeqCst), 1);
    assert_eq!(server.thread_start_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        session
            .state
            .metadata
            .get("omo_thread_id")
            .and_then(Value::as_str),
        Some("server-assigned-thread-uuid-1234")
    );
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
async fn test_omo_backend_cron_session_suppresses_activity_lines_and_delivers_only_final_result() {
    // Given: fake server emitting reasoning + commandExecution activity items followed by result
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
        "cron:omon-katok-3h-group-digest-v3",
    );
    let mut session = SessionContext::new(session_key.clone());
    session.state.metadata.insert(
        "cron_scheduler_delivery".to_string(),
        serde_json::json!(true),
    );

    // When: a cron turn runs through tool + reasoning activity
    let event = InboundEvent::message(
        session_key.clone(),
        "cron:omon-katok-3h-group-digest-v3",
        "Run the probe command",
    );
    let result = backend.run(&mut session, event).await;
    assert!(result.is_ok(), "turn failed: {:?}", result.err());

    // Then: final chunk delivered must NOT contain activity lines (no ⚙️ Running, no > 💭, no ⤷)
    let chunks = dispatcher.stream_chunks();
    assert!(!chunks.is_empty(), "no chunks emitted");

    let final_chunk = chunks
        .iter()
        .find(|c| c.is_final)
        .map(|c| c.content.as_str())
        .expect("final chunk must be emitted");

    assert!(
        !final_chunk.contains("⚙️ Running"),
        "cron final chunk must not contain tool activity lines: {final_chunk}"
    );
    assert!(
        !final_chunk.contains("> 💭"),
        "cron final chunk must not contain reasoning blocks: {final_chunk}"
    );
    assert!(
        !final_chunk.contains("⤷"),
        "cron final chunk must not contain output excerpts: {final_chunk}"
    );
    assert_eq!(
        final_chunk, "OMOACT-OK",
        "cron final chunk should contain ONLY the pure final result"
    );
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

#[tokio::test]
async fn test_omo_backend_provisions_agent_workspace_and_passes_to_thread_start() {
    // Given: a running FakeAppServer and an OmoBackend configured with a workspace root
    let server = FakeAppServer::spawn().await;
    let url = format!("ws://127.0.0.1:{}", server.port);

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let workspace_root = temp_dir.path().to_path_buf();

    let config = OmoBackendConfig::new(&url).with_workspace_root(workspace_root.clone());
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());

    let session_key = SessionKey::new(
        "discord",
        Some("guild-1"),
        "chan-1",
        None::<String>,
        "user-1",
    )
    .with_bot_id("1465631383862120451");
    let mut session = SessionContext::new(session_key.clone());

    // When: running one backend turn
    let event = InboundEvent::message(session_key.clone(), "msg-1", "Hello from bot");
    let result = backend.run(&mut session, event).await;
    assert!(
        result.is_ok(),
        "backend.run must succeed: {:?}",
        result.err()
    );

    // Then: thread/start received cwd and roots, agent and shared dirs exist, and .omo/omo.json exists
    let expected_agent_dir = workspace_root
        .join("agents")
        .join("bot-1465631383862120451");
    let expected_shared_dir = workspace_root.join("shared");
    let expected_cwd_str = expected_agent_dir.to_str().unwrap().to_string();
    let expected_shared_str = expected_shared_dir.to_str().unwrap().to_string();

    assert_eq!(
        *server.received_cwd.lock(),
        Some(expected_cwd_str.clone()),
        "FakeAppServer must receive provisioned cwd"
    );
    assert_eq!(
        *server.received_roots.lock(),
        vec![expected_cwd_str, expected_shared_str],
        "FakeAppServer must receive runtimeWorkspaceRoots"
    );
    assert!(
        expected_agent_dir.exists(),
        "agent workspace directory must exist"
    );
    assert!(
        expected_shared_dir.exists(),
        "shared workspace directory must exist"
    );

    let omo_json_path = expected_agent_dir.join(".omo").join("omo.json");
    assert!(omo_json_path.exists(), ".omo/omo.json must exist");
    let omo_json_str = std::fs::read_to_string(&omo_json_path).expect("read omo.json");
    let omo_json_val: Value = serde_json::from_str(&omo_json_str).expect("valid json in omo.json");
    assert_eq!(
        omo_json_val,
        json!({
            "memory": {
                "agent": "bot-1465631383862120451"
            }
        })
    );
}

#[tokio::test]
async fn test_omo_backend_omits_workspace_when_per_agent_disabled() {
    // Given: FakeAppServer and OmoBackend configured with workspace root but per_agent_workspace disabled
    let server = FakeAppServer::spawn().await;
    let url = format!("ws://127.0.0.1:{}", server.port);

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let workspace_root = temp_dir.path().to_path_buf();

    let config = OmoBackendConfig::new(&url)
        .with_workspace_root(workspace_root)
        .with_per_agent_workspace(false);
    let dispatcher = Arc::new(CapturingDispatcher::new());
    let backend = OmoBackend::new(config, dispatcher.clone());

    let session_key = SessionKey::new(
        "discord",
        Some("guild-1"),
        "chan-1",
        None::<String>,
        "user-1",
    )
    .with_bot_id("1465631383862120451");
    let mut session = SessionContext::new(session_key.clone());

    // When: running one backend turn
    let event = InboundEvent::message(session_key.clone(), "msg-1", "Hello");
    let result = backend.run(&mut session, event).await;
    assert!(
        result.is_ok(),
        "backend.run must succeed: {:?}",
        result.err()
    );

    // Then: thread/start received no cwd or runtimeWorkspaceRoots
    assert_eq!(
        *server.received_cwd.lock(),
        None,
        "FakeAppServer must not receive cwd when kill-switch is disabled"
    );
    assert!(
        server.received_roots.lock().is_empty(),
        "FakeAppServer must not receive runtimeWorkspaceRoots when kill-switch is disabled"
    );
}
