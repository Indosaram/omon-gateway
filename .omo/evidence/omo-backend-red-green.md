# OmoBackend RED -> GREEN Verification Evidence

## RED Phase

### Test: `test_omo_backend_e2e_thread_lifecycle_and_streaming`
Test runs an in-process fake `omo app-server` on `127.0.0.1:0` (ephemeral port), asserting:
- Connects to daemon via WebSocket
- Initializes session with protocol handshake (`initialize`)
- Starts thread, injecting `developerInstructions` persona system prompt + `model` (`thread/start`)
- Persists server-assigned `threadId` in gateway session metadata
- Streams `item/agentMessage/delta` fragments as ordered `StreamChunk`s
- Handles server approval requests with policy denial
- Completes turn (`turn/completed`)
- Reuses the existing `threadId` across subsequent turns for the same session key

### Captured RED Failure Output
```
running 2 tests
test test_omo_backend_unreachable_daemon_error ... ok
test test_omo_backend_e2e_thread_lifecycle_and_streaming ... FAILED

failures:

---- test_omo_backend_e2e_thread_lifecycle_and_streaming stdout ----

thread 'test_omo_backend_e2e_thread_lifecycle_and_streaming' (28416676) panicked at tests/test_omo_backend.rs:260:5:
First turn failed: Some(Multiplexer("OmoBackend not yet implemented (RED phase)"))
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

failures:
    test_omo_backend_e2e_thread_lifecycle_and_streaming

test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

---

## GREEN Phase

### Implementation Summary
- `src/agent/omo_config.rs`: `AgentBackendKind` (`Llm`, `Omo`) and `OmoBackendConfig` with environment variable parsing (`OMON_AGENT_BACKEND`, `OMON_OMO_APPSERVER_URL`, `OMON_OMO_APPSERVER_AUTH_TOKEN`, `OMON_DEFAULT_MODEL`), default fallbacks, URL validation, and fail-fast configuration checks.
- `src/agent/omo_protocol.rs`: Framing and JSON-RPC message builders for `initialize`, `thread/start`, `turn/start`, approval denial responses, and approval request detection.
- `src/agent/omo_backend.rs`: `OmoBackend` implementing `AgentBackend` over tokio-tungstenite WebSocket transport, bounded timeouts, session metadata persistence (`omo_thread_id`), streaming delta chunk emission via `OutboundDispatcher`, and immediate denial of server approval requests.
- `tests/test_omo_backend.rs`: Integration tests with in-process fake `omo app-server` validating full lifecycle, threadId reuse across turns, streaming chunk ordering, approval rejection, and unreachable daemon handling.

### Captured GREEN Success Output
```
running 2 tests
test test_omo_backend_unreachable_daemon_error ... ok
test test_omo_backend_e2e_thread_lifecycle_and_streaming ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s
```

### Config Unit Tests Output
```
running 5 tests
test agent::omo_config::tests::test_agent_backend_kind_parses_supported_aliases ... ok
test agent::omo_config::tests::test_agent_backend_kind_defaults_to_llm_when_absent ... ok
test agent::omo_config::tests::test_agent_backend_kind_invalid_returns_descriptive_error ... ok
test agent::omo_config::tests::test_omo_backend_config_validates_websocket_scheme ... ok
test agent::omo_config::tests::test_omo_backend_config_from_env_parses_url_and_token ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 239 filtered out; finished in 0.00s
```

### Full Workspace Test Suite Output
```
test result: ok. 244 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.52s
test result: ok. 53 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.25s
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.19s
test result: ok. 38 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.09s
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.41s
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.09s
Total: 378 passed; 0 failed.
```
