# Backend Selection & End-to-End Wiring: RED -> GREEN Evidence

## Task Specification
- **Task**: Wire backend selection end-to-end in the Rust gateway so `OMON_AGENT_BACKEND=omo` routes the Discord actor path AND the dashboard playground path through `OmoBackend`.
- **Deliverables**:
  1. Boot-time selection: absent or `llm` -> `LlmBackend`; `omo` -> `OmoBackend` from `OMON_OMO_APPSERVER_URL`; any other value -> fail boot with an error naming the variable `OMON_AGENT_BACKEND`.
  2. Inject chosen backend at both consumer sites: Discord gateway (`src/main.rs`) and dashboard runtime (`src/dashboard_runtime.rs`).
  3. Persona/model flow: bot profile `system_prompt` + `model` reach the backend per turn/session. For `OmoBackend`, map them to `thread/start` `developerInstructions` + `model` overrides per the protocol. Document in a code comment what happens when a profile changes mid-session.
  4. RED-first e2e integration test: in-process fake app-server + actor over `sqlite::memory:` - one inbound event persists an assistant message produced via `OmoBackend` (assert DB rows); captured failing first in RED, then implemented to GREEN.

---

## RED Phase

### Test: `test_e2e_actor_omo_backend_persists_assistant_message` (`tests/test_wiring_e2e.rs`)
Test spins up an in-process `FakeAppServer` (WebSocket server on `127.0.0.1:0`), initializes an in-memory SQLite database, constructs `OmoBackend` with `with_pool(pool.clone())`, attaches a `ProfileRouter` with specialized `system_prompt` and `model`, starts `SessionMultiplexer`, and routes an `InboundEvent`.

### Captured RED Failure Output
```
   Compiling omon-gateway v0.1.0 (/Users/indo/code/project/omon-gateway)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 5.93s
     Running tests/test_wiring_e2e.rs (target/debug/deps/test_wiring_e2e-1fa2a59bb0908e3d)

running 2 tests
test test_boot_time_backend_selection_parsing ... ok
test test_e2e_actor_omo_backend_persists_assistant_message ... FAILED

failures:

---- test_e2e_actor_omo_backend_persists_assistant_message stdout ----

thread 'test_e2e_actor_omo_backend_persists_assistant_message' (28609182) panicked at tests/test_wiring_e2e.rs:334:5:
assertion `left == right` failed
  left: None
 right: Some("server-assigned-thread-e2e-9999")
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

failures:
    test_e2e_actor_omo_backend_persists_assistant_message

test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.17s
```

### Root Cause Analysis in RED Phase
1. `SessionActor` marked `self.context` updated in memory with the returned `turn_context` (which contained the newly assigned `omo_thread_id` in `session.state.metadata`), but only called `self.flush_if_dirty()` during actor Stop / Evict / Shutdown commands, omitting the database state flush at the end of regular turn completion.
2. End-to-end boot-time wiring was not yet integrated into `src/main.rs` and `src/dashboard_runtime.rs`.

---

## GREEN Phase

### Implementation Details
1. **Boot-Time Selection & Consumer Injection**:
   - `src/main.rs`: Parses `AgentBackendKind::from_env()?` at boot. For `Llm`, instantiates `LiveAgentRunner` with configured `LlmClient`. For `Omo`, loads `OmoBackendConfig::from_env()?` and instantiates `OmoBackend` with `with_pool(pool.clone())`. Injects the chosen `Arc<dyn AgentBackend>` into `SessionMultiplexer`. Unrecognized backend values return a configuration error explicitly identifying `OMON_AGENT_BACKEND`.
   - `src/dashboard_runtime.rs`: Standalone dashboard runtime parses `AgentBackendKind::from_env()?`. When `Omo`, connects to the remote daemon via `OmoBackend` and provides dashboard chat via the multiplexer. When `Llm`, uses `LiveAgentRunner` if `DEFAULT_MODEL` is provided.
2. **Actor Session State Flush**:
   - `src/multiplexer/actor.rs`: Added `self.flush_if_dirty().await` on `TurnOutcome::Completed` so session state mutations (such as `omo_thread_id` recorded in `session.state.metadata`) are committed immediately to the `sessions` table in SQLite upon turn completion.
3. **Persona & Model Flow Documentation**:
   - `src/agent/omo_backend.rs`: Documented how initial `system_prompt` is mapped to `developerInstructions` and `model` is passed on `thread/start`, and how mid-session `/model` changes take effect on each subsequent `turn/start` while `developerInstructions` remain bound to the thread.

### Captured GREEN Success Output (`tests/test_wiring_e2e.rs`)
```
     Running tests/test_wiring_e2e.rs (target/debug/deps/test_wiring_e2e-1fa2a59bb0908e3d)

running 2 tests
test test_boot_time_backend_selection_parsing ... ok
test test_e2e_actor_omo_backend_persists_assistant_message ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.16s
```

### Full Workspace Test Suite Verification
```
     Running unittests src/lib.rs (target/debug/deps/omon_gateway-a631ad16aab756b8)
test result: ok. 244 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.93s

     Running unittests src/entry.rs (target/debug/deps/omon_gateway-d712d8fb25cf78e5)
test result: ok. 53 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.20s

     Running tests/test_agent_tools.rs (target/debug/deps/test_agent_tools-00e544c49d44701f)
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.17s

     Running tests/test_discord_adapter.rs (target/debug/deps/test_discord_adapter-3b140e551b7b013e)
test result: ok. 38 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running tests/test_e2e_stress.rs (target/debug/deps/test_e2e_stress-76e7783be55b1fd5)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s

     Running tests/test_message_context.rs (target/debug/deps/test_message_context-7b01363ee1b7df8a)
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s

     Running tests/test_migrate.rs (target/debug/deps/test_migrate-9fe114aae742f984)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s

     Running tests/test_multiplexer.rs (target/debug/deps/test_multiplexer-dc26e32387330a69)
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.41s

     Running tests/test_omo_backend.rs (target/debug/deps/test_omo_backend-f99aad94037d1f6e)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

     Running tests/test_profile_routing.rs (target/debug/deps/test_profile_routing-2323ccada3894374)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

     Running tests/test_voice_cron.rs (target/debug/deps/test_voice_cron-8e6e91668ee335a7)
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.12s

     Running tests/test_wiring_e2e.rs (target/debug/deps/test_wiring_e2e-1fa2a59bb0908e3d)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.16s

Total: 381 tests passed; 0 failed; 0 ignored.
```
