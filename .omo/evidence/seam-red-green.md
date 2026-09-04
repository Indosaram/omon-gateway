# AgentBackend Trait Seam & Refactor Verification Evidence

## 1. Overview & Architecture Decision

### Trait Boundary Choice: Runner Boundary vs LlmClient Boundary
The `AgentBackend` trait seam is introduced at the **Runner boundary** (`run`, `run_cancelable`, `cancel`) rather than the lower-level `LlmClient` boundary (`stream_with_tool_calls`):

1. **Per-Turn Model Overrides**: The session context (`session.state.active_model`) determines which model is used for a given turn. At the runner boundary, the backend has full access to `SessionContext` to dynamically construct, select, or configure per-turn LLM clients or backend targets.
2. **Tool Definitions & Filtering Flow**: Tool availability depends on session-level configuration (`session.state.enabled_toolsets`). The runner boundary encapsulates the conversion of `ToolRegistry` into `Vec<ToolDefinition>`, tool dispatch execution, output truncation, status notifications to the dispatcher, and multi-round tool execution loops.
3. **Backend Abstraction Parity**: Future backend implementations (such as remote agent protocol / omo appserver workers) manage their own tool loops and agent runtimes remotely. Placing the trait at the runner boundary allows `SessionActor` to treat direct-LLM execution (`LlmBackend`) and remote agent backends uniformly.
4. **Delivery Ledger & Lifecycle Integrity**: Cancellation (`cancel`), streaming token chunks (`StreamChunk`), media directives, silence sentinels, and message transcript persistence natively align with the turn lifecycle managed at the runner boundary.

### Module Layout & Note on `repair.rs`
- `src/agent/backend.rs`: Defines `#[async_trait] pub trait AgentBackend` and trait documentation.
- `src/agent/llm_backend.rs`: Implements `LlmBackend` and `impl AgentBackend for LlmBackend`. Message parsing and role sanitization helpers (`ThinkStripper`, `repair_message_sequence`, `truncate_large_content`) were initially explored in a separate `repair.rs` file, but were folded directly into `llm_backend.rs` to keep the file structure cohesive and avoid extra scratch files.
- `src/agent/mod.rs`: Exports `AgentBackend`, `LlmBackend`, `StreamEmissionState`, `ThinkStripper`, `repair_message_sequence`, `truncate_large_content`.
- `src/multiplexer/actor.rs`: Uses `AgentBackend` (with `AgentRunner` aliased to `AgentBackend`), and contains the scripted fake backend characterization test.

---

## 2. Pre-Refactor Test Suite Run

```text
     Running unittests src/lib.rs (target/debug/deps/omon_gateway-14abbd0622951339)
test result: ok. 238 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.92s

     Running unittests src/entry.rs (target/debug/deps/omon_gateway-7899c782664c3f23)
test result: ok. 53 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s

     Running tests/test_agent_tools.rs (target/debug/deps/test_agent_tools-944a7aa2c09529dc)
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.30s

     Running tests/test_discord_adapter.rs (target/debug/deps/test_discord_adapter-b00b05a5b6f854de)
test result: ok. 38 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.47s

     Running tests/test_e2e_stress.rs (target/debug/deps/test_e2e_stress-810e666b3aa3772b)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.09s

     Running tests/test_message_context.rs (target/debug/deps/test_message_context-ec4e81ba4fc297c5)
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.09s

     Running tests/test_migrate.rs (target/debug/deps/test_migrate-d699e3e6db6bfd04)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

     Running tests/test_multiplexer.rs (target/debug/deps/test_multiplexer-c9b7f813ebcbadf2)
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.39s

     Running tests/test_profile_routing.rs (target/debug/deps/test_profile_routing-46960591f67d7054)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

     Running tests/test_voice_cron.rs (target/debug/deps/test_voice_cron-8c1d9e12562c7b17)
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s

   Doc-tests omon_gateway
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

Total tests passed: 374 passed, 0 failed
```

---

## 3. Characterization Test

Added in `src/multiplexer/actor.rs`: `actor_turn_loop_with_scripted_fake_backend`.
Pins the actor turn loop against a scripted fake `AgentBackend`, asserting:
- User inbound message is persisted to the database transcript with correct session key and platform message ID.
- Assistant output chunks are delivered sequentially through `OutboundDispatcher` (`StreamChunk` sequence 0 and sequence 1 final).
- Assistant output message is persisted to the database transcript.
- Delivery ledger claim transitions to `delivered`.

```text
test multiplexer::actor::tests::actor_turn_loop_with_scripted_fake_backend ... ok
```

---

## 4. Post-Refactor Verification Gates

### Gate 1: `cargo build`
```text
$ cargo build
   Compiling omon-gateway v0.1.0 (/Users/indo/code/project/omon-gateway)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.56s
```

### Gate 2: `cargo test`
```text
$ cargo test
     Running unittests src/lib.rs (target/debug/deps/omon_gateway-14abbd0622951339)
test result: ok. 239 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.85s

     Running unittests src/entry.rs (target/debug/deps/omon_gateway-7899c782664c3f23)
test result: ok. 53 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s

     Running tests/test_agent_tools.rs (target/debug/deps/test_agent_tools-944a7aa2c09529dc)
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s

     Running tests/test_discord_adapter.rs (target/debug/deps/test_discord_adapter-b00b05a5b6f854de)
test result: ok. 38 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running tests/test_e2e_stress.rs (target/debug/deps/test_e2e_stress-810e666b3aa3772b)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.09s

     Running tests/test_message_context.rs (target/debug/deps/test_message_context-ec4e81ba4fc297c5)
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s

     Running tests/test_migrate.rs (target/debug/deps/test_migrate-d699e3e6db6bfd04)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

     Running tests/test_multiplexer.rs (target/debug/deps/test_multiplexer-c9b7f813ebcbadf2)
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.39s

     Running tests/test_profile_routing.rs (target/debug/deps/test_profile_routing-46960591f67d7054)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

     Running tests/test_voice_cron.rs (target/debug/deps/test_voice_cron-8c1d9e12562c7b17)
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s

   Doc-tests omon_gateway
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

Total tests passed: 375 passed, 0 failed
```

### Gate 3: `cargo clippy --all-targets -- -D warnings`
```text
$ cargo clippy --all-targets -- -D warnings
    Checking omon-gateway v0.1.0 (/Users/indo/code/project/omon-gateway)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 7.04s
```

### Gate 4: `cargo fmt --check`
```text
$ cargo fmt --check
(clean, exit 0)
```
