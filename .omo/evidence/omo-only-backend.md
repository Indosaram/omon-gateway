# Evidence: OMO-Only Agent Backend Migration

## 1. RED Phase Test Failures

### `cargo test test_validate_agent_backend_env_contract`
```
running 1 test
test agent::omo_config::tests::test_validate_agent_backend_env_contract ... FAILED

failures:

---- agent::omo_config::tests::test_validate_agent_backend_env_contract stdout ----

thread 'agent::omo_config::tests::test_validate_agent_backend_env_contract' (29671881) panicked at src/agent/omo_config.rs:6:5:
not yet implemented: validate_agent_backend_value not implemented yet
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    agent::omo_config::tests::test_validate_agent_backend_env_contract

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 241 filtered out; finished in 0.00s
```

### `cargo test --test test_wiring_e2e test_boot_time_backend_selection_parsing`
```
running 1 test
test test_boot_time_backend_selection_parsing ... FAILED

failures:

---- test_boot_time_backend_selection_parsing stdout ----

thread 'test_boot_time_backend_selection_parsing' (29672572) panicked at src/agent/omo_config.rs:6:5:
not yet implemented: validate_agent_backend_value not implemented yet
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    test_boot_time_backend_selection_parsing

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s
```

---

## 2. GREEN Phase Test Passes

### `cargo test agent::omo_config::tests::test_validate_agent_backend_env_contract`
```
running 1 test
test agent::omo_config::tests::test_validate_agent_backend_env_contract ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 241 filtered out; finished in 0.00s
```

### `cargo test --test test_wiring_e2e test_boot_time_backend_selection_parsing`
```
running 1 test
test test_boot_time_backend_selection_parsing ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s
```

### Full Test Suite Run (`cargo test`)
```
test result: ok. 242 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.80s (lib)
test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s (bin entry)
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.16s (test_agent_tools)
test result: ok. 38 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s (test_discord_adapter)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.09s (test_e2e_stress)
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s (test_message_context)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s (test_migrate)
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.39s (test_multiplexer)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s (test_omo_backend)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s (test_profile_routing)
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s (test_voice_cron)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.16s (test_wiring_e2e)
```

## Auto-spawn real-surface proof (lead-verified, 2026-08-28)

- Gateway booted with NO env vars (only temp DATABASE_URL) and NO daemon running.
- Log: `spawning omo app-server daemon url=ws://127.0.0.1:19742 bin=omo` -> `omo app-server daemon ready` (1.6s).
- POST /api/sessions/test-autospawn/chat -> HTTP 202; assistant row `OMOAUTOSPAWN-OK` in temp DB after ~4s.
- Keep-alive observed: daemon pid changed (30822 -> 31554) after child death — watcher restart works.
- Abrupt parent kill edge: daemon orphans (macOS has no PDEATHSIG); next boot reuses it via readyz probe. Graceful shutdown (Drop) kills it.
- Cleanup: orphan pid 31554 killed, port 19742 freed, /tmp/omon-verify2 removed.
