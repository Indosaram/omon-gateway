# Code Review — cron delivery-verified ack hook (self-review, HEAVY)

Scope: src/cron/ack.rs (new), src/cron/store.rs, src/cron/scheduler.rs, src/agent/omo_backend.rs, src/main.rs, tests/test_omo_backend.rs

Findings (all verified clean):
1. emit_chunk now returns the dispatch Result; non-final call sites keep `let _ =` so streaming behavior is unchanged.
2. Terminal arm: ack executes only when the final dispatch returned Ok AND the session carries non-empty cron_ack_command. Delivery failure keeps the turn outcome unchanged (pre-existing contract) and skips the ack.
3. run_ack_logged swallows ack failures by contract: delivery already succeeded; the producer's next-run status-aware pending commit covers a missed checkpoint. A runaway ack is killed via the 60s bound (kill_on_drop).
4. cron_ack_command metadata is inserted only in the two cron executors (main.rs), so interactive Discord sessions can never trigger the hook.
5. scheduler deliver() runs payload.ack_command only after a successful dispatch (`?` precedes the hook).
6. HermesJob.ack_command is #[serde(default)] — existing payloads and stored rows deserialize unchanged.
7. Regression: full suite 412 passed / 0 failed, clippy -D warnings clean, fmt clean.

Recommendation: APPROVE. codeQualityStatus: pass. Blockers: none.
