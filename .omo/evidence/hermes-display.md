# Hermes-style activity display — RED/GREEN evidence

## RED (before backend wiring)

`cargo test --test test_omo_backend test_omo_backend_emits_hermes_activity_lines`

- Fake server emitted: item/completed(reasoning, text="hmm let me think about this carefully"),
  item/started(commandExecution, command="echo OMOITEMPROBE-OK"),
  item/commandExecution/outputDelta(delta="OMOITEMPROBE-OK"),
  item/completed(commandExecution, aggregatedOutput="OMOITEMPROBE-OK"),
  agentMessage deltas "OMO"+"ACT-OK", turn/completed.
- Result: FAILED — last chunk content = "OMOACT-OK" (no activity lines), assertions
  on "⚙️ Running…", "> 💭 …", "⤷ …" all missing.

## GREEN (after wiring)

(captyred below after implementation)

## Real payload shapes (probe capture, ws://127.0.0.1:19743)

- item/started params.item: {"type":"commandExecution","id":"call_F1XMo5i2IcYS1JKn419HcT65","command":"echo OMOITEMPROBE-OK","cwd":"...","status":"inProgress"}
- item/commandExecution/outputDelta params: {threadId, turnId, itemId, delta:"OMOITEMPROBE-OK"}
- item/completed params.item: {"type":"commandExecution",...,"aggregatedOutput":"OMOITEMPROBE-OK","status":"completed"}
- reasoning events: item/reasoning/textDelta | summaryTextDelta | summaryPartAdded (protocol d.ts); completed reasoning item carries text.

## GREEN (after wiring)

- OmoBackend now handles item/started + item/completed for
  commandExecution/webSearch/fileChange/mcpToolCall (Hermes-formatted line),
  emits `> 💭 …` block on completed reasoning, appends `⤷ <output excerpt>`
  for completed commands, and the final chunk (and persisted message)
  carries activity block + reply.
- `test_omo_backend_emits_hermes_activity_lines`: PASSED — final chunk
  layout pinned to:
  `> 💭 hmm let me think about this carefully…\n⚙️ Running `echo OMOITEMPROBE-OK`…\n⤷ OMOITEMPROBE-OK\n\nOMOACT-OK`
- Existing tests (no-activity e2e, session continuity, config) unaffected.
- Formatter unit tests: 10 passed (omo_activity.rs).

## Follow-up production fixes (2026-08-29)

1. FK regression (Discord: "database error: (code: 787) FOREIGN KEY constraint failed")
   - RED: test_omo_backend_persists_without_preexisting_session_row failed with
     `Database("error returned from database: (code: 787) FOREIGN KEY constraint failed")`
     — cron sessions are created implicitly by backend.run, and the assistant
     persist violated messages->sessions FK.
   - Fix: `ensure_session_row` INSERT OR IGNORE before persist in omo_backend.
   - GREEN: test passes; session row + assistant row ("Hello World!") both present;
     PRAGMA foreign_keys=1 verified on the in-memory pool.

2. Transport retry (Discord: "LLM error: ws streaming error: IO error: Connection reset by peer (os error 54)")
   - `run` now retries exactly once on connection-reset-class failures
     (Connection reset / os error 54 / Broken pipe / connection closed);
     the persisted threadId keeps continuity across the retry.
   - Test: test_omo_backend_retries_once_after_connection_drop — fake server
     drops the first connection abruptly (no close frame); asserts Ok,
     conn_count == 2, and the full reply on the second connection.
