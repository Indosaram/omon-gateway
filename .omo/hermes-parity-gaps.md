# Hermes → omon-gateway Behavioral Parity Gaps

**Scope:** Discord user-facing runtime behavior. Excludes non-Discord platforms
(Signal/WhatsApp/QQ/WeChat) and infra a single local launchd instance doesn't need
(systemd/cgroup/scale-to-zero/memory_monitor/shutdown_forensics).

**Why gaps exist:** omon-gateway is a fresh ~13k-LOC Rust reimplementation covering
~13% of Hermes's ~100k-LOC runtime. The completed migration work built the `migrate`
*mover*, not gateway *behavior parity*. This is the parity audit that was never done.

Evidence cites `file:line` on both sides. Status: PRESENT / PARTIAL / MISSING.
Priority: **P0** breaks basic use or is a security hole · **P1** important · **P2** nice-to-have.

---

## P0 — Must fix (security / correctness / basic use)

### Security

1. **Approval button has NO authorization check** *(flagged by 2 independent audits)*
   - Hermes `plugins/platforms/discord/adapter.py:7819-7851` (`_check_auth` validates clicker vs allowed/admin users).
   - omon MISSING `src/discord/adapter.rs:146-163` — `InteractionCreate` resolves the UUID without checking `component.user.id`.
   - Impact: **any user in the server can click "Approve" on a dangerous-command prompt.**
   - Fill: verify `component.user.id ∈ config.allowed_users` (+ optional admin list) before `resolve_custom_id`; ephemeral refusal otherwise.

2. **No hardline blocklist under `never`/`yolo`**
   - Hermes `tools/approval.py:334-466` (`detect_hardline_command` blocks `rm -rf /`, `mkfs`, raw `dd`/`>` to `/dev/sd*`, fork bombs, `shutdown/reboot`, etc. unconditionally, before any bypass).
   - omon MISSING `src/tools/terminal.rs:137-141` — `Never` bypasses all checks.
   - Impact: `APPROVAL_MODE=never` lets catastrophic commands destroy the host with zero guard.
   - Fill: port `detect_hardline_command` + `sudo -S` stdin guard; enforce before policy/allowlist.

3. **`is_dangerous` is a ~25-line stub (misses 40+ patterns, no payload unwrapping)**
   - Hermes `tools/approval.py:606-825` (47+ regexes) + `866-1988` (quote-strip, `python -c`/`bash -c`/`node -e` payload extraction, grep false-positive filtering).
   - omon PARTIAL `src/tools/terminal.rs:356-382` — bare lowercased whitespace split; only sudo/`rm -rf`/mkfs/dd/chmod777/`curl|sh`/forkbomb.
   - Impact: git force-push, SQL drops, `sed -i`, killall, encoded `base64|bash`, wrapped `python -c "shutil.rmtree('/')"` all bypass approval; benign `grep "rm -rf"` falsely blocked.
   - Fill: port the regex library (use `regex::RegexSet`) + shell tokenizer/deobfuscator + interpreter payload extractors.

4. **No approval memory / decision scopes**
   - Hermes `tools/approval.py:2015-2042` (Once/Session/Always/Deny + `_session_approved` cache).
   - omon MISSING `src/discord/approval.rs:14-17` — binary Approved/Rejected, no cache.
   - Impact: multi-step agent loops re-prompt on *every* repeat of the same command.
   - Fill: extend `ApprovalDecision` to Once/Session/Always/Deny; add `SessionKey → HashSet<PatternKey>` cache; 4-button UI.

### Correctness / basic use

5. **New messages CANCEL the in-flight turn instead of queueing**
   - Hermes `turn_lease.py:73-231` (serializes load→run→flush per session).
   - omon PARTIAL/MISALIGNED `src/multiplexer/actor.rs:114-117,168-176` + `router.rs:212-228` — incoming event calls `cancellation.cancel()` (Superseded), rolling back history.
   - Impact: a quick follow-up (or client-split long paste) aborts the running answer mid-sentence.
   - Fill: queue subsequent events behind the active turn (or finish it first) rather than auto-cancel.

6. **Client-split long messages (>2000 chars) race and abort**
   - Hermes `adapter.py:7558-7660` (`_enqueue_text_event` debounces/merges split chunks).
   - omon MISSING (`adapter.rs` + `multiplexer/actor.rs`) — chunk 1 executes, chunk 2 supersedes/cancels it.
   - Fill: ~500ms debounce aggregation buffer before routing text events. (Tightly related to #5.)

7. **Message history not repaired → provider 400s**
   - Hermes `session.py:2705-2725` (merges consecutive same-role, enforces `system* (user→assistant)* user`).
   - omon MISSING `src/main.rs` `LiveAgentRunner::messages` — raw rows + injected system/memory, no sanitize.
   - Impact: consecutive user turns / memory injection make Anthropic (and strict OpenAI) reject with 400.
   - Fill: sequence sanitizer/merger in `messages`.

8. **No outbound delivery obligation ledger (responses lost on crash)**
   - Hermes `delivery_ledger.py:1-244` (durable outbox pending→attempting→delivered + startup `sweep_recoverable`).
   - omon MISSING — `ledger/service.rs` records inbound only; outbound is fire-and-forget.
   - Impact: crash/restart after generating but before Discord ACK = response silently lost.
   - Fill: `delivery_obligations` table + outbound state + boot-time recovery sweep.

9. **Replied-to / referenced message context is dropped**
   - Hermes `adapter.py:7190-7195,7505-7510` (resolves `reference.resolved` text + attachments).
   - omon PARTIAL `src/discord/adapter.rs:230-235` — accepts InlineReply kind but ignores `referenced_message`.
   - Impact: replying to a message sends only the reply text; the quoted content/attachments are invisible to the model.
   - Fill: hydrate `message.referenced_message` text+attachments into the InboundEvent.

10. **Unsafe allowed-mentions default (`@everyone`/`@here`/role pings)**
    - Hermes `adapter.py:333-365` (`_build_allowed_mentions` denies everyone/here/roles by default).
    - omon MISSING `src/discord/adapter.rs` — no `AllowedMentions` set.
    - Impact: any LLM output or echoed text containing `@everyone`/`@here`/`<@&role>` pings the whole server.
    - Fill: set default `AllowedMentions` (users+replied_user only) on client + every `CreateMessage`/`EditMessage`.

11. **Reasoning `<think>` blocks leak to Discord**
    - Hermes `stream_consumer.py:348-472` (strips `<think>/<thought>/<reasoning>` + orphan tags mid-stream).
    - omon MISSING `src/main.rs:420-435`.
    - Impact: with reasoning models (DeepSeek R1, Qwen, MiniMax) the raw scratchpad streams to users.
    - Fill: streaming think-block state machine buffering partial tags before emit.

12. **Tool-status text corrupts the accumulated response buffer**
    - Hermes `stream_dispatch.py:73-95` (tool progress on a separate queue, decoupled from prose).
    - omon PARTIAL `src/main.rs:470-474` — `⚙️ Running tool...` is pushed into `StreamEmissionState.content`. (We already gate emission off for cron, but for interactive it still concatenates into the prose buffer.)
    - Fill: emit tool progress as separate ephemeral/interim updates, not into the content accumulator.

13. **Cron silent-run suppression missing (`[SILENT]` / empty output spam)**
    - Hermes `cron/scheduler.py:286-347,2440-2449,3833-3837` (suppresses `[SILENT]` and empty/`wakeAgent:false`).
    - omon MISSING `src/cron/scheduler.rs:743-752` + `main.rs:613,658` — empty → "Cron job completed"; `[SILENT]` posted literally.
    - Impact: watchdog/no-alert cron jobs spam Discord every run.
    - Fill: treat `Ok(None)`/empty as silent (skip `deliver`); add `is_cron_silence_response` for `[SILENT]`/`NO_REPLY`.

14. **Cron `repeat.times` ignored — finite jobs run forever**
    - Hermes `cron/jobs.py:1519-1538` (tracks `completed`, deletes at `>= times`).
    - omon MISSING `src/cron/scheduler.rs:589-617` + `store.rs:100` — parsed but discarded.
    - Fill: persist+increment `completed`; disable/delete at limit.

---

## P1 — Important

- **Intentional-silence + anti-loop filter** (`NO_REPLY`/`[SILENT]`/`*(silent)*`): Hermes `response_filters.py:1-87`, `delivery.py:290-302`; omon MISSING `main.rs:445-458` — emits "empty response" text, bot-to-bot ping-pong risk.
- **Reply threading on send** (`reply_to`): Hermes `adapter.py:2880-2917`; omon MISSING `adapter.rs:498-508` — `reply_to` discarded; wire `CreateMessage::reference_message` w/ fallback.
- **Processing reactions** (👀 → ✅/❌): Hermes `adapter.py:2761-2815`; omon MISSING — no ack on user message.
- **Live-edit flicker / multi-message churn**: Hermes single preview + `_edit_overflow_split` on finalize (`adapter.py:3085-3289`); omon PARTIAL `throttler.rs:125-161` — edits multiple live messages, burns rate limit.
- **Outbound `MEDIA:<path>` upload**: Hermes `base.py:101-115` + `adapter.py:3291-3440`; omon PARTIAL — literal `MEDIA:` text instead of attachment upload (also cron `scheduler.py:1330-1365`).
- **Cron `context_from` chaining**: Hermes `cron/scheduler.py:2384-2432`; omon MISSING `store.rs:114` + `main.rs:595-667` — parsed, never injected; breaks multi-step pipelines.
- **Cron missing-skill resilience**: Hermes warns+continues (`scheduler.py:2530-2543`); omon PARTIAL `main.rs:736-741` — hard-fails whole job.
- **Cron gateway-lifecycle footgun guard**: Hermes `cron/lifecycle_guard.py`; omon MISSING — a job scheduling `launchctl kickstart`/restart can crash-loop the daemon.
- **Cron prompt-injection/threat scanner**: Hermes `cronjob_tools.py:80-135`; omon MISSING.
- **Cron `wakeAgent:false` pre-run gate**: Hermes `scheduler.py:2312-2335`; omon MISSING `main.rs:608-620` — can't skip the LLM call.
- **`CronTool` surface**: Hermes has pause/resume/update/trigger/runs/status; omon PARTIAL `tools/cron.rs` — only list/get/add/delete (scheduler *has* pause/resume/trigger, just not exposed to the agent).
- **Restart recovery of in-flight work** (`mark_resume_pending`): Hermes `run.py:9019-9045,7233-7275`; omon MISSING — deploys silently abort long tasks.
- **Restart-loop circuit breaker**: Hermes `restart_loop_guard.py`; omon MISSING — poisoned prompt → infinite crash/respawn.
- **Transcript-level inbound dedup**: Hermes `session.py:2663-2680`; omon PARTIAL — `messages` table has no `platform_message_id` index; ledger-eviction replays duplicate turns.
- **Message timestamp context**: Hermes `message_timestamps.py`; omon MISSING — no temporal context for "yesterday"/"5 min ago".
- **Untrusted prompt-metadata neutralization**: Hermes `session.py:373-402`; omon MISSING `main.rs:225-245` — raw usernames/titles into system prompt (injection surface).
- **Auto-threading on channel mention** + **scoped thread participation**: Hermes `adapter.py:7139-7188`; omon MISSING/PARTIAL — and `adapter.rs:257` makes the primary bot answer **every** message in **every** guild thread unmentioned (spam).
- **Channel allow/ignore lists**, **role/pairing auth**, **channel history backfill**, **inbound text-file inlining**, **profile routing**, **slash-command catalog** (steer/undo/retry/compress/title/thread): all MISSING/PARTIAL per inbound audit.
- **Approval UX**: rich embed + reason, 4-button scopes, timeout auto-expiry of buttons, `/yolo` session toggle, `/deny <reason>` feedback, approval-wait activity heartbeat (prevents GC killing a session mid-approval), config `approvals.deny` globs, permanent allowlist persistence, approval mentions — all MISSING/PARTIAL per approval audit.
- **Memory/Skill write-approval staging**: Hermes `write_approval.py`; omon MISSING — memory/skills written immediately, no review.
- **Session context isolation** (per-session env for subprocesses): Hermes `session_context.py`; omon MISSING.
- **Dead-target short-circuiting**: Hermes `dead_targets.py`; omon MISSING — repeated 403/404 to deleted channels.

---

## P2 — Nice-to-have

- `(1/N)` chunk pagination headers · Forum-channel (type 15) posting · Discord native voice notes (OGG+waveform) · runtime metadata footer (model·ctx%·cwd) · cron delivery fan-out (comma/`all`) · one-shot 120s grace · skill-bundle resolution · cron session mirroring · provider/base_url per-job overrides · workspace `AGENTS.md` injection for cron · `/stop` suspended-state + typing cleanup · drain-control file marker · system readiness probes · DM pairing system · cross-platform transcript mirroring · `thread_require_mention` · `allow_bots` modes · forwarded-message snapshots · channel-topic/per-channel prompts · missed-message backfill on startup · inbound voice-note STT · multi-tool approval hooks · external scanner (Tirith).

---

## Recommended waves

- **Wave A (P0 security):** #1 button auth, #2 hardline floor, #3 dangerous-command engine, #4 approval scopes. Highest risk; ship first.
- **Wave B (P0 correctness):** #5/#6 turn serialization + split-message debounce, #7 history repair, #8 outbound delivery ledger.
- **Wave C (P0 UX/output):** #9 reply context, #10 allowed-mentions, #11 think-block strip, #12 tool-status decouple, #13 cron silence, #14 cron repeat.
- **Wave D+:** P1 by usage frequency (threading/spam fixes, cron chaining, approval UX), then P2 opportunistically.
