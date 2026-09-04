# daemon-multiplexer-agent-workspaces - Work Plan

## TL;DR (For humans)
<!-- Fill this LAST, after the detailed plan below is written, so it summarizes the REAL plan. -->
<!-- Plain English for a non-engineer: NO file paths, NO todo numbers, NO wave/agent/tool names. -->

**What you'll get:** 디스코드 봇(에리스·wawabot·실피)과 크론 작업마다 자기 전용 작업 폴더와 자기 전용 기억장소가 생깁니다. 서로의 파일과 기억을 보지 못하고, 각자의 폴더에서 작업합니다. 기존 대화 기록은 그대로 보존됩니다(봇의 대화 맥락만 새로 시작).

**Why this approach:** 데몬 하나를 그대로 쓰면서 스레드마다 작업 폴더(cwd)만 다르게 지정하는 방식이 프로토콜에서 공식 지원되고, 메모리 위치도 그 폴더에서 자동으로 분리되기 때문입니다. 봇마다 데몬을 여러 개 띄우는 방식보다 운영이 단순하고 장애 영향이 작습니다.

**What it will NOT do:** 보안 샌드박스(강제 접근 차단)가 아니라 작업 공간 분리입니다. 봇별 데몬을 여러 개 띄우지 않습니다. 기존 대화 맥락은 강제로 초기화합니다(사용자가 (b)를 선택). Discord 화면 조작은 하지 않습니다.

**Effort:** Short
**Risk:** Medium - 기존 스레드 전체 리바인딩(봇 대화 맥락 초기화)이 사용자 경로에 직접 닿음
**Decisions to sanity-check:** 강제 마이그레이션(b) 확정 · 기본 ON 킬스위치 · shared 루트는 메타데이터로만 전송

Your next move: momus 고정밀 리뷰 결과를 확인한 뒤, /ulw-execute로 실행 지시. Full execution detail follows below.

---

> TL;DR (machine): <1 line - effort, risk, deliverables>

## Scope
### Must have
### Must NOT have (guardrails, anti-slop, scope boundaries)

## Verification strategy
> Zero human intervention - all verification is agent-executed.
- Test decision: <TDD | tests-after | none> + framework
- Evidence: <attemptDir>/task-<N>-daemon-multiplexer-agent-workspaces.<ext> (attemptDir = currentAttemptDir from 'omo-agent-toolkit ulw-loop status --json', .omo/evidence/ulw/<session>/<goalId>/a<attempt>; outside ulw-loop use .omo/evidence/)

## Execution strategy
### Parallel execution waves
> Target 5-8 todos per wave. Fewer than 3 (except the final) means you under-split.

### Dependency matrix
| Todo | Depends on | Blocks | Can parallelize with |
| --- | --- | --- | --- |

## Todos
> Implementation + Test = ONE todo. Never separate.
<!-- APPEND TASK BATCHES BELOW THIS LINE WITH edit/apply_patch - never rewrite the headers above. -->
- [x] 1. <title>
  What to do / Must NOT do: <...>
  Parallelization: Wave <N> | Blocked by: <...> | Blocks: <...>
  References (executor has NO interview context - be exhaustive): <src/path:lines>
  Acceptance criteria (agent-executable): <exact command or assertion>
  QA scenarios (name the exact tool + invocation): happy + failure, Evidence <attemptDir>/task-1-daemon-multiplexer-agent-workspaces.<ext>
  Commit: <Y/N> | <type>(<scope>): <summary>

## Final verification wave
> Runs in parallel after ALL todos. ALL must APPROVE. Surface results and wait for the user's explicit okay before declaring complete.
- [x] F1. Plan compliance audit
- [x] F2. Code quality review
- [x] F3. Real manual QA
- [x] F4. Scope fidelity

## Commit strategy

## Success criteria

## Context (verified facts, file:line)
- Senpi daemon `thread/start` consumes `cwd` (handlers.js:131), persists it in the session JSONL header (session-manager.js:579-588), and routes ALL tool execution through it (bash spawn tools/bash.js:53-58; relative paths via resolveToCwd path-utils.js:40-42). One daemon process isolates threads by threadId with per-thread cwd (registry.js:14-22,174-188).
- `thread/resume` IGNORES cwd overrides (handlers.js:143) and always restores the persisted cwd (registry.js:43-45) — overrides are immutable per thread.
- `runtimeWorkspaceRoots` is NOT enforced by the daemon; it is response metadata only (handlers.js:353). Daemon sandbox defaults to dangerFullAccess. Workspace separation = working-context separation, NOT a hard security sandbox.
- Daemon does NOT create the cwd directory; its bash tool fails with "Working directory does not exist" (tools/bash.js:46-51) → the GATEWAY must create it before thread/start.
- Memory identity auto-derives from cwd: id = `${sanitizeToSlug(basename(cwd))}-${sha256(cwd)[0..8]}` (memory-core resolve.ts:38-47); explicit identity via `.omo/omo.json` `{"memory":{"agent":"<name>"}}` in the workspace (memory/index.ts:114). Memory projection runs in app-server mode (runtime.js:121-128 → agent-session.js:2575-2595 → prompt.ts:47-75). Identity binds at session_start; resume enforces the binding unchanged.
- Gateway today sends only developerInstructions + model (src/agent/omo_protocol.rs:21-36; src/agent/omo_backend.rs:163-170). Injection point: `resolve_thread_id` (omo_backend.rs:137-259) owns `&mut SessionContext`. Construction sites: main.rs:1218-1229 (interactive), main.rs:1306-1315 (cron), dashboard_runtime.rs:116-126/139-148. OMON_WORKSPACE_ROOT parsed at main.rs:150-158 / dashboard_runtime.rs:205-214 but does NOT reach OmoBackend today.
- SessionKey fields: platform/guild/channel/thread/user/bot (src/models/session.rs:8-20). Cron sessions: user_id = "cron:<job_id>" (main.rs:553,563). Dashboard: platform "web", user "dashboard" (dashboard.rs:2144).
- User decision (approved): fork (b) FORCE MIGRATION — on deploy, clear existing `omo_thread_id` from all sessions so every session rebinds to its per-agent workspace at its next turn (DB message history is untouched; bots lose OMO-side conversation context by explicit choice).

## Decisions (all resolved — executor has zero judgment calls)
- D1 Slug formula: from SessionKey → `bot-{bot_id}` | `cron-{job_id}` | `web-dashboard` (platform=="web" or user=="dashboard") | `user-{user_id}` | `sys-default`. Sanitize: lowercase; chars outside [a-z0-9_-] → '-'; collapse '-'; trim '-'. If sanitized != raw or len > 48 → append `-` + first 8 hex of sha256(raw_identity_string). raw_identity_string = the unsanitized category value (bot_id / job_id / "dashboard" / user_id).
- D2 Layout: cwd = `{OMON_WORKSPACE_ROOT}/agents/<slug>`; `runtimeWorkspaceRoots` = [`{...}/agents/<slug>`, `{...}/shared`] (metadata only, sent for forward-compat). Gateway creates BOTH directories (tokio::fs::create_dir_all, idempotent) before sending thread/start.
- D3 Memory binding: gateway writes `{agents/<slug>}/.omo/omo.json` = `{"memory":{"agent":"<slug>"}}` if the file does not exist (stable explicit identity, independent of path hashing). Never overwrite an existing omo.json.
- D4 Env surface: `OMON_PER_AGENT_WORKSPACE` bool, default ON; values false/0/off/no → legacy behavior (no cwd/roots sent, no dirs created). Parsed in OmoBackendConfig as `per_agent_workspace: bool` + `workspace_root: Option<PathBuf>` (from OMON_WORKSPACE_ROOT; None disables).
- D5 Migration (b): one-time step at gateway boot (after DB pool init, before routing starts): if per-agent workspace enabled → `UPDATE sessions SET state_json = json_remove(state_json, '$.omo_thread_id') WHERE json_extract(state_json,'$.metadata.omo_thread_id') IS NOT NULL` + log wiped count. In-memory thread cache starts empty per process (already the case). Cron-lane daemon restarts on deploy, so lane state resets naturally.
- D6 Failure policy: workspace dir creation/canonicalization failure → turn fails fast with typed OmonError (no silent fallback to global workspace).
- D7 thread/resume is NOT modified (daemon ignores overrides there). Replacement-thread path (stale eviction → thread/start) automatically gains the workspace via the same thread/start change.

## Scope
### Must have
- Deterministic identity→slug resolver (unit-tested, incl. cron/dashboard/snowflake-collision/sanitize/hash cases)
- Workspace dir + .omo/omo.json provisioning in OmoBackend before thread/start
- cwd + runtimeWorkspaceRoots propagation in thread_start_request
- OMON_PER_AGENT_WORKSPACE kill-switch (default ON) + workspace_root wiring into OmoBackend at all 4 construction sites
- Boot-time one-time omo_thread_id wipe (fork b) with count logging
- RED-first tests for every behavior above + full gates + production deploy + live proof that a per-agent memory repo materializes
### Must NOT have (guardrails)
- NO per-bot daemon spawning (Option C deferred — single daemon per lane stays)
- NO changes to thread_resume_request signature/behavior
- NO gateway-side sandbox claims: runtimeWorkspaceRoots is metadata; do not present workspace separation as a security boundary in code comments or docs
- NO modification of Discord adapter, cron scheduling semantics, or .env
- NO silent fallback to the global workspace on provisioning failure
- NO git commit/push without explicit user go-ahead at that moment (user controls repo state changes)

## Todos
<!-- APPEND TASK BATCHES BELOW THIS LINE WITH edit/apply_patch - never rewrite the headers above. -->
- [x] 1. Identity slug resolver (src/agent/agent_workspace.rs NEW)
  What to do / Must NOT do: Implement `pub fn agent_workspace_slug(key: &SessionKey) -> String` per D1 + `pub struct AgentWorkspace { pub cwd: PathBuf, pub roots: Vec<PathBuf> }` resolver `pub fn resolve(base: &Path, key: &SessionKey) -> AgentWorkspace`. Unit tests FIRST (RED): bot session, cron session, web/dashboard, snowflake-vs-cron no-collision, sanitize+hash path (id with '/' and spaces), sys-default when both absent. Must NOT touch other modules.
  Parallelization: Wave 1 | Blocked by: none | Blocks: 2,3
  References: src/models/session.rs:8-20; decisions D1
  Acceptance criteria (agent-executable): `cargo test --lib agent_workspace` green after RED capture; all 8 unit cases pass
  QA scenarios: cargo test output tail as evidence. Evidence <attemptDir>/task-1-daemon-multiplexer-agent-workspaces.log
  Commit: N (single commit at end per user preference)
- [x] 2. Protocol payload propagation (src/agent/omo_protocol.rs)
  What to do / Must NOT do: Extend `thread_start_request(system_prompt, model, workspace: Option<&AgentWorkspace>)` to serialize `cwd` + `runtimeWorkspaceRoots` per D2. Unit test FIRST (RED): workspace Some → JSON contains exact cwd string + roots array in order [private, shared]; None → params unchanged from today. Do NOT modify thread_resume_request.
  Parallelization: Wave 1 | Blocked by: 1 (type only) | Blocks: 3
  References: src/agent/omo_protocol.rs:21-36; tests/test_omo_backend.rs FakeAppServer pattern
  Acceptance criteria: `cargo test --lib omo_protocol` green after RED; JSON shape asserted literally
  QA scenarios: unit tails. Evidence <attemptDir>/task-2-daemon-multiplexer-agent-workspaces.log
  Commit: N
- [x] 3. OmoBackend workspace wiring + provisioning (src/agent/omo_backend.rs, src/agent/omo_config.rs, src/main.rs, src/dashboard_runtime.rs)
  What to do / Must NOT do: (a) Add `per_agent_workspace: bool` + `workspace_root: Option<PathBuf>` to OmoBackendConfig (from_env/cron_from_env parse OMON_PER_AGENT_WORKSPACE/OMON_WORKSPACE_ROOT; kill-switch RED test first). (b) In resolve_thread_id, when enabled and starting a NEW thread (including stale-replacement): resolve AgentWorkspace, tokio::fs::create_dir_all private+shared dirs, write .omo/omo.json per D3 if absent (RED integration test first with FakeAppServer capturing received cwd/roots — add `received_cwd`/`received_roots` ParkingMutex fields per tests/test_omo_backend.rs:201-224 pattern; use a tempdir OMON_WORKSPACE_ROOT in test config). (c) Inject workspace_root into all 4 OmoBackend construction sites. (d) Provisioning failure → typed error fail-fast (test). Must NOT change resume flow or Discord adapter.
  Parallelization: Wave 2 | Blocked by: 1,2 | Blocks: 4,5
  References: src/agent/omo_backend.rs:137-259 (esp. 163-170, 201-204); src/agent/omo_config.rs:38-45; src/main.rs:1218-1229,1306-1315; src/dashboard_runtime.rs:116-126,139-148; tests/test_omo_backend.rs:46-245
  Acceptance criteria: RED captured (missing params) → GREEN; `cargo test --test test_omo_backend` 12+ green; LSP clean on changed files
  QA scenarios: integration test asserts received_cwd == tmp/agents/bot-123 and omo.json content; failure case asserts typed error. Evidence <attemptDir>/task-3-daemon-multiplexer-agent-workspaces.log
  Commit: N
- [x] 4. Boot-time forced migration (src/main.rs, src/storage/db.rs or new src/agent/workspace_migration.rs)
  What to do / Must NOT do: On boot, after pool init and only when per-agent workspace enabled: run D5 UPDATE wiping `$.omo_thread_id` from all session state_json; log wiped count at INFO. RED test FIRST: in-memory DB seeded with 2 sessions having omo_thread_id + 1 without → migration fn removes exactly 2 and leaves other metadata intact. Must NOT touch other metadata keys or messages.
  Parallelization: Wave 2 | Blocked by: 3 | Blocks: 6
  References: src/main.rs pool init region; src/storage/db.rs session schema; decision D5
  Acceptance criteria: migration unit test green after RED; boot log line present in test capture
  QA scenarios: unit test + assertion on state_json post-condition. Evidence <attemptDir>/task-4-daemon-multiplexer-agent-workspaces.log
  Commit: N
- [x] 5. Full gates + regression sweep
  What to do / Must NOT do: cargo fmt --check, clippy --all-targets --all-features -- -D warnings, cargo test --all-targets --all-features, cargo build --release, LSP diagnostics on all changed files. Fix any regression introduced by this feature only. Must NOT fix unrelated pre-existing failures.
  Parallelization: Wave 3 | Blocked by: 4 | Blocks: 6
  References: repo gate conventions
  Acceptance criteria: all gates exit 0; e2e tests (test_wiring_e2e, test_omo_backend e2e) green — characterization for unchanged resume flow
  QA scenarios: gate tails. Evidence <attemptDir>/task-5-daemon-multiplexer-agent-workspaces.log
  Commit: N
- [x] 6. Production deploy + live-surface proof (fork b executes here)
  What to do / Must NOT do: launchctl kickstart -k gui/$(id -u)/ai.omon.gateway; verify health + 3 bots ready + boot log shows migration wipe count; verify dirs created: ls ~/.omon/workspace/agents/; trigger omon-katok-3h-group-digest-v3 via dashboard API and confirm its per-agent workspace + memory repo materialize: ls ~/.omon/workspace/agents/cron-omon-katok-3h-group-digest-v3* and ~/.omo/memory/agents/ | grep cron-omon-katok. NO Discord GUI interaction (user does manual QA).
  Parallelization: Wave 4 | Blocked by: 5 | Blocks: F1-F4
  References: launchd plist ai.omon.gateway; dashboard POST /api/cron/jobs/{id}/trigger
  Acceptance criteria: health ok; agents/<slug> dirs exist; cron run succeeded; per-agent memory repo directory exists after the turn
  QA scenarios: curl /api/health; ls -d on dirs; sqlite read-only cron_runs check. Evidence <attemptDir>/task-6-daemon-multiplexer-agent-workspaces.log
  Commit: gated on explicit user go-ahead at that moment | fix(agent,discord)→ fix(agent): per-agent workspaces and memory identity via thread cwd

## Final verification wave
> Runs in parallel after ALL todos. ALL must APPROVE. Surface results and wait for the user's explicit okay before declaring complete.
- [x] F1. Plan compliance audit
  Recommended task executor category: unspecified-high
  Verify every Must-have shipped, every Must-NOT-have violated nowhere, D1-D7 implemented exactly (diff vs this plan).
- [x] F2. Code quality review
  Recommended task executor category: unspecified-high
  Adversarial review of the full diff: slug correctness, provisioning failure paths, migration SQL safety, kill-switch completeness, test honesty (no tautology).
- [x] F3. Real manual QA
  Recommended task executor category: unspecified-high
  Re-run live proof independently: health, agents/ dirs, cron trigger, memory repo materialization, logs. NO Discord GUI.
- [x] F4. Scope fidelity
  Recommended task executor category: unspecified-high
  git diff vs plan scope; confirm no drive-by changes; user WIP files untouched (src/agent/llm.rs, src/tools/cron.rs).

## Commit strategy
Single commit after F1-F4 approve AND the user gives explicit go-ahead in that turn (standing rule: user controls repo state changes). Style: `fix(agent): per-agent workspaces and memory identity via thread cwd` + body per repo convention + omo attribution footer. Push only on explicit user instruction.

## Success criteria
- SC1 (happy): session with bot_id produces thread/start containing cwd `$OMON_WORKSPACE_ROOT/agents/bot-<bot_id>` — RED(G missing param)→GREEN, FakeAppServer capture
- SC2 (edge): cron/dashboard/missing-identity sessions map to cron-/web-/sys- slugs; sanitize+hash collision cases deterministic — unit RED→GREEN
- SC3 (kill-switch): OMON_PER_AGENT_WORKSPACE=false → no cwd/roots sent, no dirs created — RED→GREEN
- SC4 (migration): boot wipe removes omo_thread_id from all sessions exactly once — RED→GREEN unit
- SC5 (regression): existing e2e/OMO suites green (resume flow unchanged) — full suite exit 0
- SC6 (live): after deploy, per-agent dirs + per-agent memory repo materialize on real daemon traffic; cron run succeeds — shell evidence
- SC7 (safety): provisioning failure fails the turn with typed error, never silent global fallback — RED→GREEN
