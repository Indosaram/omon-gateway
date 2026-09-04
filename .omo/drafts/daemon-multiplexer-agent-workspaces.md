---
slug: daemon-multiplexer-agent-workspaces
status: drafting
intent: clear
review_required: true
plan_path: .omo/plans/daemon-multiplexer-agent-workspaces.md
plan_sha256: null
review_round_id: null
review_round_limit: 5
pending-action: write and review .omo/plans/daemon-multiplexer-agent-workspaces.md
review:
  momus:
    status: pending
    workspace_root: null
    runtime_home: null
    target: .omo/plans/daemon-multiplexer-agent-workspaces.md
    round_id: null
    plan_sha256: null
    launch_id: null
    session: null
    result: null
approach: <fill: the approach you intend to plan>
---

# Draft: daemon-multiplexer-agent-workspaces

## Components (topology ledger)
<!-- Lock the SHAPE before depth. One row per top-level component that can succeed or fail independently. -->
<!-- id | outcome (one line) | status: active|deferred | evidence path -->

## Open assumptions (announced defaults)
<!-- Record any default you adopt instead of asking, so the user can veto it at the gate. -->
<!-- assumption | adopted default | rationale | reversible? -->

## Findings (cited - path:lines)

## Decisions (with rationale)

## Scope IN

## Scope OUT (Must NOT have)

## Open questions

## Approval gate
status: drafting
<!-- When exploration is exhausted and unknowns are answered, set status: awaiting-approval. -->
<!-- That durable record is the loop guard: on a later turn read it and resume at the gate instead of re-running exploration. -->

## Request state (2026-08-30)
intent: clear
review_required: true
classification: Architecture (system design, daemon topology, memory identity, multi-module gateway)
status: exploring
user_directive: "무조건 멀티플렉서로 강제, 에이전트별 워크스페이스+메모리 구현"
pending-action: explore → resolve forks via research → approval brief → (on approval) write .omo/plans/daemon-multiplexer-agent-workspaces.md → momus review

## Lanes dispatched (background, read-only)
- st_01a05375 explore-memory-identity: senpi memory repo identity derivation + per-thread memory support (SUPPORTED/NOT)
- st_01a05376 explore-thread-overrides: thread/start-resume cwd + runtimeWorkspaceRoots semantics
- st_01a05377 explore-gateway-wiring: session key identity fields, OmoBackend construction, injection points
- st_01a05378 advisory-architect (category): topology options A/B/C + recommendation
- st_01a05379 advisory-ultrabrain (category): identity slug formula, override payload, compat, env surface

## Verified facts (pre-wave)
- protocol/thread.d.ts: ThreadRuntimeOverrides { cwd, runtimeWorkspaceRoots, developerInstructions, baseInstructions }; ThreadResumeParams extends ThreadRuntimeOverrides (file: protocol/thread.d.ts:30-41,58-75)
- gateway thread_start_request sends ONLY developerInstructions + model (src/agent/omo_protocol.rs:19-36)
- gateway spawns 2 daemons: interactive ws://127.0.0.1:19742, cron ws://127.0.0.1:19743 (cron_from_env)
- global workspace root: OMON_WORKSPACE_ROOT, default ~/.omon/workspace (src/main.rs:150-158); .env has it set
- memory repo for this project: single ~/.omo/memory/agents/omon-gateway-cd6f536b (cwd-derived slug+hash), no per-bot repos exist

## Findings — advisory-architect (st_01a05378) [complete]
- Recommendation: **Option A now** (single daemon per lane, per-thread cwd+runtimeWorkspaceRoots overrides), architecture hooks for Option C (per-identity sidecar daemons) if memory contamination becomes real. Weighted score A 7.45 / B 7.70 / C 8.15; C wins only via memory isolation but costs 3-5+ processes.
- Risks flagged: (1) existing omo_thread_id sessions lack overrides → must re-send overrides on thread/resume, fallback to fresh start on resume failure; (2) daemon idle-unload may revert to process cwd if overrides not persisted → always pass full overrides on resume; (3) gateway vs daemon double-authorization path canonicalization (symlinks, /var vs /private/var) — canonicalize before sending; (4) shared memory repo contamination in Option A — memory isolation is Option A's weak spot (3/10).
- Phased rollout: P1 RPC payload (thread_start/resume + cwd/roots) → P2 workspace resolver + canonicalization → P3 resume fallback/healing → P4 deferred multi-daemon memory partitioning.

## Findings — advisory-ultrabrain (st_01a05379) [complete]
- Contract spec delivered: deterministic slug formula (category prefixes bot-/cron-/web-/sys-, sanitize to [a-z0-9_-], 48-char cap + sha256 8-hex tail on collision/length), 5 worked examples, zero collision bot- vs cron- namespaces.
- Layout: $WS_ROOT/agents/<slug> (private cwd) + $WS_ROOT/shared (collab). runtimeWorkspaceRoots = [private, shared]; legacy global root OMITTED (prevents cross-agent visibility).
- Payload: cwd + runtimeWorkspaceRoots on BOTH thread/start and thread/resume. Gateway creates dirs (create_dir_all idempotent) before dispatch; fail-fast OmonError on creation failure, no silent fallback.
- Env surface: OMON_PER_AGENT_WORKSPACE (default true, kill-switch), OMON_SHARED_WORKSPACE_ROOT, OMON_AGENT_WORKSPACE_BASE; OMON_WORKSPACE_ROOT reused as base.
- Edge cases covered: snowflake/cron collision (namespace prefixes), read-only root (fail fast), legacy daemon rejecting unknown params (fallback retry without overrides), missing bot+user (sys-default), concurrent dir creation (idempotent).
- Migration: existing sessions get overrides re-sent on every resume; if daemon ignores resume overrides, isolation applies to new threads (safe).
- Note: ultrabrain result echoed the architect framing verbatim in its preamble (duplicated result content) — treat the CONTRACT sections (slug formula, payload, env) as its unique contribution; architecture recommendation comes from advisory-architect.

## Findings — explore-gateway-wiring (st_01a05377) [complete]
- SessionKey (src/models/session.rs:8-20): platform/guild/channel/thread/user/bot; storage_key() length-prefixed pipes (:48-67). bot_id from serenity current user (discord/adapter.rs:1288). Cron: user_id="cron:<job_id>" (main.rs:553,563,577), metadata hermes_cron_job_id (:567-570). Dashboard: platform "web", user "dashboard" (dashboard.rs:2144).
- OmoBackend construction: main.rs:1218-1229 (interactive), main.rs:1306-1315 (cron), dashboard_runtime.rs:116-126/139-148. OmoBackendConfig fields at omo_config.rs:38-45 — workspace_root does NOT reach OmoBackend today. OMON_WORKSPACE_ROOT parsed main.rs:150-158, dashboard_runtime.rs:205-214 → AppConfig.workspace_root → cron executor/tools, NOT backend.
- Injection point: OmoBackend::resolve_thread_id (omo_backend.rs:137-259) has &mut SessionContext — bot_id/user_id/metadata available exactly where thread_start_request is sent (omo_backend.rs:201-204). Add workspace resolver to OmoBackend (base root + enabled flag) → thread_start_request(prompt, model, workspace) / thread_resume_request(thread_id, workspace).
- Profile (system_prompt/active_model) resolved before backend.run via actor load_context (actor.rs:446-486) + profile_routing.rs:141-164.
- FakeAppServer harness (tests/test_omo_backend.rs:46-245, thread/start handler :201-224): add received_cwd + received_workspace_roots ParkingMutex fields to assert propagation — existing pattern.
- Env conventions list confirmed; per-agent workspace kill-switch fits OMON_PER_AGENT_WORKSPACE naming.

## Findings — explore-thread-overrides (st_01a05376) [complete] ⚠ DESIGN-CHANGING
- **cwd IS consumed at thread/start**: handlers.js:131 `optionalString(params.cwd) ?? process.cwd()` → registry.js:23-33 resolvePath → persisted in session JSONL header (session-manager.js:579-588) → AgentSession._cwd drives ALL tool execution: bash spawn cwd (tools/bash.js:53-58), relative path resolution via resolveToCwd (path-utils.js:40-42; read/write/edit/ls/find). Per-thread cwd isolation in ONE daemon process is fully supported (registry.js:14-22,174-188 per-thread entries; docs/app-server.md:500).
- **runtimeWorkspaceRoots is IGNORED by the daemon**: not read in start(); synthesized into responses as [entry.cwd] (handlers.js:353). It does NOT constrain tool access; daemon sandbox defaults dangerFullAccess (handlers.js:357). → Sending it is harmless metadata only. Real enforcement: gateway-side authorized roots (main.rs:1105-1122 canonical_authorized_directory) for gateway tools. Daemon-side agent tools are NOT path-contained — workspace separation = working-context separation, NOT a hard security sandbox (must state honestly in plan).
- **thread/resume IGNORES cwd overrides**: resume handler reads only threadId (handlers.js:143); thread ALWAYS restores its original persisted cwd (registry.js:43-45). No re-send needed; overrides are immutable per thread. ⚠ Existing omo_thread_id sessions created without cwd keep daemon process cwd forever — migration to per-agent workspace requires a NEW thread (or explicit reset). Resume continuity itself is unaffected.
- **thread/start does NOT create or validate the cwd dir**: no mkdir (session-manager.js creates only sessionDir); bash tool throws "Working directory does not exist" at execution (bash.js:46-51). → GATEWAY MUST create_dir_all the per-agent dir before thread/start.
- Current gateway params confirmed: only developerInstructions + model (omo_protocol.rs:21-36, omo_backend.rs:163-170).
- Design implications: (1) plan sends cwd only (runtimeWorkspaceRoots optional metadata — send for forward-compat, document non-enforcement); (2) gateway pre-creates dir, fail-fast; (3) migration: existing sessions keep old cwd until thread reset — acceptable, continuity first; (4) daemon-as-multiplexer confirmed feasible.

## Findings — explore-memory-identity (st_01a05375) [complete] ⚠ KEY DISCOVERY
- Memory identity derivation: memory-core/src/identity/resolve.ts:38-47 — auto mode id = `${sanitizeToSlug(basename(cwd))}-${sha256(cwd)[0..8]}`; explicit mode via config memory.agent = `${sanitizeToSlug(v)}-${sha256(v)[0..8]}` (MAX_SLUG 40, NFKD).
- **Per-thread cwd → per-thread memory repo**: thread/start cwd feeds session cwd → memory identity auto-derived per cwd → each agent workspace gets its OWN ~/.omo/memory/agents/<slug>-<hash>/repo. Deamon as memory multiplexer confirmed FEASIBLE via cwd alone.
- Explicit identity per workspace: `.omo/omo.json` {"memory":{"agent":"<name>"}} in the workspace (config resolution .omo/omo.json / ~/.omo/omo.json / .senpi/settings.json; index.ts:114) → explicit stable id independent of path hash.
- Per-thread memory identity via protocol params: NOT SUPPORTED (no agentId/memoryRepo param in thread/start; memory/reset & thread/memoryMode/set are non-functional stubs — methods.js:104,123). Identity binds at session_start and resume enforces binding unchanged (index.ts:114-131).
- Memory projection DOES run in app-server mode: runtime.js:121-128 bindExtensions(mode:"app-server") → agent-session.js:2575-2595 emitBeforeAgentStart → prompt.ts:47-75 replaceMemoryBlock.
- OMO_MEMORY_HOME env overrides memory root (layout.ts:31-38); OMO_MEMORY_PUSH_SYNC forces sync git push.
- Identity binding lock means: same thread always keeps its identity (stable across resumes) — no drift.

## Topology lock (components, one per independently-verifiable outcome)
1. identity-slug: deterministic session→slug resolver (gateway) — RED unit tests
2. workspace-dir: gateway creates $WS/agents/<slug> (+.omo/omo.json memory.agent) — RED unit/integration
3. thread-override-propagation: thread_start_request sends cwd (+roots metadata) — RED fake-app-server asserts params
4. resume-compat: existing threads unaffected; resume flow unchanged — characterization
5. kill-switch: OMON_PER_AGENT_WORKSPACE=false → legacy behavior — RED config test
6. memory-binding: per-agent memory repo materializes on real daemon turn — live-surface proof

## Surviving owner-decision (only one)
- Migration of EXISTING omo_thread_id sessions: (a) default recommended: new threads only, existing conversations keep old shared workspace until natural reset (continuity first); (b) force-migrate: clear all existing omo_thread_id on deploy (bots lose OMO-side conversation context, DB history intact); (c) manual per-bot reset later. → asked in approval brief.

## Review round initialized (2026-08-30)
status: plan-complete, review in flight
plan_path: .omo/plans/daemon-multiplexer-agent-workspaces.md
plan_sha256: 247dfcc5ca7a2970770beac93ac9f5c5a56677d6ae5c26b6232c09cfdd7515fa
review_round_id: rr-daemon-mux-001
review_round_limit: 5
user_fork_decision: (b) forced migration approved
pending-action: momus review of the complete plan; report result; execution starts only via user's /ulw-execute

## Momus review result (2026-08-30)
review_round_id: rr-daemon-mux-001
plan_sha256: 247dfcc5ca7a2970770beac93ac9f5c5a56677d6ae5c26b6232c09cfdd7515fa
momus_task: st_01a054d4
verdict: APPROVED ([OKAY])
notes: all referenced files/line ranges verified against codebase; D1-D7 deterministic; zero blocking issues. Remaining items: none blocking.
status: plan approved — awaiting user's /ulw-execute to start implementation
