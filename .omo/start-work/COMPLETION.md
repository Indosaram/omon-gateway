# Hermes → OMO Gateway one-click migration — Completion Manifest

Plan: `.omo/plans/hermes-oneclick-migration.md` · HEAD: `81df9cf` (branch main)
Status: **COMPLETE & VERIFIED** — 15/15 todos done, plan 0 unchecked, F1–F4 APPROVE, 101 tests pass, clippy `-D warnings` clean, fmt clean.
Note: goal-tracker object is absent (`get_goal`→null, `update_goal`→"no goal exists"); completion recorded here + in Boulder (`status: completed`).

## Requirement → evidence

| Objective requirement | Evidence (commit / test / proof) |
|---|---|
| `migrate` clap subcommand alongside default `run` | T1 `787d75a`; CLI-parse tests `runner_tests::cli_*`; `migrate --help` shows `--dry-run`/`--no-cutover` |
| Import Hermes config (~/.hermes/.env + config.yaml + profiles/*/.env) → gateway .env, backup existing first | T3 `c53dfc4`; backup-before-overwrite test; round-trip parse test (R1) |
| **Preserve gateway-own .env keys** (DATABASE_URL etc.), overlay Hermes keys | R2 `81df9cf`; real dry-run shows `= DATABASE_URL (unchanged)`, 0 removed markers |
| Import cron via `HermesStoreSynchronizer::sync()` | T7 `6bdcb08` orchestration step 2 |
| Opt-in cutover: verified + backed-up cron deletion | T4 `2717666`; verify-before-delete + backup==pre-delete + idempotent tests |
| Cron cutover lock-aware (`.jobs.lock`) | R1 `217b673`; fs2 `acquire_jobs_lock`, lock-lifetime test |
| launchd-aware permanent shutdown: bootout + rename plists `.disabled` | T5 `1a20a1f`; bootout + `.disabled` rename tests |
| Bounded SIGTERM→SIGKILL + stale `gateway.lock` removal | R1 `217b673`; dies-during-wait / alive-after-wait / stale-lock-removed tests |
| `--dry-run` read-only projection, zero writes | T7 `6bdcb08`; real dry-run: jobs.json SHA + omon_gateway.db mtime unchanged |
| `--no-cutover` import-only | T7 `6bdcb08`; integration test |
| `APPROVAL_MODE` wired into terminal approval guard | T6 `2659c30`; ApprovalPolicy parse + gate tests (smart/always/never/yolo) |
| skills dir → ~/.hermes/skills + ~/.omon/skills | T8 `7c85f58`; `hermes_skill_dirs` helper test; no workspace/.hermes ref |
| Truthful README/.env.example | T9 `b66d306`; rg spot-checks (one-click/migrate present, no "reserved") |
| Injectable MigrationEnv seam, fakes, no real launchctl/kill in tests | T2 `28485d9` + R1; F2 confirmed real calls confined to OsEnv |

## Final verification wave
- F1 plan compliance: APPROVE (re-run after R1) — all gaps closed, 101 tests
- F2 code quality: APPROVE — clippy/fmt clean, seam confined, no unwrap/panic in prod
- F3 real manual QA: APPROVE — release build, dry-run read-only proof, routing fails-fast
- F4 scope fidelity: APPROVE (corrected allowlist) — no out-of-scope files, foreign f552795 isolated

## Not done by design (user's call)
Executing the real destructive `migrate` (or `--no-cutover`) against live `~/.hermes`. Cutover is opt-in; running it unbidden would be a destructive act. Recommended: `cargo run -- migrate --dry-run` (preview) → `cargo run -- migrate` when ready.

## Infra events handled (no faked progress)
- unspecified-high → Anthropic OAuth broken; quotio/gemini 429 (metis); routed all impl via deep/gpt-5.6-sol.
- One lost worker (T7) + one transient 503 (R1) → idempotent re-dispatch.
- Rogue QA-spawned gateway killed; QA hardened to never boot the gateway.
- Foreign concurrent commit `f552795` (discord attachments) isolated & excluded.
- Planning: momus review approved (4 rounds); metis gap-analysis unavailable (429).
