---
slug: hermes-oneclick-migration
status: awaiting-approval
intent: clear
review_required: true
plan_path: .omo/plans/hermes-oneclick-migration.md
plan_sha256: null
review_round_id: null
review_round_limit: 5
pending-action: write and review .omo/plans/hermes-oneclick-migration.md
review:
  momus:
    status: pending
    workspace_root: null
    runtime_home: null
    target: .omo/plans/hermes-oneclick-migration.md
    round_id: null
    plan_sha256: null
    launch_id: null
    session: null
    result: null
approach: >
  Add a first-class `migrate` subcommand to the omon-gateway Rust binary that (1) imports
  Hermes config (~/.hermes/.env + config.yaml + per-profile) into omon-gateway .env, (2)
  triggers the already-implemented cron import, and (3) performs an explicit, opt-in,
  backup-first cutover: delete Hermes cron jobs across all profiles and bring the Hermes
  gateway down permanently (stop process + launchctl bootout + disable the 3 LaunchAgents).
  Also close the review-surfaced gaps: wire APPROVAL_MODE, fix skills-dir mismatch, correct
  README/.env.example. Cutover is never automatic; config import never clobbers existing secrets.
---

# Draft: hermes-oneclick-migration

## Components (topology ledger)
- config-importer | read ~/.hermes/.env + config.yaml (+ profiles) -> omon .env, no clobber | active | ~/.hermes/config.yaml, ~/.hermes/.env
- gap-fixes | wire APPROVAL_MODE, fix skills dir, correct README/.env.example | active | src/main.rs:911-920, src/discord/approval.rs, README.md:109-142
- cutover-cron-delete | backup + empty jobs.json across default+profiles, idempotent, guarded | active | ~/.hermes/cron/jobs.json, profiles/*/cron/jobs.json
- cutover-gateway-down | hermes gateway stop + launchctl bootout + disable 3 LaunchAgents | active | ~/Library/LaunchAgents/ai.hermes.gateway*.plist, hermes_cli/gateway.py:1573
- migrate-cli | new `omon-gateway migrate` subcommand (arg parsing, orchestration, tests) | active | src/main.rs (single #[tokio::main])

## Open assumptions (announced defaults)
- config.yaml parsing | add serde_yaml dep | repo has no yaml parser yet; needed to read Hermes config | reversible
- multi-bot tokens | collect DISCORD_BOT_TOKEN from ~/.hermes/.env + every profile .env into DISCORD_BOT_TOKENS | matches README section 2 | reversible
- provider mapping | model.base_url->OPENAI_API_BASE, model.api_key->OPENAI_API_KEY, model.default->DEFAULT_MODEL | README table | reversible

## Findings (cited - path:lines)
- omon-gateway RUNNING now (PID 45696, target/release/omon-gateway); already imported Hermes crons (omon_gateway.db mtime 21:06).
- Hermes default cron/jobs.json now empty; jobs.json.bak-omon-migration-20260815-210636 (31843B) is a HAND-MADE backup - no reusable code created it (rg found nothing).
- Hermes has 3 LaunchAgents: ~/Library/LaunchAgents/ai.hermes.gateway.plist, -advisor, -marketer. Killing PID alone is insufficient; launchd relaunches.
- Hermes CLI `hermes gateway stop` -> stop_profile_gateway() (SIGTERM pidfile pid + planned-stop marker). hermes_cli/gateway.py:1573.
- Hermes profiles: default + advisor, kkachile, marketer. Each has own cron/, config.yaml, gateway state.
- No migrate subcommand/script exists in omon-gateway; main.rs is a single #[tokio::main] that runs the gateway.
- Review gaps confirmed: APPROVAL_MODE never read in src/; skills dir wired to ~/.omon/workspace/.hermes/skills not README's ~/.omon/skills; config import table is manual-only.

## Decisions (with rationale)
- APPROVED 2026-08-15. User: no users yet, compat may break, pick the most fundamental direction.
- A: introduce clap; subcommands `run` (default gateway) + `migrate`. Fundamental over ad-hoc script.
- B: `migrate` runs full one-click by default (import config -> ensure cron import -> cutover). Backup-first, idempotent, `--dry-run` and `--no-cutover` escapes for engineering safety (not compat).
- C: authoritative .env write; back up any existing .env to .env.bak-<ts> first (non-destructive via backup).
- D: stop all 3 profiles + launchctl bootout + move plists to .disabled (full down, reversible rollback).
- E: wire APPROVAL_MODE into the real approval guard (smart/yolo/always/never).
- F: tests-after; side effects (process kill, launchctl, fs writes) behind injectable seams so destructive paths are unit-tested without real calls.

## Scope IN
- Config auto-importer (Hermes -> omon .env), the missing "one-click" piece.
- Explicit opt-in cutover: delete Hermes crons (backup-first) + bring Hermes gateway down permanently (launchd-aware).
- Close review gaps: APPROVAL_MODE, skills dir, README/.env.example accuracy, HERMES_HOME/OMON_HERMES_PROFILES docs.
- Rust unit + integration tests; verification.

## Scope OUT (Must NOT have)
- No automatic destructive cutover on startup.
- No migration of Hermes state.db/memories/sessions/kanban (only config + cron + gateway lifecycle).
- No new feature work beyond migration + the named gaps.

## Open questions
See brief - 6 forks: (A) CLI-subcommand vs shell script, (B) cutover trigger/safety, (C) existing-.env behavior, (D) launchd scope, (E) APPROVAL_MODE wire vs remove, (F) test strategy.

## Approval gate
status: awaiting-approval
Approach recorded in frontmatter. Post-approval: write plan -> metis gap analysis -> momus high-accuracy review (default-on) -> deliver. Next action: write and review .omo/plans/hermes-oneclick-migration.md.
