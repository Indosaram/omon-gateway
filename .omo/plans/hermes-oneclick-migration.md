# hermes-oneclick-migration - Work Plan

## TL;DR (For humans)

**What you'll get:** A single `omon-gateway migrate` command that turns a running legacy Hermes install into a fully migrated OMO Gateway in one shot: it copies Hermes's settings and API keys into the gateway, moves the scheduled jobs over, then cleanly retires Hermes — deleting its scheduled jobs and permanently shutting its background service down so nothing runs twice.

**Why this approach:** Today only the scheduled-job import is automatic; the settings copy is a manual copy-paste chart and there is no safe way to retire Hermes (its macOS background services relaunch it after a kill). We make the whole thing one real, testable command, and we wire up the previously dead "approval mode" setting so migrated config actually works.

**What it will NOT do:** It will not migrate Hermes chat history / memories / kanban; it will not run the destructive retirement silently on gateway startup; it will not invent a reduced "phase 1" subset.

**Effort:** Large
**Risk:** Medium - the retirement step kills processes, rewrites files, and touches macOS LaunchAgents; every destructive action is backup-first, idempotent, reversible, and unit-tested behind fakes.
**Decisions to sanity-check:** full one-click (import + retire) is the default with `--dry-run`/`--no-cutover` escapes; existing `.env` is backed up then authoritatively overwritten; Hermes LaunchAgents are booted out and renamed `.disabled` (reversible), not deleted.

Your next move: approve, and I will run the high-accuracy review (momus, default-on) before handing off — say so if you want to skip it.

---

> TL;DR (machine): Large / Medium risk. New `migrate` clap subcommand: Hermes config importer + cron cutover (delete) + launchd-aware gateway shutdown, injectable side-effect seam, APPROVAL_MODE wiring, skills-dir fix, truthful docs. 9 impl todos + 4 final verifiers.

## Scope
### Must have
- New `clap` subcommand dispatch: `run` (current gateway behavior, default) and `migrate`.
- `migrate` end-to-end one-click flow (default): import Hermes config -> ensure cron import -> cutover (delete Hermes crons + bring Hermes gateway permanently down). Flags: `--dry-run` (no writes/no side effects, print plan), `--no-cutover` (import only).
- Config importer: parse `~/.hermes/config.yaml` + `~/.hermes/.env` + `~/.hermes/profiles/*/.env`, map to gateway `.env` keys, dedupe multi-profile bot tokens, provider-by-model-name; back up any existing `.env` to `.env.bak-<ts>` then write authoritative `.env`.
- Cron cutover: per profile, verify the gateway SQLite `cron_jobs` table already holds `hermes:<profile>:<id>` for each Hermes job, back up `jobs.json`, then atomically write the emptied store; idempotent; lock-aware.
- Gateway-down cutover (macOS launchd-aware): stop live PIDs from `gateway.lock` (default + profiles), `launchctl bootout` the 3 LaunchAgents, rename plists to `.disabled` (reversible); idempotent.
- Injectable side-effect seam (fs / process-signal / launchctl / clock) so destructive logic is unit-tested with fakes and never runs real commands in CI.
- Wire `APPROVAL_MODE` into a real `ApprovalPolicy` that governs whether the terminal tool routes dangerous commands through `SmartApprovalGuard` (`smart`/`always` request; `never`/`yolo` bypass).
- Fix skills-dir wiring to match documented behavior (`~/.hermes/skills` + `~/.omon/skills`).
- Truthful docs: rewrite README migration section around the real command; document `HERMES_HOME`, `OMON_HERMES_PROFILES`, `APPROVAL_MODE`; fix `OMON_WORKSPACE_ROOT` default mismatch.

### Must NOT have (guardrails, anti-slop, scope boundaries)
- No automatic destructive cutover on gateway startup (`run` never deletes crons or stops Hermes).
- No migration of Hermes `state.db`, `memories/`, `sessions/`, `kanban.db`, plans, or workspace contents.
- No deletion of Hermes LaunchAgent plists (rename to `.disabled` only) and no deletion of Hermes `jobs.json` (empty + backup only).
- No new gateway features beyond migration + the named review gaps.
- No `--make-pr`/branching assumptions baked into the plan; execution surface is a normal build.
- No reduced "MVP/phase-1" subset.

## Verification strategy
> Zero human intervention - all verification is agent-executed.
- Test decision: **tests-after** + Rust built-in test harness (`cargo test`), `#[tokio::test]` for async/sqlx, temp-dir fixtures for a fake `HERMES_HOME`, `sqlite::memory:` for the cron DB, and a `FakeMigrationEnv` for all destructive side effects.
- Every destructive path (process kill, `launchctl`, plist rename, `.env`/`jobs.json` writes) MUST be exercised only against the fake seam in tests; a test that shells out to real `launchctl`/`kill` is a defect.
- Evidence: `.omo/evidence/task-<N>-hermes-oneclick-migration.<ext>` (outside ulw-loop, per template default).
- Gate commands: `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, and `cargo run -- migrate --dry-run` against the real (already-migrated) `~/.hermes` producing a non-destructive plan printout.

## Execution strategy
### Parallel execution waves
- **Wave 1 (foundation):** T1 (clap + subcommand + `src/migrate` scaffold), T2 (injectable side-effect seam).
- **Wave 2 (feature modules, parallel):** T3 (config importer), T4 (cron delete), T5 (gateway-down/launchd), T6 (APPROVAL_MODE wiring), T8 (skills-dir fix).
- **Wave 3 (integration + docs):** T7 (`migrate` orchestration + integration test), T9 (docs).
- **Final wave:** F1-F4.

### Dependency matrix
| Todo | Depends on | Blocks | Can parallelize with |
| --- | --- | --- | --- |
| 1 | - | 3,4,5,7,8 | 2 |
| 2 | - | 3,4,5,7 | 1 |
| 3 | 1,2 | 7 | 4,5,6,8 |
| 4 | 1,2 | 7 | 3,5,6,8 |
| 5 | 1,2 | 7 | 3,4,6,8 |
| 6 | 1 | 9 | 3,4,5,8 |
| 8 | 1 | 9 | 3,4,5,6 |
| 7 | 3,4,5 | 9 | - |
| 9 | 6,7,8 | - | - |
| F1-F4 | 1-9 | - | each other |

## Todos
> Implementation + Test = ONE todo. Never separate.

- [x] 1. Introduce clap subcommands and the migrate module scaffold
  What to do / Must NOT do: Add `clap` (derive) to Cargo.toml. In `src/main.rs`, replace the single `#[tokio::main]` body with a top-level CLI: `enum Command { Run, Migrate(MigrateArgs) }` defaulting to `Run` when no subcommand is given (preserve current behavior exactly). Extract the existing gateway startup body verbatim into a `run` path (e.g. `async fn run_gateway(config: Config) -> Result<()>` in `src/main.rs` or a new `src/app.rs`), called by `Command::Run`. Create `src/migrate/mod.rs` exported from `src/lib.rs` as `pub mod migrate;` with a `MigrateArgs` struct (`--dry-run`, `--no-cutover` bool flags) and a `pub async fn run_migrate(args: MigrateArgs) -> Result<()>` stub that returns `Ok(())` for now. Wire `Command::Migrate` to call it. Must NOT change any runtime gateway behavior for the default/`run` path; must NOT add migration logic yet.
  Parallelization: Wave 1 | Blocked by: - | Blocks: 3,4,5,7,8
  References (executor has NO interview context - be exhaustive): `src/main.rs:26-128` (`Config` struct + `Config::from_env`), `src/main.rs:894-1032` (`#[tokio::main]` body to extract), `src/lib.rs` (module exports), `Cargo.toml:20-46` (deps table), README quickstart `cargo run --release`.
  Acceptance criteria (agent-executable): `cargo build` succeeds; `cargo run -- --help` lists `run` and `migrate`; `cargo run -- run` and bare `cargo run --` both take the gateway path (fails only on missing DISCORD_BOT_TOKEN as today); `cargo run -- migrate --help` shows `--dry-run` and `--no-cutover`.
  QA scenarios (name the exact tool + invocation): happy - `cargo run -- migrate --help` prints flags; failure - `cargo run -- bogus` exits non-zero with a clap error. Evidence `.omo/evidence/task-1-hermes-oneclick-migration.txt`
  Commit: Y | feat(cli): add clap run/migrate subcommands and migrate module scaffold
  Recommended task executor category: unspecified-high - multi-file structural change to the binary entrypoint.

- [x] 2. Add the injectable migration side-effect seam
  What to do / Must NOT do: Create `src/migrate/sys.rs` defining a `MigrationEnv` trait abstracting every side effect the migration needs: read/write/rename/exists for files, list dir, current unix user id, wall clock `now()` (for `-<ts>` suffixes), signal/terminate a pid, run a `launchctl` invocation (args -> exit status + captured stdout/stderr), and check whether a pid is alive. Provide `OsEnv` (real impl using std::fs, `nix`/`libc` or `std::process` for signals, `std::process::Command` for launchctl, `chrono::Utc::now`) and `FakeMigrationEnv` (in-memory fs map, recorded launchctl/kill calls, injectable clock, configurable pid-alive set) behind `#[cfg(test)]` or a `test-support` path usable by integration tests. Add `nix` (or reuse libc) only if needed for signals; prefer `std`/existing deps. Must NOT perform any real destructive action in the trait's fake; must NOT leak `OsEnv` details into feature modules (they depend on the trait only).
  Parallelization: Wave 1 | Blocked by: - | Blocks: 3,4,5,7
  References (executor has NO interview context - be exhaustive): `src/main.rs:849-891` (existing `hermes_home`/`canonical_directory`/path patterns), `src/cron/store.rs:207-243` (tokio::fs usage style), `Cargo.toml` (chrono already present), gateway.lock JSON shape `{"pid":35221,...}` at `~/.hermes/gateway.lock`.
  Acceptance criteria (agent-executable): `cargo test migrate::sys` passes; unit test proves `FakeMigrationEnv` records a launchctl call and a kill call without executing them, and that `now()` is injectable.
  QA scenarios: happy - fake records ops and returns programmed results; failure - a fake write to a read-only-marked path returns an error the caller can handle. Evidence `.omo/evidence/task-2-hermes-oneclick-migration.txt`
  Commit: Y | feat(migrate): add injectable MigrationEnv seam with OsEnv and fake
  Recommended task executor category: unspecified-high - trait design that all destructive modules build on.

- [x] 3. Config importer (Hermes -> gateway .env)
  What to do / Must NOT do: Create `src/migrate/config_import.rs`. Add `serde_yaml` to Cargo.toml. Parse `~/.hermes/config.yaml` (`model.default`, `model.provider`, `model.base_url`, `model.api_key`, `model.api_mode`) and `.env` files (`~/.hermes/.env` + each `~/.hermes/profiles/*/.env`) into a normalized map. Mapping rules (fundamental, compat may break): `model.default -> DEFAULT_MODEL`; if default model starts with `claude` set `ANTHROPIC_BASE_URL`/`ANTHROPIC_API_KEY` from `model.base_url`/`model.api_key`, else set `OPENAI_API_BASE`/`OPENAI_API_KEY`; primary `DISCORD_BOT_TOKEN` from `~/.hermes/.env`; collect every profile+root `DISCORD_BOT_TOKEN` into deduped `DISCORD_BOT_TOKENS` (comma-joined, stable order, drop empties/dupes incl. primary); pass through `DISCORD_ALLOWED_USERS`, `DISCORD_FREE_RESPONSE_CHANNELS`, and fold `DISCORD_HOME_CHANNEL` into free-response per existing runtime semantics; carry `approvals.mode`/`APPROVAL_MODE` -> `APPROVAL_MODE`. Strip surrounding quotes; ignore commented lines; ignore unknown keys. Root `~/.hermes/.env` wins on scalar conflicts; tokens union across all. Output: via `MigrationEnv`, if `.env` exists back it up to `.env.bak-<ts>` first, then write the fully-mapped authoritative `.env` (respect `--dry-run` = compute + print diff, write nothing). Must NOT echo secret values to logs (mask); must NOT clobber without backup.
  Parallelization: Wave 2 | Blocked by: 1,2 | Blocks: 7
  References: `src/main.rs:26-140` (`Config` struct, `Config::from_env`, `llm_config` - the exact keys/semantics to target), `.env.example:1-58`, `~/.hermes/config.yaml` `model:` block, `~/.hermes/.env` DISCORD_* keys, ultrabrain advisory (task st_01a0056e) section 1.
  Acceptance criteria (agent-executable): `cargo test migrate::config_import` passes covering: claude vs non-claude provider routing, multi-profile token dedupe/order, quoted-value stripping, missing-key omission, `provider: custom:quotio` handled, existing-.env backup path. A round-trip test: importing a fixture Hermes home yields a `.env` that `Config::from_env` parses without error.
  QA scenarios: happy - fixture home -> expected .env map (asserted key-by-key); failure - malformed config.yaml yields a typed `OmonError::Config`, no partial `.env` write. Evidence `.omo/evidence/task-3-hermes-oneclick-migration.txt`
  Commit: Y | feat(migrate): import Hermes config into authoritative gateway .env
  Recommended task executor category: unspecified-high - mapping logic with many edge cases and tests.

- [x] 4. Cron cutover: verified, idempotent Hermes cron deletion
  What to do / Must NOT do: Create `src/migrate/cron_cutover.rs`. For default + each discovered profile, resolve `cron/jobs.json` (reuse `HermesStore::jobs_path` semantics from `src/cron/store.rs`). Before deleting, for every job in the Hermes store confirm the gateway SQLite `cron_jobs` table contains id `hermes:<profile>:<jobid>` (the exact id format written by `HermesStoreSynchronizer::sync`); if any job is not yet imported, do NOT delete and return an actionable error (unless already empty). Back up the store to `jobs.json.bak-omon-migration-<ts>` via `MigrationEnv`, then atomically write `{"jobs": [], "updated_at": "<rfc3339 now>"}` (write temp + rename). Idempotent: an already-empty store is a no-op success. Honor `--dry-run` (report only). Must NOT touch `executions.db`, `output/`, or non-cron files; must NOT delete jobs that were not confirmed imported.
  Parallelization: Wave 2 | Blocked by: 1,2 | Blocks: 7
  References: `src/cron/store.rs:207-243` (`jobs_path`, `load`), `src/cron/store.rs:295-330` (`sync`, `hermes:<profile>:<id>` id format at line 308), `~/.hermes/cron/jobs.json` shape `{"jobs":[],"updated_at":...}`, `.jobs.lock` present in that dir, ultrabrain advisory section 2.
  Acceptance criteria (agent-executable): `cargo test migrate::cron_cutover` passes: (a) with jobs imported into an in-memory `cron_jobs`, cutover backs up and empties the fake store; (b) re-running is a no-op; (c) an unimported job blocks deletion with a typed error; (d) backup file content equals pre-delete content.
  QA scenarios: happy - imported -> emptied + backup present; failure - missing import -> error, original store untouched. Evidence `.omo/evidence/task-4-hermes-oneclick-migration.txt`
  Commit: Y | feat(migrate): verified idempotent Hermes cron deletion with backup
  Recommended task executor category: unspecified-high - stateful DB+fs logic with safety guards.

- [x] 5. Gateway-down cutover (macOS launchd-aware, reversible)
  What to do / Must NOT do: Create `src/migrate/gateway_down.rs`. Steps via `MigrationEnv`, all idempotent: (1) read `gateway.lock` JSON for default + each profile, and if the recorded pid is alive, send SIGTERM then (after a bounded wait) SIGKILL; remove stale lock only if process gone. (2) For each LaunchAgent `~/Library/LaunchAgents/ai.hermes.gateway.plist`, `ai.hermes.gateway-advisor.plist`, `ai.hermes.gateway-marketer.plist` that exists: `launchctl bootout gui/<uid>/<label>` (tolerate "not loaded" as success), then rename the plist to `<name>.disabled` so it will not relaunch on reboot. (3) Report what was stopped/disabled. Reversible by design (rename, not delete). Honor `--dry-run`. Discover plists dynamically (glob `ai.hermes.gateway*.plist`) rather than hardcoding, but include the 3 known labels in tests. Must NOT delete plists; must NOT touch non-hermes LaunchAgents; must NOT run real launchctl/kill in tests.
  Parallelization: Wave 2 | Blocked by: 1,2 | Blocks: 7
  References: `~/.hermes/gateway.lock` (`{"pid":...,"kind":"hermes-gateway",...}`), `~/.hermes/profiles/*/gateway.lock`, `/Users/indo/.hermes/hermes-agent/hermes_cli/gateway.py:1477` (`kill_gateway_processes`), `:1573` (`stop_profile_gateway`) reference behavior (external, not in this repo), `~/Library/LaunchAgents/ai.hermes.gateway*.plist`, ultrabrain advisory section 3.
  Acceptance criteria (agent-executable): `cargo test migrate::gateway_down` passes with `FakeMigrationEnv`: asserts SIGTERM->SIGKILL escalation ordering for an alive pid, `launchctl bootout` invoked with `gui/<uid>/<label>` for each present plist, plist renamed to `.disabled`, and full idempotency on a second run (no pid, plists already `.disabled` -> no-op success).
  QA scenarios: happy - alive gateway + 3 plists -> stopped + booted out + disabled; failure - bootout returns "not loaded" -> treated as success, rename still happens. Evidence `.omo/evidence/task-5-hermes-oneclick-migration.txt`
  Commit: Y | feat(migrate): launchd-aware reversible Hermes gateway shutdown
  Recommended task executor category: deep - macOS launchctl/process-signal correctness and ordering.

- [x] 6. Wire APPROVAL_MODE into a real approval policy
  What to do / Must NOT do: Add `APPROVAL_MODE` parsing to `Config::from_env` into an `ApprovalPolicy` enum (`Smart`, `Always`, `Never`/`Yolo`), defaulting to `Smart`. Thread the policy + a `SmartApprovalGuard` handle into the terminal tool execution path so that when policy is `Smart`/`Always` a dangerous shell command (define a conservative dangerous-pattern check, e.g. destructive fs ops / privilege / network-exec) requests approval via the guard and awaits the decision before running; `Never`/`Yolo` bypasses. Ensure the Discord button round-trip (`resolve_custom_id` already wired in `adapter.rs:149`) resolves the request. Must NOT weaken existing behavior when policy unset (default Smart, but only gate genuinely dangerous commands to avoid blocking every command); must NOT block cron/non-interactive executions indefinitely (respect a timeout -> reject/deny path).
  Parallelization: Wave 2 | Blocked by: 1 | Blocks: 9
  References: `src/discord/approval.rs:41-90` (`SmartApprovalGuard`, `request`, `resolve_custom_id`, `approval_buttons`), `src/discord/adapter.rs:27-43,149` (guard storage + interaction resolution), `src/discord/commands.rs:19,42`, `src/tools/terminal.rs` (execution entrypoint to gate), `src/main.rs:26-128` (`Config`), `.env.example:52-58` (APPROVAL_MODE doc).
  Acceptance criteria (agent-executable): `cargo test` covers `ApprovalPolicy` parsing (smart/always/never/yolo/unknown->default) and that a dangerous command under `Never` executes without requesting, while under `Smart` it issues an approval request (guard mock resolves approve->runs, reject->refuses). `cargo clippy -D warnings` clean.
  QA scenarios: happy - `APPROVAL_MODE=never` runs `rm`-style command directly (test harness, no real fs); `APPROVAL_MODE=smart` requests then runs on approve. failure - approval timeout -> command refused with a typed error. Evidence `.omo/evidence/task-6-hermes-oneclick-migration.txt`
  Commit: Y | feat(approval): govern terminal tool via APPROVAL_MODE policy
  Recommended task executor category: deep - cross-module plumbing from tool exec to Discord guard.

- [x] 7. `migrate` orchestration + integration test
  What to do / Must NOT do: Implement `run_migrate` to orchestrate in strict order for a REAL run: (1) config import (T3); (2) ensure cron import by running `HermesStoreSynchronizer::from_environment(pool).sync()` against the gateway DB (this writes `cron_jobs`); (3) cron cutover (T4) - only after import succeeds; (4) gateway-down (T5). `--no-cutover` runs only 1-2. `--dry-run` MUST take a distinct READ-ONLY projection path: it does NOT call `sync()` and does NOT open a writable pool (no schema migration). Instead it opens the existing gateway DB read-only (`sqlite://<db>?mode=ro`) to read current `cron_jobs`, parses the Hermes stores, and PRINTS the projected plan: which config keys would change, which cron jobs would be imported vs. already present, which Hermes crons would be deleted, which pids would be signaled, which plists would be booted out/disabled - performing zero writes and zero destructive side effects. If the gateway DB file does not exist, dry-run reports that DB creation + migration + import WOULD run, without performing it. Print a structured summary on real runs too (masked key count, tokens count, jobs backed up + emptied per profile, pids stopped, plists disabled). On any real-run step failure, stop and report which step failed and what remains; never proceed to a later destructive step. Wire real `OsEnv` here. Must NOT swallow errors; must NOT reorder so cutover precedes verified import; must NOT let `--dry-run` call `sync()` or open a writable/migrating pool.
  Parallelization: Wave 3 | Blocked by: 3,4,5 | Blocks: 9
  References: `src/main.rs:966-967` (`HermesStoreSynchronizer::from_environment(pool).sync()` real-run call to reuse), `src/cron/store.rs:295-330` (`sync` mutates `cron_jobs` - why dry-run must avoid it), `src/storage/db.rs:9-18,44` (`init_pool`/`MIGRATOR.run` applies migrations on connect - why dry-run must open read-only), `src/storage/mod.rs` (`init_pool`), T3/T4/T5 modules.
  Acceptance criteria (agent-executable): `cargo test --test test_migrate` (new integration test) passes end-to-end against a temp `HERMES_HOME` fixture (config.yaml + .env + one cron job) with a writable `sqlite::memory:` and `FakeMigrationEnv`: asserts `.env` written, cron job imported into `cron_jobs`, Hermes store emptied + backed up, fake launchctl/kill recorded. A separate `--dry-run` test asserts: `sync()` is NOT invoked, the pool is opened read-only (or not created), and ZERO fake writes/side effects occurred while a projection is still printed. `cargo run -- migrate --dry-run` against the real `~/.hermes` exits 0 and prints a plan without mutating `omon_gateway.db`.
  QA scenarios: happy - full flow on fixture -> all four steps recorded; failure - cutover blocked when import step reports an unimported job (destructive steps skipped). Evidence `.omo/evidence/task-7-hermes-oneclick-migration.txt`
  Commit: Y | feat(migrate): orchestrate one-click import + cutover with dry-run
  Recommended task executor category: unspecified-high - integration wiring + end-to-end test.

- [x] 8. Fix skills-dir wiring to documented behavior
  What to do / Must NOT do: In `src/main.rs` skills registration, scan the documented set `~/.hermes/skills` (or `$HERMES_HOME/skills`) AND `~/.omon/skills` (matching README section 4 and `SkillsTool::default()`), instead of the current `workspace_root/.hermes/skills` + `~/.hermes/skills`. Keep `SkillsTool::new(dirs)` API. Must NOT change `SkillsTool` scanning logic; only the dirs passed in `main.rs`. Keep cron per-job skill loading (`load_cron_skills` reads the actual Hermes home) unchanged.
  Parallelization: Wave 2 | Blocked by: 1 | Blocks: 9
  References: `src/main.rs:914-922` (skills_dirs vec + `SkillsTool::new`), `src/tools/skills.rs:11-21` (`SkillsTool::default` documents intended dirs), README.md:140-142.
  Acceptance criteria (agent-executable): `cargo build` clean; a unit/asserted check that the dirs passed to `SkillsTool::new` are `[$HOME/.hermes/skills (or HERMES_HOME), $HOME/.omon/skills]`; `~/.omon/workspace/.hermes/skills` no longer referenced.
  QA scenarios: happy - dirs match README; failure - n/a (pure wiring), covered by build + assertion. Evidence `.omo/evidence/task-8-hermes-oneclick-migration.txt`
  Commit: Y | fix(skills): scan documented ~/.hermes and ~/.omon skills dirs
  Recommended task executor category: quick - localized wiring fix in main.rs.

- [x] 9. Truthful migration docs
  What to do / Must NOT do: Rewrite README "Hermes to OMO Gateway Migration Guide" to describe the real `omon-gateway migrate` command (one-click import + cutover, `--dry-run`, `--no-cutover`), replacing the "manual copy-paste table" framing (keep the mapping table as a reference of WHAT gets migrated, but state it is automated). Document `HERMES_HOME`, `OMON_HERMES_PROFILES`, and a now-functional `APPROVAL_MODE` in both `.env.example` and the README Configuration Reference. Fix the `OMON_WORKSPACE_ROOT` default mismatch (`.env.example` `./workspace` vs README `~/.omon/workspace`) to one truthful value. Correct the skills-dir description to the T8 reality. Must NOT document behavior that does not exist; every claim must map to shipped code.
  Parallelization: Wave 3 | Blocked by: 6,7,8 | Blocks: -
  References: README.md:109-198, `.env.example:44-58`, `src/main.rs` skills/workspace defaults, final `migrate --help` output.
  Acceptance criteria (agent-executable): `rg -n "one-click|migrate" README.md` reflects the real command; no README claim about APPROVAL_MODE/skills/config-import contradicts code (spot-checked against src); `.env.example` documents HERMES_HOME + OMON_HERMES_PROFILES + APPROVAL_MODE.
  QA scenarios: happy - doc matches `cargo run -- migrate --help`; failure - a reviewer grep finds a claim with no code backing (must be zero). Evidence `.omo/evidence/task-9-hermes-oneclick-migration.txt`
  Commit: Y | docs: describe real one-click migrate command and config surface
  Recommended task executor category: writing - documentation accuracy pass.

## Final verification wave
> Runs in parallel after ALL todos. ALL must APPROVE. Surface results and wait for the user's explicit okay before declaring complete.
- [x] F1. Plan compliance audit - every todo's acceptance criteria met; `migrate` does import + cutover with dry-run/no-cutover; skills-dir + APPROVAL_MODE gaps closed.
  QA (agent-executable): run `cargo run -- migrate --help` and assert output contains `--dry-run` and `--no-cutover`; run `cargo test` and assert the test names from T3 (`migrate::config_import`), T4 (`migrate::cron_cutover`), T5 (`migrate::gateway_down`), and T7 (`--test test_migrate`) all appear and pass; `rg -n "APPROVAL_MODE" src/main.rs` returns a parse into `ApprovalPolicy`; `rg -n "\.omon.*workspace.*hermes" src/main.rs` returns zero hits. Expected: all assertions hold. Evidence `.omo/evidence/task-F1-hermes-oneclick-migration.txt`
  Recommended task executor category: unspecified-high
- [x] F2. Code quality review - `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, no real destructive calls in tests, seam boundaries respected, typed errors (no unwrap/panic in migration paths).
  QA (agent-executable): run `cargo clippy --all-targets -- -D warnings` and assert exit code 0; run `cargo fmt --check` and assert exit code 0; run `rg -n "Command::new\(\"launchctl\"|Command::new\(\"kill\"|libc::kill|nix::.*kill" src tests` and assert every hit is inside `src/migrate/sys.rs` `OsEnv` only (zero hits in `tests/` and zero in other `src/migrate/*.rs`); run `rg -n "unwrap\(\)|expect\(|panic!|unreachable!" src/migrate` and assert zero hits. Expected: all assertions hold. Evidence `.omo/evidence/task-F2-hermes-oneclick-migration.txt`
  Recommended task executor category: unspecified-high
- [x] F3. Real manual QA - build release; `cargo run -- migrate --dry-run` against real `~/.hermes` prints a coherent non-destructive plan; `cargo run -- run`/bare run still take the gateway path; full `cargo test` green in one run.
  QA (agent-executable): (1) `cargo build --release` exits 0. (2) capture the SHA-256 of `~/.hermes/cron/jobs.json` and the mtime of `omon_gateway.db`, run `cargo run --release -- migrate --dry-run`, assert exit 0 and stdout contains the projected sections (config keys / cron import / cron delete / gateway-down), then assert the jobs.json SHA-256 and the db mtime are UNCHANGED (dry-run mutated nothing). (3) prove run/default routing WITHOUT booting the live gateway (dotenvy loads .env which provides DISCORD_BOT_TOKENS, so a bare `cargo run -- run` would actually start it): run `cargo run -- run` and bare `cargo run --` from a temp CWD containing no .env and with `env -u DISCORD_BOT_TOKEN -u DISCORD_BOT_TOKENS`, asserting both reach Config::from_env and exit fast with the missing-token error (not a clap error, not a running gateway); if an empty-token env cannot be guaranteed, rely on the T1 CLI-parse unit test for routing and omit the live `run` invocation. (4) `cargo test` exits 0 in a single invocation. Expected: all four pass. Evidence `.omo/evidence/task-F3-hermes-oneclick-migration.txt`
  Recommended task executor category: deep
- [x] F4. Scope fidelity - nothing outside scope changed (no state.db/memories/sessions migration, no plist deletion, no auto-cutover on startup); Must-NOT-Have honored.
  QA (agent-executable): `git -C /Users/indo/code/project/omon-gateway diff --name-only` lists only src/, Cargo.toml, Cargo.lock, README.md, .env.example (no unrelated files); `rg -n "state\.db|memories/|sessions/|kanban" src/migrate` returns zero hits (no unintended state migration); `rg -n "remove_file|std::fs::remove|unlink|rm \b" src/migrate/gateway_down.rs` returns zero plist-deletion hits (rename-to-.disabled only); `rg -n "run_migrate|cutover|delete" src/main.rs` shows migration invoked only under `Command::Migrate`, never in the `run`/startup path. Expected: all four checks pass. Evidence `.omo/evidence/task-F4-hermes-oneclick-migration.txt`
  Recommended task executor category: unspecified-high

## Commit strategy
- One commit per todo using the `Commit:` lines above (conventional commits), committed only by the executor, not the planner.
- No commit is created for the planning artifacts.
- Group: T1-T2 foundation, T3-T6+T8 features, T7 integration, T9 docs; each builds green (`cargo build` + relevant `cargo test`) before its commit.

## Success criteria
- `omon-gateway migrate` performs the full one-click migration: authoritative `.env` written (existing backed up), Hermes crons imported and then deleted (backup-first, verified), Hermes gateway stopped and its 3 LaunchAgents booted out + disabled - all idempotent and reversible.
- `--dry-run` performs zero writes/side effects; `--no-cutover` imports without retiring Hermes.
- `APPROVAL_MODE` actually governs the terminal tool via `SmartApprovalGuard`.
- Skills dirs match documentation; README/.env.example contain no claim unbacked by code.
- `cargo build`, `cargo test`, `cargo clippy -D warnings`, `cargo fmt --check` all clean in a single run; no test invokes real `launchctl`/`kill`.
