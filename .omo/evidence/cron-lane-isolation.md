# Cron lane isolation — Discord turns starving behind cron turns

## Symptom (production, 2026-08-29)

Discord messages received no reply. Gateway log:

```
08:57:14  Cron job omon-katok-3h-group-digest-v3 failed:
          LLM error: turn exceeded total deadline of 900s; turn/interrupt sent
08:58:40  ERROR agent runner failed session=7:discord|...|414011632306618368
          error=LLM error: turn exceeded total deadline of 900s; turn/interrupt sent
09:15:50  Cron job omon-katok-3h-group-digest-v3 failed: (same, next fire)
09:15:50  Cron job hermes-aside-feature-request-browser-daily failed: (same)
```

The user message arrived 08:42:32Z; only the `user` row was persisted, no
`assistant` row. Daemon `thread/list` showed the digest thread `01a04caf`
`{"type":"active"}` while the user thread `01a04a8e` was `idle` — the user's
turn had not started at all.

## Root cause

An app-server agent thread executes **one turn at a time**. The 3-hourly digest
cron occupied the single daemon that the interactive Discord lane also used, so
every Discord turn queued behind it and then died on the same 900s deadline.

The 900s total deadline (943b1d1) is a failure stop, not a fix: it bounds the
stall, it does not prevent cron from owning the interactive lane.

## Fix

Cron gets its own app-server instance.

- `OmoBackendConfig::cron_from_env()` — default `ws://127.0.0.1:19743`
  (interactive stays `ws://127.0.0.1:19742`), tighter default total deadline
  600s, inherits auth token and default model from the interactive lane.
  Overrides: `OMON_OMO_CRON_APPSERVER_URL`,
  `OMON_OMO_CRON_TURN_TOTAL_TIMEOUT_SECS` (invalid value fails boot).
- `src/main.rs` and `src/dashboard_runtime.rs`: a second `OmoDaemonSupervisor`
  (same zero-config spawn/adopt/restart/kill behaviour) plus a dedicated
  `OmoBackend` handed to `AgentCronExecutor`. The interactive backend and
  multiplexer wiring are unchanged.

No `.env` change — defaults are compiled in.

## RED -> GREEN

RED (`cargo test --lib agent::omo_config`):

```
error[E0599]: no function or associated item named `cron_from_env` found
              for struct `omo_config::OmoBackendConfig`
error: could not compile `omon-gateway` (lib test) due to 3 previous errors
```

GREEN:

```
running 7 tests
test agent::omo_config::tests::test_cron_from_env_uses_isolated_daemon_lane ... ok
test agent::omo_config::tests::test_cron_from_env_honours_overrides_and_inherits_credentials ... ok
test agent::omo_config::tests::test_omo_backend_config_validates_websocket_scheme ... ok
test agent::omo_config::tests::test_validate_agent_backend_env_contract ... ok
test agent::omo_config::tests::test_omo_backend_config_from_env_parses_url_and_token ... ok
test agent::omo_config::tests::test_turn_stream_timeout_env_contract ... ok
test agent::omo_config::tests::test_from_env_defaults_to_local_daemon_when_env_absent ... ok
test result: ok. 7 passed; 0 failed
```

Asserted contract: `cron.appserver_url != interactive.appserver_url` and
`cron.total_timeout < interactive.total_timeout`.

## Second defect found during restart: daemon binary not resolvable under launchd

The gateway runs under launchd (`~/Library/LaunchAgents/ai.omon.gateway.plist`,
`KeepAlive`). After restart it crash-looped every ~10s. `gateway.log` showed only
repeated boot lines; the cause was in `gateway.err`:

```
Error: Config("failed to spawn 'omo app-server' for ws://127.0.0.1:19742:
  No such file or directory (os error 2)")
```

launchd supplies a minimal `PATH` (`/usr/bin:/bin:/usr/sbin:/sbin`) which
excludes `~/.bun/bin`, where `omo` is installed. A bare `omo` therefore fails
with ENOENT under launchd while resolving fine in an interactive shell.

Fix: `resolve_daemon_bin(bin, path_env)` in `src/agent/omo_daemon.rs`. Names
containing `/` are returned verbatim; a bare name found on `PATH` is used as-is;
otherwise it falls back to `~/.bun/bin`, `~/.local/bin`, `~/.npm-global/bin`,
`/opt/homebrew/bin`, `/usr/local/bin`. No env var or plist edit required.

RED:

```
error[E0425]: cannot find function `resolve_daemon_bin` in this scope
```

GREEN: `test_resolve_daemon_bin_falls_back_to_known_install_paths ... ok`

## Production verification (2026-08-29 12:16-12:19Z)

Boot, gateway pid 47116 (parent pid 1 = launchd):

```
12:16:49  omo app-server daemon ready url=ws://127.0.0.1:19742
12:16:50  omo app-server daemon ready url=ws://127.0.0.1:19743
12:16:51  Initializing isolated cron agent backend: OMO app-server
          appserver_url=ws://127.0.0.1:19743 total_timeout_secs=600
12:16:53  Discord client ready bot_name=실피 / wawabot / 에리스
```

Listeners: 9119 (dashboard), 19742 (interactive), 19743 (cron). No new
`gateway.err` output after 12:16:18.

### Interactive turn completes while the cron lane is occupied

A long turn was started on the cron lane and held open, then an interactive
turn was driven through `POST /api/sessions/{id}/chat`:

```
CRONLANE TURN-SENT 12:18:42.938Z      (still running, no TURN-COMPLETED)

12:19:18.509  user       Reply with exactly: CONCURRENT-OK
12:19:21.646  assistant  CONCURRENT-OK          <- 3.1s, cron lane still busy
```

Control run on an idle system: `LANECHECK-OK` in ~9s (12:17:51 -> 12:18:00).

Before this change the same overlap left the user's message with a `user` row
and no `assistant` row for 15 minutes, then failed on the 900s deadline.

## Gates

```
cargo fmt      clean
cargo clippy --all-targets -- -D warnings    clean
cargo test     380 passed; 0 failed
cargo build --release    ok
```

## Note on a reverted edit

An attempt to make the total deadline fire without inbound events wrapped the
stream loop in `tokio::time::timeout`, but the scripted anchor matched
`do_initialize` instead of `run_once`, producing a non-compiling file. The three
bad hunks were reverse-applied (`git apply -R`, backup at
`/tmp/omo_backend_corrupt.patch`); `src/agent/omo_backend.rs` is byte-identical
to the committed version and builds clean. The passive-deadline improvement is
not part of this change.
