use chrono::{TimeZone, Utc};
use omon_gateway::migrate::sys::{FakeMigrationEnv, MigrationEnv};
use omon_gateway::migrate::{run_migrate_with, MigrateArgs, MigrationPaths};
use omon_gateway::Database;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

const JOBS: &str = r#"{"jobs":[{"id":"daily","name":"Daily","prompt":"status","schedule":{"kind":"cron","expr":"0 9 * * *"},"enabled":true}],"updated_at":"old"}"#;

struct Fixture {
    root: PathBuf,
    target_env: PathBuf,
    launch_agents: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let nonce = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("omon-migrate-{}-{nonce}", std::process::id()));
        let target_env = root.join("gateway.env");
        let launch_agents = root.join("Library/LaunchAgents");
        fs::create_dir_all(root.join("cron")).unwrap();
        fs::write(root.join("cron/jobs.json"), JOBS).unwrap();
        Self {
            root,
            target_env,
            launch_agents,
        }
    }

    fn paths(&self) -> MigrationPaths {
        MigrationPaths {
            hermes_root: self.root.clone(),
            target_env: self.target_env.clone(),
            launch_agents_dir: self.launch_agents.clone(),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn seed_fake(env: &FakeMigrationEnv, fixture: &Fixture, include_cutover: bool) {
    env.write(
        &fixture.root.join("config.yaml"),
        b"model:\n  default: gpt-4o\n  base_url: https://example.test/v1\n  api_key: provider-secret\napprovals:\n  mode: smart\n",
    )
    .unwrap();
    env.write(
        &fixture.root.join(".env"),
        b"DISCORD_BOT_TOKEN=bot-secret\nDISCORD_ALLOWED_USERS=42\n",
    )
    .unwrap();
    env.write(&fixture.root.join("cron/jobs.json"), JOBS.as_bytes())
        .unwrap();
    env.write(
        &fixture.target_env,
        b"DATABASE_URL=sqlite://custom.db\nOMON_WORKSPACE_ROOT=/x\nDEFAULT_MODEL=old\n",
    )
    .unwrap();

    if include_cutover {
        env.write(
            &fixture.root.join("gateway.lock"),
            br#"{"pid":4242,"kind":"hermes-gateway"}"#,
        )
        .unwrap();
        env.set_pid_alive(4242, true);
        env.set_current_uid(501);
        env.write(
            &fixture.launch_agents.join("ai.hermes.gateway.plist"),
            b"<plist/>",
        )
        .unwrap();
    }
}

#[tokio::test]
async fn full_migration_imports_config_and_cron_before_cutover() {
    let fixture = Fixture::new();
    let env = FakeMigrationEnv::new(Utc.with_ymd_and_hms(2026, 8, 15, 15, 0, 0).unwrap());
    seed_fake(&env, &fixture, true);
    let database = Database::connect("sqlite::memory:").await.unwrap();

    let summary = run_migrate_with(
        MigrateArgs {
            dry_run: false,
            no_cutover: false,
        },
        &env,
        fixture.paths(),
        Some(database.pool().clone()),
    )
    .await
    .unwrap();

    let migrated_env = env.read_to_string(&fixture.target_env).unwrap();
    assert_eq!(
        migrated_env,
        "DATABASE_URL=sqlite://custom.db\nOMON_WORKSPACE_ROOT=/x\nDEFAULT_MODEL=gpt-4o\nAPPROVAL_MODE=smart\nDISCORD_ALLOWED_USERS=42\nDISCORD_BOT_TOKEN=bot-secret\nOPENAI_API_BASE=https://example.test/v1\nOPENAI_API_KEY=provider-secret\n"
    );
    assert_eq!(
        env.read_to_string(
            &fixture
                .target_env
                .with_file_name("gateway.env.bak-20260815T150000Z")
        )
        .unwrap(),
        "DATABASE_URL=sqlite://custom.db\nOMON_WORKSPACE_ROOT=/x\nDEFAULT_MODEL=old\n"
    );
    assert!(summary.config_diff.contains("= DATABASE_URL="));
    assert!(summary.config_diff.contains("= OMON_WORKSPACE_ROOT="));
    assert!(!summary
        .config_diff
        .lines()
        .any(|line| line.starts_with("- ")));

    let imported: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM cron_jobs WHERE id = 'hermes:default:daily'")
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(imported, 1);

    let emptied: serde_json::Value =
        serde_json::from_slice(&env.read(&fixture.root.join("cron/jobs.json")).unwrap()).unwrap();
    assert_eq!(emptied["jobs"], serde_json::json!([]));
    assert!(env.write_calls().iter().any(|(path, bytes)| path
        .to_string_lossy()
        .contains("bak-omon-migration-")
        && bytes == JOBS.as_bytes()));
    assert_eq!(env.terminate_calls(), [4242]);
    assert_eq!(env.kill_calls(), [4242]);
    assert_eq!(env.launchctl_calls().len(), 1);
    assert!(env.rename_calls().iter().any(|(_, to)| to
        == &fixture
            .launch_agents
            .join("ai.hermes.gateway.plist.disabled")));
    assert_eq!(summary.cron_imported, 1);
    assert_eq!(summary.cron_stores[0].jobs_found, 1);
    assert_eq!(summary.pids_stopped, vec![4242]);
}

#[tokio::test]
async fn dry_run_projects_every_step_with_zero_writes_or_side_effects() {
    let fixture = Fixture::new();
    let env = FakeMigrationEnv::new(Utc.with_ymd_and_hms(2026, 8, 15, 15, 0, 0).unwrap());
    seed_fake(&env, &fixture, true);
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let writes_before = env.write_calls();
    let renames_before = env.rename_calls();

    let summary = run_migrate_with(
        MigrateArgs {
            dry_run: true,
            no_cutover: false,
        },
        &env,
        fixture.paths(),
        Some(database.pool().clone()),
    )
    .await
    .unwrap();

    assert_eq!(env.write_calls(), writes_before);
    assert_eq!(env.rename_calls(), renames_before);
    assert!(env.terminate_calls().is_empty());
    assert!(env.kill_calls().is_empty());
    assert!(env.launchctl_calls().is_empty());
    assert_eq!(
        env.read_to_string(&fixture.root.join("cron/jobs.json"))
            .unwrap(),
        JOBS
    );
    let imported: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cron_jobs")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(imported, 0, "dry-run must not invoke sync()");
    assert_eq!(summary.cron_importable, vec!["hermes:default:daily"]);
    assert!(summary.cron_already_present.is_empty());
    assert_eq!(summary.cron_stores[0].unverified_jobs, ["daily"]);
    assert_eq!(summary.pids_stopped, vec![4242]);
    assert_eq!(summary.plists_disabled.len(), 1);
}

#[tokio::test]
async fn dry_run_without_database_reports_creation_without_creating_it() {
    let fixture = Fixture::new();
    let env = FakeMigrationEnv::new(Utc.with_ymd_and_hms(2026, 8, 15, 15, 0, 0).unwrap());
    seed_fake(&env, &fixture, false);
    let writes_before = env.write_calls();

    let summary = run_migrate_with(
        MigrateArgs {
            dry_run: true,
            no_cutover: true,
        },
        &env,
        fixture.paths(),
        None,
    )
    .await
    .unwrap();

    assert!(summary.database_would_be_created);
    assert_eq!(summary.cron_importable, vec!["hermes:default:daily"]);
    assert_eq!(env.write_calls(), writes_before);
    assert!(!Path::new(&fixture.root.join("missing.db")).exists());
}
