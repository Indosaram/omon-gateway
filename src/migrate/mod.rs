pub mod config_import;
pub mod cron_cutover;
pub mod gateway_down;
pub mod sys;

use crate::cron::{HermesStore, HermesStoreSynchronizer};
use crate::migrate::config_import::import_config;
use crate::migrate::cron_cutover::{cutover_cron_stores, CronStoreCutoverSummary};
use crate::migrate::gateway_down::bring_gateway_down;
use crate::migrate::sys::{MigrationEnv, OsEnv};
use crate::{Database, OmonError, Result};
use serde::Deserialize;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(clap::Args, Debug, Clone)]
pub struct MigrateArgs {
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long)]
    pub no_cutover: bool,
}

#[derive(Debug, Clone)]
pub struct MigrationPaths {
    pub hermes_root: PathBuf,
    pub target_env: PathBuf,
    pub launch_agents_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationSummary {
    pub dry_run: bool,
    pub database_would_be_created: bool,
    pub config_keys: usize,
    pub config_tokens: usize,
    pub config_diff: String,
    pub cron_imported: usize,
    pub cron_importable: Vec<String>,
    pub cron_already_present: Vec<String>,
    pub cron_stores: Vec<CronStoreCutoverSummary>,
    pub pids_stopped: Vec<i32>,
    pub plists_disabled: Vec<PathBuf>,
}

pub async fn run_migrate(args: MigrateArgs) -> Result<()> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| OmonError::Config("HOME is required for migration".into()))?;
    let hermes_root = std::env::var_os("HERMES_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".hermes"));
    let current_dir = std::env::current_dir().map_err(|error| {
        OmonError::Config(format!("failed to determine current directory: {error}"))
    })?;
    let paths = MigrationPaths {
        hermes_root,
        target_env: current_dir.join(".env"),
        launch_agents_dir: home.join("Library").join("LaunchAgents"),
    };
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://omon_gateway.db".to_owned());
    let env = OsEnv;

    let summary = if args.dry_run {
        let pool = open_read_only_pool_if_present(&database_url).await?;
        run_migrate_with(args, &env, paths, pool).await?
    } else {
        // Config import is intentionally performed before opening a writable database so the
        // command's externally visible order is exactly config -> cron import -> cutover.
        let config =
            import_config(&env, &paths.hermes_root, &paths.target_env, false).map_err(|error| {
                step_error(
                    "config import",
                    "cron import, cron cutover, gateway-down",
                    error,
                )
            })?;
        let database = Database::connect(&database_url).await.map_err(|error| {
            step_error(
                "database initialization",
                "cron import, cron cutover, gateway-down",
                error,
            )
        })?;
        let synchronizer = HermesStoreSynchronizer::from_environment(database.pool().clone())
            .map_err(|error| {
                step_error(
                    "cron import setup",
                    "cron import, cron cutover, gateway-down",
                    error,
                )
            })?;
        run_after_config(
            args,
            &env,
            paths,
            database.pool().clone(),
            synchronizer,
            config,
        )
        .await?
    };

    print_summary(&summary);
    Ok(())
}

pub async fn run_migrate_with(
    args: MigrateArgs,
    env: &dyn MigrationEnv,
    paths: MigrationPaths,
    pool: Option<SqlitePool>,
) -> Result<MigrationSummary> {
    let config = import_config(env, &paths.hermes_root, &paths.target_env, args.dry_run).map_err(
        |error| {
            step_error(
                "config import",
                "cron import, cron cutover, gateway-down",
                error,
            )
        },
    )?;

    if args.dry_run {
        return project_migration(args, env, paths, pool, config).await;
    }

    let pool = pool.ok_or_else(|| {
        step_error(
            "database initialization",
            "cron import, cron cutover, gateway-down",
            OmonError::Config("a writable gateway database pool is required".into()),
        )
    })?;
    let stores = discover_hermes_stores(env, &paths.hermes_root)?;
    let synchronizer = HermesStoreSynchronizer::new(pool.clone(), stores);
    run_after_config(args, env, paths, pool, synchronizer, config).await
}

async fn run_after_config(
    args: MigrateArgs,
    env: &dyn MigrationEnv,
    paths: MigrationPaths,
    pool: SqlitePool,
    synchronizer: HermesStoreSynchronizer,
    config: config_import::ConfigImportResult,
) -> Result<MigrationSummary> {
    let cron_imported = synchronizer
        .sync()
        .await
        .map_err(|error| step_error("cron import", "cron cutover, gateway-down", error))?;

    let mut summary = MigrationSummary {
        dry_run: false,
        database_would_be_created: false,
        config_keys: config.values.len(),
        config_tokens: imported_token_count(&config.values),
        config_diff: config.diff,
        cron_imported,
        cron_importable: Vec::new(),
        cron_already_present: Vec::new(),
        cron_stores: Vec::new(),
        pids_stopped: Vec::new(),
        plists_disabled: Vec::new(),
    };
    if args.no_cutover {
        return Ok(summary);
    }

    let cron_cutover = cutover_cron_stores(env, &paths.hermes_root, &pool, false)
        .await
        .map_err(|error| step_error("cron cutover", "gateway-down", error))?;
    summary.cron_stores = cron_cutover.stores;

    let gateway = bring_gateway_down(env, &paths.hermes_root, &paths.launch_agents_dir, false)
        .map_err(|error| step_error("gateway-down", "none", error))?;
    summary.pids_stopped = gateway.pids_terminated;
    summary.plists_disabled = gateway.plists_disabled;
    Ok(summary)
}

async fn project_migration(
    args: MigrateArgs,
    env: &dyn MigrationEnv,
    paths: MigrationPaths,
    pool: Option<SqlitePool>,
    config: config_import::ConfigImportResult,
) -> Result<MigrationSummary> {
    let projected = projected_cron_jobs(env, &paths.hermes_root)?;
    let existing = existing_cron_ids(pool.as_ref()).await?;
    let (cron_already_present, cron_importable): (Vec<_>, Vec<_>) = projected
        .iter()
        .cloned()
        .partition(|id| existing.contains(id));

    let cron_stores = if args.no_cutover {
        Vec::new()
    } else if let Some(pool) = pool.as_ref() {
        let mut stores = cutover_cron_stores(env, &paths.hermes_root, pool, true)
            .await
            .map_err(|error| {
                step_error("cron cutover projection", "gateway-down projection", error)
            })?
            .stores;
        for store in &mut stores {
            store.would_empty = store.jobs_found > 0;
        }
        stores
    } else {
        projected_store_summaries(env, &paths.hermes_root)?
    };

    let mut summary = MigrationSummary {
        dry_run: true,
        database_would_be_created: pool.is_none(),
        config_keys: config.values.len(),
        config_tokens: imported_token_count(&config.values),
        config_diff: config.diff,
        cron_imported: 0,
        cron_importable,
        cron_already_present,
        cron_stores,
        pids_stopped: Vec::new(),
        plists_disabled: Vec::new(),
    };

    if !args.no_cutover {
        let gateway = bring_gateway_down(env, &paths.hermes_root, &paths.launch_agents_dir, true)
            .map_err(|error| step_error("gateway-down projection", "none", error))?;
        summary.pids_stopped = gateway.pids_terminated;
        summary.plists_disabled = gateway.plists_disabled;
    }
    Ok(summary)
}

fn discover_hermes_stores(env: &dyn MigrationEnv, root: &Path) -> Result<Vec<HermesStore>> {
    let mut stores = vec![HermesStore::new("default", root)];
    let profiles_root = root.join("profiles");
    if env.is_dir(&profiles_root) {
        let mut profiles = env
            .read_dir(&profiles_root)?
            .into_iter()
            .filter(|path| env.is_dir(path))
            .collect::<Vec<_>>();
        profiles.sort();
        for home in profiles {
            let profile = home
                .file_name()
                .ok_or_else(|| {
                    OmonError::Config(format!("invalid Hermes profile path: {}", home.display()))
                })?
                .to_string_lossy()
                .into_owned();
            stores.push(HermesStore::new(profile, home));
        }
    }
    Ok(stores)
}

#[derive(Deserialize)]
struct CronDocument {
    #[serde(default)]
    jobs: Vec<CronId>,
}

#[derive(Deserialize)]
struct CronId {
    id: String,
}

fn projected_store_summaries(
    env: &dyn MigrationEnv,
    root: &Path,
) -> Result<Vec<CronStoreCutoverSummary>> {
    cron_store_documents(env, root)?
        .into_iter()
        .map(|(profile, jobs)| {
            let ids = jobs.into_iter().map(|job| job.id).collect::<Vec<_>>();
            Ok(CronStoreCutoverSummary {
                profile,
                jobs_found: ids.len(),
                all_imported: false,
                would_empty: !ids.is_empty(),
                unverified_jobs: ids,
            })
        })
        .collect()
}

fn projected_cron_jobs(env: &dyn MigrationEnv, root: &Path) -> Result<Vec<String>> {
    Ok(cron_store_documents(env, root)?
        .into_iter()
        .flat_map(|(profile, jobs)| {
            jobs.into_iter()
                .map(move |job| format!("hermes:{profile}:{}", job.id))
        })
        .collect())
}

fn cron_store_documents(env: &dyn MigrationEnv, root: &Path) -> Result<Vec<(String, Vec<CronId>)>> {
    let stores = discover_hermes_stores(env, root)?;
    stores
        .into_iter()
        .map(|store| {
            let path = store.jobs_path();
            if !env.exists(&path) {
                return Ok((store.profile().to_owned(), Vec::new()));
            }
            let bytes = env.read(&path)?;
            let document: CronDocument = serde_json::from_slice(&bytes).map_err(|error| {
                OmonError::Config(format!(
                    "invalid Hermes cron store {}: {error}",
                    path.display()
                ))
            })?;
            Ok((store.profile().to_owned(), document.jobs))
        })
        .collect()
}

async fn existing_cron_ids(pool: Option<&SqlitePool>) -> Result<BTreeSet<String>> {
    let Some(pool) = pool else {
        return Ok(BTreeSet::new());
    };
    let rows: Vec<(String,)> = sqlx::query_as("SELECT id FROM cron_jobs")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

async fn open_read_only_pool_if_present(database_url: &str) -> Result<Option<SqlitePool>> {
    let Some(path) = sqlite_file_path(database_url) else {
        return Err(OmonError::Config(format!(
            "dry-run requires a file-backed SQLite DATABASE_URL, got {database_url}"
        )));
    };
    if !path.exists() {
        return Ok(None);
    }
    let options = SqliteConnectOptions::from_str(database_url)?
        .read_only(true)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    Ok(Some(pool))
}

fn sqlite_file_path(database_url: &str) -> Option<PathBuf> {
    let value = database_url.strip_prefix("sqlite://")?;
    let value = value.split('?').next().unwrap_or(value);
    if value.is_empty() || value == ":memory:" {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

fn imported_token_count(values: &std::collections::BTreeMap<String, String>) -> usize {
    values.get("DISCORD_BOT_TOKEN").map_or(0, |_| 1)
        + values.get("DISCORD_BOT_TOKENS").map_or(0, |tokens| {
            tokens.split(',').filter(|token| !token.is_empty()).count()
        })
}

fn step_error(step: &str, remaining: &str, error: OmonError) -> OmonError {
    OmonError::Config(format!(
        "migration step `{step}` failed: {error}; remaining steps not run: {remaining}"
    ))
}

fn print_summary(summary: &MigrationSummary) {
    let mode = if summary.dry_run {
        "DRY RUN"
    } else {
        "COMPLETE"
    };
    println!("migration_summary:");
    println!("  mode: {mode}");
    println!("  config_keys: {}", summary.config_keys);
    println!("  config_tokens: {}", summary.config_tokens);
    println!("  config_changes:");
    for line in summary.config_diff.lines() {
        println!("    {line}");
    }
    println!(
        "  database_would_be_created: {}",
        summary.database_would_be_created
    );
    println!("  cron_import:");
    println!("    imported: {}", summary.cron_imported);
    println!("    importable: {:?}", summary.cron_importable);
    println!("    already_present: {:?}", summary.cron_already_present);
    println!("  cron_delete:");
    for store in &summary.cron_stores {
        println!(
            "    {}: jobs={}, would_empty={}, currently_unverified={:?}",
            store.profile, store.jobs_found, store.would_empty, store.unverified_jobs
        );
    }
    println!("  gateway_down:");
    println!("    pids_to_stop: {:?}", summary.pids_stopped);
    println!("    plists_to_disable: {:?}", summary.plists_disabled);
}
