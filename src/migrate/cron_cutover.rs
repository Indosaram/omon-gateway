use crate::migrate::sys::MigrationEnv;
use crate::{OmonError, Result};
use serde::Deserialize;
use sqlx::SqlitePool;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronStoreCutoverSummary {
    pub profile: String,
    pub jobs_found: usize,
    pub all_imported: bool,
    pub would_empty: bool,
    pub unverified_jobs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronCutoverSummary {
    pub stores: Vec<CronStoreCutoverSummary>,
}

#[derive(Debug, Deserialize)]
struct StoreDocument {
    #[serde(default)]
    jobs: Vec<StoreJob>,
}

#[derive(Debug, Deserialize)]
struct StoreJob {
    id: String,
}

struct PreparedStore {
    profile: String,
    path: PathBuf,
    original: Vec<u8>,
    job_ids: Vec<String>,
    unverified_jobs: Vec<String>,
}

pub async fn cutover_cron_stores(
    env: &dyn MigrationEnv,
    hermes_root: &Path,
    pool: &SqlitePool,
    dry_run: bool,
) -> Result<CronCutoverSummary> {
    let stores = discover_stores(env, hermes_root)?;
    let mut prepared = Vec::with_capacity(stores.len());

    for (profile, path) in stores {
        let original = if env.exists(&path) {
            if !env.is_file(&path) {
                return Err(OmonError::Config(format!(
                    "Hermes cron store is not a file: {}",
                    path.display()
                )));
            }
            env.read(&path)?
        } else {
            Vec::new()
        };
        let job_ids = if original.is_empty() && !env.exists(&path) {
            Vec::new()
        } else {
            parse_job_ids(&path, &original)?
        };
        let mut unverified_jobs = Vec::new();
        for job_id in &job_ids {
            let imported_id = format!("hermes:{profile}:{job_id}");
            let imported: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM cron_jobs WHERE id = ?)")
                    .bind(&imported_id)
                    .fetch_one(pool)
                    .await?;
            if !imported {
                unverified_jobs.push(job_id.clone());
            }
        }
        prepared.push(PreparedStore {
            profile,
            path,
            original,
            job_ids,
            unverified_jobs,
        });
    }

    let summary = CronCutoverSummary {
        stores: prepared
            .iter()
            .map(|store| CronStoreCutoverSummary {
                profile: store.profile.clone(),
                jobs_found: store.job_ids.len(),
                all_imported: store.unverified_jobs.is_empty(),
                would_empty: !store.job_ids.is_empty() && store.unverified_jobs.is_empty(),
                unverified_jobs: store.unverified_jobs.clone(),
            })
            .collect(),
    };

    if dry_run {
        return Ok(summary);
    }

    if let Some(store) = prepared
        .iter()
        .find(|store| !store.unverified_jobs.is_empty())
    {
        return Err(OmonError::Config(format!(
            "Hermes cron job hermes:{}:{} has not been imported into cron_jobs; refusing to empty {}",
            store.profile,
            store.unverified_jobs[0],
            store.path.display()
        )));
    }

    for store in prepared {
        if store.job_ids.is_empty() {
            continue;
        }
        cutover_store(env, &store)?;
    }

    Ok(summary)
}

fn discover_stores(env: &dyn MigrationEnv, hermes_root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut stores = vec![(
        "default".to_owned(),
        hermes_root.join("cron").join("jobs.json"),
    )];
    let profiles_root = hermes_root.join("profiles");
    if !env.exists(&profiles_root) {
        return Ok(stores);
    }
    if !env.is_dir(&profiles_root) {
        return Err(OmonError::Config(format!(
            "Hermes profiles path is not a directory: {}",
            profiles_root.display()
        )));
    }
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
        stores.push((profile, home.join("cron").join("jobs.json")));
    }
    Ok(stores)
}

fn parse_job_ids(path: &Path, bytes: &[u8]) -> Result<Vec<String>> {
    let document: StoreDocument = serde_json::from_slice(bytes).map_err(|error| {
        OmonError::Config(format!(
            "invalid Hermes cron store {}: {error}",
            path.display()
        ))
    })?;
    for job in &document.jobs {
        if job.id.trim().is_empty() {
            return Err(OmonError::Config(format!(
                "Hermes cron store {} contains a job without an id",
                path.display()
            )));
        }
    }
    Ok(document.jobs.into_iter().map(|job| job.id).collect())
}

fn cutover_store(env: &dyn MigrationEnv, store: &PreparedStore) -> Result<()> {
    let directory = store.path.parent().ok_or_else(|| {
        OmonError::Config(format!(
            "invalid Hermes cron store path: {}",
            store.path.display()
        ))
    })?;
    let _lock = env.acquire_jobs_lock(&directory.join(".jobs.lock"))?;
    let current = env.read(&store.path)?;
    if current != store.original {
        return Err(OmonError::Config(format!(
            "Hermes cron store changed during migration: {}; refusing to back up or empty stale state",
            store.path.display()
        )));
    }

    let now = env.now();
    let timestamp = now.format("%Y%m%dT%H%M%S%.fZ").to_string();
    let file_name = store.path.file_name().ok_or_else(|| {
        OmonError::Config(format!(
            "invalid Hermes cron store path: {}",
            store.path.display()
        ))
    })?;
    let backup = directory.join(format!(
        "{}.bak-omon-migration-{timestamp}",
        file_name.to_string_lossy()
    ));
    let temporary = directory.join(format!(
        ".{}.tmp-omon-migration-{timestamp}",
        file_name.to_string_lossy()
    ));
    let emptied = serde_json::json!({
        "jobs": [],
        "updated_at": now.to_rfc3339(),
    });
    let emptied = serde_json::to_vec(&emptied).map_err(|error| {
        OmonError::Config(format!("failed to serialize empty cron store: {error}"))
    })?;

    env.write(&backup, &store.original)?;
    env.write(&temporary, &emptied)?;
    env.rename(&temporary, &store.path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{cutover_cron_stores, cutover_store, PreparedStore};
    use crate::migrate::sys::{FakeMigrationEnv, MigrationEnv, MigrationOperation};
    use crate::storage::Database;
    use crate::OmonError;
    use chrono::{TimeZone, Utc};
    use std::path::{Path, PathBuf};

    const ROOT: &str = "/fixtures/.hermes";
    const STORE: &str = "/fixtures/.hermes/cron/jobs.json";
    const ORIGINAL: &[u8] = br#"{"jobs":[{"id":"daily"},{"id":"weekly"}],"updated_at":"old"}"#;

    async fn fixture() -> (FakeMigrationEnv, Database) {
        let now = Utc.with_ymd_and_hms(2026, 8, 15, 12, 34, 56).unwrap();
        let env = FakeMigrationEnv::new(now);
        env.write(Path::new(STORE), ORIGINAL).unwrap();
        let database = Database::connect("sqlite::memory:").await.unwrap();
        (env, database)
    }

    async fn import_job(database: &Database, id: &str) {
        sqlx::query(
            "INSERT INTO cron_jobs (id, expression, payload_json, enabled) VALUES (?, '* * * * *', '{}', 1)",
        )
        .bind(id)
        .execute(database.pool())
        .await
        .unwrap();
    }

    fn backup_write(env: &FakeMigrationEnv) -> (PathBuf, Vec<u8>) {
        env.write_calls()
            .into_iter()
            .find(|(path, _)| path.to_string_lossy().contains("bak-omon-migration-"))
            .expect("backup write")
    }

    #[tokio::test]
    async fn verified_jobs_are_backed_up_then_atomically_emptied() {
        let (env, database) = fixture().await;
        import_job(&database, "hermes:default:daily").await;
        import_job(&database, "hermes:default:weekly").await;
        let writes_before = env.write_calls().len();

        let summary = cutover_cron_stores(&env, Path::new(ROOT), database.pool(), false)
            .await
            .unwrap();

        assert_eq!(summary.stores[0].jobs_found, 2);
        assert!(summary.stores[0].all_imported);
        assert!(summary.stores[0].would_empty);
        let migration_writes = &env.write_calls()[writes_before..];
        assert!(migration_writes[0]
            .0
            .to_string_lossy()
            .contains("jobs.json.bak-omon-migration-"));
        assert!(migration_writes[1]
            .0
            .to_string_lossy()
            .contains(".jobs.json.tmp-omon-migration-"));
        let emptied: serde_json::Value =
            serde_json::from_slice(&env.read(Path::new(STORE)).unwrap()).unwrap();
        assert_eq!(emptied["jobs"], serde_json::json!([]));
        assert_eq!(env.rename_calls().len(), 1);
        let lock_path = PathBuf::from("/fixtures/.hermes/cron/.jobs.lock");
        let operations = env.operations();
        let lock_index = operations
            .iter()
            .position(|operation| operation == &MigrationOperation::LockAcquired(lock_path.clone()))
            .expect("jobs lock acquisition");
        let first_write_index = operations
            .iter()
            .position(|operation| {
                matches!(operation, MigrationOperation::Write(path) if path.to_string_lossy().contains("bak-omon-migration-"))
            })
            .expect("first cutover write");
        let release_index = operations
            .iter()
            .position(|operation| operation == &MigrationOperation::LockReleased(lock_path.clone()))
            .expect("jobs lock release");
        assert!(lock_index < first_write_index);
        assert!(first_write_index < release_index);
        assert!(
            !env.exists(&lock_path),
            "fake lock must not create/delete store files"
        );
        println!("verified-delete summary={summary:?}");
    }

    #[tokio::test]
    async fn rerun_on_empty_store_creates_no_second_backup() {
        let (env, database) = fixture().await;
        import_job(&database, "hermes:default:daily").await;
        import_job(&database, "hermes:default:weekly").await;
        cutover_cron_stores(&env, Path::new(ROOT), database.pool(), false)
            .await
            .unwrap();
        let writes_after_first = env.write_calls().len();

        let summary = cutover_cron_stores(&env, Path::new(ROOT), database.pool(), false)
            .await
            .unwrap();

        assert_eq!(env.write_calls().len(), writes_after_first);
        assert_eq!(summary.stores[0].jobs_found, 0);
        assert!(!summary.stores[0].would_empty);
        println!("idempotent rerun summary={summary:?}");
    }

    #[tokio::test]
    async fn unimported_job_returns_typed_error_without_changing_store() {
        let (env, database) = fixture().await;
        import_job(&database, "hermes:default:daily").await;
        let writes_before = env.write_calls().len();

        let error = cutover_cron_stores(&env, Path::new(ROOT), database.pool(), false)
            .await
            .unwrap_err();

        assert!(matches!(error, OmonError::Config(_)));
        assert!(error.to_string().contains("hermes:default:weekly"));
        assert_eq!(env.read(Path::new(STORE)).unwrap(), ORIGINAL);
        assert_eq!(env.write_calls().len(), writes_before);
        assert!(env.rename_calls().is_empty());
        println!("unimported-block error={error}");
    }

    #[tokio::test]
    async fn backup_bytes_equal_the_pre_delete_store() {
        let (env, database) = fixture().await;
        import_job(&database, "hermes:default:daily").await;
        import_job(&database, "hermes:default:weekly").await;

        cutover_cron_stores(&env, Path::new(ROOT), database.pool(), false)
            .await
            .unwrap();

        let (backup_path, backup_bytes) = backup_write(&env);
        assert_eq!(backup_bytes, ORIGINAL);
        assert_eq!(env.read(&backup_path).unwrap(), ORIGINAL);
        println!("backup-equality path={}", backup_path.display());
    }

    #[tokio::test]
    async fn dry_run_reports_unverified_jobs_and_performs_zero_writes() {
        let (env, database) = fixture().await;
        import_job(&database, "hermes:default:daily").await;
        let writes_before = env.write_calls().len();

        let summary = cutover_cron_stores(&env, Path::new(ROOT), database.pool(), true)
            .await
            .unwrap();

        assert_eq!(env.write_calls().len(), writes_before);
        assert!(env.rename_calls().is_empty());
        assert_eq!(env.read(Path::new(STORE)).unwrap(), ORIGINAL);
        assert_eq!(summary.stores[0].jobs_found, 2);
        assert!(!summary.stores[0].all_imported);
        assert!(!summary.stores[0].would_empty);
        assert_eq!(summary.stores[0].unverified_jobs, ["weekly"]);
        println!("dry-run summary={summary:?}");
    }

    #[tokio::test]
    async fn profiles_are_discovered_from_the_profiles_directory() {
        let (env, database) = fixture().await;
        let profile_store = Path::new("/fixtures/.hermes/profiles/work/cron/jobs.json");
        env.write(
            profile_store,
            br#"{"jobs":[{"id":"standup"}],"updated_at":"old"}"#,
        )
        .unwrap();
        import_job(&database, "hermes:default:daily").await;
        import_job(&database, "hermes:default:weekly").await;
        import_job(&database, "hermes:work:standup").await;

        let summary = cutover_cron_stores(&env, Path::new(ROOT), database.pool(), false)
            .await
            .unwrap();

        assert_eq!(
            summary
                .stores
                .iter()
                .map(|store| store.profile.as_str())
                .collect::<Vec<_>>(),
            ["default", "work"]
        );
        let emptied: serde_json::Value =
            serde_json::from_slice(&env.read(profile_store).unwrap()).unwrap();
        assert_eq!(emptied["jobs"], serde_json::json!([]));
    }

    #[test]
    fn stale_store_state_is_rejected_before_backup() {
        let now = Utc.with_ymd_and_hms(2026, 8, 15, 12, 34, 56).unwrap();
        let env = FakeMigrationEnv::new(now);
        env.write(Path::new(STORE), ORIGINAL).unwrap();
        let prepared = PreparedStore {
            profile: "default".into(),
            path: PathBuf::from(STORE),
            original: ORIGINAL.to_vec(),
            job_ids: vec!["daily".into(), "weekly".into()],
            unverified_jobs: Vec::new(),
        };
        env.write(Path::new(STORE), b"changed concurrently")
            .unwrap();
        let writes_before = env.write_calls().len();

        let error = cutover_store(&env, &prepared).unwrap_err();

        assert!(matches!(error, OmonError::Config(_)));
        assert!(error.to_string().contains("changed during migration"));
        assert_eq!(env.write_calls().len(), writes_before);
        assert!(env.rename_calls().is_empty());
        println!("stale-state error={error}");
    }

    #[tokio::test]
    async fn malformed_store_returns_typed_error_and_performs_no_writes() {
        let (env, database) = fixture().await;
        env.write(Path::new(STORE), b"{not-json").unwrap();
        let writes_before = env.write_calls().len();

        let error = cutover_cron_stores(&env, Path::new(ROOT), database.pool(), false)
            .await
            .unwrap_err();

        assert!(matches!(error, OmonError::Config(_)));
        assert!(error.to_string().contains("invalid Hermes cron store"));
        assert_eq!(env.read(Path::new(STORE)).unwrap(), b"{not-json");
        assert_eq!(env.write_calls().len(), writes_before);
        assert!(env.rename_calls().is_empty());
        println!("malformed-input error={error}");
    }
}
