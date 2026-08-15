use crate::migrate::sys::{LaunchctlOutput, MigrationEnv};
use crate::{OmonError, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

const TERM_WAIT_POLLS: usize = 10;
const TERM_WAIT_INTERVAL: Duration = Duration::from_millis(100);

const PLIST_PREFIX: &str = "ai.hermes.gateway";
const PLIST_SUFFIX: &str = ".plist";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayDownSummary {
    pub pids_found: Vec<i32>,
    pub pids_terminated: Vec<i32>,
    pub pids_killed: Vec<i32>,
    pub plists_booted_out: Vec<String>,
    pub plists_disabled: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct GatewayLock {
    pid: i32,
}

pub fn bring_gateway_down(
    env: &dyn MigrationEnv,
    hermes_root: &Path,
    launch_agents_dir: &Path,
    dry_run: bool,
) -> Result<GatewayDownSummary> {
    let pid_locks = discover_pid_locks(env, hermes_root)?;
    let pids_found = pid_locks.keys().copied().collect::<Vec<_>>();
    let mut pids_terminated = Vec::new();
    let mut pids_killed = Vec::new();

    for (&pid, locks) in &pid_locks {
        if dry_run {
            if env.pid_alive(pid) {
                pids_terminated.push(pid);
                pids_killed.push(pid);
            }
            continue;
        }

        if env.pid_alive(pid) {
            pids_terminated.push(pid);
            env.terminate(pid)?;
            if !wait_until_dead(env, pid) {
                env.kill(pid)?;
                pids_killed.push(pid);
                if env.pid_alive(pid) {
                    return Err(OmonError::Config(format!(
                        "Hermes gateway pid {pid} remained alive after SIGKILL"
                    )));
                }
            }
        }

        for lock in locks {
            env.remove_file(lock)?;
        }
    }

    let plists = discover_plists(env, launch_agents_dir)?;
    let mut plists_booted_out = Vec::with_capacity(plists.len());
    let mut plists_disabled = Vec::with_capacity(plists.len());
    let uid = env.current_uid();

    for (plist, disabled, label) in plists {
        plists_booted_out.push(label.clone());
        plists_disabled.push(disabled.clone());
        if dry_run {
            continue;
        }

        let target = format!("gui/{uid}/{label}");
        let output = env.run_launchctl(&["bootout", &target])?;
        if !launchctl_bootout_succeeded(&output) {
            return Err(OmonError::Config(format!(
                "failed to boot out Hermes LaunchAgent {label}: status {:?}, stdout: {}, stderr: {}",
                output.status,
                output.stdout.trim(),
                output.stderr.trim()
            )));
        }
        env.rename(&plist, &disabled)?;
    }

    Ok(GatewayDownSummary {
        pids_found,
        pids_terminated,
        pids_killed,
        plists_booted_out,
        plists_disabled,
    })
}

fn wait_until_dead(env: &dyn MigrationEnv, pid: i32) -> bool {
    for _ in 0..TERM_WAIT_POLLS {
        env.sleep(TERM_WAIT_INTERVAL);
        if !env.pid_alive(pid) {
            return true;
        }
    }
    false
}

fn discover_pid_locks(
    env: &dyn MigrationEnv,
    hermes_root: &Path,
) -> Result<BTreeMap<i32, Vec<PathBuf>>> {
    let mut locks = vec![hermes_root.join("gateway.lock")];
    let profiles_root = hermes_root.join("profiles");
    if env.is_dir(&profiles_root) {
        let mut profiles = env
            .read_dir(&profiles_root)?
            .into_iter()
            .filter(|path| env.is_dir(path))
            .collect::<Vec<_>>();
        profiles.sort();
        locks.extend(profiles.into_iter().map(|path| path.join("gateway.lock")));
    }

    let mut pid_locks = BTreeMap::<i32, Vec<PathBuf>>::new();
    for lock in locks {
        if !env.is_file(&lock) {
            continue;
        }
        let Ok(contents) = env.read_to_string(&lock) else {
            continue;
        };
        let Ok(parsed) = serde_json::from_str::<GatewayLock>(&contents) else {
            continue;
        };
        if parsed.pid > 0 {
            pid_locks.entry(parsed.pid).or_default().push(lock);
        }
    }
    Ok(pid_locks)
}

fn discover_plists(
    env: &dyn MigrationEnv,
    launch_agents_dir: &Path,
) -> Result<Vec<(PathBuf, PathBuf, String)>> {
    if !env.is_dir(launch_agents_dir) {
        return Ok(Vec::new());
    }

    let mut plists = Vec::new();
    for path in env.read_dir(launch_agents_dir)? {
        if !env.is_file(&path) {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !file_name.starts_with(PLIST_PREFIX) || !file_name.ends_with(PLIST_SUFFIX) {
            continue;
        }
        let label = file_name.trim_end_matches(PLIST_SUFFIX).to_owned();
        let disabled = path.with_file_name(format!("{file_name}.disabled"));
        if env.exists(&disabled) {
            continue;
        }
        plists.push((path, disabled, label));
    }
    plists.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(plists)
}

fn launchctl_bootout_succeeded(output: &LaunchctlOutput) -> bool {
    if output.status == Some(0) {
        return true;
    }
    let message = format!("{}\n{}", output.stdout, output.stderr).to_ascii_lowercase();
    [
        "not loaded",
        "no such process",
        "could not find specified service",
        "service not found",
    ]
    .iter()
    .any(|expected| message.contains(expected))
}

#[cfg(test)]
mod tests {
    use super::bring_gateway_down;
    use crate::migrate::sys::{
        FakeMigrationEnv, LaunchctlOutput, MigrationEnv, MigrationOperation,
    };
    use crate::Result;
    use chrono::{DateTime, TimeZone, Utc};
    use parking_lot::Mutex;
    use std::collections::{HashMap, VecDeque};
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    const ROOT: &str = "/fixtures/.hermes";
    const AGENTS: &str = "/fixtures/Library/LaunchAgents";

    fn fixture() -> FakeMigrationEnv {
        let now = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
        let env = FakeMigrationEnv::new(now);
        env.set_current_uid(501);
        env
    }

    fn write(env: &FakeMigrationEnv, path: &str, contents: &str) {
        env.write(Path::new(path), contents.as_bytes()).unwrap();
    }

    struct ScriptedPidEnv {
        inner: FakeMigrationEnv,
        alive: Mutex<HashMap<i32, VecDeque<bool>>>,
        alive_calls: Mutex<HashMap<i32, usize>>,
        signal_order: Mutex<Vec<(&'static str, i32)>>,
        sleep_calls: Mutex<usize>,
    }

    impl ScriptedPidEnv {
        fn new(inner: FakeMigrationEnv) -> Self {
            Self {
                inner,
                alive: Mutex::new(HashMap::new()),
                alive_calls: Mutex::new(HashMap::new()),
                signal_order: Mutex::new(Vec::new()),
                sleep_calls: Mutex::new(0),
            }
        }

        fn script_pid(&self, pid: i32, responses: impl IntoIterator<Item = bool>) {
            self.alive
                .lock()
                .insert(pid, responses.into_iter().collect());
        }

        fn alive_calls(&self, pid: i32) -> usize {
            self.alive_calls.lock().get(&pid).copied().unwrap_or(0)
        }

        fn signal_order(&self) -> Vec<(&'static str, i32)> {
            self.signal_order.lock().clone()
        }
    }

    impl MigrationEnv for ScriptedPidEnv {
        fn read_to_string(&self, path: &Path) -> Result<String> {
            self.inner.read_to_string(path)
        }

        fn read(&self, path: &Path) -> Result<Vec<u8>> {
            self.inner.read(path)
        }

        fn write(&self, path: &Path, bytes: &[u8]) -> Result<()> {
            self.inner.write(path, bytes)
        }

        fn rename(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.rename(from, to)
        }

        fn remove_file(&self, path: &Path) -> Result<()> {
            self.inner.remove_file(path)
        }

        fn acquire_jobs_lock(
            &self,
            path: &Path,
        ) -> Result<Box<dyn crate::migrate::sys::MigrationLock>> {
            self.inner.acquire_jobs_lock(path)
        }

        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }

        fn is_file(&self, path: &Path) -> bool {
            self.inner.is_file(path)
        }

        fn is_dir(&self, path: &Path) -> bool {
            self.inner.is_dir(path)
        }

        fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
            self.inner.read_dir(path)
        }

        fn create_dir_all(&self, path: &Path) -> Result<()> {
            self.inner.create_dir_all(path)
        }

        fn current_uid(&self) -> u32 {
            self.inner.current_uid()
        }

        fn now(&self) -> DateTime<Utc> {
            self.inner.now()
        }

        fn pid_alive(&self, pid: i32) -> bool {
            *self.alive_calls.lock().entry(pid).or_default() += 1;
            let mut alive = self.alive.lock();
            let responses = alive.entry(pid).or_default();
            match responses.len() {
                0 => false,
                1 => responses[0],
                _ => responses.pop_front().unwrap_or(false),
            }
        }

        fn terminate(&self, pid: i32) -> Result<()> {
            self.signal_order.lock().push(("TERM", pid));
            self.inner.terminate(pid)
        }

        fn kill(&self, pid: i32) -> Result<()> {
            self.signal_order.lock().push(("KILL", pid));
            self.inner.kill(pid)
        }

        fn sleep(&self, _duration: Duration) {
            *self.sleep_calls.lock() += 1;
        }

        fn run_launchctl(&self, args: &[&str]) -> Result<LaunchctlOutput> {
            self.inner.run_launchctl(args)
        }
    }

    #[test]
    fn alive_pids_terminate_then_escalate_only_when_still_alive() {
        let inner = fixture();
        write(
            &inner,
            "/fixtures/.hermes/gateway.lock",
            r#"{"pid":4101,"kind":"hermes-gateway"}"#,
        );
        write(
            &inner,
            "/fixtures/.hermes/profiles/work/gateway.lock",
            r#"{"pid":4102,"kind":"hermes-gateway"}"#,
        );
        let env = ScriptedPidEnv::new(inner);
        env.script_pid(
            4101,
            std::iter::repeat_n(true, super::TERM_WAIT_POLLS + 1).chain([false]),
        );
        env.script_pid(4102, [true, false]);

        let summary = bring_gateway_down(&env, Path::new(ROOT), Path::new(AGENTS), false).unwrap();

        assert_eq!(summary.pids_found, [4101, 4102]);
        assert_eq!(summary.pids_terminated, [4101, 4102]);
        assert_eq!(summary.pids_killed, [4101]);
        assert_eq!(env.inner.terminate_calls(), [4101, 4102]);
        assert_eq!(env.inner.kill_calls(), [4101]);
        assert_eq!(
            env.signal_order(),
            [("TERM", 4101), ("KILL", 4101), ("TERM", 4102)]
        );
        assert_eq!(env.alive_calls(4101), super::TERM_WAIT_POLLS + 2);
        assert_eq!(env.alive_calls(4102), 2);
        assert_eq!(*env.sleep_calls.lock(), super::TERM_WAIT_POLLS + 1);
        assert!(!env
            .inner
            .exists(Path::new("/fixtures/.hermes/gateway.lock")));
        assert!(!env
            .inner
            .exists(Path::new("/fixtures/.hermes/profiles/work/gateway.lock")));
        println!(
            "signal-order={:?} terminated={:?} killed={:?}",
            env.signal_order(),
            summary.pids_terminated,
            summary.pids_killed
        );
    }

    #[test]
    fn pid_that_dies_during_bounded_wait_is_not_killed() {
        let env = fixture();
        write(
            &env,
            "/fixtures/.hermes/gateway.lock",
            r#"{"pid":4151,"kind":"hermes-gateway"}"#,
        );
        env.set_pid_alive(4151, true);
        env.set_pid_death_after_sleeps(4151, 3);

        let summary = bring_gateway_down(&env, Path::new(ROOT), Path::new(AGENTS), false).unwrap();

        assert_eq!(summary.pids_terminated, [4151]);
        assert!(summary.pids_killed.is_empty());
        assert_eq!(env.terminate_calls(), [4151]);
        assert!(env.kill_calls().is_empty());
        assert_eq!(
            env.operations()
                .iter()
                .filter(|operation| matches!(operation, MigrationOperation::Sleep(_)))
                .count(),
            3
        );
        assert!(!env.exists(Path::new("/fixtures/.hermes/gateway.lock")));
    }

    #[test]
    fn pid_alive_after_bounded_wait_is_killed() {
        let env = fixture();
        write(
            &env,
            "/fixtures/.hermes/gateway.lock",
            r#"{"pid":4152,"kind":"hermes-gateway"}"#,
        );
        env.set_pid_alive(4152, true);

        let summary = bring_gateway_down(&env, Path::new(ROOT), Path::new(AGENTS), false).unwrap();

        assert_eq!(summary.pids_terminated, [4152]);
        assert_eq!(summary.pids_killed, [4152]);
        assert_eq!(env.terminate_calls(), [4152]);
        assert_eq!(env.kill_calls(), [4152]);
        assert_eq!(
            env.operations()
                .iter()
                .filter(|operation| matches!(operation, MigrationOperation::Sleep(_)))
                .count(),
            super::TERM_WAIT_POLLS
        );
        assert!(!env.exists(Path::new("/fixtures/.hermes/gateway.lock")));
    }

    #[test]
    fn every_matching_plist_is_booted_out_then_renamed_disabled() {
        let env = fixture();
        for label in [
            "ai.hermes.gateway",
            "ai.hermes.gateway-advisor",
            "ai.hermes.gateway-marketer",
        ] {
            write(&env, &format!("{AGENTS}/{label}.plist"), "<plist/>");
        }
        write(
            &env,
            &format!("{AGENTS}/com.example.gateway.plist"),
            "<plist/>",
        );

        let summary = bring_gateway_down(&env, Path::new(ROOT), Path::new(AGENTS), false).unwrap();

        assert_eq!(
            env.launchctl_calls(),
            [
                vec![
                    String::from("bootout"),
                    String::from("gui/501/ai.hermes.gateway-advisor"),
                ],
                vec![
                    String::from("bootout"),
                    String::from("gui/501/ai.hermes.gateway-marketer"),
                ],
                vec![
                    String::from("bootout"),
                    String::from("gui/501/ai.hermes.gateway"),
                ],
            ]
        );
        assert_eq!(env.rename_calls().len(), 3);
        assert!(env
            .rename_calls()
            .iter()
            .all(|(from, to)| to == &PathBuf::from(format!("{}.disabled", from.display()))));
        assert_eq!(summary.plists_booted_out.len(), 3);
        assert_eq!(summary.plists_disabled.len(), 3);
        println!(
            "launchctl={:?} renames={:?}",
            env.launchctl_calls(),
            env.rename_calls()
        );
    }

    #[test]
    fn stale_pid_lock_is_removed_and_disabled_plists_remain_a_no_op() {
        let env = fixture();
        write(
            &env,
            "/fixtures/.hermes/gateway.lock",
            r#"{"pid":4201,"kind":"hermes-gateway"}"#,
        );
        write(
            &env,
            &format!("{AGENTS}/ai.hermes.gateway.plist.disabled"),
            "<plist/>",
        );

        let summary = bring_gateway_down(&env, Path::new(ROOT), Path::new(AGENTS), false).unwrap();

        assert_eq!(summary.pids_found, [4201]);
        assert!(summary.pids_terminated.is_empty());
        assert!(summary.pids_killed.is_empty());
        assert!(summary.plists_booted_out.is_empty());
        assert!(summary.plists_disabled.is_empty());
        assert!(env.terminate_calls().is_empty());
        assert!(env.kill_calls().is_empty());
        assert!(env.launchctl_calls().is_empty());
        assert!(env.rename_calls().is_empty());
        assert!(!env.exists(Path::new("/fixtures/.hermes/gateway.lock")));
        println!("stale-lock-cleanup summary={summary:?}");
    }

    #[test]
    fn not_loaded_bootout_is_success_and_plist_is_still_disabled() {
        let env = fixture();
        let plist = format!("{AGENTS}/ai.hermes.gateway.plist");
        write(&env, &plist, "<plist/>");
        env.set_launchctl_response(LaunchctlOutput {
            status: Some(3),
            stdout: String::new(),
            stderr: "Boot-out failed: 3: No such process".into(),
        });

        let summary = bring_gateway_down(&env, Path::new(ROOT), Path::new(AGENTS), false).unwrap();

        assert_eq!(env.launchctl_calls().len(), 1);
        assert_eq!(env.rename_calls().len(), 1);
        assert_eq!(summary.plists_booted_out, ["ai.hermes.gateway"]);
        assert_eq!(summary.plists_disabled.len(), 1);
        println!(
            "not-loaded tolerated launchctl={:?} rename={:?}",
            env.launchctl_calls(),
            env.rename_calls()
        );
    }

    #[test]
    fn dry_run_reports_intended_actions_with_zero_side_effects() {
        let env = fixture();
        write(
            &env,
            "/fixtures/.hermes/gateway.lock",
            r#"{"pid":4301,"kind":"hermes-gateway"}"#,
        );
        env.set_pid_alive(4301, true);
        write(
            &env,
            &format!("{AGENTS}/ai.hermes.gateway.plist"),
            "<plist/>",
        );

        let summary = bring_gateway_down(&env, Path::new(ROOT), Path::new(AGENTS), true).unwrap();

        assert_eq!(summary.pids_found, [4301]);
        assert_eq!(summary.pids_terminated, [4301]);
        assert_eq!(summary.pids_killed, [4301]);
        assert_eq!(summary.plists_booted_out, ["ai.hermes.gateway"]);
        assert_eq!(summary.plists_disabled.len(), 1);
        assert!(env.terminate_calls().is_empty());
        assert!(env.kill_calls().is_empty());
        assert!(env.launchctl_calls().is_empty());
        assert!(env.rename_calls().is_empty());
        println!("dry-run intended summary={summary:?}");
    }

    #[test]
    fn malformed_locks_are_skipped_and_escalation_is_bounded() {
        let inner = fixture();
        write(&inner, "/fixtures/.hermes/gateway.lock", "{not-json");
        write(
            &inner,
            "/fixtures/.hermes/profiles/work/gateway.lock",
            r#"{"pid":4401}"#,
        );
        let env = ScriptedPidEnv::new(inner);
        env.script_pid(
            4401,
            std::iter::repeat_n(true, super::TERM_WAIT_POLLS + 1).chain([false]),
        );

        let summary = bring_gateway_down(&env, Path::new(ROOT), Path::new(AGENTS), false).unwrap();

        assert_eq!(summary.pids_found, [4401]);
        assert_eq!(env.alive_calls(4401), super::TERM_WAIT_POLLS + 2);
        assert_eq!(env.inner.terminate_calls(), [4401]);
        assert_eq!(env.inner.kill_calls(), [4401]);
        println!(
            "malformed skipped; bounded alive-checks={}",
            env.alive_calls(4401)
        );
    }
}
