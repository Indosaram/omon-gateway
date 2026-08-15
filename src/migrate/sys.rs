use crate::{OmonError, Result};
use chrono::{DateTime, Utc};
use fs2::FileExt;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchctlOutput {
    pub status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

pub trait MigrationLock: Send {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationOperation {
    LockAcquired(PathBuf),
    LockReleased(PathBuf),
    PidAlive(i32),
    Terminate(i32),
    Kill(i32),
    Sleep(Duration),
    RemoveFile(PathBuf),
    Write(PathBuf),
    Rename(PathBuf, PathBuf),
}

pub trait MigrationEnv: Send + Sync {
    fn read_to_string(&self, path: &Path) -> Result<String>;
    fn read(&self, path: &Path) -> Result<Vec<u8>>;
    fn write(&self, path: &Path, bytes: &[u8]) -> Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;
    fn remove_file(&self, path: &Path) -> Result<()>;
    fn acquire_jobs_lock(&self, path: &Path) -> Result<Box<dyn MigrationLock>>;
    fn exists(&self, path: &Path) -> bool;
    fn is_file(&self, path: &Path) -> bool;
    fn is_dir(&self, path: &Path) -> bool;
    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;
    fn create_dir_all(&self, path: &Path) -> Result<()>;

    fn current_uid(&self) -> u32;
    fn now(&self) -> DateTime<Utc>;

    fn pid_alive(&self, pid: i32) -> bool;
    fn terminate(&self, pid: i32) -> Result<()>;
    fn kill(&self, pid: i32) -> Result<()>;
    fn sleep(&self, duration: Duration);

    fn run_launchctl(&self, args: &[&str]) -> Result<LaunchctlOutput>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct OsEnv;

struct OsMigrationLock(File);

impl MigrationLock for OsMigrationLock {}

impl Drop for OsMigrationLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

impl MigrationEnv for OsEnv {
    fn read_to_string(&self, path: &Path) -> Result<String> {
        std::fs::read_to_string(path).map_err(|error| fs_error("read", path, error))
    }

    fn read(&self, path: &Path) -> Result<Vec<u8>> {
        std::fs::read(path).map_err(|error| fs_error("read", path, error))
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        std::fs::write(path, bytes).map_err(|error| fs_error("write", path, error))
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        std::fs::rename(from, to).map_err(|error| {
            OmonError::Config(format!(
                "failed to rename {} to {}: {error}",
                from.display(),
                to.display()
            ))
        })
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(fs_error("remove", path, error)),
        }
    }

    fn acquire_jobs_lock(&self, path: &Path) -> Result<Box<dyn MigrationLock>> {
        let file = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .map_err(|error| fs_error("open lock file", path, error))?;
        file.lock_exclusive()
            .map_err(|error| fs_error("lock", path, error))?;
        Ok(Box::new(OsMigrationLock(file)))
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn is_file(&self, path: &Path) -> bool {
        path.is_file()
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let entries =
            std::fs::read_dir(path).map_err(|error| fs_error("read directory", path, error))?;
        entries
            .map(|entry| {
                entry
                    .map(|entry| entry.path())
                    .map_err(|error| fs_error("read directory entry in", path, error))
            })
            .collect()
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path).map_err(|error| fs_error("create directory", path, error))
    }

    fn current_uid(&self) -> u32 {
        // SAFETY: getuid has no preconditions and does not dereference pointers.
        unsafe { libc::getuid() }
    }

    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn pid_alive(&self, pid: i32) -> bool {
        // SAFETY: signal 0 performs an existence/permission check and sends no signal.
        let result = unsafe { libc::kill(pid, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    fn terminate(&self, pid: i32) -> Result<()> {
        send_signal(pid, libc::SIGTERM, "SIGTERM")
    }

    fn kill(&self, pid: i32) -> Result<()> {
        send_signal(pid, libc::SIGKILL, "SIGKILL")
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }

    fn run_launchctl(&self, args: &[&str]) -> Result<LaunchctlOutput> {
        let output = Command::new("launchctl")
            .args(args)
            .output()
            .map_err(|error| OmonError::Config(format!("failed to run launchctl: {error}")))?;
        Ok(LaunchctlOutput {
            status: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

fn fs_error(operation: &str, path: &Path, error: std::io::Error) -> OmonError {
    OmonError::Config(format!("failed to {operation} {}: {error}", path.display()))
}

fn send_signal(pid: i32, signal: i32, name: &str) -> Result<()> {
    // SAFETY: libc::kill accepts a process id and signal number by value.
    if unsafe { libc::kill(pid, signal) } == 0 {
        Ok(())
    } else {
        Err(OmonError::Config(format!(
            "failed to send {name} to pid {pid}: {}",
            std::io::Error::last_os_error()
        )))
    }
}

/// Public test support for migration modules and integration tests.
///
/// This fake is compiled in all builds so `tests/` can inject it without a feature flag. It never
/// calls [`OsEnv`], the operating-system filesystem, process signals, or `launchctl`.
pub struct FakeMigrationEnv {
    files: Mutex<HashMap<PathBuf, Vec<u8>>>,
    directories: Mutex<HashSet<PathBuf>>,
    read_only_paths: Mutex<HashSet<PathBuf>>,
    now: Mutex<DateTime<Utc>>,
    current_uid: Mutex<u32>,
    alive_pids: Mutex<HashSet<i32>>,
    pid_death_after_sleeps: Mutex<HashMap<i32, usize>>,
    operations: Arc<Mutex<Vec<MigrationOperation>>>,
    terminate_calls: Mutex<Vec<i32>>,
    kill_calls: Mutex<Vec<i32>>,
    rename_calls: Mutex<Vec<(PathBuf, PathBuf)>>,
    write_calls: Mutex<Vec<(PathBuf, Vec<u8>)>>,
    launchctl_calls: Mutex<Vec<Vec<String>>>,
    launchctl_response: Mutex<LaunchctlOutput>,
    launchctl_error: Mutex<Option<String>>,
}

impl FakeMigrationEnv {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
            directories: Mutex::new(HashSet::new()),
            read_only_paths: Mutex::new(HashSet::new()),
            now: Mutex::new(now),
            current_uid: Mutex::new(0),
            alive_pids: Mutex::new(HashSet::new()),
            pid_death_after_sleeps: Mutex::new(HashMap::new()),
            operations: Arc::new(Mutex::new(Vec::new())),
            terminate_calls: Mutex::new(Vec::new()),
            kill_calls: Mutex::new(Vec::new()),
            rename_calls: Mutex::new(Vec::new()),
            write_calls: Mutex::new(Vec::new()),
            launchctl_calls: Mutex::new(Vec::new()),
            launchctl_response: Mutex::new(LaunchctlOutput {
                status: Some(0),
                stdout: String::new(),
                stderr: String::new(),
            }),
            launchctl_error: Mutex::new(None),
        }
    }

    pub fn set_now(&self, now: DateTime<Utc>) {
        *self.now.lock() = now;
    }

    pub fn set_current_uid(&self, uid: u32) {
        *self.current_uid.lock() = uid;
    }

    pub fn set_pid_alive(&self, pid: i32, alive: bool) {
        if alive {
            self.alive_pids.lock().insert(pid);
        } else {
            self.alive_pids.lock().remove(&pid);
        }
    }

    pub fn set_pid_death_after_sleeps(&self, pid: i32, sleeps: usize) {
        self.pid_death_after_sleeps.lock().insert(pid, sleeps);
    }

    pub fn operations(&self) -> Vec<MigrationOperation> {
        self.operations.lock().clone()
    }

    pub fn set_read_only(&self, path: impl Into<PathBuf>, read_only: bool) {
        let path = path.into();
        if read_only {
            self.read_only_paths.lock().insert(path);
        } else {
            self.read_only_paths.lock().remove(&path);
        }
    }

    pub fn set_launchctl_response(&self, response: LaunchctlOutput) {
        *self.launchctl_response.lock() = response;
        *self.launchctl_error.lock() = None;
    }

    pub fn set_launchctl_error(&self, message: impl Into<String>) {
        *self.launchctl_error.lock() = Some(message.into());
    }

    pub fn terminate_calls(&self) -> Vec<i32> {
        self.terminate_calls.lock().clone()
    }

    pub fn kill_calls(&self) -> Vec<i32> {
        self.kill_calls.lock().clone()
    }

    pub fn rename_calls(&self) -> Vec<(PathBuf, PathBuf)> {
        self.rename_calls.lock().clone()
    }

    pub fn write_calls(&self) -> Vec<(PathBuf, Vec<u8>)> {
        self.write_calls.lock().clone()
    }

    pub fn launchctl_calls(&self) -> Vec<Vec<String>> {
        self.launchctl_calls.lock().clone()
    }

    fn ensure_writable(&self, path: &Path) -> Result<()> {
        if self.read_only_paths.lock().contains(path) {
            Err(OmonError::Config(format!(
                "fake path is read-only: {}",
                path.display()
            )))
        } else {
            Ok(())
        }
    }

    fn record_parent_directories(&self, path: &Path) {
        let mut directories = self.directories.lock();
        let mut parent = path.parent();
        while let Some(path) = parent {
            if path.as_os_str().is_empty() {
                break;
            }
            directories.insert(path.to_path_buf());
            parent = path.parent();
        }
    }
}

struct FakeMigrationLock {
    path: PathBuf,
    operations: Arc<Mutex<Vec<MigrationOperation>>>,
}

impl MigrationLock for FakeMigrationLock {}

impl Drop for FakeMigrationLock {
    fn drop(&mut self) {
        self.operations
            .lock()
            .push(MigrationOperation::LockReleased(self.path.clone()));
    }
}

impl MigrationEnv for FakeMigrationEnv {
    fn read_to_string(&self, path: &Path) -> Result<String> {
        String::from_utf8(self.read(path)?).map_err(|error| {
            OmonError::Config(format!(
                "fake file {} is not UTF-8: {error}",
                path.display()
            ))
        })
    }

    fn read(&self, path: &Path) -> Result<Vec<u8>> {
        self.files.lock().get(path).cloned().ok_or_else(|| {
            OmonError::Config(format!("fake file does not exist: {}", path.display()))
        })
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        self.ensure_writable(path)?;
        self.record_parent_directories(path);
        self.files.lock().insert(path.to_path_buf(), bytes.to_vec());
        self.write_calls
            .lock()
            .push((path.to_path_buf(), bytes.to_vec()));
        self.operations
            .lock()
            .push(MigrationOperation::Write(path.to_path_buf()));
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.ensure_writable(from)?;
        self.ensure_writable(to)?;
        let bytes = self.files.lock().remove(from).ok_or_else(|| {
            OmonError::Config(format!("fake file does not exist: {}", from.display()))
        })?;
        self.record_parent_directories(to);
        self.files.lock().insert(to.to_path_buf(), bytes);
        self.rename_calls
            .lock()
            .push((from.to_path_buf(), to.to_path_buf()));
        self.operations.lock().push(MigrationOperation::Rename(
            from.to_path_buf(),
            to.to_path_buf(),
        ));
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        self.ensure_writable(path)?;
        self.files.lock().remove(path);
        self.operations
            .lock()
            .push(MigrationOperation::RemoveFile(path.to_path_buf()));
        Ok(())
    }

    fn acquire_jobs_lock(&self, path: &Path) -> Result<Box<dyn MigrationLock>> {
        self.operations
            .lock()
            .push(MigrationOperation::LockAcquired(path.to_path_buf()));
        Ok(Box::new(FakeMigrationLock {
            path: path.to_path_buf(),
            operations: Arc::clone(&self.operations),
        }))
    }

    fn exists(&self, path: &Path) -> bool {
        self.is_file(path) || self.is_dir(path)
    }

    fn is_file(&self, path: &Path) -> bool {
        self.files.lock().contains_key(path)
    }

    fn is_dir(&self, path: &Path) -> bool {
        self.directories.lock().contains(path)
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        if !self.is_dir(path) {
            return Err(OmonError::Config(format!(
                "fake directory does not exist: {}",
                path.display()
            )));
        }
        let files = self.files.lock();
        let directories = self.directories.lock();
        let mut entries: HashSet<PathBuf> = files
            .keys()
            .chain(directories.iter())
            .filter(|entry| entry.parent() == Some(path) && entry.as_path() != path)
            .cloned()
            .collect();
        let mut entries: Vec<_> = entries.drain().collect();
        entries.sort();
        Ok(entries)
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        self.ensure_writable(path)?;
        let mut directories = self.directories.lock();
        for ancestor in path.ancestors().filter(|path| !path.as_os_str().is_empty()) {
            directories.insert(ancestor.to_path_buf());
        }
        Ok(())
    }

    fn current_uid(&self) -> u32 {
        *self.current_uid.lock()
    }

    fn now(&self) -> DateTime<Utc> {
        *self.now.lock()
    }

    fn pid_alive(&self, pid: i32) -> bool {
        self.operations
            .lock()
            .push(MigrationOperation::PidAlive(pid));
        self.alive_pids.lock().contains(&pid)
    }

    fn terminate(&self, pid: i32) -> Result<()> {
        self.terminate_calls.lock().push(pid);
        self.operations
            .lock()
            .push(MigrationOperation::Terminate(pid));
        Ok(())
    }

    fn kill(&self, pid: i32) -> Result<()> {
        self.kill_calls.lock().push(pid);
        self.operations.lock().push(MigrationOperation::Kill(pid));
        self.alive_pids.lock().remove(&pid);
        Ok(())
    }

    fn sleep(&self, duration: Duration) {
        self.operations
            .lock()
            .push(MigrationOperation::Sleep(duration));
        let mut schedules = self.pid_death_after_sleeps.lock();
        let mut dead = Vec::new();
        for (&pid, remaining) in schedules.iter_mut() {
            if *remaining <= 1 {
                dead.push(pid);
            } else {
                *remaining -= 1;
            }
        }
        for pid in dead {
            schedules.remove(&pid);
            self.alive_pids.lock().remove(&pid);
        }
    }

    fn run_launchctl(&self, args: &[&str]) -> Result<LaunchctlOutput> {
        self.launchctl_calls
            .lock()
            .push(args.iter().map(|arg| (*arg).to_string()).collect());
        if let Some(message) = self.launchctl_error.lock().clone() {
            Err(OmonError::Config(message))
        } else {
            Ok(self.launchctl_response.lock().clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FakeMigrationEnv, LaunchctlOutput, MigrationEnv, OsEnv};
    use chrono::{TimeZone, Utc};
    use std::any::{type_name, type_name_of_val};
    use std::path::Path;

    #[test]
    fn fake_records_process_and_launchctl_calls_without_os_env() {
        let now = Utc.with_ymd_and_hms(2026, 8, 15, 12, 30, 0).unwrap();
        let env = FakeMigrationEnv::new(now);
        env.set_pid_alive(4242, true);
        env.set_launchctl_response(LaunchctlOutput {
            status: Some(0),
            stdout: "booted out".into(),
            stderr: String::new(),
        });
        assert_eq!(type_name_of_val(&env), type_name::<FakeMigrationEnv>());
        assert_ne!(type_name_of_val(&env), type_name::<OsEnv>());

        let output = env
            .run_launchctl(&["bootout", "gui/501/ai.hermes.gateway"])
            .unwrap();
        env.terminate(4242).unwrap();
        env.kill(4242).unwrap();

        assert_eq!(output.status, Some(0));
        assert_eq!(
            env.launchctl_calls(),
            vec![vec![
                "bootout".to_string(),
                "gui/501/ai.hermes.gateway".to_string()
            ]]
        );
        assert_eq!(env.terminate_calls(), vec![4242]);
        assert_eq!(env.kill_calls(), vec![4242]);
        assert!(!env.pid_alive(4242));
        println!("recorded launchctl={:?}", env.launchctl_calls());
        println!(
            "recorded terminate={:?} kill={:?}",
            env.terminate_calls(),
            env.kill_calls()
        );
    }

    #[test]
    fn fake_clock_is_injectable() {
        let first = Utc.with_ymd_and_hms(2026, 8, 15, 1, 2, 3).unwrap();
        let second = Utc.with_ymd_and_hms(2027, 1, 2, 3, 4, 5).unwrap();
        let env = FakeMigrationEnv::new(first);

        assert_eq!(env.now(), first);
        env.set_now(second);
        assert_eq!(env.now(), second);
        println!("injectable clock={}", env.now().to_rfc3339());
    }

    #[test]
    fn fake_filesystem_write_read_rename_and_exists_are_coherent() {
        let now = Utc.with_ymd_and_hms(2026, 8, 15, 0, 0, 0).unwrap();
        let env = FakeMigrationEnv::new(now);
        let source = Path::new("/migration/source.env");
        let destination = Path::new("/migration/destination.env");

        env.create_dir_all(Path::new("/migration")).unwrap();
        env.write(source, b"TOKEN=secret\n").unwrap();
        assert_eq!(env.read(source).unwrap(), b"TOKEN=secret\n");
        assert_eq!(env.read_to_string(source).unwrap(), "TOKEN=secret\n");
        assert!(env.exists(source));
        assert!(env.is_file(source));
        assert!(env.is_dir(Path::new("/migration")));

        env.rename(source, destination).unwrap();

        assert!(!env.exists(source));
        assert!(env.exists(destination));
        assert_eq!(env.read(destination).unwrap(), b"TOKEN=secret\n");
        assert_eq!(
            env.rename_calls(),
            vec![(source.into(), destination.into())]
        );
        assert_eq!(
            env.write_calls(),
            vec![(source.into(), b"TOKEN=secret\n".to_vec())]
        );
        assert_eq!(
            env.read_dir(Path::new("/migration")).unwrap(),
            vec![destination.to_path_buf()]
        );
        println!(
            "in-memory fs files={:?}",
            env.read_dir(Path::new("/migration")).unwrap()
        );
    }

    #[test]
    fn fake_read_only_path_rejects_writes() {
        let env = FakeMigrationEnv::new(Utc::now());
        let path = Path::new("/migration/read-only.env");
        env.set_read_only(path, true);

        let error = env.write(path, b"data").unwrap_err();

        assert!(error.to_string().contains("read-only"));
        assert!(env.write_calls().is_empty());
    }
}
