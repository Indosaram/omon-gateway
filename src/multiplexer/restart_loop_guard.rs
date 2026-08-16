use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

pub const DEFAULT_MAX_RESTARTS: usize = 3;
pub const DEFAULT_WINDOW_SECONDS: u64 = 60;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BootLog {
    #[serde(default)]
    boots: Vec<f64>,
}

/// Persistent sliding-window circuit breaker to suppress crash-loop auto-resumes.
///
/// When an agent or turn triggers a fatal crash/SIGTERM, the supervisor (launchd /
/// systemd) automatically restarts the gateway. On boot, if restart-interrupted
/// sessions are pending and the gateway restarts >= `max_restarts` times within
/// `window_seconds`, the breaker trips and skips auto-resuming those sessions.
#[derive(Clone, Debug)]
pub struct RestartLoopGuard {
    path: PathBuf,
    max_restarts: usize,
    window_seconds: u64,
}

impl RestartLoopGuard {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_config(path, DEFAULT_MAX_RESTARTS, DEFAULT_WINDOW_SECONDS)
    }

    pub fn with_config(path: impl Into<PathBuf>, max_restarts: usize, window_seconds: u64) -> Self {
        Self {
            path: path.into(),
            max_restarts,
            window_seconds,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn load_boots(&self) -> Vec<f64> {
        let raw = match fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(_) => return Vec::new(),
        };
        match serde_json::from_str::<BootLog>(&raw) {
            Ok(log) => log.boots,
            Err(err) => {
                debug!(
                    path = %self.path.display(),
                    %err,
                    "restart_loop_guard: failed to parse JSON, starting empty"
                );
                Vec::new()
            }
        }
    }

    fn save_boots(&self, boots: &[f64]) {
        if let Some(parent) = self.path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let data = BootLog {
            boots: boots.to_vec(),
        };
        if let Ok(raw) = serde_json::to_string_pretty(&data) {
            let tmp_path = self.path.with_extension("tmp");
            if fs::write(&tmp_path, raw).is_ok() {
                let _ = fs::rename(&tmp_path, &self.path);
            }
        }
    }

    /// Records that the gateway just booted with resume-pending sessions.
    /// Prunes boots older than `window_seconds` and appends `now`.
    pub fn record_boot_at(&self, now: f64) -> Vec<f64> {
        let cutoff = now - (self.window_seconds.max(1) as f64);
        let mut boots: Vec<f64> = self
            .load_boots()
            .into_iter()
            .filter(|&t| t >= cutoff)
            .collect();
        boots.push(now);
        self.save_boots(&boots);
        boots
    }

    /// Returns `true` if the number of recent boots within `window_seconds` >= `max_restarts`.
    pub fn is_tripped_at(&self, now: f64) -> bool {
        if self.max_restarts == 0 {
            return false;
        }
        let cutoff = now - (self.window_seconds.max(1) as f64);
        let recent_count = self
            .load_boots()
            .into_iter()
            .filter(|&t| t >= cutoff)
            .count();
        recent_count >= self.max_restarts
    }

    /// Records this restart boot timestamp and checks if the breaker is tripped.
    /// Returns `true` if auto-resume should be SKIPPED.
    pub fn check_and_record_at(&self, now: f64) -> bool {
        let boots = self.record_boot_at(now);
        let tripped = if self.max_restarts > 0 {
            boots.len() >= self.max_restarts
        } else {
            false
        };
        if tripped {
            warn!(
                boots = boots.len(),
                window_seconds = self.window_seconds,
                max_restarts = self.max_restarts,
                path = %self.path.display(),
                "Restart-loop breaker TRIPPED: {} boots within {}s (threshold {}). Skipping auto-resume to break crash loop.",
                boots.len(),
                self.window_seconds,
                self.max_restarts,
            );
        }
        tripped
    }

    /// Uses the current wall clock time to record and check.
    pub fn check_and_record(&self) -> bool {
        let now = Utc::now().timestamp_millis() as f64 / 1000.0;
        self.check_and_record_at(now)
    }

    /// Clears the boot log file.
    pub fn clear(&self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sliding_window_trip_logic() {
        let temp = tempfile::tempdir().unwrap();
        let guard_file = temp.path().join("restart_loop.json");
        let guard = RestartLoopGuard::with_config(&guard_file, 3, 60);

        // Boot 1 at t=10.0: count=1, not tripped
        assert!(!guard.check_and_record_at(10.0));
        assert!(!guard.is_tripped_at(10.0));

        // Boot 2 at t=25.0: count=2, not tripped
        assert!(!guard.check_and_record_at(25.0));
        assert!(!guard.is_tripped_at(25.0));

        // Boot 3 at t=40.0: count=3 within 60s -> tripped!
        assert!(guard.check_and_record_at(40.0));
        assert!(guard.is_tripped_at(40.0));
    }

    #[test]
    fn test_sliding_window_prunes_expired_boots() {
        let temp = tempfile::tempdir().unwrap();
        let guard_file = temp.path().join("restart_loop.json");
        let guard = RestartLoopGuard::with_config(&guard_file, 3, 60);

        // Boot 1 at t=0.0
        assert!(!guard.check_and_record_at(0.0));
        // Boot 2 at t=30.0
        assert!(!guard.check_and_record_at(30.0));

        // Boot 3 arrives at t=70.0 (t=0.0 is now outside 60s window: cutoff 10.0)
        // Window now contains: [30.0, 70.0] -> count=2 -> not tripped!
        assert!(!guard.check_and_record_at(70.0));
        assert!(!guard.is_tripped_at(70.0));

        // Boot 4 at t=80.0 (cutoff 20.0: 30.0, 70.0 are within window) -> Window: [30.0, 70.0, 80.0] -> count=3 -> tripped!
        assert!(guard.check_and_record_at(80.0));
        assert!(guard.is_tripped_at(80.0));
    }

    #[test]
    fn test_clear_removes_state() {
        let temp = tempfile::tempdir().unwrap();
        let guard_file = temp.path().join("restart_loop.json");
        let guard = RestartLoopGuard::with_config(&guard_file, 2, 60);

        assert!(!guard.check_and_record_at(10.0));
        assert!(guard.check_and_record_at(20.0)); // tripped
        assert!(guard_file.exists());

        guard.clear();
        assert!(!guard_file.exists());
        assert!(!guard.is_tripped_at(25.0));
    }

    #[test]
    fn test_corrupted_json_fails_open() {
        let temp = tempfile::tempdir().unwrap();
        let guard_file = temp.path().join("restart_loop.json");
        fs::write(&guard_file, "{ corrupted json !").unwrap();

        let guard = RestartLoopGuard::with_config(&guard_file, 3, 60);
        // Fails open -> starts fresh with 1 boot, not tripped
        assert!(!guard.check_and_record_at(10.0));
    }
}
