use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

use crate::error::{OmonError, Result};

pub const DRAIN_REQUEST_FILENAME: &str = ".drain_request.json";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DrainRequest {
    #[serde(default = "default_action")]
    pub action: String,
    #[serde(default)]
    pub requested_at: Option<String>,
    #[serde(default)]
    pub principal: Option<String>,
    #[serde(default)]
    pub epoch: Option<String>,
    #[serde(default)]
    pub suppress_notification: bool,
}

fn default_action() -> String {
    "drain".to_string()
}

impl Default for DrainRequest {
    fn default() -> Self {
        Self {
            action: default_action(),
            requested_at: Some(Utc::now().to_rfc3339()),
            principal: Some("drain-control".to_string()),
            epoch: Some(current_instantiation_epoch().to_string()),
            suppress_notification: false,
        }
    }
}

static PROCESS_EPOCH: OnceLock<String> = OnceLock::new();

/// Computes the instantiation epoch for this container/VM/process.
///
/// On Linux: reads boot_id from /proc and starttime of PID 1.
/// On macOS / non-Linux: generates a process-boot identifier stable for the process lifetime.
pub fn current_instantiation_epoch() -> &'static str {
    PROCESS_EPOCH.get_or_init(|| {
        let mut boot_id = String::new();
        if let Ok(content) = fs::read_to_string("/proc/sys/kernel/random/boot_id") {
            boot_id = content.trim().to_string();
        }

        let mut pid1_start = String::new();
        if let Ok(stat) = fs::read_to_string("/proc/1/stat") {
            if let Some((_, tail)) = stat.rsplit_once(')') {
                let fields: Vec<&str> = tail.split_whitespace().collect();
                if fields.len() >= 20 {
                    pid1_start = fields[19].to_string();
                }
            }
        }

        if !boot_id.is_empty() || !pid1_start.is_empty() {
            format!("{boot_id}:{pid1_start}")
        } else {
            // macOS / non-Linux fallback: stable process boot UUID
            format!("boot:{}", Uuid::new_v4())
        }
    })
}

/// Checks if a marker's epoch is a definite mismatch with the current process boot epoch.
///
/// Lenient by design: returns false (not stale) if either current epoch or marker epoch is empty.
pub fn is_marker_epoch_stale(marker_epoch: Option<&str>, current_epoch: &str) -> bool {
    let current_trimmed = current_epoch.trim();
    if current_trimmed.is_empty() {
        return false;
    }
    match marker_epoch.map(str::trim).filter(|s| !s.is_empty()) {
        None => false, // legacy or contentless marker -> not stale (fail-safe)
        Some(m_epoch) => m_epoch != current_trimmed,
    }
}

/// Validates the raw JSON marker content against the current epoch.
///
/// Returns `Some(DrainRequest)` if a valid (non-stale) drain request is present,
/// or `None` if the marker is stale or explicitly requests a non-drain action.
pub fn validate_marker(content: &str, current_epoch: &str) -> Option<DrainRequest> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Some(DrainRequest::default());
    }

    match serde_json::from_str::<DrainRequest>(trimmed) {
        Ok(req) => {
            if req.action != "drain" {
                return None;
            }
            if is_marker_epoch_stale(req.epoch.as_deref(), current_epoch) {
                return None;
            }
            Some(req)
        }
        Err(_) => {
            // Corrupt / unparseable file is treated as a valid contentless drain request (fail-safe toward quiescing)
            Some(DrainRequest {
                action: "drain".to_string(),
                requested_at: None,
                principal: Some("malformed-marker".to_string()),
                epoch: None,
                suppress_notification: false,
            })
        }
    }
}

pub fn drain_request_path(dir: &Path) -> PathBuf {
    dir.join(DRAIN_REQUEST_FILENAME)
}

/// Writes a fresh `.drain_request.json` marker file atomically.
pub fn write_drain_request(
    dir: &Path,
    principal: Option<&str>,
    suppress_notification: bool,
) -> Result<DrainRequest> {
    let req = DrainRequest {
        action: "drain".to_string(),
        requested_at: Some(Utc::now().to_rfc3339()),
        principal: principal.map(str::to_string),
        epoch: Some(current_instantiation_epoch().to_string()),
        suppress_notification,
    };
    let path = drain_request_path(dir);
    let tmp_path = dir.join(format!(".drain_request.{}.tmp", Uuid::new_v4()));
    let payload = serde_json::to_string_pretty(&req)
        .map_err(|e| OmonError::Config(format!("failed to serialize drain request: {e}")))?;

    fs::write(&tmp_path, payload)
        .map_err(|e| OmonError::Config(format!("failed to write drain request temp file: {e}")))?;
    fs::rename(&tmp_path, &path)
        .map_err(|e| OmonError::Config(format!("failed to commit drain request marker: {e}")))?;

    Ok(req)
}

/// Removes the `.drain_request.json` marker file (cancel drain). Returns true if one was present.
pub fn clear_drain_request(dir: &Path) -> Result<bool> {
    let path = drain_request_path(dir);
    if path.exists() {
        fs::remove_file(&path).map_err(|e| {
            OmonError::Config(format!("failed to remove drain request marker: {e}"))
        })?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Checks the state dir for an active, valid drain request marker.
pub fn check_drain_requested(dir: &Path, current_epoch: &str) -> Option<DrainRequest> {
    let path = drain_request_path(dir);
    if !path.exists() {
        return None;
    }
    match fs::read_to_string(&path) {
        Ok(content) => validate_marker(&content, current_epoch),
        Err(_) => None,
    }
}

/// Background watcher for the `.drain_request.json` file.
pub struct DrainWatcher {
    state_dir: PathBuf,
    interval: Duration,
    drain_tx: watch::Sender<bool>,
    drain_rx: watch::Receiver<bool>,
}

impl DrainWatcher {
    pub fn new(state_dir: PathBuf, interval: Duration) -> Self {
        let (drain_tx, drain_rx) = watch::channel(false);
        Self {
            state_dir,
            interval,
            drain_tx,
            drain_rx,
        }
    }

    pub fn receiver(&self) -> watch::Receiver<bool> {
        self.drain_rx.clone()
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        let epoch = current_instantiation_epoch().to_string();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.interval);
            loop {
                ticker.tick().await;
                if let Some(req) = check_drain_requested(&self.state_dir, &epoch) {
                    tracing::warn!(
                        principal = ?req.principal,
                        epoch = ?req.epoch,
                        "valid drain request detected by drain watcher"
                    );
                    let _ = self.drain_tx.send(true);
                    break;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_marker_epoch_validation_fresh_vs_stale() {
        let current = "boot-123:pid-456";

        // 1. Same epoch -> valid, not stale
        assert!(!is_marker_epoch_stale(Some("boot-123:pid-456"), current));

        // 2. Different epoch -> stale
        assert!(is_marker_epoch_stale(Some("boot-999:pid-000"), current));

        // 3. None or empty epoch in marker -> not stale (lenient / backward-compatible)
        assert!(!is_marker_epoch_stale(None, current));
        assert!(!is_marker_epoch_stale(Some(""), current));
        assert!(!is_marker_epoch_stale(Some("   "), current));

        // 4. Empty current epoch -> not stale
        assert!(!is_marker_epoch_stale(Some("boot-999:pid-000"), ""));
    }

    #[test]
    fn test_validate_marker_content() {
        let current = "epoch-abc";

        // Valid payload with matching epoch
        let json_matching = r#"{"action":"drain","requested_at":"2026-08-16T12:00:00Z","principal":"admin","epoch":"epoch-abc"}"#;
        let validated = validate_marker(json_matching, current);
        assert!(validated.is_some());
        let req = validated.unwrap();
        assert_eq!(req.action, "drain");
        assert_eq!(req.principal.as_deref(), Some("admin"));
        assert_eq!(req.epoch.as_deref(), Some("epoch-abc"));

        // Payload with mismatched epoch -> rejected as stale (None)
        let json_stale = r#"{"action":"drain","requested_at":"2026-08-16T12:00:00Z","principal":"admin","epoch":"old-epoch-xyz"}"#;
        assert!(validate_marker(json_stale, current).is_none());

        // Payload with legacy/missing epoch -> accepted
        let json_no_epoch = r#"{"action":"drain","requested_at":"2026-08-16T12:00:00Z"}"#;
        assert!(validate_marker(json_no_epoch, current).is_some());

        // Action not drain -> rejected
        let json_other_action = r#"{"action":"restart","epoch":"epoch-abc"}"#;
        assert!(validate_marker(json_other_action, current).is_none());

        // Corrupt JSON -> accepted as fail-safe drain
        let json_corrupt = "{malformed json";
        let fallback = validate_marker(json_corrupt, current);
        assert!(fallback.is_some());
        assert_eq!(fallback.unwrap().action, "drain");
    }

    #[test]
    fn test_write_and_clear_drain_request_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();

        assert!(check_drain_requested(dir, current_instantiation_epoch()).is_none());

        let req = write_drain_request(dir, Some("test-op"), true).unwrap();
        assert_eq!(req.action, "drain");
        assert_eq!(req.principal.as_deref(), Some("test-op"));
        assert!(req.suppress_notification);

        let detected = check_drain_requested(dir, current_instantiation_epoch());
        assert!(detected.is_some());
        assert_eq!(detected.unwrap().principal.as_deref(), Some("test-op"));

        let cleared = clear_drain_request(dir).unwrap();
        assert!(cleared);
        assert!(check_drain_requested(dir, current_instantiation_epoch()).is_none());
    }
}
