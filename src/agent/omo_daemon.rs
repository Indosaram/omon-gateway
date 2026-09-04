//! Managed lifecycle for the local `omo app-server` daemon.
//!
//! The gateway is zero-config: when the app-server URL points at a local
//! address and nothing is listening there yet, the gateway spawns
//! `omo app-server --listen <url>` itself, waits for readiness, keeps it
//! alive (bounded restarts), and kills it on shutdown. Externally managed
//! daemons (already listening, or non-local URLs) are detected and left
//! alone.

use crate::agent::OmoBackendConfig;
use crate::{OmonError, Result};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

static SPAWN_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const READY_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const READY_WAIT_AFTER_SPAWN: Duration = Duration::from_secs(30);
const RESTART_BACKOFF: Duration = Duration::from_secs(2);

/// True when the daemon URL targets this machine (auto-spawn eligible).
pub fn is_local_url(url: &str) -> bool {
    let host = url
        .trim()
        .strip_prefix("ws://")
        .unwrap_or_else(|| url.trim())
        .split(['/', ':'])
        .next()
        .unwrap_or("");
    host.eq_ignore_ascii_case("127.0.0.1") || host.eq_ignore_ascii_case("localhost")
}

/// GET `<http-url>/readyz` over a raw TCP connection; true on HTTP 200.
async fn probe_readyz(ws_url: &str, limit: Duration) -> bool {
    let Some(rest) = ws_url.trim().strip_prefix("ws://") else {
        return false; // wss:// daemons are externally managed; no local probe
    };
    let Some(authority) = rest.split('/').next() else {
        return false;
    };
    let request = format!("GET /readyz HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    timeout(limit, async move {
        let mut stream = tokio::net::TcpStream::connect(authority).await.ok()?;
        stream.write_all(request.as_bytes()).await.ok()?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.ok()?;
        let head = String::from_utf8_lossy(&buf);
        Some(head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.0 200"))
    })
    .await
    .unwrap_or(Some(false))
    .unwrap_or(false)
}

/// Resolve the daemon binary to something spawnable.
///
/// Under launchd the gateway inherits a minimal `PATH`
/// (`/usr/bin:/bin:/usr/sbin:/sbin`) which excludes user-level install
/// directories such as `~/.bun/bin`, so a bare `omo` fails with ENOENT even
/// though it resolves fine in an interactive shell. When the name carries no
/// path separator and cannot be found on `path_env`, fall back to well-known
/// absolute install locations. Explicit paths are returned untouched.
fn resolve_daemon_bin(bin: &str, path_env: &str) -> String {
    if bin.contains('/') {
        return bin.to_string();
    }

    let on_path = path_env
        .split(':')
        .filter(|dir| !dir.is_empty())
        .any(|dir| {
            std::path::Path::new(dir)
                .join(bin)
                .try_exists()
                .unwrap_or(false)
        });
    if on_path {
        return bin.to_string();
    }

    if let Ok(home) = std::env::var("HOME") {
        for candidate in [
            format!("{home}/.bun/bin/{bin}"),
            format!("{home}/.local/bin/{bin}"),
            format!("{home}/.npm-global/bin/{bin}"),
        ] {
            if std::path::Path::new(&candidate)
                .try_exists()
                .unwrap_or(false)
            {
                return candidate;
            }
        }
    }
    for candidate in [
        format!("/opt/homebrew/bin/{bin}"),
        format!("/usr/local/bin/{bin}"),
    ] {
        if std::path::Path::new(&candidate)
            .try_exists()
            .unwrap_or(false)
        {
            return candidate;
        }
    }

    bin.to_string()
}

/// Build the spawn command for a local daemon. Exposed for tests.
fn daemon_command(bin: &str, listen_url: &str) -> Command {
    let mut cmd = Command::new(bin);
    cmd.args(["app-server", "--listen", listen_url, "--ws-auth", "off"])
        .stdin(Stdio::null())
        .kill_on_drop(true);

    let log_file = std::env::var("HOME")
        .ok()
        .map(|home| {
            let port = listen_url
                .rsplit(':')
                .next()
                .and_then(|p| p.split('/').next())
                .unwrap_or("default");
            std::path::PathBuf::from(home)
                .join(".omon")
                .join(format!("omo-appserver-{port}.log"))
        })
        .and_then(|path| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok()
        });

    if let Some(file) = log_file {
        if let Ok(file_clone) = file.try_clone() {
            cmd.stdout(Stdio::from(file));
            cmd.stderr(Stdio::from(file_clone));
            return cmd;
        }
    }

    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    cmd
}

fn autospawn_enabled() -> bool {
    !matches!(
        std::env::var("OMON_OMO_AUTOSPAWN")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "false" | "0"
    )
}

/// Owns the spawned daemon child for the lifetime of the gateway and
/// restarts it (bounded backoff) if it dies. Killing the supervisor kills
/// the daemon.
pub struct OmoDaemonSupervisor {
    child: Arc<Mutex<Option<Child>>>,
    shutdown: Arc<AtomicBool>,
}

impl OmoDaemonSupervisor {
    /// Ensure a daemon is serving `cfg.appserver_url`, spawning a local one
    /// when necessary. Returns `Ok(None)` when an externally managed daemon
    /// is already serving (or autospawn is disabled / URL is non-local).
    pub async fn ensure(cfg: &OmoBackendConfig) -> Result<Option<Self>> {
        let _guard = SPAWN_LOCK.lock().await;
        let bin = resolve_daemon_bin(
            &std::env::var("OMON_OMO_BIN").unwrap_or_else(|_| "omo".to_string()),
            &std::env::var("PATH").unwrap_or_default(),
        );
        if !autospawn_enabled() || !is_local_url(&cfg.appserver_url) {
            return Ok(None);
        }
        if probe_readyz(&cfg.appserver_url, READY_PROBE_TIMEOUT).await {
            tracing::debug!(url = %cfg.appserver_url, "external omo app-server already serving");
            return Ok(None);
        }

        tracing::info!(url = %cfg.appserver_url, bin = %bin, "spawning omo app-server daemon");
        let child = daemon_command(&bin, &cfg.appserver_url)
            .spawn()
            .map_err(|e| {
                OmonError::Config(format!(
                    "failed to spawn '{bin} app-server' for {url}: {e} \
                     (is the omo CLI installed? set OMON_OMO_BIN or OMON_OMO_AUTOSPAWN=off to manage the daemon yourself)",
                    url = cfg.appserver_url
                ))
            })?;

        let supervisor = Self {
            child: Arc::new(Mutex::new(Some(child))),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let deadline = Instant::now() + READY_WAIT_AFTER_SPAWN;
        while Instant::now() < deadline {
            if probe_readyz(&cfg.appserver_url, READY_PROBE_TIMEOUT).await {
                tracing::info!(url = %cfg.appserver_url, "omo app-server daemon ready");
                supervisor.spawn_watcher(cfg.appserver_url.clone(), bin);
                return Ok(Some(supervisor));
            }
            if supervisor.child_exited() {
                break;
            }
            sleep(Duration::from_millis(400)).await;
        }

        supervisor.kill();
        Err(OmonError::Config(format!(
            "spawned 'omo app-server' did not become ready at {} within {}s",
            cfg.appserver_url,
            READY_WAIT_AFTER_SPAWN.as_secs()
        )))
    }

    fn child_exited(&self) -> bool {
        self.child
            .try_lock()
            .ok()
            .and_then(|mut guard| {
                guard
                    .as_mut()
                    .and_then(|child| child.try_wait().ok().flatten())
                    .map(|_| true)
            })
            .unwrap_or(false)
    }

    fn spawn_watcher(&self, url: String, bin: String) {
        let child_slot = Arc::clone(&self.child);
        let shutdown = Arc::clone(&self.shutdown);
        tokio::spawn(async move {
            loop {
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    let exited = {
                        let mut guard = child_slot.lock().await;
                        match guard.as_mut() {
                            Some(child) => matches!(child.try_wait(), Ok(Some(_))),
                            None => true,
                        }
                    };
                    if exited {
                        break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                tracing::warn!(url = %url, "omo app-server daemon exited; checking before restart");
                sleep(Duration::from_millis(100)).await;
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                // A forked survivor or an external daemon may have taken over
                // the port; adopt it instead of spawning a child that would
                // die on bind conflict forever.
                if probe_readyz(&url, READY_PROBE_TIMEOUT).await {
                    tracing::info!(
                        url = %url,
                        "port already serving after daemon exit; deferring to the external daemon"
                    );
                    return;
                }
                match daemon_command(&bin, &url).spawn() {
                    Ok(child) => {
                        child_slot.lock().await.replace(child);
                        let deadline = Instant::now() + READY_WAIT_AFTER_SPAWN;
                        while Instant::now() < deadline {
                            if shutdown.load(Ordering::Acquire) {
                                return;
                            }
                            if probe_readyz(&url, READY_PROBE_TIMEOUT).await {
                                tracing::info!(url = %url, "restarted omo app-server daemon ready");
                                break;
                            }
                            sleep(Duration::from_millis(200)).await;
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to restart omo app-server daemon");
                        sleep(RESTART_BACKOFF).await;
                    }
                }
            }
        });
    }

    fn kill(&self) {
        self.shutdown.store(true, Ordering::Release);
        if let Ok(mut guard) = self.child.try_lock() {
            if let Some(child) = guard.as_mut() {
                let _ = child.start_kill();
            }
        }
    }
}

impl Drop for OmoDaemonSupervisor {
    fn drop(&mut self) {
        self.kill();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_local_url() {
        assert!(is_local_url("ws://127.0.0.1:19742"));
        assert!(is_local_url("ws://localhost:19742"));
        assert!(is_local_url("ws://LOCALHOST:9"));
        assert!(!is_local_url("ws://10.1.2.3:19742"));
        assert!(!is_local_url("wss://relay.example.com"));
    }

    #[tokio::test]
    async fn test_probe_readyz_detects_http_200_and_refusal() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let _ = sock.read(&mut buf).await;
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
        });
        assert!(probe_readyz(&format!("ws://{addr}"), Duration::from_secs(2)).await);
        server.await.unwrap();

        // Nothing listening on this ephemeral port range slot: probe fails.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);
        assert!(
            !probe_readyz(
                &format!("ws://127.0.0.1:{dead_port}"),
                Duration::from_secs(2)
            )
            .await
        );
    }

    #[test]
    fn test_resolve_daemon_bin_falls_back_to_known_install_paths() {
        // launchd hands the gateway a minimal PATH (/usr/bin:/bin:/usr/sbin:/sbin)
        // that excludes ~/.bun/bin, so a bare "omo" fails to spawn with ENOENT.
        // Resolution must fall back to well-known absolute install locations.
        let home = std::env::var("HOME").unwrap();
        let bun_omo = format!("{home}/.bun/bin/omo");
        if !std::path::Path::new(&bun_omo).exists() {
            return; // fallback target absent on this machine
        }

        let resolved = resolve_daemon_bin("omo", "/usr/bin:/bin:/usr/sbin:/sbin");
        assert_eq!(
            resolved, bun_omo,
            "bare 'omo' must resolve to an absolute path when PATH lacks the install dir"
        );

        // An explicit absolute override is always honoured verbatim.
        assert_eq!(
            resolve_daemon_bin("/custom/omo", "/usr/bin:/bin"),
            "/custom/omo"
        );
    }

    #[test]
    fn test_daemon_command_arguments() {
        let cmd = daemon_command("omo", "ws://127.0.0.1:19742");
        // Command internals are not inspectable portably; assert via as_std.
        let std_cmd = cmd.as_std();
        let args: Vec<String> = std_cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert_eq!(
            args,
            vec![
                "app-server",
                "--listen",
                "ws://127.0.0.1:19742",
                "--ws-auth",
                "off"
            ]
        );
        assert_eq!(std_cmd.get_program().to_string_lossy(), "omo");
    }
}
