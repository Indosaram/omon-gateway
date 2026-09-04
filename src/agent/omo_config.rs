use crate::{OmonError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

pub fn validate_agent_backend_value(value: Option<&str>) -> Result<()> {
    match value.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(()),
        Some(v)
            if v.eq_ignore_ascii_case("omo")
                || v.eq_ignore_ascii_case("omo-appserver")
                || v.eq_ignore_ascii_case("appserver") =>
        {
            Ok(())
        }
        Some(v)
            if v.eq_ignore_ascii_case("llm")
                || v.eq_ignore_ascii_case("hermes")
                || v.eq_ignore_ascii_case("direct") =>
        {
            Err(OmonError::Config(
                "direct LLM backend has been removed; only 'omo' (app-server) backend is supported"
                    .to_string(),
            ))
        }
        Some(invalid) => Err(OmonError::Config(format!(
            "invalid OMON_AGENT_BACKEND '{invalid}': direct LLM backend has been removed and only 'omo' is supported"
        ))),
    }
}

pub fn validate_agent_backend_env() -> Result<()> {
    let val = std::env::var("OMON_AGENT_BACKEND").ok();
    validate_agent_backend_value(val.as_deref())
}

/// Configuration for the OMO app-server WebSocket daemon backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmoBackendConfig {
    pub appserver_url: String,
    pub auth_token: Option<String>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub total_timeout: Duration,
    pub default_model: Option<String>,
    pub per_agent_workspace: bool,
    pub workspace_root: Option<PathBuf>,
}

/// Default daemon URL for the isolated cron lane (see [`OmoBackendConfig::cron_from_env`]).
pub const CRON_APPSERVER_URL_DEFAULT: &str = "ws://127.0.0.1:19743";

impl Default for OmoBackendConfig {
    fn default() -> Self {
        Self {
            appserver_url: "ws://127.0.0.1:19742".to_string(),
            auth_token: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(600),
            total_timeout: Duration::from_secs(1800),
            default_model: None,
            per_agent_workspace: true,
            workspace_root: None,
        }
    }
}

impl OmoBackendConfig {
    pub fn new(appserver_url: impl Into<String>) -> Self {
        Self {
            appserver_url: appserver_url.into(),
            ..Default::default()
        }
    }

    pub fn with_auth_token(mut self, auth_token: Option<impl Into<String>>) -> Self {
        self.auth_token = auth_token.map(Into::into);
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_total_timeout(mut self, timeout: Duration) -> Self {
        self.total_timeout = timeout;
        self
    }

    pub fn with_default_model(mut self, default_model: Option<impl Into<String>>) -> Self {
        self.default_model = default_model.map(Into::into);
        self
    }

    pub fn with_workspace_root(mut self, workspace_root: impl Into<PathBuf>) -> Self {
        self.workspace_root = Some(workspace_root.into());
        self
    }

    pub fn with_per_agent_workspace(mut self, per_agent_workspace: bool) -> Self {
        self.per_agent_workspace = per_agent_workspace;
        self
    }

    pub fn validate_url(url: &str) -> Result<()> {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            return Err(OmonError::Config(
                "OMON_OMO_APPSERVER_URL is required when OMON_AGENT_BACKEND is 'omo'".to_string(),
            ));
        }
        if !trimmed.starts_with("ws://") && !trimmed.starts_with("wss://") {
            return Err(OmonError::Config(format!(
                "invalid OMON_OMO_APPSERVER_URL '{trimmed}': must start with ws:// or wss://"
            )));
        }
        Ok(())
    }

    pub fn from_env() -> Result<Self> {
        // Zero-config default: when the env var is absent, target the local
        // daemon URL that the supervisor (see omo_daemon) auto-spawns.
        let appserver_url = std::env::var("OMON_OMO_APPSERVER_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "ws://127.0.0.1:19742".to_string());

        Self::validate_url(&appserver_url)?;

        // Idle-gap tolerance between streamed daemon events; long agent turns
        // (digests, refactors) need minutes, not seconds.
        let request_timeout = match std::env::var("OMON_OMO_TURN_TIMEOUT_SECS") {
            Ok(v) if !v.trim().is_empty() => {
                Duration::from_secs(v.trim().parse::<u64>().map_err(|_| {
                    OmonError::Config(format!(
                        "invalid OMON_OMO_TURN_TIMEOUT_SECS: '{v}', expected a positive integer"
                    ))
                })?)
            }
            _ => Duration::from_secs(600),
        };

        // Hard ceiling for a whole turn: a looping agent keeps emitting
        // events, so the per-event gap timeout never fires. On deadline the
        // backend sends turn/interrupt so the daemon thread is freed.
        // 1800s fits a multi-round investigation turn: the reasoning model
        // needs 1-4min per tool round, and cutting at 900s killed turns that
        // had already found the answer.
        let total_timeout = match std::env::var("OMON_OMO_TURN_TOTAL_TIMEOUT_SECS") {
            Ok(v) if !v.trim().is_empty() => Duration::from_secs(
                v.trim().parse::<u64>().map_err(|_| {
                    OmonError::Config(format!(
                        "invalid OMON_OMO_TURN_TOTAL_TIMEOUT_SECS: '{v}', expected a positive integer"
                    ))
                })?,
            ),
            _ => Duration::from_secs(1800),
        };

        let auth_token = std::env::var("OMON_OMO_APPSERVER_AUTH_TOKEN")
            .or_else(|_| std::env::var("OMON_OMO_AUTH_TOKEN"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let default_model = std::env::var("OMON_DEFAULT_MODEL")
            .or_else(|_| std::env::var("DEFAULT_MODEL"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let per_agent_workspace = match std::env::var("OMON_PER_AGENT_WORKSPACE") {
            Ok(v) => {
                let trimmed = v.trim().to_ascii_lowercase();
                if trimmed.is_empty() {
                    true
                } else {
                    !matches!(trimmed.as_str(), "false" | "0" | "off" | "no")
                }
            }
            Err(_) => true,
        };

        let workspace_root = std::env::var_os("OMON_WORKSPACE_ROOT").map(PathBuf::from);

        Ok(Self {
            appserver_url,
            auth_token,
            connect_timeout: Duration::from_secs(15),
            request_timeout,
            total_timeout,
            default_model,
            per_agent_workspace,
            workspace_root,
        })
    }

    /// Configuration for the **cron lane**.
    ///
    /// An app-server thread runs one turn at a time, so a multi-minute cron
    /// turn (digests, syncs) would otherwise queue every Discord message
    /// behind it until the total deadline fires. Cron therefore targets its
    /// own daemon instance on a separate port, with a tighter ceiling, while
    /// inheriting the credentials and model of the interactive lane.
    pub fn cron_from_env() -> Result<Self> {
        let mut config = Self::from_env()?;

        config.appserver_url = std::env::var("OMON_OMO_CRON_APPSERVER_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| CRON_APPSERVER_URL_DEFAULT.to_string());
        Self::validate_url(&config.appserver_url)?;

        config.total_timeout = match std::env::var("OMON_OMO_CRON_TURN_TOTAL_TIMEOUT_SECS") {
            Ok(v) if !v.trim().is_empty() => Duration::from_secs(v.trim().parse::<u64>().map_err(|_| {
                OmonError::Config(format!(
                    "invalid OMON_OMO_CRON_TURN_TOTAL_TIMEOUT_SECS: '{v}', expected a positive integer"
                ))
            })?),
            _ => Duration::from_secs(600),
        };

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_turn_stream_timeout_env_contract() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Default: 10 minutes of event-gap tolerance for long agent turns
        std::env::remove_var("OMON_OMO_TURN_TIMEOUT_SECS");
        let cfg = OmoBackendConfig::from_env().unwrap();
        assert_eq!(cfg.request_timeout, Duration::from_secs(600));

        // Explicit override
        std::env::set_var("OMON_OMO_TURN_TIMEOUT_SECS", "300");
        let cfg = OmoBackendConfig::from_env().unwrap();
        assert_eq!(cfg.request_timeout, Duration::from_secs(300));

        // Invalid value fails boot (fail-fast convention)
        std::env::set_var("OMON_OMO_TURN_TIMEOUT_SECS", "soon");
        assert!(OmoBackendConfig::from_env().is_err());
        std::env::remove_var("OMON_OMO_TURN_TIMEOUT_SECS");
    }

    #[test]
    fn test_validate_agent_backend_env_contract() {
        // Default (absent or empty) succeeds as Omo backend
        assert!(validate_agent_backend_value(None).is_ok());
        assert!(validate_agent_backend_value(Some("")).is_ok());
        assert!(validate_agent_backend_value(Some("   ")).is_ok());

        // Supported OMO aliases succeed
        assert!(validate_agent_backend_value(Some("omo")).is_ok());
        assert!(validate_agent_backend_value(Some("omo-appserver")).is_ok());
        assert!(validate_agent_backend_value(Some("appserver")).is_ok());
        assert!(validate_agent_backend_value(Some("OMO")).is_ok());

        // Legacy direct-LLM aliases return Config error stating removal
        let err_llm = validate_agent_backend_value(Some("llm")).unwrap_err();
        assert!(
            matches!(err_llm, OmonError::Config(ref msg) if msg.contains("direct LLM backend has been removed") && msg.contains("omo")),
            "Expected Config error for 'llm', got {err_llm:?}"
        );
        let err_hermes = validate_agent_backend_value(Some("hermes")).unwrap_err();
        assert!(
            matches!(err_hermes, OmonError::Config(ref msg) if msg.contains("direct LLM backend has been removed") && msg.contains("omo")),
            "Expected Config error for 'hermes', got {err_hermes:?}"
        );
        let err_direct = validate_agent_backend_value(Some("direct")).unwrap_err();
        assert!(
            matches!(err_direct, OmonError::Config(ref msg) if msg.contains("direct LLM backend has been removed") && msg.contains("omo")),
            "Expected Config error for 'direct', got {err_direct:?}"
        );

        // Unknown backend returns Config error
        let err_unknown = validate_agent_backend_value(Some("unknown_backend")).unwrap_err();
        assert!(
            matches!(err_unknown, OmonError::Config(ref msg) if msg.contains("invalid OMON_AGENT_BACKEND")),
            "Expected Config error for unknown backend, got {err_unknown:?}"
        );
    }

    #[test]
    fn test_omo_backend_config_validates_websocket_scheme() {
        let err = OmoBackendConfig::validate_url("http://127.0.0.1:19742").unwrap_err();
        assert!(
            matches!(err, OmonError::Config(ref msg) if msg.contains("must start with ws:// or wss://")),
            "Expected Config error for invalid scheme, got {err:?}"
        );

        let err_empty = OmoBackendConfig::validate_url("   ").unwrap_err();
        assert!(
            matches!(err_empty, OmonError::Config(ref msg) if msg.contains("OMON_OMO_APPSERVER_URL is required")),
            "Expected Config error for empty url, got {err_empty:?}"
        );

        assert!(OmoBackendConfig::validate_url("ws://127.0.0.1:19742").is_ok());
        assert!(OmoBackendConfig::validate_url("wss://example.com/ws").is_ok());
    }

    #[test]
    fn test_cron_from_env_uses_isolated_daemon_lane() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("OMON_OMO_APPSERVER_URL");
        std::env::remove_var("OMON_OMO_CRON_APPSERVER_URL");
        std::env::remove_var("OMON_OMO_CRON_TURN_TOTAL_TIMEOUT_SECS");

        let interactive = OmoBackendConfig::from_env().unwrap();
        let cron = OmoBackendConfig::cron_from_env().unwrap();

        // A long cron turn must never occupy the interactive daemon: the two
        // lanes target different app-server instances.
        assert_eq!(interactive.appserver_url, "ws://127.0.0.1:19742");
        assert_eq!(cron.appserver_url, "ws://127.0.0.1:19743");
        assert_ne!(cron.appserver_url, interactive.appserver_url);

        // Cron turns get a tighter ceiling than interactive turns.
        assert_eq!(cron.total_timeout, Duration::from_secs(600));
        assert!(cron.total_timeout < interactive.total_timeout);
    }

    #[test]
    fn test_cron_from_env_honours_overrides_and_inherits_credentials() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OMON_OMO_CRON_APPSERVER_URL", "ws://127.0.0.1:29999");
        std::env::set_var("OMON_OMO_CRON_TURN_TOTAL_TIMEOUT_SECS", "120");
        std::env::set_var("OMON_OMO_APPSERVER_AUTH_TOKEN", "shared-token");
        std::env::set_var("OMON_DEFAULT_MODEL", "glm-5.3-flash");

        let cron = OmoBackendConfig::cron_from_env().unwrap();
        assert_eq!(cron.appserver_url, "ws://127.0.0.1:29999");
        assert_eq!(cron.total_timeout, Duration::from_secs(120));
        assert_eq!(cron.auth_token.as_deref(), Some("shared-token"));
        assert_eq!(cron.default_model.as_deref(), Some("glm-5.3-flash"));

        // Invalid override fails boot, consistent with the other timeouts.
        std::env::set_var("OMON_OMO_CRON_TURN_TOTAL_TIMEOUT_SECS", "later");
        assert!(OmoBackendConfig::cron_from_env().is_err());

        std::env::remove_var("OMON_OMO_CRON_APPSERVER_URL");
        std::env::remove_var("OMON_OMO_CRON_TURN_TOTAL_TIMEOUT_SECS");
        std::env::remove_var("OMON_OMO_APPSERVER_AUTH_TOKEN");
        std::env::remove_var("OMON_DEFAULT_MODEL");
    }

    #[test]
    fn test_from_env_defaults_to_local_daemon_when_env_absent() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("OMON_OMO_APPSERVER_URL");

        // The gateway must be zero-config: with no env var, the backend targets
        // the local default daemon URL that the supervisor auto-spawns.
        let config = OmoBackendConfig::from_env()
            .expect("from_env must succeed with default local daemon URL");
        assert_eq!(config.appserver_url, "ws://127.0.0.1:19742");
    }

    #[test]
    fn test_omo_backend_config_from_env_parses_url_and_token() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OMON_OMO_APPSERVER_URL", "ws://127.0.0.1:19742");
        std::env::set_var("OMON_OMO_APPSERVER_AUTH_TOKEN", "secret-token-123");
        std::env::set_var("OMON_DEFAULT_MODEL", "claude-3-5-sonnet");

        let config = OmoBackendConfig::from_env().unwrap();
        assert_eq!(config.appserver_url, "ws://127.0.0.1:19742");
        assert_eq!(config.auth_token.as_deref(), Some("secret-token-123"));
        assert_eq!(config.default_model.as_deref(), Some("claude-3-5-sonnet"));

        std::env::remove_var("OMON_OMO_APPSERVER_URL");
        std::env::remove_var("OMON_OMO_APPSERVER_AUTH_TOKEN");
        std::env::remove_var("OMON_DEFAULT_MODEL");
    }

    #[test]
    fn test_per_agent_workspace_and_workspace_root_env_parsing() {
        let _guard = ENV_LOCK.lock().unwrap();

        // Default when unset
        std::env::remove_var("OMON_PER_AGENT_WORKSPACE");
        std::env::remove_var("OMON_WORKSPACE_ROOT");
        let cfg = OmoBackendConfig::from_env().unwrap();
        assert!(cfg.per_agent_workspace);
        assert_eq!(cfg.workspace_root, None);

        // Workspace root parsed
        std::env::set_var("OMON_WORKSPACE_ROOT", "/tmp/omon-test-ws");
        let cfg = OmoBackendConfig::from_env().unwrap();
        assert_eq!(cfg.workspace_root, Some(PathBuf::from("/tmp/omon-test-ws")));

        // Kill-switch: false / 0 / off / no (case-insensitive)
        for falsy in ["false", "False", "0", "off", "OFF", "no", "NO"] {
            std::env::set_var("OMON_PER_AGENT_WORKSPACE", falsy);
            let cfg = OmoBackendConfig::from_env().unwrap();
            assert!(
                !cfg.per_agent_workspace,
                "Expected per_agent_workspace=false for '{falsy}'"
            );
        }

        // Truthy values
        for truthy in ["true", "True", "1", "on", "yes", "YES", ""] {
            std::env::set_var("OMON_PER_AGENT_WORKSPACE", truthy);
            let cfg = OmoBackendConfig::from_env().unwrap();
            assert!(
                cfg.per_agent_workspace,
                "Expected per_agent_workspace=true for '{truthy}'"
            );
        }

        std::env::remove_var("OMON_PER_AGENT_WORKSPACE");
        std::env::remove_var("OMON_WORKSPACE_ROOT");
    }
}
