use crate::{OmonError, Result};
use serde::{Deserialize, Serialize};
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
    pub default_model: Option<String>,
}

impl Default for OmoBackendConfig {
    fn default() -> Self {
        Self {
            appserver_url: "ws://127.0.0.1:19742".to_string(),
            auth_token: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(600),
            default_model: None,
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

    pub fn with_default_model(mut self, default_model: Option<impl Into<String>>) -> Self {
        self.default_model = default_model.map(Into::into);
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

        Ok(Self {
            appserver_url,
            auth_token,
            connect_timeout: Duration::from_secs(5),
            request_timeout,
            default_model,
        })
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
}
