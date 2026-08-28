use crate::{OmonError, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Agent backend engine variant selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentBackendKind {
    #[default]
    Llm,
    Omo,
}

impl AgentBackendKind {
    pub fn parse(value: Option<&str>) -> Result<Self> {
        match value.map(str::trim).filter(|s| !s.is_empty()) {
            None => Ok(Self::Llm),
            Some(v)
                if v.eq_ignore_ascii_case("llm")
                    || v.eq_ignore_ascii_case("hermes")
                    || v.eq_ignore_ascii_case("direct") =>
            {
                Ok(Self::Llm)
            }
            Some(v)
                if v.eq_ignore_ascii_case("omo")
                    || v.eq_ignore_ascii_case("omo-appserver")
                    || v.eq_ignore_ascii_case("appserver") =>
            {
                Ok(Self::Omo)
            }
            Some(invalid) => Err(OmonError::Config(format!(
                "invalid OMON_AGENT_BACKEND: '{invalid}', expected 'llm' or 'omo'"
            ))),
        }
    }

    pub fn from_env() -> Result<Self> {
        let val = std::env::var("OMON_AGENT_BACKEND").ok();
        Self::parse(val.as_deref())
    }
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
            request_timeout: Duration::from_secs(60),
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
        let appserver_url = std::env::var("OMON_OMO_APPSERVER_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                OmonError::Config(
                    "OMON_OMO_APPSERVER_URL is required when OMON_AGENT_BACKEND is 'omo'"
                        .to_string(),
                )
            })?;

        Self::validate_url(&appserver_url)?;

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
            request_timeout: Duration::from_secs(60),
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
    fn test_agent_backend_kind_defaults_to_llm_when_absent() {
        assert_eq!(
            AgentBackendKind::parse(None).unwrap(),
            AgentBackendKind::Llm
        );
        assert_eq!(
            AgentBackendKind::parse(Some("")).unwrap(),
            AgentBackendKind::Llm
        );
        assert_eq!(
            AgentBackendKind::parse(Some("   ")).unwrap(),
            AgentBackendKind::Llm
        );
    }

    #[test]
    fn test_agent_backend_kind_parses_supported_aliases() {
        assert_eq!(
            AgentBackendKind::parse(Some("llm")).unwrap(),
            AgentBackendKind::Llm
        );
        assert_eq!(
            AgentBackendKind::parse(Some("hermes")).unwrap(),
            AgentBackendKind::Llm
        );
        assert_eq!(
            AgentBackendKind::parse(Some("direct")).unwrap(),
            AgentBackendKind::Llm
        );
        assert_eq!(
            AgentBackendKind::parse(Some("omo")).unwrap(),
            AgentBackendKind::Omo
        );
        assert_eq!(
            AgentBackendKind::parse(Some("omo-appserver")).unwrap(),
            AgentBackendKind::Omo
        );
        assert_eq!(
            AgentBackendKind::parse(Some("appserver")).unwrap(),
            AgentBackendKind::Omo
        );
        assert_eq!(
            AgentBackendKind::parse(Some("OMO")).unwrap(),
            AgentBackendKind::Omo
        );
    }

    #[test]
    fn test_agent_backend_kind_invalid_returns_descriptive_error() {
        let err = AgentBackendKind::parse(Some("invalid-backend")).unwrap_err();
        assert!(
            matches!(err, OmonError::Config(ref msg) if msg.contains("invalid OMON_AGENT_BACKEND")),
            "Expected Config error with description, got {err:?}"
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
