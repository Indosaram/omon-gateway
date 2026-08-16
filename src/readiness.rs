use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

pub const DISK_DEGRADED_PERCENT: f64 = 90.0;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CheckResult {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metrics: HashMap<String, serde_json::Value>,
}

impl CheckResult {
    pub fn ok() -> Self {
        Self {
            status: "ok".to_string(),
            detail: None,
            metrics: HashMap::new(),
        }
    }

    pub fn ok_with_detail(detail: impl Into<String>) -> Self {
        Self {
            status: "ok".to_string(),
            detail: Some(detail.into()),
            metrics: HashMap::new(),
        }
    }

    pub fn degraded(detail: impl Into<String>) -> Self {
        Self {
            status: "degraded".to_string(),
            detail: Some(detail.into()),
            metrics: HashMap::new(),
        }
    }

    pub fn with_metric(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.metrics.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadinessReport {
    pub status: String,
    pub checks: HashMap<String, CheckResult>,
}

impl ReadinessReport {
    pub fn is_ok(&self) -> bool {
        self.status == "ok"
    }
}

/// Calculates disk headroom status and used percentage from total and free byte counts.
pub fn calculate_disk_headroom(
    total_bytes: u64,
    free_bytes: u64,
    threshold_pct: f64,
) -> (String, f64) {
    if total_bytes == 0 {
        return ("ok".to_string(), 0.0);
    }
    let used_bytes = total_bytes.saturating_sub(free_bytes);
    let used_pct = (used_bytes as f64 / total_bytes as f64) * 100.0;
    let rounded_pct = (used_pct * 10.0).round() / 10.0;
    let status = if rounded_pct >= threshold_pct {
        "degraded".to_string()
    } else {
        "ok".to_string()
    };
    (status, rounded_pct)
}

/// Probes SQLite database connectivity via a non-destructive read query.
pub async fn probe_database(pool: &SqlitePool) -> CheckResult {
    match sqlx::query("SELECT name FROM sqlite_master LIMIT 1")
        .fetch_optional(pool)
        .await
    {
        Ok(_) => CheckResult::ok(),
        Err(err) => CheckResult::degraded(format!("SQLite query failed: {err}")),
    }
}

/// Probes workspace disk usage and headroom.
pub fn probe_disk(workspace_root: &Path) -> CheckResult {
    let total = match fs2::total_space(workspace_root) {
        Ok(t) => t,
        Err(err) => {
            return CheckResult::degraded(format!("failed to read total disk space: {err}"))
        }
    };
    let free = match fs2::available_space(workspace_root) {
        Ok(f) => f,
        Err(err) => {
            return CheckResult::degraded(format!("failed to read available disk space: {err}"))
        }
    };

    let (status, used_pct) = calculate_disk_headroom(total, free, DISK_DEGRADED_PERCENT);
    let mut check = if status == "ok" {
        CheckResult::ok()
    } else {
        CheckResult::degraded(format!(
            "disk usage at {used_pct}% (>= {DISK_DEGRADED_PERCENT}%)"
        ))
    };
    check = check
        .with_metric("total_bytes", total)
        .with_metric("free_bytes", free)
        .with_metric("used_percent", used_pct);
    check
}

/// Probes whether required credentials for the configured default LLM provider are present in the environment.
pub fn probe_credentials(default_model: &str) -> CheckResult {
    let model = default_model.trim().to_ascii_lowercase();
    if model.is_empty() {
        return CheckResult::degraded("no default model configured");
    }

    let has_any_api_key = std::env::var("ANTHROPIC_API_KEY").is_ok()
        || std::env::var("OPENAI_API_KEY").is_ok()
        || std::env::var("DEEPSEEK_API_KEY").is_ok()
        || std::env::var("OPENROUTER_API_KEY").is_ok()
        || std::env::var("GEMINI_API_KEY").is_ok()
        || std::env::var("LLM_API_KEY").is_ok();

    if model.contains("claude") || model.starts_with("anthropic/") {
        if std::env::var("ANTHROPIC_API_KEY").is_ok() || has_any_api_key {
            CheckResult::ok_with_detail(format!("credentials present for model '{default_model}'"))
        } else {
            CheckResult::degraded("ANTHROPIC_API_KEY is not set in environment")
        }
    } else if model.contains("gpt")
        || model.contains("o1")
        || model.contains("o3")
        || model.starts_with("openai/")
    {
        if std::env::var("OPENAI_API_KEY").is_ok() || has_any_api_key {
            CheckResult::ok_with_detail(format!("credentials present for model '{default_model}'"))
        } else {
            CheckResult::degraded("OPENAI_API_KEY is not set in environment")
        }
    } else if model.contains("deepseek") {
        if std::env::var("DEEPSEEK_API_KEY").is_ok() || has_any_api_key {
            CheckResult::ok_with_detail(format!("credentials present for model '{default_model}'"))
        } else {
            CheckResult::degraded("DEEPSEEK_API_KEY is not set in environment")
        }
    } else if has_any_api_key {
        CheckResult::ok_with_detail(format!("credentials present for model '{default_model}'"))
    } else {
        CheckResult::degraded(format!(
            "no API key found in environment for model '{default_model}'"
        ))
    }
}

/// Probes connected/configured platform bots count.
pub fn probe_gateway(bot_count: usize) -> CheckResult {
    if bot_count == 0 {
        CheckResult::degraded("no platform bot tokens configured")
    } else {
        CheckResult::ok().with_metric("connected_bots", bot_count)
    }
}

/// Collects non-destructive startup and runtime readiness probes across database, disk, credentials, and gateway.
pub async fn collect_runtime_readiness(
    pool: &SqlitePool,
    workspace_root: &Path,
    default_model: &str,
    bot_count: usize,
) -> ReadinessReport {
    let mut checks = HashMap::new();

    checks.insert("state_db".to_string(), probe_database(pool).await);
    checks.insert("disk".to_string(), probe_disk(workspace_root));
    checks.insert("credentials".to_string(), probe_credentials(default_model));
    checks.insert("gateway".to_string(), probe_gateway(bot_count));

    let overall_status = if checks.values().all(|c| c.status == "ok") {
        "ok".to_string()
    } else {
        "degraded".to_string()
    };

    ReadinessReport {
        status: overall_status,
        checks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_headroom_calculation() {
        // 50% usage -> ok
        let (status, pct) = calculate_disk_headroom(1000, 500, 90.0);
        assert_eq!(status, "ok");
        assert_eq!(pct, 50.0);

        // 89.9% usage -> ok
        let (status, pct) = calculate_disk_headroom(1000, 101, 90.0);
        assert_eq!(status, "ok");
        assert_eq!(pct, 89.9);

        // 90.0% usage -> degraded
        let (status, pct) = calculate_disk_headroom(1000, 100, 90.0);
        assert_eq!(status, "degraded");
        assert_eq!(pct, 90.0);

        // 95% usage -> degraded
        let (status, pct) = calculate_disk_headroom(1000, 50, 90.0);
        assert_eq!(status, "degraded");
        assert_eq!(pct, 95.0);

        // 0 total -> ok fallback
        let (status, pct) = calculate_disk_headroom(0, 0, 90.0);
        assert_eq!(status, "ok");
        assert_eq!(pct, 0.0);
    }

    #[tokio::test]
    async fn test_database_probe_in_memory() {
        let pool = crate::storage::init_pool("sqlite::memory:").await.unwrap();
        let check = probe_database(&pool).await;
        assert_eq!(check.status, "ok");
    }

    #[test]
    fn test_gateway_probe() {
        let zero = probe_gateway(0);
        assert_eq!(zero.status, "degraded");

        let active = probe_gateway(2);
        assert_eq!(active.status, "ok");
        assert_eq!(
            active.metrics.get("connected_bots"),
            Some(&serde_json::json!(2))
        );
    }

    #[test]
    fn test_credential_probe_model_logic() {
        // Empty model -> degraded
        let empty = probe_credentials("");
        assert_eq!(empty.status, "degraded");
    }

    #[tokio::test]
    async fn test_collect_runtime_readiness_overall() {
        let pool = crate::storage::init_pool("sqlite::memory:").await.unwrap();
        let temp = tempfile::tempdir().unwrap();

        let report = collect_runtime_readiness(&pool, temp.path(), "custom-model", 1).await;
        assert!(report.checks.contains_key("state_db"));
        assert!(report.checks.contains_key("disk"));
        assert!(report.checks.contains_key("credentials"));
        assert!(report.checks.contains_key("gateway"));
        assert_eq!(report.checks["state_db"].status, "ok");
        assert!(report.checks["disk"].status == "ok" || report.checks["disk"].status == "degraded");
        assert!(report.checks["disk"].metrics.contains_key("used_percent"));
        assert_eq!(report.checks["gateway"].status, "ok");
        assert!(report.status == "ok" || report.status == "degraded");
    }
}
