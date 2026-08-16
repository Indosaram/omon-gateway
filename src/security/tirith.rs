use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const DEFAULT_TIRITH_TIMEOUT_SECS: u64 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScannerVerdict {
    Allow,
    Deny { reason: String },
}

impl ScannerVerdict {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

#[derive(Clone, Debug)]
pub struct TirithScanner {
    pub url: String,
    pub fail_open: bool,
    pub timeout: Duration,
    http: reqwest::Client,
}

impl TirithScanner {
    pub fn new(url: impl Into<String>, fail_open: bool, timeout: Duration) -> Self {
        Self {
            url: url.into(),
            fail_open,
            timeout,
            http: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Evaluates scanner response JSON or error into an allow/deny verdict.
    pub fn evaluate_response(
        response_result: std::result::Result<&Value, &str>,
        fail_open: bool,
    ) -> ScannerVerdict {
        match response_result {
            Ok(json) => {
                let action = json
                    .get("action")
                    .and_then(Value::as_str)
                    .unwrap_or("allow")
                    .to_ascii_lowercase();

                if action == "deny" || action == "block" || action == "reject" {
                    let summary = json
                        .get("summary")
                        .and_then(Value::as_str)
                        .filter(|s| !s.trim().is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| {
                            if let Some(findings) = json.get("findings").and_then(Value::as_array) {
                                let parts: Vec<String> = findings
                                    .iter()
                                    .filter_map(|f| {
                                        let title = f.get("title").and_then(Value::as_str)?;
                                        let desc = f
                                            .get("description")
                                            .and_then(Value::as_str)
                                            .unwrap_or("");
                                        if desc.is_empty() {
                                            Some(title.to_string())
                                        } else {
                                            Some(format!("{title}: {desc}"))
                                        }
                                    })
                                    .collect();
                                if !parts.is_empty() {
                                    return parts.join("; ");
                                }
                            }
                            "semantic risk analysis flagged this command".to_string()
                        });
                    ScannerVerdict::Deny { reason: summary }
                } else {
                    ScannerVerdict::Allow
                }
            }
            Err(err) => {
                if fail_open {
                    tracing::warn!(%err, "Tirith scanner call failed; failing open (command permitted)");
                    ScannerVerdict::Allow
                } else {
                    tracing::error!(%err, "Tirith scanner call failed; failing closed (command blocked)");
                    ScannerVerdict::Deny {
                        reason: format!("External security scanner unavailable: {err}"),
                    }
                }
            }
        }
    }

    /// POSTs the candidate command to the Tirith scanner and returns the verdict.
    pub async fn scan_command(&self, command: &str) -> ScannerVerdict {
        let payload = json!({
            "command": command,
        });

        let send_res = self.http.post(&self.url).json(&payload).send().await;

        match send_res {
            Ok(resp) => {
                if !resp.status().is_success() {
                    let status = resp.status();
                    return Self::evaluate_response(
                        Err(&format!("HTTP status {status}")),
                        self.fail_open,
                    );
                }
                match resp.json::<Value>().await {
                    Ok(json_body) => Self::evaluate_response(Ok(&json_body), self.fail_open),
                    Err(err) => {
                        let err_msg = format!("failed to decode scanner response JSON: {err}");
                        Self::evaluate_response(Err(&err_msg), self.fail_open)
                    }
                }
            }
            Err(err) => {
                let err_msg = format!("network error: {err}");
                Self::evaluate_response(Err(&err_msg), self.fail_open)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scanner_verdict_allow() {
        let payload = json!({
            "action": "allow",
            "summary": "safe command"
        });
        let verdict = TirithScanner::evaluate_response(Ok(&payload), true);
        assert_eq!(verdict, ScannerVerdict::Allow);
        assert!(verdict.is_allowed());

        let verdict_fail_closed = TirithScanner::evaluate_response(Ok(&payload), false);
        assert_eq!(verdict_fail_closed, ScannerVerdict::Allow);
    }

    #[test]
    fn test_scanner_verdict_deny_and_block() {
        let payload_deny = json!({
            "action": "deny",
            "summary": "reverse shell payload detected"
        });
        let verdict_deny = TirithScanner::evaluate_response(Ok(&payload_deny), true);
        assert_eq!(
            verdict_deny,
            ScannerVerdict::Deny {
                reason: "reverse shell payload detected".to_string()
            }
        );
        assert!(!verdict_deny.is_allowed());

        let payload_findings = json!({
            "action": "block",
            "findings": [
                {"title": "Exfiltration", "description": "curls sensitive tokens"}
            ]
        });
        let verdict_block = TirithScanner::evaluate_response(Ok(&payload_findings), false);
        assert!(matches!(verdict_block, ScannerVerdict::Deny { .. }));
        if let ScannerVerdict::Deny { reason } = verdict_block {
            assert!(reason.contains("Exfiltration: curls sensitive tokens"));
        }
    }

    #[test]
    fn test_scanner_fail_open_vs_fail_closed_on_error() {
        // 1. Fail open: network/timeout error -> Allow
        let verdict_open = TirithScanner::evaluate_response(Err("connection timed out"), true);
        assert_eq!(verdict_open, ScannerVerdict::Allow);

        // 2. Fail closed: network/timeout error -> Deny
        let verdict_closed = TirithScanner::evaluate_response(Err("connection timed out"), false);
        assert!(matches!(verdict_closed, ScannerVerdict::Deny { .. }));
        if let ScannerVerdict::Deny { reason } = verdict_closed {
            assert!(reason.contains("connection timed out"));
        }
    }
}
