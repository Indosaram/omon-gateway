use async_trait::async_trait;
use serde_json::{json, Value};

use super::Tool;
use crate::OmonError;

#[derive(Clone)]
pub struct BrowserTool {
    cdp_port: u16,
}

impl Default for BrowserTool {
    fn default() -> Self {
        Self { cdp_port: 9333 }
    }
}

impl BrowserTool {
    pub fn new(cdp_port: u16) -> Self {
        Self { cdp_port }
    }
}

#[async_trait]
impl Tool for BrowserTool {
    fn name(&self) -> &str {
        "browser"
    }

    fn description(&self) -> &str {
        "Control live web browser via CDP on port 9333. Actions: 'navigate' (open URL), 'snapshot' (get page title, URL, content preview), 'eval' (evaluate JavaScript in page), 'screenshot' (capture page state)."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["navigate", "snapshot", "eval", "screenshot"],
                    "description": "The browser action to perform."
                },
                "url": {
                    "type": "string",
                    "description": "URL to navigate to."
                },
                "script": {
                    "type": "string",
                    "description": "JavaScript code to evaluate on the page."
                }
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        let action = args
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| OmonError::ToolExecution("missing 'action'".into()))?;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| OmonError::ToolExecution(e.to_string()))?;

        let list_url = format!("http://127.0.0.1:{}/json/list", self.cdp_port);
        let resp = client.get(&list_url).send().await.map_err(|e| {
            OmonError::ToolExecution(format!("CDP not reachable on port {}: {e}", self.cdp_port))
        })?;
        if !resp.status().is_success() {
            return Err(OmonError::ToolExecution(format!(
                "CDP returned {} for page listing",
                resp.status()
            )));
        }

        let pages: Value = resp
            .json()
            .await
            .map_err(|e| OmonError::ToolExecution(format!("invalid CDP response: {e}")))?;

        match action {
            "snapshot" => Ok(json!({
                "cdp_port": self.cdp_port,
                "active_pages_count": pages.as_array().map(|a| a.len()).unwrap_or(0),
                "pages": pages
            })),
            "navigate" => {
                let url = args
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OmonError::ToolExecution("missing 'url'".into()))?;

                let parsed = reqwest::Url::parse(url)
                    .map_err(|error| OmonError::ToolExecution(format!("invalid URL: {error}")))?;
                if !matches!(parsed.scheme(), "http" | "https") {
                    return Err(OmonError::ToolExecution(
                        "browser navigation only supports http and https URLs".into(),
                    ));
                }
                let new_tab_url = format!(
                    "http://127.0.0.1:{}/json/new?{}",
                    self.cdp_port,
                    urlencoding::encode(parsed.as_str())
                );
                let new_resp = client.put(&new_tab_url).send().await.map_err(|e| {
                    OmonError::ToolExecution(format!("failed to open new tab: {e}"))
                })?;

                if !new_resp.status().is_success() {
                    return Err(OmonError::ToolExecution(format!(
                        "CDP returned {} while opening a tab",
                        new_resp.status()
                    )));
                }
                let tab_info: Value = new_resp.json().await.map_err(|error| {
                    OmonError::ToolExecution(format!("invalid CDP tab response: {error}"))
                })?;

                Ok(json!({
                    "status": "navigated",
                    "url": url,
                    "tab": tab_info
                }))
            }
            "eval" | "screenshot" => Err(OmonError::ToolExecution(format!(
                "browser action `{action}` requires a CDP WebSocket session and is not configured"
            ))),
            _ => Err(OmonError::ToolExecution(format!(
                "unknown browser action: {action}"
            ))),
        }
    }
}
