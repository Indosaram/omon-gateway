use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;

use super::Tool;
use crate::OmonError;

#[derive(Clone, Debug)]
pub enum McpTransport {
    Stdio {
        program: String,
        args: Vec<String>,
        cwd: Option<PathBuf>,
    },
    Sse {
        url: String,
        bearer_token: Option<String>,
    },
}

#[derive(Clone)]
pub struct McpClientTool {
    name: String,
    remote_tool: String,
    description: String,
    input_schema: Value,
    transport: McpTransport,
    timeout: Duration,
    next_id: Arc<AtomicU64>,
    http: reqwest::Client,
    stdio_lock: Arc<Mutex<()>>,
}

impl McpClientTool {
    pub fn new(
        name: impl Into<String>,
        remote_tool: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        transport: McpTransport,
    ) -> Self {
        Self {
            name: name.into(),
            remote_tool: remote_tool.into(),
            description: description.into(),
            input_schema,
            transport,
            timeout: Duration::from_secs(30),
            next_id: Arc::new(AtomicU64::new(1)),
            http: reqwest::Client::new(),
            stdio_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn request(&self, params: Value) -> Result<Value, OmonError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {"name": self.remote_tool, "arguments": params}
        });
        let response = match &self.transport {
            McpTransport::Stdio { program, args, cwd } => {
                let _guard = self.stdio_lock.lock().await;
                self.request_stdio(program, args, cwd.as_ref(), &request)
                    .await?
            }
            McpTransport::Sse { url, bearer_token } => {
                self.request_sse(url, bearer_token.as_deref(), &request)
                    .await?
            }
        };
        decode_response(response, id)
    }

    async fn request_stdio(
        &self,
        program: &str,
        args: &[String],
        cwd: Option<&PathBuf>,
        request: &Value,
    ) -> Result<Value, OmonError> {
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        let mut child = command.spawn().map_err(mcp_error)?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| mcp_error("MCP stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| mcp_error("MCP stdout unavailable"))?;
        let encoded = serde_json::to_vec(request).map_err(mcp_error)?;
        stdin.write_all(&encoded).await.map_err(mcp_error)?;
        stdin.write_all(b"\n").await.map_err(mcp_error)?;
        stdin.shutdown().await.map_err(mcp_error)?;

        let read = async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Some(line) = lines.next_line().await.map_err(mcp_error)? {
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    return Ok(value);
                }
            }
            Err(mcp_error("MCP server closed without a JSON-RPC response"))
        };
        tokio::time::timeout(self.timeout, read)
            .await
            .map_err(|_| mcp_error("MCP stdio request timed out"))?
    }

    async fn request_sse(
        &self,
        url: &str,
        bearer_token: Option<&str>,
        request: &Value,
    ) -> Result<Value, OmonError> {
        let mut builder = self
            .http
            .post(url)
            .header("accept", "application/json, text/event-stream")
            .json(request);
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = tokio::time::timeout(self.timeout, builder.send())
            .await
            .map_err(|_| mcp_error("MCP HTTP request timed out"))?
            .map_err(mcp_error)?;
        if !response.status().is_success() {
            return Err(mcp_error(format!(
                "MCP server returned {}",
                response.status()
            )));
        }
        if response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("text/event-stream"))
        {
            return parse_sse_response(response, self.timeout).await;
        }
        response.json().await.map_err(mcp_error)
    }
}

#[derive(Clone, Default)]
pub struct McpTool {
    clients: Vec<McpClientTool>,
}

impl McpTool {
    pub fn new(clients: Vec<McpClientTool>) -> Self {
        Self { clients }
    }
}

#[async_trait]
impl Tool for McpTool {
    fn name(&self) -> &str {
        "mcp"
    }

    fn description(&self) -> &str {
        "Call a configured Model Context Protocol tool"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "tool": {"type": "string"},
                "arguments": {"type": "object"}
            },
            "required": ["tool"]
        })
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        let name = args
            .get("tool")
            .and_then(Value::as_str)
            .ok_or_else(|| mcp_error("missing string argument: tool"))?;
        let client = self
            .clients
            .iter()
            .find(|client| client.name() == name)
            .ok_or_else(|| mcp_error(format!("unknown MCP tool: {name}")))?;
        client
            .execute(args.get("arguments").cloned().unwrap_or_else(|| json!({})))
            .await
    }
}

#[async_trait]
impl Tool for McpClientTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn input_schema(&self) -> Value {
        self.input_schema.clone()
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        self.request(args).await
    }
}

async fn parse_sse_response(
    response: reqwest::Response,
    timeout: Duration,
) -> Result<Value, OmonError> {
    let read = async move {
        let mut stream = response.bytes_stream();
        let mut buffer = String::new();
        while let Some(chunk) = stream.next().await {
            buffer.push_str(&String::from_utf8_lossy(&chunk.map_err(mcp_error)?));
            while let Some(index) = buffer.find('\n') {
                let line = buffer[..index].trim_end_matches('\r').to_owned();
                buffer.drain(..=index);
                if let Some(data) = line.strip_prefix("data:").map(str::trim) {
                    if let Ok(value) = serde_json::from_str::<Value>(data) {
                        return Ok(value);
                    }
                }
            }
        }
        Err(mcp_error(
            "MCP SSE stream closed without a JSON-RPC response",
        ))
    };
    tokio::time::timeout(timeout, read)
        .await
        .map_err(|_| mcp_error("MCP SSE response timed out"))?
}

fn decode_response(response: Value, id: u64) -> Result<Value, OmonError> {
    if response.get("id").and_then(Value::as_u64) != Some(id) {
        return Err(mcp_error("MCP response ID did not match request"));
    }
    if let Some(error) = response.get("error") {
        return Err(mcp_error(format!("MCP JSON-RPC error: {error}")));
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| mcp_error("MCP response omitted result"))
}

fn mcp_error(error: impl std::fmt::Display) -> OmonError {
    OmonError::ToolExecution(error.to_string())
}
