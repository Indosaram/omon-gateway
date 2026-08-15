use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::process::Command;

use super::Tool;
use crate::OmonError;

#[derive(Clone, Debug)]
pub struct TerminalTool {
    root: PathBuf,
    timeout: Duration,
    max_output_bytes: usize,
}

impl TerminalTool {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            timeout: Duration::from_secs(600),
            max_output_bytes: 50 * 1024 * 1024,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_max_output_bytes(mut self, max: usize) -> Self {
        self.max_output_bytes = max;
        self
    }

    fn canonical_root(&self) -> Result<PathBuf, OmonError> {
        canonical(&self.root)
    }

    fn working_directory(&self, requested: Option<&str>) -> Result<PathBuf, OmonError> {
        let root = self.canonical_root()?;
        let requested = requested.map(Path::new).unwrap_or_else(|| Path::new("."));
        ensure_safe_relative(requested, "working directory")?;
        let path = canonical(&root.join(requested))?;
        if !path.starts_with(&root) || !path.is_dir() {
            return Err(OmonError::ToolExecution(
                "working directory escapes tool root".into(),
            ));
        }
        Ok(path)
    }

    fn executable(&self, program: &str, cwd: &Path) -> Result<PathBufOrName, OmonError> {
        let path = Path::new(program);
        let has_path_syntax = path.components().count() > 1
            || path.is_absolute()
            || program.contains(std::path::MAIN_SEPARATOR);
        if !has_path_syntax {
            return Ok(PathBufOrName::Name(program.to_owned()));
        }

        ensure_safe_relative(path, "program path")?;
        let root = self.canonical_root()?;
        let executable = canonical(&cwd.join(path))?;
        if !executable.starts_with(&root) || !executable.is_file() {
            return Err(OmonError::ToolExecution(
                "program path escapes tool root".into(),
            ));
        }
        Ok(PathBufOrName::Path(executable))
    }
}

enum PathBufOrName {
    Path(PathBuf),
    Name(String),
}

impl PathBufOrName {
    fn as_os_str(&self) -> &std::ffi::OsStr {
        match self {
            Self::Path(path) => path.as_os_str(),
            Self::Name(name) => std::ffi::OsStr::new(name),
        }
    }
}

#[async_trait]
impl Tool for TerminalTool {
    fn name(&self) -> &str {
        "terminal"
    }

    fn description(&self) -> &str {
        "Execute a process with arguments inside the configured workspace"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "program": {"type": "string"},
                "args": {"type": "array", "items": {"type": "string"}},
                "cwd": {"type": "string"},
                "env": {"type": "object", "additionalProperties": {"type": "string"}}
            },
            "required": ["program"]
        })
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        let program = required_string(&args, "program")?;
        let process_args: Vec<&str> = args
            .get("args")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| OmonError::ToolExecution("terminal args must be strings".into()))
            })
            .collect::<Result<_, _>>()?;
        let cwd = self.working_directory(args.get("cwd").and_then(Value::as_str))?;
        let executable = self.executable(program, &cwd)?;
        let mut command = Command::new(executable.as_os_str());
        command
            .args(process_args)
            .current_dir(cwd)
            .kill_on_drop(true);
        if let Some(env) = args.get("env").and_then(Value::as_object) {
            for (key, value) in env {
                let value = value.as_str().ok_or_else(|| {
                    OmonError::ToolExecution("terminal env values must be strings".into())
                })?;
                command.env(key, value);
            }
        }
        let output = tokio::time::timeout(self.timeout, command.output())
            .await
            .map_err(|_| {
                OmonError::ToolExecution(format!("process timed out after {:?}", self.timeout))
            })?
            .map_err(|error| OmonError::ToolExecution(error.to_string()))?;
        let (stdout, stdout_truncated) = capture(&output.stdout, self.max_output_bytes);
        let (stderr, stderr_truncated) = capture(&output.stderr, self.max_output_bytes);
        Ok(json!({
            "success": output.status.success(),
            "exit_code": output.status.code(),
            "stdout": stdout,
            "stderr": stderr,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated
        }))
    }
}

fn required_string<'a>(args: &'a Value, key: &str) -> Result<&'a str, OmonError> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| OmonError::ToolExecution(format!("missing string argument: {key}")))
}

fn ensure_safe_relative(path: &Path, kind: &str) -> Result<(), OmonError> {
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(OmonError::ToolExecution(format!(
            "{kind} escapes tool root"
        )));
    }
    Ok(())
}

fn canonical(path: &Path) -> Result<PathBuf, OmonError> {
    std::fs::canonicalize(path).map_err(|error| OmonError::ToolExecution(error.to_string()))
}

fn capture(bytes: &[u8], limit: usize) -> (String, bool) {
    let truncated = bytes.len() > limit;
    let bytes = &bytes[..bytes.len().min(limit)];
    (String::from_utf8_lossy(bytes).into_owned(), truncated)
}
