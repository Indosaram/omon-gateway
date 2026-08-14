use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::fs;

use super::Tool;
use crate::OmonError;

#[derive(Clone, Debug)]
pub struct FileTool {
    root: PathBuf,
    max_read_bytes: usize,
    max_search_results: usize,
}

impl FileTool {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_read_bytes: 1024 * 1024,
            max_search_results: 100,
        }
    }

    fn path(&self, value: &str) -> Result<PathBuf, OmonError> {
        let relative = Path::new(value);
        if relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(OmonError::ToolExecution("path escapes tool root".into()));
        }
        Ok(self.root.join(relative))
    }

    async fn read(&self, args: &Value) -> Result<Value, OmonError> {
        let path = self.checked_existing(required(args, "path")?)?;
        let bytes = fs::read(path).await.map_err(tool_error)?;
        if bytes.len() > self.max_read_bytes {
            return Err(OmonError::ToolExecution(format!(
                "file exceeds {} byte read limit",
                self.max_read_bytes
            )));
        }
        let content = String::from_utf8(bytes)
            .map_err(|_| OmonError::ToolExecution("file is not valid UTF-8".into()))?;
        Ok(json!({"content": content}))
    }

    async fn write(&self, args: &Value) -> Result<Value, OmonError> {
        let path = self.path(required(args, "path")?)?;
        let content = required(args, "content")?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(tool_error)?;
            self.ensure_inside_root(parent)?;
        }
        if path.exists() {
            self.ensure_inside_root(&path)?;
        }
        fs::write(&path, content).await.map_err(tool_error)?;
        Ok(json!({"path": relative_string(&self.root, &path), "bytes_written": content.len()}))
    }

    async fn list(&self, args: &Value) -> Result<Value, OmonError> {
        let path =
            self.checked_existing(args.get("path").and_then(Value::as_str).unwrap_or("."))?;
        let mut reader = fs::read_dir(&path).await.map_err(tool_error)?;
        let mut entries = Vec::new();
        while let Some(entry) = reader.next_entry().await.map_err(tool_error)? {
            let metadata = entry.metadata().await.map_err(tool_error)?;
            entries.push(json!({
                "name": entry.file_name().to_string_lossy(),
                "path": relative_string(&self.root, &entry.path()),
                "is_dir": metadata.is_dir(),
                "size": metadata.len()
            }));
        }
        entries.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));
        Ok(json!({"entries": entries}))
    }

    async fn search(&self, args: &Value) -> Result<Value, OmonError> {
        let query = required(args, "query")?.to_owned();
        let start =
            self.checked_existing(args.get("path").and_then(Value::as_str).unwrap_or("."))?;
        let root = std::fs::canonicalize(&self.root).map_err(tool_error)?;
        let limit = self.max_search_results;
        let matches =
            tokio::task::spawn_blocking(move || search_files(&root, &start, &query, limit))
                .await
                .map_err(|error| OmonError::ToolExecution(error.to_string()))??;
        Ok(json!({"matches": matches}))
    }

    fn checked_existing(&self, value: &str) -> Result<PathBuf, OmonError> {
        let path = self.path(value)?;
        self.ensure_inside_root(&path)
    }

    fn ensure_inside_root(&self, path: &Path) -> Result<PathBuf, OmonError> {
        let root = std::fs::canonicalize(&self.root).map_err(tool_error)?;
        let path = std::fs::canonicalize(path).map_err(tool_error)?;
        if !path.starts_with(root) {
            return Err(OmonError::ToolExecution("path escapes tool root".into()));
        }
        Ok(path)
    }
}

#[async_trait]
impl Tool for FileTool {
    fn name(&self) -> &str {
        "file"
    }

    fn description(&self) -> &str {
        "Read, write, list, and search UTF-8 files inside the configured workspace"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "operation": {"enum": ["read", "write", "list", "search"]},
                "path": {"type": "string"},
                "content": {"type": "string"},
                "query": {"type": "string"}
            },
            "required": ["operation"]
        })
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        match required(&args, "operation")? {
            "read" => self.read(&args).await,
            "write" => self.write(&args).await,
            "list" => self.list(&args).await,
            "search" => self.search(&args).await,
            operation => Err(OmonError::ToolExecution(format!(
                "unsupported file operation: {operation}"
            ))),
        }
    }
}

fn search_files(
    root: &Path,
    start: &Path,
    query: &str,
    limit: usize,
) -> Result<Vec<Value>, OmonError> {
    let mut pending = vec![start.to_owned()];
    let mut matches = Vec::new();
    while let Some(path) = pending.pop() {
        let metadata = std::fs::symlink_metadata(&path).map_err(tool_error)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            for entry in std::fs::read_dir(path).map_err(tool_error)? {
                pending.push(entry.map_err(tool_error)?.path());
            }
            continue;
        }
        if metadata.len() > 1024 * 1024 {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        for (index, line) in content.lines().enumerate() {
            if line.contains(query) {
                matches.push(json!({
                    "path": relative_string(root, &path),
                    "line": index + 1,
                    "content": line
                }));
                if matches.len() >= limit {
                    return Ok(matches);
                }
            }
        }
    }
    matches.sort_by(|left, right| {
        left["path"]
            .as_str()
            .cmp(&right["path"].as_str())
            .then_with(|| left["line"].as_u64().cmp(&right["line"].as_u64()))
    });
    Ok(matches)
}

fn required<'a>(args: &'a Value, key: &str) -> Result<&'a str, OmonError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| OmonError::ToolExecution(format!("missing string argument: {key}")))
}

fn relative_string(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

fn tool_error(error: impl std::fmt::Display) -> OmonError {
    OmonError::ToolExecution(error.to_string())
}
