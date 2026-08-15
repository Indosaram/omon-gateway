use std::io::ErrorKind;
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

    fn canonical_root(&self) -> Result<PathBuf, OmonError> {
        std::fs::canonicalize(&self.root).map_err(tool_error)
    }

    fn relative_path(value: &str) -> Result<&Path, OmonError> {
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
        Ok(relative)
    }

    async fn read(&self, args: &Value) -> Result<Value, OmonError> {
        let path = self.checked_existing(required(args, "path")?)?;
        let metadata = std::fs::symlink_metadata(&path).map_err(tool_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(OmonError::ToolExecution(
                "read path must resolve to a regular file inside tool root".into(),
            ));
        }
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
        let relative = Self::relative_path(required(args, "path")?)?;
        let content = required(args, "content")?;
        let root = self.canonical_root()?;
        let path = root.join(relative);
        let parent = path
            .parent()
            .ok_or_else(|| OmonError::ToolExecution("invalid write path".into()))?;
        self.create_checked_directories(&root, parent).await?;

        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(OmonError::ToolExecution(
                    "refusing to write through a symbolic link".into(),
                ));
            }
            Ok(metadata) if metadata.is_dir() => {
                return Err(OmonError::ToolExecution(
                    "write path resolves to a directory".into(),
                ));
            }
            Ok(_) => {
                self.ensure_inside_root(&path)?;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(tool_error(error)),
        }

        fs::write(&path, content).await.map_err(tool_error)?;
        let written = self.ensure_inside_root(&path)?;
        let metadata = std::fs::symlink_metadata(&written).map_err(tool_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(OmonError::ToolExecution(
                "written path is not a regular file inside tool root".into(),
            ));
        }
        Ok(json!({"path": relative_string(&root, &written), "bytes_written": content.len()}))
    }

    async fn create_checked_directories(
        &self,
        root: &Path,
        parent: &Path,
    ) -> Result<(), OmonError> {
        let relative = parent
            .strip_prefix(root)
            .map_err(|_| OmonError::ToolExecution("path escapes tool root".into()))?;
        let mut current = root.to_path_buf();
        for component in relative.components() {
            match component {
                Component::CurDir => continue,
                Component::Normal(name) => current.push(name),
                _ => {
                    return Err(OmonError::ToolExecution("path escapes tool root".into()));
                }
            }
            match std::fs::symlink_metadata(&current) {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        return Err(OmonError::ToolExecution(
                            "write path contains a non-directory or symbolic link".into(),
                        ));
                    }
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    fs::create_dir(&current).await.map_err(tool_error)?;
                    let metadata = std::fs::symlink_metadata(&current).map_err(tool_error)?;
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        return Err(OmonError::ToolExecution(
                            "created path is not a directory inside tool root".into(),
                        ));
                    }
                }
                Err(error) => return Err(tool_error(error)),
            }
            let canonical = std::fs::canonicalize(&current).map_err(tool_error)?;
            if !canonical.starts_with(root) {
                return Err(OmonError::ToolExecution("path escapes tool root".into()));
            }
        }
        Ok(())
    }

    async fn list(&self, args: &Value) -> Result<Value, OmonError> {
        let path =
            self.checked_existing(args.get("path").and_then(Value::as_str).unwrap_or("."))?;
        let mut reader = fs::read_dir(&path).await.map_err(tool_error)?;
        let root = self.canonical_root()?;
        let mut entries = Vec::new();
        while let Some(entry) = reader.next_entry().await.map_err(tool_error)? {
            let metadata = entry.metadata().await.map_err(tool_error)?;
            entries.push(json!({
                "name": entry.file_name().to_string_lossy(),
                "path": relative_string(&root, &entry.path()),
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
        let root = self.canonical_root()?;
        let limit = self.max_search_results;
        let matches =
            tokio::task::spawn_blocking(move || search_files(&root, &start, &query, limit))
                .await
                .map_err(|error| OmonError::ToolExecution(error.to_string()))??;
        Ok(json!({"matches": matches}))
    }

    fn checked_existing(&self, value: &str) -> Result<PathBuf, OmonError> {
        let relative = Self::relative_path(value)?;
        let root = self.canonical_root()?;
        let path = std::fs::canonicalize(root.join(relative)).map_err(tool_error)?;
        if !path.starts_with(&root) {
            return Err(OmonError::ToolExecution("path escapes tool root".into()));
        }
        Ok(path)
    }

    fn ensure_inside_root(&self, path: &Path) -> Result<PathBuf, OmonError> {
        let root = self.canonical_root()?;
        let path = std::fs::canonicalize(path).map_err(tool_error)?;
        if !path.starts_with(&root) {
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
                let path = entry.map_err(tool_error)?.path();
                if path.starts_with(root) {
                    pending.push(path);
                }
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
