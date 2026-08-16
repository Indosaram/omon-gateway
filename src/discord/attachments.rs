use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{MessageAttachment, OmonError, Result};

pub const DISCORD_ATTACHMENT_MAX_BYTES: u64 = 25 * 1024 * 1024;
pub const DISCORD_ATTACHMENT_TIMEOUT: Duration = Duration::from_secs(15);
pub const MAX_INLINED_ATTACHMENT_BYTES: u64 = 100 * 1024;
const ATTACHMENT_DIR: &str = ".discord-attachments";

/// Determines whether an inbound attachment is a text or code document
/// that should have its content inlined directly into the prompt context.
pub fn is_text_attachment(filename: &str, content_type: Option<&str>) -> bool {
    if let Some(ct) = content_type {
        let ct_lower = ct.to_ascii_lowercase();
        let mime = ct_lower.split(';').next().unwrap_or("").trim();
        if mime.starts_with("text/")
            || mime == "application/json"
            || mime == "application/xml"
            || mime == "application/javascript"
            || mime == "application/x-javascript"
            || mime == "application/typescript"
            || mime == "application/x-typescript"
            || mime == "application/x-sh"
            || mime == "application/x-bash"
            || mime == "application/x-shellscript"
            || mime == "application/x-yaml"
            || mime == "application/yaml"
            || mime == "application/x-toml"
            || mime == "application/toml"
            || mime == "application/sql"
            || mime == "application/graphql"
            || mime == "application/x-httpd-php"
            || mime.ends_with("+json")
            || mime.ends_with("+xml")
            || mime.ends_with("+yaml")
        {
            return true;
        }
    }

    let path = Path::new(filename);
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext_lower = ext.to_ascii_lowercase();
        match ext_lower.as_str() {
            "txt" | "md" | "markdown" | "csv" | "tsv" | "log" | "json" | "jsonl" | "ndjson"
            | "xml" | "yaml" | "yml" | "toml" | "ini" | "cfg" | "conf" | "env" | "properties"
            | "html" | "htm" | "css" | "scss" | "sass" | "less" | "py" | "pyi" | "js" | "mjs"
            | "cjs" | "ts" | "tsx" | "jsx" | "sh" | "bash" | "zsh" | "fish" | "ps1" | "bat"
            | "c" | "h" | "cpp" | "cc" | "hpp" | "cs" | "java" | "kt" | "go" | "rs" | "rb"
            | "php" | "pl" | "lua" | "r" | "jl" | "swift" | "m" | "scala" | "clj" | "ex"
            | "exs" | "erl" | "sql" | "graphql" | "proto" | "tf" | "hcl" | "dockerfile"
            | "makefile" | "cmake" | "gradle" | "rst" | "tex" | "srt" | "vtt" | "diff"
            | "patch" => return true,
            _ => {}
        }
    }

    if let Some(file_stem) = path.file_name().and_then(|s| s.to_str()) {
        let stem_lower = file_stem.to_ascii_lowercase();
        if stem_lower == "dockerfile"
            || stem_lower == "makefile"
            || stem_lower == "gemfile"
            || stem_lower == "licence"
            || stem_lower == "license"
            || stem_lower == ".env"
            || stem_lower == ".gitignore"
            || stem_lower == "cargo.lock"
        {
            return true;
        }
    }

    false
}

#[derive(Clone)]
pub struct AttachmentDownloader {
    workspace_root: PathBuf,
    attachment_root: PathBuf,
    http: reqwest::Client,
}

impl AttachmentDownloader {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(DISCORD_ATTACHMENT_TIMEOUT)
            .build()
            .map_err(|error| attachment_error(format!("failed to build HTTP client: {error}")))?;
        Self::with_client(workspace_root, http)
    }

    pub fn with_client(workspace_root: impl AsRef<Path>, http: reqwest::Client) -> Result<Self> {
        let workspace_root = workspace_root.as_ref();
        std::fs::create_dir_all(workspace_root).map_err(|error| {
            attachment_error(format!(
                "failed to create workspace {}: {error}",
                workspace_root.display()
            ))
        })?;
        let workspace_root = std::fs::canonicalize(workspace_root).map_err(|error| {
            attachment_error(format!(
                "failed to resolve workspace {}: {error}",
                workspace_root.display()
            ))
        })?;
        let attachment_root = workspace_root.join(ATTACHMENT_DIR);
        std::fs::create_dir_all(&attachment_root).map_err(|error| {
            attachment_error(format!(
                "failed to create attachment cache {}: {error}",
                attachment_root.display()
            ))
        })?;
        let attachment_root = std::fs::canonicalize(&attachment_root).map_err(|error| {
            attachment_error(format!(
                "failed to resolve attachment cache {}: {error}",
                attachment_root.display()
            ))
        })?;
        if !attachment_root.starts_with(&workspace_root) {
            return Err(attachment_error(format!(
                "attachment cache escapes workspace: {}",
                attachment_root.display()
            )));
        }

        Ok(Self {
            workspace_root,
            attachment_root,
            http,
        })
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn attachment_root(&self) -> &Path {
        &self.attachment_root
    }

    pub async fn hydrate(&self, attachment: &mut MessageAttachment) -> Result<()> {
        let path = self.download(attachment).await?;
        attachment.local_path = Some(path.clone());
        if is_text_attachment(&attachment.filename, attachment.content_type.as_deref()) {
            let size = attachment.size_bytes.unwrap_or(0);
            if size <= MAX_INLINED_ATTACHMENT_BYTES {
                if let Ok(metadata) = tokio::fs::metadata(&path).await {
                    if metadata.len() <= MAX_INLINED_ATTACHMENT_BYTES {
                        if let Ok(bytes) = tokio::fs::read(&path).await {
                            let text = String::from_utf8_lossy(&bytes).into_owned();
                            attachment.text_content = Some(text);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn download_attachment(&self, attachment: &MessageAttachment) -> Result<PathBuf> {
        self.download(attachment).await
    }

    pub async fn download(&self, attachment: &MessageAttachment) -> Result<PathBuf> {
        if attachment
            .size_bytes
            .is_some_and(|size| size > DISCORD_ATTACHMENT_MAX_BYTES)
        {
            return Err(attachment_error(format!(
                "attachment {} exceeds the 25 MB limit",
                attachment.filename
            )));
        }

        self.verify_cache_root().await?;
        let target = self.target_path(attachment)?;
        if let Some(cached) = self.cached_path(&target, attachment.size_bytes).await? {
            return Ok(cached);
        }

        let response = self
            .http
            .get(&attachment.url)
            .send()
            .await
            .map_err(|error| attachment_error(format!("download failed: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(attachment_error(format!(
                "download returned HTTP {status} for {}",
                attachment.filename
            )));
        }
        if response
            .content_length()
            .is_some_and(|size| size > DISCORD_ATTACHMENT_MAX_BYTES)
        {
            return Err(attachment_error(format!(
                "attachment {} exceeds the 25 MB limit",
                attachment.filename
            )));
        }

        let temp = self
            .attachment_root
            .join(format!(".{}.part", Uuid::new_v4()));
        let result = self.stream_to_file(response, &temp).await;
        if let Err(error) = result {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(error);
        }

        if let Err(error) = tokio::fs::rename(&temp, &target).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(attachment_error(format!(
                "failed to cache attachment at {}: {error}",
                target.display()
            )));
        }
        self.ensure_contained_file(&target).await?;
        Ok(target)
    }

    async fn stream_to_file(&self, response: reqwest::Response, path: &Path) -> Result<()> {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .await
            .map_err(|error| {
                attachment_error(format!(
                    "failed to create attachment cache file {}: {error}",
                    path.display()
                ))
            })?;
        let mut total = 0_u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|error| attachment_error(format!("download stream failed: {error}")))?;
            total = total.saturating_add(chunk.len() as u64);
            if total > DISCORD_ATTACHMENT_MAX_BYTES {
                return Err(attachment_error("attachment exceeds the 25 MB limit"));
            }
            file.write_all(&chunk).await.map_err(|error| {
                attachment_error(format!(
                    "failed writing attachment cache file {}: {error}",
                    path.display()
                ))
            })?;
        }
        file.flush().await.map_err(|error| {
            attachment_error(format!(
                "failed flushing attachment cache file {}: {error}",
                path.display()
            ))
        })?;
        Ok(())
    }

    async fn verify_cache_root(&self) -> Result<()> {
        let root = tokio::fs::canonicalize(&self.attachment_root)
            .await
            .map_err(|error| {
                attachment_error(format!(
                    "failed to resolve attachment cache {}: {error}",
                    self.attachment_root.display()
                ))
            })?;
        if root != self.attachment_root || !root.starts_with(&self.workspace_root) {
            return Err(attachment_error(format!(
                "attachment cache escapes workspace: {}",
                root.display()
            )));
        }
        Ok(())
    }

    fn target_path(&self, attachment: &MessageAttachment) -> Result<PathBuf> {
        let filename = Path::new(&attachment.filename)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("attachment");
        let id = sanitize_component(&attachment.id, "attachment");
        let filename = sanitize_component(filename, "attachment");
        let target = self.attachment_root.join(format!("{id}-{filename}"));
        if target.parent() != Some(self.attachment_root.as_path()) {
            return Err(attachment_error("attachment path escapes workspace"));
        }
        Ok(target)
    }

    async fn cached_path(
        &self,
        path: &Path,
        expected_size: Option<u64>,
    ) -> Result<Option<PathBuf>> {
        let metadata = match tokio::fs::symlink_metadata(path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(attachment_error(format!(
                    "failed to inspect cached attachment {}: {error}",
                    path.display()
                )))
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(attachment_error(format!(
                "cached attachment is not a regular file: {}",
                path.display()
            )));
        }
        if metadata.len() > DISCORD_ATTACHMENT_MAX_BYTES
            || expected_size.is_some_and(|size| metadata.len() != size)
        {
            tokio::fs::remove_file(path).await.map_err(|error| {
                attachment_error(format!(
                    "failed to replace stale attachment cache {}: {error}",
                    path.display()
                ))
            })?;
            return Ok(None);
        }
        self.ensure_contained_file(path).await?;
        Ok(Some(path.to_owned()))
    }

    async fn ensure_contained_file(&self, path: &Path) -> Result<()> {
        let path = tokio::fs::canonicalize(path).await.map_err(|error| {
            attachment_error(format!(
                "failed to resolve cached attachment {}: {error}",
                path.display()
            ))
        })?;
        if !path.starts_with(&self.attachment_root) || !path.starts_with(&self.workspace_root) {
            return Err(attachment_error(format!(
                "attachment path escapes workspace: {}",
                path.display()
            )));
        }
        Ok(())
    }
}

fn sanitize_component(value: &str, fallback: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let sanitized = sanitized.trim_matches('.');
    if sanitized.is_empty() {
        fallback.to_owned()
    } else {
        sanitized.to_owned()
    }
}

fn attachment_error(message: impl Into<String>) -> OmonError {
    OmonError::Config(format!("Discord attachment error: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_text_attachment_by_content_type() {
        assert!(is_text_attachment("unknown_file", Some("text/plain")));
        assert!(is_text_attachment("file.bin", Some("text/x-rust")));
        assert!(is_text_attachment("data", Some("application/json")));
        assert!(is_text_attachment(
            "data",
            Some("application/problem+json; charset=utf-8")
        ));
        assert!(is_text_attachment("build", Some("application/xml")));
        assert!(is_text_attachment("script", Some("application/javascript")));
        assert!(is_text_attachment("script", Some("application/x-sh")));
        assert!(is_text_attachment("config", Some("application/x-yaml")));
        assert!(is_text_attachment("config", Some("application/toml")));

        // Binary types must return false
        assert!(!is_text_attachment("photo.png", Some("image/png")));
        assert!(!is_text_attachment("doc.pdf", Some("application/pdf")));
        assert!(!is_text_attachment("archive.zip", Some("application/zip")));
        assert!(!is_text_attachment(
            "binary.bin",
            Some("application/octet-stream")
        ));
    }

    #[test]
    fn test_is_text_attachment_by_extension_and_name() {
        assert!(is_text_attachment("main.rs", None));
        assert!(is_text_attachment(
            "script.py",
            Some("application/octet-stream")
        ));
        assert!(is_text_attachment("app.ts", None));
        assert!(is_text_attachment("index.js", None));
        assert!(is_text_attachment("README.md", None));
        assert!(is_text_attachment("data.csv", None));
        assert!(is_text_attachment("Cargo.toml", None));
        assert!(is_text_attachment("config.yaml", None));
        assert!(is_text_attachment("config.yml", None));
        assert!(is_text_attachment("run.sh", None));
        assert!(is_text_attachment("Dockerfile", None));
        assert!(is_text_attachment("Makefile", None));
        assert!(is_text_attachment(".env", None));
        assert!(is_text_attachment(".gitignore", None));

        // Binary extensions must return false
        assert!(!is_text_attachment("photo.jpg", None));
        assert!(!is_text_attachment("document.pdf", None));
        assert!(!is_text_attachment("archive.tar.gz", None));
        assert!(!is_text_attachment("audio.mp3", None));
    }
}
