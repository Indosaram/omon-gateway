use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{MessageAttachment, OmonError, Result};

pub const DISCORD_ATTACHMENT_MAX_BYTES: u64 = 25 * 1024 * 1024;
pub const DISCORD_ATTACHMENT_TIMEOUT: Duration = Duration::from_secs(15);
const ATTACHMENT_DIR: &str = ".discord-attachments";

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
        attachment.local_path = Some(path);
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
