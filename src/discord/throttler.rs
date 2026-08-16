use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serenity::all::{ChannelId, CreateMessage, EditMessage, Http, MessageId};
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::adapter::safe_allowed_mentions;
use crate::Result;

pub const DISCORD_MESSAGE_LIMIT: usize = 2_000;
const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(800);

#[async_trait]
pub trait DiscordMessageTransport: Send + Sync + 'static {
    async fn start_typing(&self, channel_id: ChannelId) -> Result<()>;
    async fn edit_message(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        content: String,
    ) -> Result<()>;
    async fn send_message(&self, channel_id: ChannelId, content: String) -> Result<MessageId>;
    async fn delete_message(&self, channel_id: ChannelId, message_id: MessageId) -> Result<()>;
}

#[derive(Clone)]
pub struct SerenityMessageTransport {
    http: Arc<Http>,
}

impl SerenityMessageTransport {
    pub fn new(http: Arc<Http>) -> Self {
        Self { http }
    }
}

#[async_trait]
impl DiscordMessageTransport for SerenityMessageTransport {
    async fn start_typing(&self, channel_id: ChannelId) -> Result<()> {
        self.http.broadcast_typing(channel_id).await?;
        Ok(())
    }

    async fn edit_message(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        content: String,
    ) -> Result<()> {
        channel_id
            .edit_message(
                &self.http,
                message_id,
                EditMessage::new()
                    .content(content)
                    .allowed_mentions(safe_allowed_mentions()),
            )
            .await?;
        Ok(())
    }

    async fn send_message(&self, channel_id: ChannelId, content: String) -> Result<MessageId> {
        let message = channel_id
            .send_message(
                &self.http,
                CreateMessage::new()
                    .content(content)
                    .allowed_mentions(safe_allowed_mentions()),
            )
            .await?;
        Ok(message.id)
    }

    async fn delete_message(&self, channel_id: ChannelId, message_id: MessageId) -> Result<()> {
        channel_id.delete_message(&self.http, message_id).await?;
        Ok(())
    }
}

struct LiveEditState {
    last_edit: Option<Instant>,
    message_ids: Vec<MessageId>,
}

/// Serializes and rate-limits updates for one streaming Discord response.
///
/// Non-final updates are coalesced: when a newer update arrives during the
/// debounce window, the stale update returns without touching Discord. The
/// debounce sleep happens outside the state mutex, so a final update does not
/// queue behind a sleeping intermediate update. Network mutations remain
/// serialized to preserve message ordering and the chunk/message-id mapping.
pub struct LiveEditThrottler<T: DiscordMessageTransport> {
    transport: Arc<T>,
    channel_id: ChannelId,
    debounce: Duration,
    revision: AtomicU64,
    state: Mutex<LiveEditState>,
}

impl<T: DiscordMessageTransport> LiveEditThrottler<T> {
    pub fn new(transport: Arc<T>, channel_id: ChannelId, message_id: MessageId) -> Self {
        Self::with_debounce(transport, channel_id, message_id, DEFAULT_DEBOUNCE)
    }

    pub fn with_debounce(
        transport: Arc<T>,
        channel_id: ChannelId,
        message_id: MessageId,
        debounce: Duration,
    ) -> Self {
        Self {
            transport,
            channel_id,
            debounce,
            revision: AtomicU64::new(0),
            state: Mutex::new(LiveEditState {
                last_edit: None,
                message_ids: vec![message_id],
            }),
        }
    }

    pub fn debounce(&self) -> Duration {
        self.debounce
    }

    pub async fn update(&self, content: &str, is_final: bool) -> Result<()> {
        let revision = self.revision.fetch_add(1, Ordering::AcqRel) + 1;

        if !is_final && !self.wait_for_debounce(revision).await {
            return Ok(());
        }

        let mut state = self.state.lock().await;
        if !is_final && self.revision.load(Ordering::Acquire) != revision {
            return Ok(());
        }

        // A previous edit may have finished while this update was waiting for
        // the wire-serialization mutex. Re-check the deadline without holding
        // the mutex across the sleep.
        if !is_final {
            if let Some(last_edit) = state.last_edit {
                let deadline = last_edit + self.debounce;
                if deadline > Instant::now() {
                    drop(state);
                    tokio::time::sleep_until(deadline).await;
                    if self.revision.load(Ordering::Acquire) != revision {
                        return Ok(());
                    }
                    state = self.state.lock().await;
                    if self.revision.load(Ordering::Acquire) != revision {
                        return Ok(());
                    }
                }
            }
        }

        self.transport.start_typing(self.channel_id).await?;

        if !is_final {
            let preview = truncate_live_preview(content, DISCORD_MESSAGE_LIMIT);
            if let Some(&first_id) = state.message_ids.first() {
                self.transport
                    .edit_message(self.channel_id, first_id, preview)
                    .await?;
            }
        } else {
            let chunks = chunk_markdown(content, DISCORD_MESSAGE_LIMIT);
            for (index, chunk) in chunks.iter().enumerate() {
                if index < state.message_ids.len() {
                    self.transport
                        .edit_message(self.channel_id, state.message_ids[index], chunk.clone())
                        .await?;
                } else {
                    let message_id = self
                        .transport
                        .send_message(self.channel_id, chunk.clone())
                        .await?;
                    state.message_ids.push(message_id);
                }
            }

            let stale: Vec<_> = state.message_ids.drain(chunks.len()..).collect();
            for message_id in stale {
                self.transport
                    .delete_message(self.channel_id, message_id)
                    .await?;
            }
        }

        state.last_edit = Some(Instant::now());
        Ok(())
    }

    async fn wait_for_debounce(&self, revision: u64) -> bool {
        loop {
            if self.revision.load(Ordering::Acquire) != revision {
                return false;
            }
            let deadline = {
                let state = self.state.lock().await;
                state.last_edit.map(|last_edit| last_edit + self.debounce)
            };
            let Some(deadline) = deadline else {
                return self.revision.load(Ordering::Acquire) == revision;
            };
            if deadline <= Instant::now() {
                return self.revision.load(Ordering::Acquire) == revision;
            }
            tokio::time::sleep_until(deadline).await;
        }
    }
}

/// Truncates streamed live content for a single preview message up to `limit` characters,
/// appending a trailing indicator (` …`) when content exceeds the limit.
pub fn truncate_live_preview(content: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    if content.is_empty() {
        return "\u{200b}".into();
    }
    if content.chars().count() <= limit {
        return content.to_string();
    }
    let max_chars = limit.saturating_sub(2);
    let (prefix, _) = take_chars(content, max_chars);
    format!("{prefix} …")
}

/// Splits Markdown by Unicode characters while keeping fenced code valid in
/// every Discord message. A fence crossing a boundary is closed in the old
/// chunk and reopened with its original language tag in the next chunk.
pub fn chunk_markdown(content: &str, limit: usize) -> Vec<String> {
    if limit == 0 {
        return Vec::new();
    }
    if content.is_empty() {
        return vec!["\u{200b}".into()];
    }

    let mut chunks = Vec::new();
    let mut remaining = content;
    let mut reopen: Option<String> = None;

    while !remaining.is_empty() {
        let prefix = reopen
            .as_ref()
            .map(|language| format!("```{language}\n"))
            .unwrap_or_default();
        let closing = if reopen.is_some() { "\n```" } else { "" };
        let budget = limit.saturating_sub(prefix.chars().count() + closing.chars().count());
        if budget == 0 {
            chunks.push(take_chars(remaining, limit).0.to_owned());
            remaining = take_chars(remaining, limit).1;
            reopen = None;
            continue;
        }

        let (candidate, rest) = take_chars(remaining, budget);
        let split_at = if rest.is_empty() {
            candidate.len()
        } else {
            preferred_boundary(candidate).unwrap_or(candidate.len())
        };
        let piece = &remaining[..split_at];
        let mut fence = reopen.clone();
        scan_fences(piece, &mut fence);

        let suffix = if !rest.is_empty() && fence.is_some() {
            "\n```"
        } else {
            ""
        };
        chunks.push(format!("{prefix}{piece}{suffix}"));
        remaining = &remaining[split_at..];
        reopen = fence;
    }

    chunks
}

fn take_chars(value: &str, count: usize) -> (&str, &str) {
    let split = value
        .char_indices()
        .nth(count)
        .map(|(index, _)| index)
        .unwrap_or(value.len());
    value.split_at(split)
}

fn preferred_boundary(candidate: &str) -> Option<usize> {
    candidate
        .rmatch_indices('\n')
        .next()
        .filter(|(index, _)| *index > candidate.len() / 2)
        .map(|(index, _)| index + 1)
        .or_else(|| {
            candidate
                .rmatch_indices(char::is_whitespace)
                .next()
                .filter(|(index, _)| *index > candidate.len() / 2)
                .map(|(index, character)| index + character.len())
        })
}

fn scan_fences(text: &str, open: &mut Option<String>) {
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if let Some(after_fence) = trimmed.strip_prefix("```") {
            if open.is_some() {
                *open = None;
            } else {
                *open = Some(after_fence.trim().to_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_live_preview_empty_and_short() {
        assert_eq!(truncate_live_preview("", 2000), "\u{200b}");
        assert_eq!(truncate_live_preview("hello world", 2000), "hello world");
        assert_eq!(truncate_live_preview("hello", 0), "");
    }

    #[test]
    fn test_truncate_live_preview_oversized() {
        let content = "a".repeat(2500);
        let preview = truncate_live_preview(&content, 2000);
        assert_eq!(preview.chars().count(), 2000);
        assert!(preview.ends_with(" …"));
        assert_eq!(&preview[..1998], &"a".repeat(1998));
    }
}
