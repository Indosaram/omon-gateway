use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serenity::all::{ChannelId, CreateMessage, EditMessage, Http, MessageId};
use tokio::sync::Mutex;
use tokio::time::Instant;

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
            .edit_message(&self.http, message_id, EditMessage::new().content(content))
            .await?;
        Ok(())
    }

    async fn send_message(&self, channel_id: ChannelId, content: String) -> Result<MessageId> {
        let message = channel_id
            .send_message(&self.http, CreateMessage::new().content(content))
            .await?;
        Ok(message.id)
    }
}

struct LiveEditState {
    last_edit: Option<Instant>,
    message_ids: Vec<MessageId>,
}

/// Serializes and rate-limits updates for one streaming Discord response.
///
/// Updates inside the debounce window slide the deadline forward. The final
/// update bypasses the delay while still preserving edit ordering.
pub struct LiveEditThrottler<T: DiscordMessageTransport> {
    transport: Arc<T>,
    channel_id: ChannelId,
    initial_message_id: MessageId,
    debounce: Duration,
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
            initial_message_id: message_id,
            debounce,
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
        self.transport.start_typing(self.channel_id).await?;
        let mut state = self.state.lock().await;

        if !is_final {
            if let Some(last_edit) = state.last_edit {
                let deadline = last_edit + self.debounce;
                if deadline > Instant::now() {
                    tokio::time::sleep_until(deadline).await;
                }
            }
        }

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

        for message_id in state.message_ids.drain(chunks.len()..) {
            self.transport
                .edit_message(self.channel_id, message_id, "\u{200b}".into())
                .await?;
        }
        if state.message_ids.is_empty() {
            state.message_ids.push(self.initial_message_id);
        }
        state.last_edit = Some(Instant::now());
        Ok(())
    }
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
