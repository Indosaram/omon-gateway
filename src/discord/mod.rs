pub mod adapter;
pub mod approval;
pub mod attachments;
pub mod commands;
pub mod throttler;

pub use adapter::{
    coalesce_inbound_events, compose_reply_context, derive_auto_thread_name,
    extract_media_directives, format_channel_context, global_debouncer, is_authorized_clicker,
    is_discord_dead_target_error, is_silence_response, safe_allowed_mentions,
    should_chunk_reference, DeadTargetEntry, DeadTargetRegistry, DiscordAdapter, DiscordEgress,
    DiscordFileUploader, InboundFilterConfig, SerenityFileUploader, SplitMessageDebouncer,
    DEFAULT_CHANNEL_CONTEXT_LIMIT, DEFAULT_DEBOUNCE_DURATION, MAX_CHANNEL_CONTEXT_LIMIT,
    MAX_CONTEXT_LINE_CHARS, REFERENCED_CONTENT_CAP,
};
pub use approval::{
    approval_buttons, is_approval_custom_id, parse_custom_id, ApprovalDecision, ApprovalError,
    ApprovalPrompt, ApprovalRequester, DiscordApprovalRequester, SmartApprovalGuard,
};
pub use attachments::{
    is_text_attachment, AttachmentDownloader, DISCORD_ATTACHMENT_MAX_BYTES,
    DISCORD_ATTACHMENT_TIMEOUT, MAX_INLINED_ATTACHMENT_BYTES,
};
pub use commands::{is_user_authorized, GatewayStats, PoiseContext, PoiseData};
pub use throttler::{
    chunk_markdown, truncate_live_preview, DiscordMessageTransport, LiveEditThrottler,
};
