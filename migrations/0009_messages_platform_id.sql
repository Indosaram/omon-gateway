-- Transcript-level deduplication for inbound user turns by platform_message_id.
ALTER TABLE messages ADD COLUMN platform_message_id TEXT;
CREATE INDEX IF NOT EXISTS idx_messages_session_platform_id
    ON messages(session_key, platform_message_id);
