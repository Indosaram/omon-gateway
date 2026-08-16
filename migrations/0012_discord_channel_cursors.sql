CREATE TABLE IF NOT EXISTS discord_channel_cursors (
    channel_id TEXT PRIMARY KEY,
    last_message_id TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
