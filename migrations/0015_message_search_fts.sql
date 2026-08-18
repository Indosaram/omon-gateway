CREATE TABLE IF NOT EXISTS message_search_documents (
    platform TEXT NOT NULL,
    guild_id TEXT,
    channel_id TEXT NOT NULL,
    thread_id TEXT,
    message_id TEXT NOT NULL,
    author_id TEXT NOT NULL DEFAULT '',
    author_name TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    attachment_names TEXT NOT NULL DEFAULT '',
    timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (platform, channel_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_message_search_documents_scope_time
    ON message_search_documents(platform, channel_id, timestamp DESC);

CREATE VIRTUAL TABLE IF NOT EXISTS message_search_fts USING fts5(
    content,
    author_name,
    attachment_names,
    content='message_search_documents',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS message_search_documents_ai AFTER INSERT ON message_search_documents BEGIN
    INSERT INTO message_search_fts(rowid, content, author_name, attachment_names)
    VALUES (new.rowid, new.content, new.author_name, new.attachment_names);
END;

CREATE TRIGGER IF NOT EXISTS message_search_documents_ad AFTER DELETE ON message_search_documents BEGIN
    INSERT INTO message_search_fts(message_search_fts, rowid, content, author_name, attachment_names)
    VALUES ('delete', old.rowid, old.content, old.author_name, old.attachment_names);
END;

CREATE TRIGGER IF NOT EXISTS message_search_documents_au AFTER UPDATE ON message_search_documents BEGIN
    INSERT INTO message_search_fts(message_search_fts, rowid, content, author_name, attachment_names)
    VALUES ('delete', old.rowid, old.content, old.author_name, old.attachment_names);
    INSERT INTO message_search_fts(rowid, content, author_name, attachment_names)
    VALUES (new.rowid, new.content, new.author_name, new.attachment_names);
END;

-- Keep future persisted inbound messages searchable without coupling the
-- multiplexer actor to a particular search implementation. Richer provider
-- metadata can overwrite this minimal transcript document later.
CREATE TRIGGER IF NOT EXISTS message_search_transcript_ai
AFTER INSERT ON messages
WHEN new.platform_message_id IS NOT NULL AND trim(new.platform_message_id) <> ''
BEGIN
    INSERT OR IGNORE INTO message_search_documents (
        platform, guild_id, channel_id, thread_id, message_id,
        author_id, author_name, content, attachment_names, timestamp, metadata_json
    )
    SELECT
        s.platform,
        s.guild_id,
        s.channel_id,
        s.thread_id,
        new.platform_message_id,
        s.user_id,
        new.role,
        new.content,
        '',
        new.created_at,
        json_object('source', 'transcript', 'role', new.role)
    FROM sessions s
    WHERE s.session_key = new.session_key;
END;

-- Seed the index from already persisted inbound platform messages. Richer Discord
-- metadata is filled opportunistically by message_context REST backfill later.
INSERT OR IGNORE INTO message_search_documents (
    platform, guild_id, channel_id, thread_id, message_id,
    author_id, author_name, content, attachment_names, timestamp, metadata_json
)
SELECT
    s.platform,
    s.guild_id,
    s.channel_id,
    s.thread_id,
    m.platform_message_id,
    s.user_id,
    m.role,
    m.content,
    '',
    m.created_at,
    json_object('source', 'transcript', 'role', m.role)
FROM messages m
JOIN sessions s ON s.session_key = m.session_key
WHERE m.platform_message_id IS NOT NULL AND trim(m.platform_message_id) <> '';
