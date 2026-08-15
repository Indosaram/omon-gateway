-- Message causality must not depend on wall-clock timestamps or random UUIDs.
-- Rebuild the table with an explicit monotonic insertion sequence while
-- preserving the externally visible UUID id and all existing rows.
CREATE TABLE messages_v2 (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    session_key TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
);

INSERT INTO messages_v2 (id, session_key, role, content, metadata_json, created_at)
SELECT id, session_key, role, content, metadata_json, created_at
FROM messages
ORDER BY rowid;

DROP TABLE messages;
ALTER TABLE messages_v2 RENAME TO messages;

CREATE INDEX idx_messages_session_sequence ON messages(session_key, sequence);
CREATE INDEX idx_messages_session_created ON messages(session_key, created_at);
