CREATE TABLE IF NOT EXISTS sessions (
    session_key TEXT PRIMARY KEY NOT NULL,
    platform TEXT NOT NULL,
    guild_id TEXT,
    channel_id TEXT NOT NULL,
    thread_id TEXT,
    user_id TEXT NOT NULL,
    state_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY NOT NULL,
    session_key TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_messages_session_created
    ON messages(session_key, created_at);

CREATE TABLE IF NOT EXISTS delivery_ledger (
    delivery_id TEXT PRIMARY KEY NOT NULL,
    session_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    status TEXT NOT NULL,
    platform_message_id TEXT,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_ledger_event
    ON delivery_ledger(session_key, event_id);

CREATE TABLE IF NOT EXISTS cron_jobs (
    id TEXT PRIMARY KEY NOT NULL,
    session_key TEXT,
    expression TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    next_run_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_next_run
    ON cron_jobs(enabled, next_run_at);

CREATE TABLE IF NOT EXISTS memories (
    id TEXT PRIMARY KEY NOT NULL,
    session_key TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_memories_session
    ON memories(session_key, created_at);
