CREATE TABLE IF NOT EXISTS delivery_obligations (
    id TEXT PRIMARY KEY NOT NULL,
    session_key TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    thread_id TEXT,
    content TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'attempting', 'delivered', 'failed', 'abandoned')),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    owner_pid INTEGER,
    last_error TEXT,
    FOREIGN KEY (session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_delivery_obligations_state
    ON delivery_obligations(state, attempts, created_at);

CREATE INDEX IF NOT EXISTS idx_delivery_obligations_session
    ON delivery_obligations(session_key);
