-- Pending writes staging table for Memory and Skills write-approval gating.
CREATE TABLE IF NOT EXISTS pending_writes (
    id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL, -- 'memory' | 'skill'
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pending_writes_kind_created
    ON pending_writes(kind, created_at);
