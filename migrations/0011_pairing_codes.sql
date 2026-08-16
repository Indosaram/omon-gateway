CREATE TABLE IF NOT EXISTS pairing_codes (
    code TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS paired_users (
    user_id TEXT PRIMARY KEY,
    paired_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pairing_codes_user_id ON pairing_codes(user_id);
