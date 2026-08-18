CREATE TABLE IF NOT EXISTS messenger_policy_overrides (
    platform TEXT PRIMARY KEY NOT NULL,
    policy_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
