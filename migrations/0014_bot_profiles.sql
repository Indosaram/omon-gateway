CREATE TABLE IF NOT EXISTS bot_profiles (
    bot_id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    model TEXT,
    system_prompt TEXT,
    enabled_toolsets TEXT,
    custom_settings_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
