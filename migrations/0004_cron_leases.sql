CREATE TABLE IF NOT EXISTS cron_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    claim_token TEXT NOT NULL UNIQUE,
    lease_expires_at TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    error TEXT,
    FOREIGN KEY (job_id) REFERENCES cron_jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cron_runs_job_started
    ON cron_runs(job_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_cron_runs_active_lease
    ON cron_runs(job_id, status, lease_expires_at);
