CREATE INDEX IF NOT EXISTS idx_cron_runs_job_status
    ON cron_runs(job_id, status);

CREATE INDEX IF NOT EXISTS idx_cron_runs_job_attempt
    ON cron_runs(job_id, attempt DESC);
