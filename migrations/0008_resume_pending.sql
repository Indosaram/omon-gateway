-- Track sessions with in-flight or queued work across daemon restarts.
ALTER TABLE sessions ADD COLUMN resume_pending INTEGER NOT NULL DEFAULT 0 CHECK (resume_pending IN (0, 1));
CREATE INDEX IF NOT EXISTS idx_sessions_resume_pending ON sessions(resume_pending);
