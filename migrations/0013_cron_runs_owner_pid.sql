-- Track process ID that claimed a cron run lease to prevent premature reclamation
ALTER TABLE cron_runs ADD COLUMN owner_pid INTEGER;
