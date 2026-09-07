-- Forge V2: PR 11 - Job Priority Scheduling
ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;

ALTER TABLE jobs
DROP CONSTRAINT IF EXISTS chk_jobs_priority;

ALTER TABLE jobs
ADD CONSTRAINT chk_jobs_priority CHECK (priority >= -1000 AND priority <= 1000);

CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority DESC);
