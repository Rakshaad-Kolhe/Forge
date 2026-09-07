-- Forge V2: PR 14 - Job Retry Policy and Backoff Scheduling
ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS retry_policy JSONB,
ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_jobs_retry_schedulable
ON jobs(status, next_attempt_at, priority DESC)
WHERE status = 'QUEUED';
