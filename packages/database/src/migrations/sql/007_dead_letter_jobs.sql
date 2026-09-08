-- Forge V2: PR 15 - Dead-Letter Queue (DLQ)
CREATE TABLE IF NOT EXISTS dead_letter_jobs (
  id VARCHAR(255) PRIMARY KEY,
  job_id VARCHAR(255) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  pipeline_run_id VARCHAR(255) NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
  reason VARCHAR(100) NOT NULL,
  failed_attempt_count INTEGER NOT NULL,
  last_attempt_id VARCHAR(255) REFERENCES job_attempts(id) ON DELETE SET NULL,
  last_worker_id VARCHAR(255),
  error_details TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_dead_letter_jobs_job_id UNIQUE (job_id)
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_pipeline_run_id ON dead_letter_jobs(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_reason ON dead_letter_jobs(reason);
CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_created_at ON dead_letter_jobs(created_at);
