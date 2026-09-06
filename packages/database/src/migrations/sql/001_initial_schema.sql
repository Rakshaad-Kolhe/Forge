-- Forge V2: PR 04 - Initial PostgreSQL Schema
-- Authoritative persistent store for pipelines, pipeline runs, jobs, and job attempts.

-- Migration tracker
CREATE TABLE IF NOT EXISTS forge_migrations (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pipelines
CREATE TABLE IF NOT EXISTS pipelines (
  id VARCHAR(255) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  steps JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pipeline Runs
CREATE TABLE IF NOT EXISTS pipeline_runs (
  id VARCHAR(255) PRIMARY KEY,
  pipeline_id VARCHAR(255) NOT NULL REFERENCES pipelines(id) ON DELETE RESTRICT,
  pipeline_name VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  CONSTRAINT chk_pipeline_runs_status CHECK (
    status IN ('PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
  )
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_id ON pipeline_runs(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);

-- Jobs
CREATE TABLE IF NOT EXISTS jobs (
  id VARCHAR(255) PRIMARY KEY,
  pipeline_run_id VARCHAR(255) NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
  step_name VARCHAR(255) NOT NULL,
  command TEXT NOT NULL,
  depends_on JSONB NOT NULL DEFAULT '[]',
  status VARCHAR(50) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_jobs_run_step UNIQUE (pipeline_run_id, step_name),
  CONSTRAINT chk_jobs_status CHECK (
    status IN ('PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
  )
);

CREATE INDEX IF NOT EXISTS idx_jobs_pipeline_run_id ON jobs(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

-- Job Attempts
CREATE TABLE IF NOT EXISTS job_attempts (
  id VARCHAR(255) PRIMARY KEY,
  job_id VARCHAR(255) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
  status VARCHAR(50) NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  exit_code INTEGER,
  failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_job_attempts_job_number UNIQUE (job_id, attempt_number),
  CONSTRAINT chk_job_attempts_status CHECK (
    status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
  )
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id ON job_attempts(job_id);
