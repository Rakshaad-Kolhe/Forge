-- Forge V2: PR 12 - Distributed Worker Leases & Job Claiming
CREATE TABLE IF NOT EXISTS worker_leases (
  id VARCHAR(255) PRIMARY KEY,
  job_id VARCHAR(255) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  worker_id VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL,
  duration_ms INTEGER NOT NULL CHECK (duration_ms > 0),
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_worker_leases_status CHECK (
    status IN ('ACTIVE', 'RELEASED', 'EXPIRED')
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_worker_leases_active_job
  ON worker_leases(job_id)
  WHERE status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_worker_leases_job_id ON worker_leases(job_id);
CREATE INDEX IF NOT EXISTS idx_worker_leases_worker_id ON worker_leases(worker_id);
CREATE INDEX IF NOT EXISTS idx_worker_leases_status ON worker_leases(status);
CREATE INDEX IF NOT EXISTS idx_worker_leases_expires_at ON worker_leases(expires_at);
