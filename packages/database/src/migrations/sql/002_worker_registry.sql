-- Forge V2: PR 08 - Worker Registry Schema
-- Authoritative persistent store for worker identity, status, capabilities, and resource capacity.

CREATE TABLE IF NOT EXISTS workers (
  id VARCHAR(255) PRIMARY KEY,
  status VARCHAR(50) NOT NULL,
  hostname VARCHAR(255),
  executors JSONB NOT NULL DEFAULT '[]',
  resources JSONB NOT NULL,
  registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_workers_status CHECK (
    status IN ('STARTING', 'READY', 'DRAINING', 'OFFLINE')
  )
);

CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);
CREATE INDEX IF NOT EXISTS idx_workers_registered_at ON workers(registered_at);
