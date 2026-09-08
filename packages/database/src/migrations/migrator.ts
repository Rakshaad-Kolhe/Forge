import { MigrationError } from '../errors.js';
import type { DatabaseClient } from '../types.js';

export interface Migration {
  name: string;
  sql: string;
}

export const INITIAL_SCHEMA_SQL = `-- Forge V2: PR 04 - Initial PostgreSQL Schema
CREATE TABLE IF NOT EXISTS forge_migrations (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipelines (
  id VARCHAR(255) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  steps JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
`;

export const WORKER_REGISTRY_SQL = `-- Forge V2: PR 08 - Worker Registry Schema
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
`;

export const JOB_REQUIREMENTS_SQL = `-- Forge V2: PR 09 - Job Execution Requirements
ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS requirements JSONB NOT NULL DEFAULT '{}'::jsonb;
`;

export const JOB_PRIORITY_SQL = `-- Forge V2: PR 11 - Job Priority Scheduling
ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;

ALTER TABLE jobs
DROP CONSTRAINT IF EXISTS chk_jobs_priority;

ALTER TABLE jobs
ADD CONSTRAINT chk_jobs_priority CHECK (priority >= -1000 AND priority <= 1000);

CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority DESC);
`;

export const WORKER_LEASES_SQL = `-- Forge V2: PR 12 - Distributed Worker Leases & Job Claiming
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
`;

export const JOB_RETRIES_SQL = `-- Forge V2: PR 14 - Job Retry Policy and Backoff Scheduling
ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS retry_policy JSONB,
ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_jobs_retry_schedulable
ON jobs(status, next_attempt_at, priority DESC)
WHERE status = 'QUEUED';
`;

export const DEAD_LETTER_JOBS_SQL = `-- Forge V2: PR 15 - Dead-Letter Queue (DLQ)
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
`;

export const OUTBOX_EVENTS_SQL = `-- Forge V2: PR 21 - Durable Transactional Outbox
CREATE TABLE IF NOT EXISTS outbox_events (
  id                     VARCHAR(255) PRIMARY KEY,
  event_id               VARCHAR(255) NOT NULL,
  event_type             VARCHAR(64)  NOT NULL,
  version                INTEGER      NOT NULL,
  occurred_at            TIMESTAMPTZ  NOT NULL,
  pipeline_id            VARCHAR(255),
  run_id                 VARCHAR(255),
  job_id                 VARCHAR(255),
  attempt_id             VARCHAR(255),
  worker_id              VARCHAR(255),
  payload                JSONB        NOT NULL,
  status                 VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
  delivery_attempt_count INTEGER      NOT NULL DEFAULT 0,
  dispatch_count         INTEGER      NOT NULL DEFAULT 0,
  available_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  claimed_at             TIMESTAMPTZ,
  claimed_by             VARCHAR(255),
  claim_token            VARCHAR(255),
  published_at           TIMESTAMPTZ,
  last_error             TEXT,
  created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_outbox_events_event_id UNIQUE (event_id),
  CONSTRAINT chk_outbox_events_status CHECK (status IN ('PENDING','CLAIMED','PUBLISHED','DEAD'))
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_claimable
  ON outbox_events (available_at, occurred_at, id) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_outbox_events_claimed
  ON outbox_events (claimed_at) WHERE status = 'CLAIMED';
CREATE INDEX IF NOT EXISTS idx_outbox_events_retention
  ON outbox_events (published_at) WHERE status = 'PUBLISHED';
`;

export const MIGRATIONS: readonly Migration[] = Object.freeze([
  {
    name: '001_initial_schema',
    sql: INITIAL_SCHEMA_SQL,
  },
  {
    name: '002_worker_registry',
    sql: WORKER_REGISTRY_SQL,
  },
  {
    name: '003_job_requirements',
    sql: JOB_REQUIREMENTS_SQL,
  },
  {
    name: '004_job_priority',
    sql: JOB_PRIORITY_SQL,
  },
  {
    name: '005_worker_leases',
    sql: WORKER_LEASES_SQL,
  },
  {
    name: '006_job_retries',
    sql: JOB_RETRIES_SQL,
  },
  {
    name: '007_dead_letter_jobs',
    sql: DEAD_LETTER_JOBS_SQL,
  },
  {
    name: '008_outbox_events',
    sql: OUTBOX_EVENTS_SQL,
  },
]);

/**
 * Runs all pending migrations against the database in deterministic order.
 *
 * @returns Array of migration names that were executed.
 */
export async function runMigrations(client: DatabaseClient): Promise<string[]> {
  try {
    // Ensure migrations table exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS forge_migrations (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    // Fetch applied migrations
    const res = await client.query<{ name: string }>(
      'SELECT name FROM forge_migrations ORDER BY id ASC;',
    );
    const applied = new Set(res.rows.map((row) => row.name));

    const newlyApplied: string[] = [];

    for (const migration of MIGRATIONS) {
      if (applied.has(migration.name)) {
        continue;
      }

      await client.query('BEGIN');
      try {
        await client.query(migration.sql);
        await client.query('INSERT INTO forge_migrations (name) VALUES ($1);', [migration.name]);
        await client.query('COMMIT');
        newlyApplied.push(migration.name);
      } catch (migrationErr) {
        await client.query('ROLLBACK');
        throw new MigrationError(
          `Failed executing migration "${migration.name}": ${(migrationErr as Error).message}`,
          migration.name,
          migrationErr as Error,
        );
      }
    }

    return newlyApplied;
  } catch (err) {
    if (err instanceof MigrationError) {
      throw err;
    }
    throw new MigrationError(
      `Migration runner failed: ${(err as Error).message}`,
      undefined,
      err as Error,
    );
  }
}

/**
 * Drops all application tables and resets the schema for isolated test environments.
 */
export async function resetDatabase(client: DatabaseClient): Promise<void> {
  await client.query(`
    DROP TABLE IF EXISTS outbox_events CASCADE;
    DROP TABLE IF EXISTS dead_letter_jobs CASCADE;
    DROP TABLE IF EXISTS worker_leases CASCADE;
    DROP TABLE IF EXISTS workers CASCADE;
    DROP TABLE IF EXISTS job_attempts CASCADE;
    DROP TABLE IF EXISTS jobs CASCADE;
    DROP TABLE IF EXISTS pipeline_runs CASCADE;
    DROP TABLE IF EXISTS pipelines CASCADE;
    DROP TABLE IF EXISTS forge_migrations CASCADE;
  `);
}
