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
    DROP TABLE IF EXISTS workers CASCADE;
    DROP TABLE IF EXISTS job_attempts CASCADE;
    DROP TABLE IF EXISTS jobs CASCADE;
    DROP TABLE IF EXISTS pipeline_runs CASCADE;
    DROP TABLE IF EXISTS pipelines CASCADE;
    DROP TABLE IF EXISTS forge_migrations CASCADE;
  `);
}
