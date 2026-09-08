import type pg from 'pg';

/**
 * Database connection configuration.
 */
export interface DatabaseConfig {
  connectionString: string;
  maxConnections?: number;
  idleTimeoutMillis?: number;
  connectionTimeoutMillis?: number;
}

/**
 * Generic queryable client interface shared by pg.Pool and pg.PoolClient.
 */
export interface DatabaseClient {
  query<R extends pg.QueryResultRow = pg.QueryResultRow>(
    queryText: string,
    values?: unknown[],
  ): Promise<pg.QueryResult<R>>;
}

/**
 * High-level connection pool interface providing lifecycle and health check utilities.
 */
export interface DatabasePool extends DatabaseClient {
  connect(): Promise<pg.PoolClient>;
  healthCheck(): Promise<boolean>;
  close(): Promise<void>;
  getPool(): pg.Pool;
}

/**
 * Relational representation of a Pipeline record.
 */
export interface PipelineRow {
  id: string;
  name: string;
  steps: unknown;
  created_at: Date;
  updated_at: Date;
}

/**
 * Relational representation of a PipelineRun record.
 */
export interface PipelineRunRow {
  id: string;
  pipeline_id: string;
  pipeline_name: string;
  status: string;
  created_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
}

/**
 * Relational representation of a Job record.
 */
export interface JobRow {
  id: string;
  pipeline_run_id: string;
  step_name: string;
  command: string;
  depends_on: unknown;
  requirements?: unknown;
  priority?: number;
  retry_policy?: unknown;
  next_attempt_at?: Date | null;
  status: string;
  created_at: Date;
}

/**
 * Relational representation of a JobAttempt record.
 */
export interface JobAttemptRow {
  id: string;
  job_id: string;
  attempt_number: number;
  status: string;
  started_at: Date | null;
  finished_at: Date | null;
  exit_code: number | null;
  failure_reason: string | null;
  created_at: Date;
}

/**
 * Relational representation of a WorkerLease record.
 */
export interface WorkerLeaseRow {
  id: string;
  job_id: string;
  worker_id: string;
  status: string;
  duration_ms: number;
  acquired_at: Date;
  renewed_at: Date;
  expires_at: Date;
  created_at: Date;
}

/**
 * Relational representation of a DeadLetterJob record.
 */
export interface DeadLetterJobRow {
  id: string;
  job_id: string;
  pipeline_run_id: string;
  reason: string;
  failed_attempt_count: number;
  last_attempt_id: string | null;
  last_worker_id: string | null;
  error_details: string | null;
  metadata: unknown;
  created_at: Date;
}
