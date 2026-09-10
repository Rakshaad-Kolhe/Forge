import type pg from 'pg';
import { PgDeadLetterRepository } from './repositories/pg-dead-letter-repository.js';
import { PgJobAttemptRepository } from './repositories/pg-job-attempt-repository.js';
import { PgJobRepository } from './repositories/pg-job-repository.js';
import { PgOutboxRepository } from './repositories/pg-outbox-repository.js';
import { PgPipelineRepository } from './repositories/pg-pipeline-repository.js';
import { PgPipelineRunRepository } from './repositories/pg-pipeline-run-repository.js';
import { PgWorkerLeaseRepository } from './repositories/pg-worker-lease-repository.js';
import type { DatabasePool } from './types.js';

export interface TransactionContext {
  client: pg.PoolClient;
  pipelines: PgPipelineRepository;
  pipelineRuns: PgPipelineRunRepository;
  jobs: PgJobRepository;
  jobAttempts: PgJobAttemptRepository;
  workerLeases: PgWorkerLeaseRepository;
  deadLetterJobs: PgDeadLetterRepository;
  outbox: PgOutboxRepository;
}

/**
 * Executes a callback within a managed PostgreSQL ACID transaction.
 *
 * Automatically issues BEGIN, commits on success, rolls back on error,
 * and releases the acquired pool connection back to the pool.
 */
export async function withTransaction<T>(
  pool: DatabasePool,
  callback: (tx: TransactionContext) => Promise<T>,
  options?: { readonly outboxMaxPayloadBytes?: number },
): Promise<T> {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const txContext: TransactionContext = {
      client,
      pipelines: new PgPipelineRepository(client),
      pipelineRuns: new PgPipelineRunRepository(client),
      jobs: new PgJobRepository(client),
      jobAttempts: new PgJobAttemptRepository(client),
      workerLeases: new PgWorkerLeaseRepository(client),
      deadLetterJobs: new PgDeadLetterRepository(client),
      outbox: new PgOutboxRepository(
        client,
        options?.outboxMaxPayloadBytes !== undefined
          ? { maxPayloadBytes: options.outboxMaxPayloadBytes }
          : {},
      ),
    };

    const result = await callback(txContext);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      console.error(
        '[forge-database] Transaction rollback failed:',
        (rollbackErr as Error).message,
      );
    }
    throw err;
  } finally {
    client.release();
  }
}
