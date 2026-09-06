import { createPipelineId, createPipelineRunId, Pipeline, PipelineRun } from '@forge/pipeline';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDatabasePool } from './client.js';
import { DEFAULT_DATABASE_URL } from './config.js';
import { resetDatabase, runMigrations } from './migrations/migrator.js';
import { withTransaction } from './transaction.js';
import type { DatabasePool } from './types.js';

describe('PostgreSQL Transactions & Rollback Verification', () => {
  let pool: DatabasePool;

  beforeAll(async () => {
    pool = createDatabasePool({
      connectionString: DEFAULT_DATABASE_URL,
    });
    await resetDatabase(pool);
    await runMigrations(pool);
  });

  afterAll(async () => {
    await resetDatabase(pool);
    await pool.close();
  });

  beforeEach(async () => {
    await pool.query('DELETE FROM job_attempts;');
    await pool.query('DELETE FROM jobs;');
    await pool.query('DELETE FROM pipeline_runs;');
    await pool.query('DELETE FROM pipelines;');
  });

  it('commits multi-step atomic operations successfully', async () => {
    const pipelineId = createPipelineId('tx-pipe-success');
    const runId = createPipelineRunId('tx-run-success');

    const pipeline = new Pipeline({
      id: pipelineId,
      name: 'Atomic Success Pipeline',
      steps: [
        { name: 'step-a', command: 'echo a' },
        { name: 'step-b', command: 'echo b', dependsOn: ['step-a'] },
      ],
    });

    const run = PipelineRun.create(runId, pipeline);

    await withTransaction(pool, async (tx) => {
      await tx.pipelines.save(pipeline);
      await tx.pipelineRuns.save(run);
    });

    // Verify both pipeline and run exist committed in PostgreSQL
    const checkPipe = await pool.query('SELECT id FROM pipelines WHERE id = $1;', [pipelineId]);
    expect(checkPipe.rows).toHaveLength(1);

    const checkRun = await pool.query('SELECT id FROM pipeline_runs WHERE id = $1;', [runId]);
    expect(checkRun.rows).toHaveLength(1);

    const checkJobs = await pool.query('SELECT id FROM jobs WHERE pipeline_run_id = $1;', [runId]);
    expect(checkJobs.rows).toHaveLength(2);
  });

  it('rolls back completely when an error occurs mid-transaction leaving zero partial state', async () => {
    const pipelineId = createPipelineId('tx-pipe-rollback');
    const runId = createPipelineRunId('tx-run-rollback');

    const pipeline = new Pipeline({
      id: pipelineId,
      name: 'Rollback Test Pipeline',
      steps: [{ name: 'step-fail', command: 'exit 1' }],
    });

    const run = PipelineRun.create(runId, pipeline);

    // Save the parent pipeline outside the transaction so it's a valid foreign key target
    await withTransaction(pool, async (tx) => {
      await tx.pipelines.save(pipeline);
    });

    // Deliberate forced failure inside transaction
    const forcedFailure = new Error('FORCED_SIMULATED_FAILURE_MID_TRANSACTION');

    await expect(
      withTransaction(pool, async (tx) => {
        // Intermediate write 1: Save pipeline run
        await tx.pipelineRuns.save(run);

        // Verify write was visible within the transaction
        const inTxCheck = await tx.client.query('SELECT id FROM pipeline_runs WHERE id = $1;', [
          runId,
        ]);
        expect(inTxCheck.rows).toHaveLength(1);

        // Force an exception before COMMIT
        throw forcedFailure;
      }),
    ).rejects.toThrow('FORCED_SIMULATED_FAILURE_MID_TRANSACTION');

    // VERIFY ROLLBACK: Run and jobs must NOT exist in the database
    const checkRun = await pool.query('SELECT id FROM pipeline_runs WHERE id = $1;', [runId]);
    expect(checkRun.rows).toHaveLength(0);

    const checkJobs = await pool.query('SELECT id FROM jobs WHERE pipeline_run_id = $1;', [runId]);
    expect(checkJobs.rows).toHaveLength(0);
  });
});
