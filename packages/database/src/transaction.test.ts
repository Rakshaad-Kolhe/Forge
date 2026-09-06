import { createPipelineId, createPipelineRunId, Job, Pipeline, PipelineRun } from '@forge/pipeline';
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

  it('isolates uncommitted writes so they are invisible to external pool connections until commit', async () => {
    const pipelineId = createPipelineId('tx-pipe-isolation');

    const pipeline = new Pipeline({
      id: pipelineId,
      name: 'Isolation Test Pipeline',
      steps: [{ name: 'step-iso', command: 'echo iso' }],
    });

    await withTransaction(pool, async (tx) => {
      await tx.pipelines.save(pipeline);

      // Visible within transaction
      const inTxCheck = await tx.client.query('SELECT id FROM pipelines WHERE id = $1;', [
        pipelineId,
      ]);
      expect(inTxCheck.rows).toHaveLength(1);

      // Invisible outside transaction via independent pool connection
      const outsideCheck = await pool.query('SELECT id FROM pipelines WHERE id = $1;', [
        pipelineId,
      ]);
      expect(outsideCheck.rows).toHaveLength(0);
    });

    // Visible outside transaction after commit
    const afterCommitCheck = await pool.query('SELECT id FROM pipelines WHERE id = $1;', [
      pipelineId,
    ]);
    expect(afterCommitCheck.rows).toHaveLength(1);
  });

  it('enforces atomic aggregate rollback when constituent job persistence fails', async () => {
    const pipelineId = createPipelineId('tx-pipe-aggregate');
    const runId = createPipelineRunId('tx-run-aggregate');

    const pipeline = new Pipeline({
      id: pipelineId,
      name: 'Aggregate Test Pipeline',
      steps: [
        { name: 'step-1', command: 'echo 1' },
        { name: 'step-2', command: 'echo 2', dependsOn: ['step-1'] },
      ],
    });

    await withTransaction(pool, async (tx) => {
      await tx.pipelines.save(pipeline);
    });

    const run = PipelineRun.create(runId, pipeline);

    // Initial save of the run with jobs as PENDING
    await withTransaction(pool, async (tx) => {
      await tx.pipelineRuns.save(run);
    });

    const job2 = run.getJob('step-2')!;
    // Advance job2 in DB through valid lifecycle: PENDING -> QUEUED -> RUNNING -> SUCCEEDED
    job2.markQueued();
    await withTransaction(pool, async (tx) => {
      await tx.jobs.save(job2);
    });
    job2.start();
    await withTransaction(pool, async (tx) => {
      await tx.jobs.save(job2);
    });
    job2.succeed();
    await withTransaction(pool, async (tx) => {
      await tx.jobs.save(job2);
    });

    const checkJob2 = await pool.query<{ status: string }>(
      'SELECT status FROM jobs WHERE id = $1;',
      [job2.id],
    );
    expect(checkJob2.rows[0]?.status).toBe('SUCCEEDED');

    // Create a mutated run aggregate:
    // job1 attempts to advance PENDING -> QUEUED (valid)
    // job2 attempts illegal regression SUCCEEDED -> RUNNING (invalid terminal transition)
    const mutatedRun = new PipelineRun({
      id: run.id,
      pipelineId: pipeline.id,
      pipelineName: pipeline.name,
      initialStatus: 'RUNNING',
    });

    const job1Mutated = new Job({
      id: run.getJob('step-1')!.id,
      pipelineRunId: run.id,
      stepName: 'step-1',
      command: 'echo 1',
      initialStatus: 'QUEUED',
    });

    const job2Regressed = new Job({
      id: job2.id,
      pipelineRunId: run.id,
      stepName: 'step-2',
      command: 'echo 2',
      dependsOn: ['step-1'],
      initialStatus: 'RUNNING', // Illegal: DB is SUCCEEDED
    });

    mutatedRun.addJob(job1Mutated);
    mutatedRun.addJob(job2Regressed);

    // Saving mutatedRun must fail and roll back everything atomically
    await expect(
      withTransaction(pool, async (tx) => {
        await tx.pipelineRuns.save(mutatedRun);
      }),
    ).rejects.toThrow();

    // Verify rollback: run status was NOT updated to RUNNING
    const checkRunAfter = await pool.query<{ status: string }>(
      'SELECT status FROM pipeline_runs WHERE id = $1;',
      [runId],
    );
    expect(checkRunAfter.rows[0]?.status).toBe('PENDING');

    // Verify rollback: job1 was NOT updated to QUEUED
    const checkJob1After = await pool.query<{ status: string }>(
      'SELECT status FROM jobs WHERE id = $1;',
      [job1Mutated.id],
    );
    expect(checkJob1After.rows[0]?.status).toBe('PENDING');

    // Verify job2 remains SUCCEEDED
    const checkJob2After = await pool.query<{ status: string }>(
      'SELECT status FROM jobs WHERE id = $1;',
      [job2.id],
    );
    expect(checkJob2After.rows[0]?.status).toBe('SUCCEEDED');
  });
});
