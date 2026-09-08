import { randomUUID } from 'node:crypto';
import {
  createJobId,
  createPipelineId,
  createPipelineRunId,
  Job,
  Pipeline,
  PipelineRun,
} from '@forge/pipeline';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDatabasePool } from './client.js';
import { DEFAULT_DATABASE_URL } from './config.js';
import { OutboxPayloadError } from './errors.js';
import { resetDatabase, runMigrations } from './migrations/migrator.js';
import { PgOutboxRepository } from './repositories/pg-outbox-repository.js';
import { withTransaction } from './transaction.js';
import type { DatabasePool } from './types.js';

describe('withTransaction — jobs + outbox atomicity', () => {
  let pool: DatabasePool;
  const pipelineId = createPipelineId('pipe-outbox-tx');
  const runId = createPipelineRunId('run-outbox-tx');

  beforeAll(async () => {
    pool = createDatabasePool({ connectionString: DEFAULT_DATABASE_URL });
    await resetDatabase(pool);
    await runMigrations(pool);
  });

  afterAll(async () => {
    await resetDatabase(pool);
    await pool.close();
  });

  beforeEach(async () => {
    await pool.query('DELETE FROM outbox_events;');
    await pool.query('DELETE FROM job_attempts;');
    await pool.query('DELETE FROM jobs;');
    await pool.query('DELETE FROM pipeline_runs;');
    await pool.query('DELETE FROM pipelines;');
    await withTransaction(pool, async (tx) => {
      await tx.pipelines.save(
        new Pipeline({ id: pipelineId, name: 'tx', steps: [{ name: 's1', command: 'echo' }] }),
      );
      const run = new PipelineRun({ id: runId, pipelineId, pipelineName: 'tx' });
      run.markQueued();
      run.start();
      await tx.pipelineRuns.save(run);
    });
  });

  function makeJob(): Job {
    return new Job({
      id: createJobId(`job-${randomUUID()}`),
      pipelineRunId: runId,
      stepName: 's1',
      command: 'echo',
      dependsOn: [],
    });
  }

  function outboxInput(jobId: string) {
    const eventId = randomUUID();
    return {
      id: `outbox_${randomUUID()}`,
      eventId,
      eventType: 'JobStarted',
      version: 1,
      occurredAt: new Date().toISOString(),
      correlation: { jobId },
      payload: {
        event_id: eventId,
        event_type: 'JobStarted',
        version: 1,
        payload: { job_id: jobId },
      },
    };
  }

  it('commits the job row and the outbox row together', async () => {
    const job = makeJob();
    await withTransaction(pool, async (tx) => {
      await tx.jobs.save(job);
      await tx.outbox.enqueue(outboxInput(job.id));
    });

    const jobRes = await pool.query('SELECT 1 FROM jobs WHERE id = $1;', [job.id]);
    expect(jobRes.rowCount).toBe(1);
    expect(await new PgOutboxRepository(pool).listByStatus('PENDING', 10)).toHaveLength(1);
  });

  it('rolls back the job row when the callback throws after enqueue', async () => {
    const job = makeJob();
    await expect(
      withTransaction(pool, async (tx) => {
        await tx.jobs.save(job);
        await tx.outbox.enqueue(outboxInput(job.id));
        throw new Error('boom after enqueue');
      }),
    ).rejects.toThrow('boom after enqueue');

    const jobRes = await pool.query('SELECT 1 FROM jobs WHERE id = $1;', [job.id]);
    expect(jobRes.rowCount).toBe(0);
    expect(await new PgOutboxRepository(pool).listByStatus('PENDING', 10)).toHaveLength(0);
  });

  it('rolls back the job row when the outbox payload is oversized', async () => {
    const job = makeJob();
    await expect(
      withTransaction(
        pool,
        async (tx) => {
          await tx.jobs.save(job);
          await tx.outbox.enqueue({
            ...outboxInput(job.id),
            payload: { blob: 'x'.repeat(1000) },
          });
        },
        { outboxMaxPayloadBytes: 64 },
      ),
    ).rejects.toBeInstanceOf(OutboxPayloadError);

    const jobRes = await pool.query('SELECT 1 FROM jobs WHERE id = $1;', [job.id]);
    expect(jobRes.rowCount).toBe(0);
  });
});
