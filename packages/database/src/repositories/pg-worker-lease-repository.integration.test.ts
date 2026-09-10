import { randomUUID } from 'node:crypto';
import type { BatchClaimItem, OutboxEnqueueInput, WorkerLease } from '@forge/contracts';
import {
  createJobId,
  createPipelineId,
  createPipelineRunId,
  Job,
  Pipeline,
  PipelineRun,
} from '@forge/pipeline';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDatabasePool } from '../client.js';
import { DEFAULT_DATABASE_URL } from '../config.js';
import { resetDatabase, runMigrations } from '../migrations/migrator.js';
import type { DatabasePool } from '../types.js';
import { PgJobRepository } from './pg-job-repository.js';
import { PgOutboxRepository } from './pg-outbox-repository.js';
import { PgPipelineRepository } from './pg-pipeline-repository.js';
import { PgPipelineRunRepository } from './pg-pipeline-run-repository.js';
import { PgWorkerLeaseRepository } from './pg-worker-lease-repository.js';

/**
 * Pure mapper: builds a fully-formed `JobClaimed` outbox envelope from a freshly
 * minted lease. Runs no SQL; safe to hand to `claimBatch({ pendingOutbox })`.
 */
function jobClaimedRow(item: BatchClaimItem, lease: WorkerLease): OutboxEnqueueInput {
  const eventId = randomUUID();
  return {
    id: `outbox_${randomUUID()}`,
    eventId,
    eventType: 'JobClaimed',
    version: 1,
    occurredAt: new Date().toISOString(),
    correlation: { jobId: item.jobId, workerId: item.workerId },
    payload: {
      event_id: eventId,
      event_type: 'JobClaimed',
      version: 1,
      job_id: item.jobId,
      worker_id: item.workerId,
      payload: {
        job_id: item.jobId,
        worker_id: item.workerId,
        lease_id: lease.id,
        lease_expires_at: lease.expiresAt.toISOString(),
      },
    },
  };
}

describe('PgWorkerLeaseRepository — claimBatch pendingOutbox co-commit (PR 21)', () => {
  let pool: DatabasePool;
  let pipelineRepo: PgPipelineRepository;
  let pipelineRunRepo: PgPipelineRunRepository;
  let jobRepo: PgJobRepository;
  let leaseRepo: PgWorkerLeaseRepository;

  const testPipelineId = createPipelineId('pipe-outbox-claim-test');
  const testRunId = createPipelineRunId('run-outbox-claim-test');

  beforeAll(async () => {
    pool = createDatabasePool({ connectionString: DEFAULT_DATABASE_URL });
    await resetDatabase(pool);
    await runMigrations(pool);

    pipelineRepo = new PgPipelineRepository(pool);
    pipelineRunRepo = new PgPipelineRunRepository(pool);
    jobRepo = new PgJobRepository(pool);
    leaseRepo = new PgWorkerLeaseRepository(pool);
  });

  afterAll(async () => {
    await resetDatabase(pool);
    await pool.close();
  });

  beforeEach(async () => {
    await pool.query('DELETE FROM outbox_events;');
    await pool.query('DELETE FROM worker_leases;');
    await pool.query('DELETE FROM job_attempts;');
    await pool.query('DELETE FROM jobs;');
    await pool.query('DELETE FROM pipeline_runs;');
    await pool.query('DELETE FROM pipelines;');

    const pipeline = new Pipeline({
      id: testPipelineId,
      name: 'Outbox Claim Test Pipeline',
      steps: [{ name: 'step1', command: 'echo test' }],
    });
    await pipelineRepo.save(pipeline);

    const run = new PipelineRun({
      id: testRunId,
      pipelineId: testPipelineId,
      pipelineName: 'Outbox Claim Test Pipeline',
    });
    await pipelineRunRepo.save(run);

    const job = new Job({
      id: createJobId('jobA'),
      pipelineRunId: testRunId,
      stepName: 'step-jobA',
      command: 'echo test',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);
  });

  it('co-commits a JobClaimed outbox row with a fresh lease', async () => {
    const result = await leaseRepo.claimBatch({
      items: [{ jobId: 'jobA', workerId: 'w1', durationMs: 30000 }],
      pendingOutbox: { rowForAcquired: jobClaimedRow },
    });
    expect(result.acquiredCount).toBe(1);

    const rows = await new PgOutboxRepository(pool).listByStatus('PENDING', 10);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.eventType).toBe('JobClaimed');
    expect((rows[0]!.payload as Record<string, unknown>).payload).toMatchObject({ job_id: 'jobA' });
  });

  it('does not enqueue for an idempotent re-claim', async () => {
    const opts = {
      items: [{ jobId: 'jobA', workerId: 'w1', durationMs: 30000 }],
      pendingOutbox: { rowForAcquired: jobClaimedRow },
    };
    const first = await leaseRepo.claimBatch(opts);
    const second = await leaseRepo.claimBatch(opts);

    expect(first.acquiredCount).toBe(1);
    expect(second.acquiredCount).toBe(1);
    expect(second.results[0]!.status).toBe('ACQUIRED');
    expect(second.results[0]!.status === 'ACQUIRED' && second.results[0]!.isIdempotent).toBe(true);

    const rows = await new PgOutboxRepository(pool).listByStatus('PENDING', 10);
    expect(rows).toHaveLength(1);
  });

  it('rolls back the lease when the outbox enqueue fails', async () => {
    await expect(
      leaseRepo.claimBatch({
        items: [{ jobId: 'jobA', workerId: 'w1', durationMs: 30000 }],
        pendingOutbox: {
          rowForAcquired: (item, lease) => ({
            ...jobClaimedRow(item, lease),
            payload: { blob: 'x'.repeat(200000) },
          }),
        },
      }),
    ).rejects.toBeTruthy();

    const leases = await pool.query(
      `SELECT 1 FROM worker_leases WHERE job_id = 'jobA' AND status = 'ACTIVE';`,
    );
    expect(leases.rowCount).toBe(0);

    const rows = await new PgOutboxRepository(pool).listByStatus('PENDING', 10);
    expect(rows).toHaveLength(0);
  });

  it('the single-item claim() path writes no outbox row', async () => {
    const res = await leaseRepo.claim({ jobId: 'jobA', workerId: 'w1', durationMs: 30000 });
    expect(res.status).toBe('ACQUIRED');

    const rows = await new PgOutboxRepository(pool).listByStatus('PENDING', 10);
    expect(rows).toHaveLength(0);
  });
});
