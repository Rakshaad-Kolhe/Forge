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
import { PgPipelineRepository } from './pg-pipeline-repository.js';
import { PgPipelineRunRepository } from './pg-pipeline-run-repository.js';
import { PgWorkerLeaseRepository } from './pg-worker-lease-repository.js';

describe('PgWorkerLeaseRepository Integration & Concurrency Tests', () => {
  let pool: DatabasePool;
  let pipelineRepo: PgPipelineRepository;
  let pipelineRunRepo: PgPipelineRunRepository;
  let jobRepo: PgJobRepository;
  let leaseRepo: PgWorkerLeaseRepository;

  const testPipelineId = createPipelineId('pipe-lease-test');
  const testRunId = createPipelineRunId('run-lease-test');

  beforeAll(async () => {
    pool = createDatabasePool({
      connectionString: DEFAULT_DATABASE_URL,
    });
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
    await pool.query('DELETE FROM worker_leases;');
    await pool.query('DELETE FROM jobs;');
    await pool.query('DELETE FROM pipeline_runs;');
    await pool.query('DELETE FROM pipelines;');

    // Setup base pipeline & run
    const pipeline = new Pipeline({
      id: testPipelineId,
      name: 'Lease Test Pipeline',
      steps: [{ name: 'step1', command: 'echo test' }],
    });
    await pipelineRepo.save(pipeline);

    const run = new PipelineRun({
      id: testRunId,
      pipelineId: testPipelineId,
      pipelineName: 'Lease Test Pipeline',
    });
    await pipelineRunRepo.save(run);
  });

  async function createQueuedJob(jobIdStr: string): Promise<Job> {
    const job = new Job({
      id: createJobId(jobIdStr),
      pipelineRunId: testRunId,
      stepName: `step_${jobIdStr}`,
      command: 'echo test',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);
    return job;
  }

  describe('claim', () => {
    it('successfully claims an unleased queued job with renewable lease', async () => {
      const job = await createQueuedJob('job-1');

      const claimResult = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });

      expect(claimResult.status).toBe('ACQUIRED');
      if (claimResult.status === 'ACQUIRED') {
        expect(claimResult.isIdempotent).toBe(false);
        expect(claimResult.lease.jobId).toBe(job.id);
        expect(claimResult.lease.workerId).toBe('worker-alpha');
        expect(claimResult.lease.status).toBe('ACTIVE');
        expect(claimResult.lease.durationMs).toBe(30000);
        expect(claimResult.lease.expiresAt.getTime()).toBeGreaterThan(
          claimResult.lease.acquiredAt.getTime(),
        );
      }

      const activeLease = await leaseRepo.findActiveByJobId(job.id);
      expect(activeLease).not.toBeNull();
      expect(activeLease?.workerId).toBe('worker-alpha');
    });

    it('returns idempotent success if claimed again by the same worker', async () => {
      const job = await createQueuedJob('job-2');

      const firstClaim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });
      expect(firstClaim.status).toBe('ACQUIRED');

      const secondClaim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });

      expect(secondClaim.status).toBe('ACQUIRED');
      if (firstClaim.status === 'ACQUIRED' && secondClaim.status === 'ACQUIRED') {
        expect(secondClaim.isIdempotent).toBe(true);
        expect(secondClaim.lease.id).toBe(firstClaim.lease.id);
      }
    });

    it('returns CONFLICT when another worker holds an active lease', async () => {
      const job = await createQueuedJob('job-3');

      await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });

      const conflictClaim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-bravo',
        durationMs: 30000,
      });

      expect(conflictClaim.status).toBe('CONFLICT');
      if (conflictClaim.status === 'CONFLICT') {
        expect(conflictClaim.reason).toBe('LEASE_ALREADY_HELD');
        expect(conflictClaim.currentOwnerId).toBe('worker-alpha');
        expect(conflictClaim.expiresAt).toBeInstanceOf(Date);
      }
    });

    it('returns NOT_CLAIMABLE when job does not exist', async () => {
      const result = await leaseRepo.claim({
        jobId: 'non-existent-job',
        workerId: 'worker-alpha',
        durationMs: 30000,
      });

      expect(result.status).toBe('NOT_CLAIMABLE');
      if (result.status === 'NOT_CLAIMABLE') {
        expect(result.reason).toBe('JOB_NOT_FOUND');
      }
    });

    it('returns NOT_CLAIMABLE when job is not QUEUED', async () => {
      const job = new Job({
        id: createJobId('job-pending'),
        pipelineRunId: testRunId,
        stepName: 'step_pending',
        command: 'echo test',
        initialStatus: 'PENDING',
      });
      await jobRepo.save(job);

      const result = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });

      expect(result.status).toBe('NOT_CLAIMABLE');
      if (result.status === 'NOT_CLAIMABLE') {
        expect(result.reason).toBe('JOB_NOT_CLAIMABLE');
      }
    });

    it('allows a new worker to claim when previous lease expired', async () => {
      const job = await createQueuedJob('job-expired-claim');

      const claim1 = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });
      expect(claim1.status).toBe('ACQUIRED');

      // Artificially expire lease 1
      await pool.query(
        "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1;",
        [claim1.status === 'ACQUIRED' ? claim1.lease.id : ''],
      );

      // Worker bravo claims
      const claim2 = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-bravo',
        durationMs: 30000,
      });

      expect(claim2.status).toBe('ACQUIRED');
      if (claim2.status === 'ACQUIRED') {
        expect(claim2.isIdempotent).toBe(false);
        expect(claim2.lease.workerId).toBe('worker-bravo');
      }

      // Check status of old lease in DB
      if (claim1.status === 'ACQUIRED') {
        const oldLease = await leaseRepo.findById(claim1.lease.id);
        expect(oldLease?.status).toBe('EXPIRED');
      }
    });
  });

  describe('renew', () => {
    it('successfully extends an active lease by owner', async () => {
      const job = await createQueuedJob('job-renew-1');
      const claim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 15000,
      });
      expect(claim.status).toBe('ACQUIRED');
      const lease = (claim as { lease: { id: string; expiresAt: Date } }).lease;

      const renewResult = await leaseRepo.renew({
        leaseId: lease.id,
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 25000,
      });

      expect(renewResult.status).toBe('RENEWED');
      if (renewResult.status === 'RENEWED') {
        expect(renewResult.lease.durationMs).toBe(25000);
        expect(renewResult.lease.expiresAt.getTime()).toBeGreaterThan(lease.expiresAt.getTime());
      }
    });

    it('rejects renewal when owner does not match', async () => {
      const job = await createQueuedJob('job-renew-wrong-owner');
      const claim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 15000,
      });
      const leaseId = (claim as { lease: { id: string } }).lease.id;

      const renewResult = await leaseRepo.renew({
        leaseId,
        jobId: job.id,
        workerId: 'worker-charlie',
      });

      expect(renewResult.status).toBe('REJECTED');
      if (renewResult.status === 'REJECTED') {
        expect(renewResult.reason).toBe('LEASE_OWNER_MISMATCH');
      }
    });

    it('rejects renewal for non-existent lease', async () => {
      const renewResult = await leaseRepo.renew({
        leaseId: 'lease-does-not-exist',
        jobId: 'some-job',
        workerId: 'worker-alpha',
      });

      expect(renewResult.status).toBe('REJECTED');
      if (renewResult.status === 'REJECTED') {
        expect(renewResult.reason).toBe('LEASE_NOT_FOUND');
      }
    });

    it('rejects renewal if lease has expired', async () => {
      const job = await createQueuedJob('job-renew-expired');
      const claim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 15000,
      });
      const leaseId = (claim as { lease: { id: string } }).lease.id;

      await pool.query(
        "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1;",
        [leaseId],
      );

      const renewResult = await leaseRepo.renew({
        leaseId,
        jobId: job.id,
        workerId: 'worker-alpha',
      });

      expect(renewResult.status).toBe('REJECTED');
      if (renewResult.status === 'REJECTED') {
        expect(renewResult.reason).toBe('LEASE_EXPIRED');
      }
    });
  });

  describe('release', () => {
    it('successfully releases an active lease and makes job claimable immediately', async () => {
      const job = await createQueuedJob('job-release-1');
      const claim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });
      const leaseId = (claim as { lease: { id: string } }).lease.id;

      const releaseResult = await leaseRepo.release({
        leaseId,
        jobId: job.id,
        workerId: 'worker-alpha',
      });

      expect(releaseResult.status).toBe('RELEASED');
      expect((releaseResult as { leaseId: string }).leaseId).toBe(leaseId);

      // Verify active lease is gone
      const active = await leaseRepo.findActiveByJobId(job.id);
      expect(active).toBeNull();

      // Another worker can immediately claim
      const secondClaim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-bravo',
        durationMs: 30000,
      });
      expect(secondClaim.status).toBe('ACQUIRED');
    });

    it('rejects release if attempted by non-owner', async () => {
      const job = await createQueuedJob('job-release-wrong');
      const claim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });
      const leaseId = (claim as { lease: { id: string } }).lease.id;

      const result = await leaseRepo.release({
        leaseId,
        jobId: job.id,
        workerId: 'worker-bravo',
      });

      expect(result.status).toBe('REJECTED');
      if (result.status === 'REJECTED') {
        expect(result.reason).toBe('LEASE_OWNER_MISMATCH');
      }
    });

    it('rejects release if already released', async () => {
      const job = await createQueuedJob('job-release-double');
      const claim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });
      const leaseId = (claim as { lease: { id: string } }).lease.id;

      await leaseRepo.release({ leaseId, jobId: job.id, workerId: 'worker-alpha' });

      const doubleRelease = await leaseRepo.release({
        leaseId,
        jobId: job.id,
        workerId: 'worker-alpha',
      });
      expect(doubleRelease.status).toBe('REJECTED');
      if (doubleRelease.status === 'REJECTED') {
        expect(doubleRelease.reason).toBe('LEASE_ALREADY_INACTIVE');
      }
    });
  });

  describe('reclaimExpiredLeases', () => {
    it('transitions expired active leases to EXPIRED status in bulk', async () => {
      const job1 = await createQueuedJob('job-reclaim-1');
      const job2 = await createQueuedJob('job-reclaim-2');

      const claim1 = await leaseRepo.claim({
        jobId: job1.id,
        workerId: 'worker-alpha',
        durationMs: 30000,
      });
      const claim2 = await leaseRepo.claim({
        jobId: job2.id,
        workerId: 'worker-bravo',
        durationMs: 30000,
      });

      // Expire only job 1
      await pool.query(
        "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '10 seconds' WHERE id = $1;",
        [(claim1 as { lease: { id: string } }).lease.id],
      );

      const count = await leaseRepo.reclaimExpiredLeases();
      expect(count).toBe(1);

      const l1 = await leaseRepo.findById((claim1 as { lease: { id: string } }).lease.id);
      expect(l1?.status).toBe('EXPIRED');

      const l2 = await leaseRepo.findById((claim2 as { lease: { id: string } }).lease.id);
      expect(l2?.status).toBe('ACTIVE');
    });
  });

  describe('concurrency test', () => {
    it('guarantees exactly one worker acquires lease when 10 workers race concurrently', async () => {
      const job = await createQueuedJob('job-concurrency-race');
      const workerCount = 10;
      const workers = Array.from({ length: workerCount }, (_, i) => `worker-${i + 1}`);

      // All 10 workers attempt to claim the exact same job at the exact same instant
      const results = await Promise.all(
        workers.map((workerId) =>
          leaseRepo.claim({
            jobId: job.id,
            workerId,
            durationMs: 30000,
          }),
        ),
      );

      const acquired = results.filter((r) => r.status === 'ACQUIRED');
      const conflicts = results.filter((r) => r.status === 'CONFLICT');

      expect(acquired.length).toBe(1);
      expect(conflicts.length).toBe(workerCount - 1);

      // Verify in PostgreSQL table that only one ACTIVE lease exists
      const dbRows = await pool.query(
        "SELECT id, worker_id, status FROM worker_leases WHERE job_id = $1 AND status = 'ACTIVE';",
        [job.id],
      );
      expect(dbRows.rows.length).toBe(1);
      if (acquired[0]?.status === 'ACQUIRED') {
        expect(dbRows.rows[0]!.worker_id).toBe(acquired[0].lease.workerId);
      }
    });
  });

  describe('stale lease owner replacement', () => {
    it('prevents stale owner L1 from renewing or releasing after L2 claims', async () => {
      const job = await createQueuedJob('job-stale-replacement');

      // Worker 1 acquires L1
      const claim1 = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-1',
        durationMs: 10000,
      });
      const l1Id = (claim1 as { lease: { id: string } }).lease.id;

      // L1 expires
      await pool.query(
        "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1;",
        [l1Id],
      );

      // Worker 2 acquires L2
      const claim2 = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-2',
        durationMs: 20000,
      });
      expect(claim2.status).toBe('ACQUIRED');

      // Stale Worker 1 attempts to renew L1 -> rejected
      const renewL1 = await leaseRepo.renew({
        leaseId: l1Id,
        jobId: job.id,
        workerId: 'worker-1',
      });
      expect(renewL1.status).toBe('REJECTED');
      if (renewL1.status === 'REJECTED') {
        expect(renewL1.reason).toBe('LEASE_EXPIRED');
      }

      // Stale Worker 1 attempts to release L1 -> rejected
      const releaseL1 = await leaseRepo.release({
        leaseId: l1Id,
        jobId: job.id,
        workerId: 'worker-1',
      });
      expect(releaseL1.status).toBe('REJECTED');
      if (releaseL1.status === 'REJECTED') {
        expect(releaseL1.reason).toBe('LEASE_ALREADY_INACTIVE');
      }
    });
  });
});
