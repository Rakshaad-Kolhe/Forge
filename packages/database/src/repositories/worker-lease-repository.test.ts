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

  describe('claimBatch', () => {
    it('handles empty batch gracefully', async () => {
      const result = await leaseRepo.claimBatch({ items: [] });
      expect(result.results).toEqual([]);
      expect(result.acquiredCount).toBe(0);
      expect(result.conflictCount).toBe(0);
      expect(result.notClaimableCount).toBe(0);
    });

    it('claims single-item batch', async () => {
      const job = await createQueuedJob('batch-single');
      const result = await leaseRepo.claimBatch({
        items: [{ jobId: job.id, workerId: 'worker-single', durationMs: 30000 }],
      });

      expect(result.acquiredCount).toBe(1);
      expect(result.results[0]?.status).toBe('ACQUIRED');
      if (result.results[0]?.status === 'ACQUIRED') {
        expect(result.results[0].lease.jobId).toBe(job.id);
        expect(result.results[0].lease.workerId).toBe('worker-single');
        expect(result.results[0].isIdempotent).toBe(false);
      }
    });

    it('claims multi-item batch (50 jobs) in a single atomic operation', async () => {
      const jobs: Job[] = [];
      for (let i = 0; i < 50; i++) {
        jobs.push(await createQueuedJob(`batch-50-job-${i}`));
      }

      const result = await leaseRepo.claimBatch({
        items: jobs.map((j) => ({
          jobId: j.id,
          workerId: `worker-${j.id}`,
          durationMs: 45000,
        })),
      });

      expect(result.acquiredCount).toBe(50);
      expect(result.conflictCount).toBe(0);
      expect(result.notClaimableCount).toBe(0);
      expect(result.results.length).toBe(50);

      for (let i = 0; i < 50; i++) {
        const itemRes = result.results[i]!;
        expect(itemRes.status).toBe('ACQUIRED');
        if (itemRes.status === 'ACQUIRED') {
          expect(itemRes.lease.jobId).toBe(jobs[i]!.id);
          expect(itemRes.lease.durationMs).toBe(45000);
        }
      }

      // Verify in DB that exactly 50 ACTIVE leases exist
      const dbLeases = await pool.query(
        "SELECT COUNT(*) AS c FROM worker_leases WHERE status = 'ACTIVE'",
      );
      expect(Number(dbLeases.rows[0]!.c)).toBe(50);
    });

    it('handles partial success: queued + not found + wrong status + held by another worker', async () => {
      const job1 = await createQueuedJob('partial-1');
      const job2 = await createQueuedJob('partial-2'); // will be changed to RUNNING
      await pool.query("UPDATE jobs SET status = 'RUNNING' WHERE id = $1;", [job2.id]);
      const job3 = await createQueuedJob('partial-3'); // will have an active lease on worker-other
      await leaseRepo.claim({ jobId: job3.id, workerId: 'worker-other', durationMs: 60000 });

      const result = await leaseRepo.claimBatch({
        items: [
          { jobId: job1.id, workerId: 'worker-candidate', durationMs: 30000 },
          { jobId: 'non-existent-job-xyz', workerId: 'worker-candidate', durationMs: 30000 },
          { jobId: job2.id, workerId: 'worker-candidate', durationMs: 30000 },
          { jobId: job3.id, workerId: 'worker-candidate', durationMs: 30000 },
        ],
      });

      expect(result.acquiredCount).toBe(1);
      expect(result.notClaimableCount).toBe(2);
      expect(result.conflictCount).toBe(1);

      // job 1: ACQUIRED
      expect(result.results[0]?.status).toBe('ACQUIRED');
      // non-existent: NOT_CLAIMABLE (JOB_NOT_FOUND)
      expect(result.results[1]?.status).toBe('NOT_CLAIMABLE');
      if (result.results[1]?.status === 'NOT_CLAIMABLE') {
        expect(result.results[1].reason).toBe('JOB_NOT_FOUND');
      }
      // job 2: NOT_CLAIMABLE (JOB_NOT_CLAIMABLE)
      expect(result.results[2]?.status).toBe('NOT_CLAIMABLE');
      if (result.results[2]?.status === 'NOT_CLAIMABLE') {
        expect(result.results[2].reason).toBe('JOB_NOT_CLAIMABLE');
      }
      // job 3: CONFLICT (LEASE_ALREADY_HELD)
      expect(result.results[3]?.status).toBe('CONFLICT');
      if (result.results[3]?.status === 'CONFLICT') {
        expect(result.results[3].reason).toBe('LEASE_ALREADY_HELD');
        expect(result.results[3].currentOwnerId).toBe('worker-other');
      }
    });

    it('returns idempotent ACQUIRED when re-claiming by the same worker in batch', async () => {
      const job = await createQueuedJob('idempotent-batch-job');
      const firstClaim = await leaseRepo.claim({
        jobId: job.id,
        workerId: 'worker-same',
        durationMs: 30000,
      });
      expect(firstClaim.status).toBe('ACQUIRED');

      const batchResult = await leaseRepo.claimBatch({
        items: [{ jobId: job.id, workerId: 'worker-same', durationMs: 30000 }],
      });

      expect(batchResult.acquiredCount).toBe(1);
      expect(batchResult.results[0]?.status).toBe('ACQUIRED');
      if (batchResult.results[0]?.status === 'ACQUIRED') {
        expect(batchResult.results[0].isIdempotent).toBe(true);
        expect(batchResult.results[0].lease.id).toBe(
          (firstClaim as { lease: { id: string } }).lease.id,
        );
      }
    });

    it('replaces expired active leases in bulk with new active leases', async () => {
      const job1 = await createQueuedJob('replace-exp-1');
      const job2 = await createQueuedJob('replace-exp-2');

      const c1 = await leaseRepo.claim({
        jobId: job1.id,
        workerId: 'worker-old-1',
        durationMs: 10000,
      });
      const c2 = await leaseRepo.claim({
        jobId: job2.id,
        workerId: 'worker-old-2',
        durationMs: 10000,
      });
      const l1Id = (c1 as { lease: { id: string } }).lease.id;
      const l2Id = (c2 as { lease: { id: string } }).lease.id;

      // Expire both leases
      await pool.query(
        "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '1 second' WHERE id = ANY($1::text[]);",
        [[l1Id, l2Id]],
      );

      // Now batch claim both jobs with worker-new
      const batchResult = await leaseRepo.claimBatch({
        items: [
          { jobId: job1.id, workerId: 'worker-new', durationMs: 30000 },
          { jobId: job2.id, workerId: 'worker-new', durationMs: 30000 },
        ],
      });

      expect(batchResult.acquiredCount).toBe(2);
      expect(batchResult.results[0]?.status).toBe('ACQUIRED');
      expect(batchResult.results[1]?.status).toBe('ACQUIRED');

      // Old leases should now have status = 'EXPIRED'
      const oldCheck = await pool.query(
        'SELECT id, status FROM worker_leases WHERE id = ANY($1::text[]);',
        [[l1Id, l2Id]],
      );
      for (const row of oldCheck.rows) {
        expect(row.status).toBe('EXPIRED');
      }

      // Exactly 2 ACTIVE leases should exist now, owned by worker-new
      const newCheck = await pool.query(
        "SELECT id, worker_id, status FROM worker_leases WHERE status = 'ACTIVE';",
      );
      expect(newCheck.rows.length).toBe(2);
      expect(newCheck.rows.every((r) => r.worker_id === 'worker-new')).toBe(true);
    });

    it('handles intra-batch duplicate job IDs consistently', async () => {
      const job1 = await createQueuedJob('intra-dup-1');
      const job2 = await createQueuedJob('intra-dup-2');

      const result = await leaseRepo.claimBatch({
        items: [
          { jobId: job1.id, workerId: 'worker-A', durationMs: 30000 },
          { jobId: job1.id, workerId: 'worker-A', durationMs: 30000 }, // same worker duplicate
          { jobId: job2.id, workerId: 'worker-B', durationMs: 30000 },
          { jobId: job2.id, workerId: 'worker-C', durationMs: 30000 }, // competing worker duplicate
        ],
      });

      expect(result.results.length).toBe(4);
      // Item 0: acquired
      expect(result.results[0]?.status).toBe('ACQUIRED');
      if (result.results[0]?.status === 'ACQUIRED') {
        expect(result.results[0].isIdempotent).toBe(false);
      }
      // Item 1: duplicate for same worker -> acquired idempotent
      expect(result.results[1]?.status).toBe('ACQUIRED');
      if (result.results[1]?.status === 'ACQUIRED') {
        expect(result.results[1].isIdempotent).toBe(true);
      }
      // Item 2: acquired
      expect(result.results[2]?.status).toBe('ACQUIRED');
      // Item 3: duplicate for competing worker -> CONFLICT
      expect(result.results[3]?.status).toBe('CONFLICT');
      if (result.results[3]?.status === 'CONFLICT') {
        expect(result.results[3].currentOwnerId).toBe('worker-B');
      }
    });
  });

  describe('claimBatch concurrency and deadlock prevention', () => {
    it('prevents deadlocks when concurrent transactions claim overlapping jobs in reverse order', async () => {
      const jobA = await createQueuedJob('deadlock-job-A');
      const jobB = await createQueuedJob('deadlock-job-B');
      const jobC = await createQueuedJob('deadlock-job-C');

      // Worker 1 attempts [A, B, C], Worker 2 attempts [C, B, A]
      // Canonical sorting by ID ensures both acquire row locks in exact same order (A then B then C),
      // completely eliminating cyclic lock dependency deadlocks.
      const [res1, res2] = await Promise.all([
        leaseRepo.claimBatch({
          items: [
            { jobId: jobA.id, workerId: 'worker-1', durationMs: 30000 },
            { jobId: jobB.id, workerId: 'worker-1', durationMs: 30000 },
            { jobId: jobC.id, workerId: 'worker-1', durationMs: 30000 },
          ],
        }),
        leaseRepo.claimBatch({
          items: [
            { jobId: jobC.id, workerId: 'worker-2', durationMs: 30000 },
            { jobId: jobB.id, workerId: 'worker-2', durationMs: 30000 },
            { jobId: jobA.id, workerId: 'worker-2', durationMs: 30000 },
          ],
        }),
      ]);

      // Exactly 3 jobs total, so across res1 and res2, acquiredCount sum must equal 3
      const totalAcquired = res1.acquiredCount + res2.acquiredCount;
      const totalConflict = res1.conflictCount + res2.conflictCount;
      expect(totalAcquired).toBe(3);
      expect(totalConflict).toBe(3);

      // Verify in DB that each job has exactly 1 active lease
      const activeRows = await pool.query(
        "SELECT job_id, worker_id FROM worker_leases WHERE status = 'ACTIVE' ORDER BY job_id;",
      );
      expect(activeRows.rows.length).toBe(3);
    });

    it('handles 10 workers concurrently claiming 20 jobs without conflict errors or deadlocks', async () => {
      const jobs: Job[] = [];
      for (let i = 0; i < 20; i++) {
        jobs.push(await createQueuedJob(`concurrent-batch-job-${i}`));
      }

      // 10 workers, each attempting to claim all 20 jobs (shuffled / various orderings)
      const workerCount = 10;
      const workers = Array.from({ length: workerCount }, (_, i) => `worker-conc-${i}`);

      const promises = workers.map((workerId, idx) => {
        // Shuffle or rotate jobs
        const rotatedJobs = [...jobs.slice(idx), ...jobs.slice(0, idx)];
        return leaseRepo.claimBatch({
          items: rotatedJobs.map((j) => ({
            jobId: j.id,
            workerId,
            durationMs: 30000,
          })),
        });
      });

      const results = await Promise.all(promises);

      // Exactly 20 leases acquired across all 10 workers
      const totalAcquired = results.reduce((sum, r) => sum + r.acquiredCount, 0);
      expect(totalAcquired).toBe(20);

      // In database, exactly 20 active leases exist
      const activeRows = await pool.query(
        "SELECT COUNT(*) AS c FROM worker_leases WHERE status = 'ACTIVE';",
      );
      expect(Number(activeRows.rows[0]!.c)).toBe(20);
    });
  });
});
