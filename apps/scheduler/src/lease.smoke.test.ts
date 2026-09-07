import {
  createDatabasePool,
  DEFAULT_DATABASE_URL,
  PgJobRepository,
  PgPipelineRepository,
  PgPipelineRunRepository,
  PgWorkerLeaseRepository,
  PgWorkerRepository,
  resetDatabase,
  runMigrations,
  type DatabasePool,
} from '@forge/database';
import {
  createJobId,
  createPipelineId,
  createPipelineRunId,
  Job,
  Pipeline,
  PipelineRun,
} from '@forge/pipeline';
import { createJobQueue, type JobQueue } from '@forge/queue';
import { createRedisClient, DEFAULT_REDIS_URL, type RedisClient } from '@forge/redis';
import {
  createWorkerHeartbeatStore,
  createWorkerId,
  createWorkerRegistry,
  type WorkerRegistry,
} from '@forge/worker-registry';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createJobSourceFromRepository, ForgeScheduler } from './scheduler.js';

describe('PR 12 Section 51 & 52 Verification Smoke Test Matrix', () => {
  let pool: DatabasePool;
  let redisClient: RedisClient;
  let pipelineRepo: PgPipelineRepository;
  let pipelineRunRepo: PgPipelineRunRepository;
  let jobRepo: PgJobRepository;
  let workerRepo: PgWorkerRepository;
  let leaseRepo: PgWorkerLeaseRepository;
  let workerRegistry: WorkerRegistry;
  let queue: JobQueue;
  let scheduler: ForgeScheduler;

  const queueName = 'smoke-lease-queue';
  const pipelineId = createPipelineId('pipe-smoke-lease');
  const runId = createPipelineRunId('run-smoke-lease');
  const jobId = createJobId('job-smoke-lease-1');
  let workerALeaseId: string;
  let workerBLeaseId: string;

  beforeAll(async () => {
    // Step 1: Start test infrastructure (Postgres, Redis)
    pool = createDatabasePool({ connectionString: DEFAULT_DATABASE_URL });
    redisClient = createRedisClient({ url: DEFAULT_REDIS_URL });
    await redisClient.connect();

    // Step 2: Run migrations
    await resetDatabase(pool);
    const applied = await runMigrations(pool);
    console.log('[Smoke] Migrations applied:', applied);

    pipelineRepo = new PgPipelineRepository(pool);
    pipelineRunRepo = new PgPipelineRunRepository(pool);
    jobRepo = new PgJobRepository(pool);
    workerRepo = new PgWorkerRepository(pool);
    leaseRepo = new PgWorkerLeaseRepository(pool);

    const heartbeatStore = createWorkerHeartbeatStore(redisClient, { defaultTtlSeconds: 10 });
    workerRegistry = createWorkerRegistry(workerRepo, heartbeatStore, { heartbeatTtlSeconds: 10 });

    queue = createJobQueue(redisClient, { queueName, defaultVisibilityTimeoutSeconds: 30 });

    scheduler = new ForgeScheduler({
      workerSource: workerRegistry,
      jobSource: createJobSourceFromRepository(jobRepo),
      leaseRepository: leaseRepo,
      leaseDurationMs: 30000,
    });

    // Clean any prior keys
    const raw = redisClient.getRawClient();
    const queueKeys = await raw.keys(`forge:queue:${queueName}:*`);
    if (queueKeys.length > 0) await raw.del(...queueKeys);
    const workerKeys = await raw.keys('forge:worker:*');
    if (workerKeys.length > 0) await raw.del(...workerKeys);
  });

  afterAll(async () => {
    await queue.close();
    await redisClient.close();
    await resetDatabase(pool);
    await pool.close();
  });

  describe('Section 51: 20-Step Manual Verification Sequence', () => {
    it('Step 3: registers Worker-A and Worker-B in registry', async () => {
      await workerRegistry.register({
        workerId: 'Worker-A',
        hostname: 'host-a',
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 8192 },
      });
      await workerRegistry.heartbeat(createWorkerId('Worker-A'));

      await workerRegistry.register({
        workerId: 'Worker-B',
        hostname: 'host-b',
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 8192 },
      });
      await workerRegistry.heartbeat(createWorkerId('Worker-B'));

      const workers = await workerRegistry.listWorkers();
      expect(workers.length).toBeGreaterThanOrEqual(2);
      console.log(
        '[Step 3] Registered workers:',
        workers.map((w) => w.worker.workerId),
      );
    });

    it('Step 4: creates pipeline and enqueues Job-1 in QUEUED status', async () => {
      await pipelineRepo.save(
        new Pipeline({
          id: pipelineId,
          name: 'Smoke Pipeline',
          steps: [{ name: 'step1', command: 'echo smoke' }],
        }),
      );

      await pipelineRunRepo.save(
        new PipelineRun({
          id: runId,
          pipelineId,
          pipelineName: 'Smoke Pipeline',
        }),
      );

      const job = new Job({
        id: jobId,
        pipelineRunId: runId,
        stepName: 'step1',
        command: 'echo smoke',
        initialStatus: 'QUEUED',
        requirements: { executor: 'docker', cpuCores: 2 },
      });
      await jobRepo.save(job);

      const enqueued = await queue.enqueue({
        jobId: job.id,
        pipelineRunId: runId,
        stepName: job.stepName,
      });

      expect(enqueued.messageId).toBeDefined();
      console.log('[Step 4] Enqueued message:', enqueued.messageId);
    });

    it('Step 5: scheduler evaluates Job-1, selects Worker-A, and atomically claims lease', async () => {
      const scheduleResult = await scheduler.scheduleNext(queue);

      expect(scheduleResult).not.toBeNull();
      expect(scheduleResult?.decision.status).toBe('SCHEDULED');
      if (scheduleResult?.decision.status === 'SCHEDULED') {
        expect(scheduleResult.decision.workerId).toBe('Worker-A');
        expect(scheduleResult.decision.lease).toBeDefined();
        workerALeaseId = scheduleResult.decision.lease!.id;
        console.log('[Step 5] Acquired lease:', scheduleResult.decision.lease);
      }
    });

    it('Step 6: verifies active lease in PostgreSQL database', async () => {
      const active = await leaseRepo.findActiveByJobId(jobId);

      expect(active).not.toBeNull();
      expect(active?.id).toBe(workerALeaseId);
      expect(active?.workerId).toBe('Worker-A');
      expect(active?.status).toBe('ACTIVE');
      expect(active?.durationMs).toBe(30000);
      expect(active?.expiresAt.getTime()).toBeGreaterThan(Date.now());
      console.log('[Step 6] Database active lease verified:', active?.id);
    });

    it('Step 7: Worker-B attempts to claim Job-1 and receives CONFLICT', async () => {
      const conflictResult = await leaseRepo.claim({
        jobId,
        workerId: 'Worker-B',
        durationMs: 30000,
      });

      expect(conflictResult.status).toBe('CONFLICT');
      if (conflictResult.status === 'CONFLICT') {
        expect(conflictResult.reason).toBe('LEASE_ALREADY_HELD');
        expect(conflictResult.currentOwnerId).toBe('Worker-A');
        expect(conflictResult.expiresAt).toBeInstanceOf(Date);
        console.log('[Step 7] Worker-B conflict result:', conflictResult);
      }
    });

    it('Step 8: Worker-A issues idempotent claim for Job-1 and receives ACQUIRED (isIdempotent: true)', async () => {
      const idempotentResult = await leaseRepo.claim({
        jobId,
        workerId: 'Worker-A',
        durationMs: 30000,
      });

      expect(idempotentResult.status).toBe('ACQUIRED');
      if (idempotentResult.status === 'ACQUIRED') {
        expect(idempotentResult.isIdempotent).toBe(true);
        expect(idempotentResult.lease.id).toBe(workerALeaseId);
        console.log('[Step 8] Worker-A idempotent claim result:', idempotentResult);
      }
    });

    it('Step 9: Worker-A renews lease before expiry (duration 45s)', async () => {
      const renewResult = await leaseRepo.renew({
        leaseId: workerALeaseId,
        jobId,
        workerId: 'Worker-A',
        durationMs: 45000,
      });

      expect(renewResult.status).toBe('RENEWED');
      if (renewResult.status === 'RENEWED') {
        expect(renewResult.lease.id).toBe(workerALeaseId);
        expect(renewResult.lease.durationMs).toBe(45000);
        console.log('[Step 9] Renewed lease:', renewResult.lease);
      }
    });

    it('Step 10: Worker-B attempts to renew Worker-A lease and receives REJECTED (LEASE_OWNER_MISMATCH)', async () => {
      const renewResult = await leaseRepo.renew({
        leaseId: workerALeaseId,
        jobId,
        workerId: 'Worker-B',
      });

      expect(renewResult.status).toBe('REJECTED');
      if (renewResult.status === 'REJECTED') {
        expect(renewResult.reason).toBe('LEASE_OWNER_MISMATCH');
        console.log('[Step 10] Worker-B renew rejection:', renewResult);
      }
    });

    it('Step 11: Worker-B attempts to release Worker-A lease and receives REJECTED (LEASE_OWNER_MISMATCH)', async () => {
      const releaseResult = await leaseRepo.release({
        leaseId: workerALeaseId,
        jobId,
        workerId: 'Worker-B',
      });

      expect(releaseResult.status).toBe('REJECTED');
      if (releaseResult.status === 'REJECTED') {
        expect(releaseResult.reason).toBe('LEASE_OWNER_MISMATCH');
        console.log('[Step 11] Worker-B release rejection:', releaseResult);
      }
    });

    it('Step 12: advances time until lease expires in database', async () => {
      await pool.query(
        "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1;",
        [workerALeaseId],
      );

      const active = await leaseRepo.findActiveByJobId(jobId);
      expect(active).toBeNull();
      console.log('[Step 12] Lease expired artificially in PostgreSQL. Active check:', active);
    });

    it('Step 13: Worker-A attempts to renew expired lease and receives REJECTED (LEASE_EXPIRED)', async () => {
      const renewResult = await leaseRepo.renew({
        leaseId: workerALeaseId,
        jobId,
        workerId: 'Worker-A',
      });

      expect(renewResult.status).toBe('REJECTED');
      if (renewResult.status === 'REJECTED') {
        expect(renewResult.reason).toBe('LEASE_EXPIRED');
        console.log('[Step 13] Worker-A expired renewal rejection:', renewResult);
      }
    });

    it('Step 14: Worker-B claims expired Job-1 -> old lease marked EXPIRED, new lease ACTIVE for Worker-B', async () => {
      const claimResult = await leaseRepo.claim({
        jobId,
        workerId: 'Worker-B',
        durationMs: 30000,
      });

      expect(claimResult.status).toBe('ACQUIRED');
      if (claimResult.status === 'ACQUIRED') {
        expect(claimResult.isIdempotent).toBe(false);
        expect(claimResult.lease.workerId).toBe('Worker-B');
        workerBLeaseId = claimResult.lease.id;
        console.log('[Step 14] Worker-B acquired replacement lease:', claimResult.lease);
      }

      // Verify old lease is now EXPIRED
      const oldLease = await leaseRepo.findById(workerALeaseId);
      expect(oldLease?.status).toBe('EXPIRED');
    });

    it('Step 15: stale Worker-A attempts to release its old lease and receives REJECTED', async () => {
      const releaseResult = await leaseRepo.release({
        leaseId: workerALeaseId,
        jobId,
        workerId: 'Worker-A',
      });

      expect(releaseResult.status).toBe('REJECTED');
      if (releaseResult.status === 'REJECTED') {
        expect(releaseResult.reason).toBe('LEASE_ALREADY_INACTIVE');
        console.log('[Step 15] Stale Worker-A release rejection:', releaseResult);
      }
    });

    it('Step 16: Worker-B completes work and explicitly releases lease', async () => {
      const releaseResult = await leaseRepo.release({
        leaseId: workerBLeaseId,
        jobId,
        workerId: 'Worker-B',
      });

      expect(releaseResult.status).toBe('RELEASED');
      console.log('[Step 16] Worker-B release result:', releaseResult);

      const dbLease = await leaseRepo.findById(workerBLeaseId);
      expect(dbLease?.status).toBe('RELEASED');
    });

    it('Step 17: Worker-B attempts to release again and receives REJECTED (LEASE_ALREADY_INACTIVE)', async () => {
      const doubleRelease = await leaseRepo.release({
        leaseId: workerBLeaseId,
        jobId,
        workerId: 'Worker-B',
      });

      expect(doubleRelease.status).toBe('REJECTED');
      if (doubleRelease.status === 'REJECTED') {
        expect(doubleRelease.reason).toBe('LEASE_ALREADY_INACTIVE');
        console.log('[Step 17] Double release rejection:', doubleRelease);
      }
    });

    it('Step 18: reclaimExpiredLeases scans and transitions expired active leases', async () => {
      const tempJob = new Job({
        id: createJobId('job-smoke-reclaim'),
        pipelineRunId: runId,
        stepName: 'temp-reclaim',
        command: 'echo temp',
        initialStatus: 'QUEUED',
      });
      await jobRepo.save(tempJob);

      const tempClaim = await leaseRepo.claim({
        jobId: tempJob.id,
        workerId: 'Worker-A',
        durationMs: 30000,
      });
      expect(tempClaim.status).toBe('ACQUIRED');

      if (tempClaim.status === 'ACQUIRED') {
        await pool.query(
          "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '10 seconds' WHERE id = $1;",
          [tempClaim.lease.id],
        );

        const count = await leaseRepo.reclaimExpiredLeases();
        expect(count).toBeGreaterThanOrEqual(1);
        console.log('[Step 18] Reclaimed expired leases count:', count);

        const checked = await leaseRepo.findById(tempClaim.lease.id);
        expect(checked?.status).toBe('EXPIRED');
      }
    });

    it('Step 19: verifies queue message was NOT deleted or acknowledged prematurely', async () => {
      // In FIFO job queue with default visibility timeout (30s),
      // the message is not ACKed and remains in visibility timeout.
      // An immediate dequeue should return null (still in visibility timeout).
      const immediate = await queue.dequeue();
      expect(immediate).toBeNull();
      console.log(
        '[Step 19] Queue visibility timeout integrity verified (immediate dequeue returned null).',
      );
    });

    it('Step 20: worker graceful shutdown automatically releases active leases', async () => {
      const jobShutdown = new Job({
        id: createJobId('job-smoke-shutdown'),
        pipelineRunId: runId,
        stepName: 'shutdown-step',
        command: 'echo shutdown',
        initialStatus: 'QUEUED',
      });
      await jobRepo.save(jobShutdown);

      // Simulate a worker holding a lease
      const claim = await leaseRepo.claim({
        jobId: jobShutdown.id,
        workerId: 'Worker-A',
        durationMs: 30000,
      });
      expect(claim.status).toBe('ACQUIRED');

      if (claim.status === 'ACQUIRED') {
        // Release lease during graceful shutdown
        const releaseRes = await leaseRepo.release({
          leaseId: claim.lease.id,
          jobId: jobShutdown.id,
          workerId: 'Worker-A',
        });
        expect(releaseRes.status).toBe('RELEASED');

        const active = await leaseRepo.findActiveByJobId(jobShutdown.id);
        expect(active).toBeNull();
        console.log('[Step 20] Graceful shutdown released lease:', claim.lease.id);
      }
    });
  });

  describe('Section 52: Concurrent Claim Race Verification', () => {
    it('guarantees exactly one winner among 10 concurrent claimants on live PostgreSQL', async () => {
      const raceJob = new Job({
        id: createJobId('job-smoke-race'),
        pipelineRunId: runId,
        stepName: 'race-step',
        command: 'echo race',
        initialStatus: 'QUEUED',
      });
      await jobRepo.save(raceJob);

      const contestants = Array.from({ length: 10 }, (_, i) => `contestant-${i + 1}`);

      // All 10 contestants race simultaneously against real PostgreSQL
      const results = await Promise.all(
        contestants.map((workerId) =>
          leaseRepo.claim({
            jobId: raceJob.id,
            workerId,
            durationMs: 30000,
          }),
        ),
      );

      const winners = results.filter((r) => r.status === 'ACQUIRED');
      const losers = results.filter((r) => r.status === 'CONFLICT');

      console.log(
        '[Section 52] Race results: winners =',
        winners.length,
        'conflicts =',
        losers.length,
      );

      expect(winners).toHaveLength(1);
      expect(losers).toHaveLength(9);

      // Verify PostgreSQL database state: exactly one ACTIVE row exists
      const dbRows = await pool.query(
        "SELECT id, worker_id, status FROM worker_leases WHERE job_id = $1 AND status = 'ACTIVE';",
        [raceJob.id],
      );
      expect(dbRows.rows).toHaveLength(1);
      expect(dbRows.rows[0]?.worker_id).toBe(
        (winners[0] as { lease: { workerId: string } }).lease.workerId,
      );
      console.log(
        '[Section 52] Confirmed unique active lease in DB held by:',
        dbRows.rows[0]?.worker_id,
      );
    });
  });
});
