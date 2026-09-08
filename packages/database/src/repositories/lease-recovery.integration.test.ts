import type { WorkerLease } from '@forge/contracts';
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
import { LeaseRecoveryService } from '../lease-recovery-service.js';
import { resetDatabase, runMigrations } from '../migrations/migrator.js';
import type { DatabasePool } from '../types.js';
import { PgDeadLetterRepository } from './pg-dead-letter-repository.js';
import { PgJobAttemptRepository } from './pg-job-attempt-repository.js';
import { PgJobRepository } from './pg-job-repository.js';
import { PgPipelineRepository } from './pg-pipeline-repository.js';
import { PgPipelineRunRepository } from './pg-pipeline-run-repository.js';
import { PgWorkerLeaseRepository } from './pg-worker-lease-repository.js';

describe('Real PostgreSQL Lease Recovery & DLQ Integration Tests', () => {
  let pool: DatabasePool;
  let pipelineRepo: PgPipelineRepository;
  let pipelineRunRepo: PgPipelineRunRepository;
  let jobRepo: PgJobRepository;
  let attemptRepo: PgJobAttemptRepository;
  let leaseRepo: PgWorkerLeaseRepository;
  let dlqRepo: PgDeadLetterRepository;
  let recoveryService: LeaseRecoveryService;

  const testPipelineId = createPipelineId('pipe-recovery-test');
  const testRunId = createPipelineRunId('run-recovery-test');

  beforeAll(async () => {
    pool = createDatabasePool({
      connectionString: DEFAULT_DATABASE_URL,
    });
    await resetDatabase(pool);
    await runMigrations(pool);

    pipelineRepo = new PgPipelineRepository(pool);
    pipelineRunRepo = new PgPipelineRunRepository(pool);
    jobRepo = new PgJobRepository(pool);
    attemptRepo = new PgJobAttemptRepository(pool);
    leaseRepo = new PgWorkerLeaseRepository(pool);
    dlqRepo = new PgDeadLetterRepository(pool);
    recoveryService = new LeaseRecoveryService(pool);
  });

  afterAll(async () => {
    await resetDatabase(pool);
    await pool.close();
  });

  beforeEach(async () => {
    await pool.query('DELETE FROM dead_letter_jobs;');
    await pool.query('DELETE FROM worker_leases;');
    await pool.query('DELETE FROM job_attempts;');
    await pool.query('DELETE FROM jobs;');
    await pool.query('DELETE FROM pipeline_runs;');
    await pool.query('DELETE FROM pipelines;');

    const pipeline = new Pipeline({
      id: testPipelineId,
      name: 'Recovery Integration Test Pipeline',
      steps: [{ name: 'step1', command: 'echo test' }],
    });
    await pipelineRepo.save(pipeline);

    const run = new PipelineRun({
      id: testRunId,
      pipelineId: testPipelineId,
      pipelineName: 'Recovery Integration Test Pipeline',
    });
    run.markQueued();
    run.start();
    await pipelineRunRepo.save(run);
  });

  it('recovers expired lease for retryable job: reconciles attempt as FAILED, requeues job with backoff', async () => {
    const jobId = createJobId('job-recover-retry');
    const job = new Job({
      id: jobId,
      pipelineRunId: testRunId,
      stepName: 'retry-step',
      command: 'exit 1',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 3,
        backoff: { baseDelayMs: 1500, factor: 2, maxDelayMs: 10000 },
        retryOn: ['FAILED'],
      },
    });
    await jobRepo.save(job);

    // Worker 1 claims job
    const claimRes = await leaseRepo.claim({
      jobId,
      workerId: 'worker-crashed-1',
      durationMs: 5000,
    });
    expect(claimRes.status).toBe('ACQUIRED');
    const lease = (claimRes as { lease: WorkerLease }).lease;

    // Worker 1 starts executing attempt 1
    const attempt = job.createAttempt();
    attempt.start();
    job.start();
    await jobRepo.save(job);

    // Simulate worker 1 disappearing: lease expires in database
    await pool.query(
      "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '10 seconds' WHERE id = $1;",
      [lease.id],
    );

    // Run recovery
    const recoveryResult = await recoveryService.recoverExpiredLeases();
    expect(recoveryResult.recoveredCount).toBe(1);
    expect(recoveryResult.details[0]?.action).toBe('REQUEUED');
    expect(recoveryResult.details[0]?.leaseId).toBe(lease.id);
    expect(recoveryResult.details[0]?.nextAttemptAt).toBeDefined();

    // Verify lease row is now EXPIRED
    const updatedLease = await leaseRepo.findById(lease.id);
    expect(updatedLease?.status).toBe('EXPIRED');

    // Verify JobAttempt 1 is reconciled as FAILED with failure_reason = WORKER_LOST
    const attempts = await attemptRepo.findByJobId(jobId);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.status).toBe('FAILED');
    expect(attempts[0]?.failureReason).toBe('WORKER_LOST');
    expect(attempts[0]?.finishedAt).toBeDefined();

    // Verify Job is QUEUED with next_attempt_at in the future
    const updatedJob = await jobRepo.findById(jobId);
    expect(updatedJob?.status).toBe('QUEUED');
    expect(updatedJob?.nextAttemptAt).toBeDefined();
    expect(updatedJob!.nextAttemptAt!.getTime()).toBeGreaterThan(Date.now() - 1000);

    // Verify NO DLQ record was created
    const dlqRecord = await dlqRepo.findByJobId(jobId);
    expect(dlqRecord).toBeNull();
  });

  it('concurrent recovery race: two concurrent recovery workers racing on same expired lease', async () => {
    const jobId = createJobId('job-concurrent-race');
    const job = new Job({
      id: jobId,
      pipelineRunId: testRunId,
      stepName: 'concurrent-step',
      command: 'echo test',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 2,
      },
    });
    await jobRepo.save(job);

    // Claim lease
    const claimRes = await leaseRepo.claim({
      jobId,
      workerId: 'worker-lost',
      durationMs: 5000,
    });
    const lease = (claimRes as { lease: WorkerLease }).lease;

    // Start attempt
    const attempt = job.createAttempt();
    attempt.start();
    job.start();
    await jobRepo.save(job);

    // Expire lease
    await pool.query(
      "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1;",
      [lease.id],
    );

    // Spawn two concurrent recovery operations using separate connections
    const service1 = new LeaseRecoveryService(pool);
    const service2 = new LeaseRecoveryService(pool);

    const [res1, res2] = await Promise.all([
      service1.recoverExpiredLeases(),
      service2.recoverExpiredLeases(),
    ]);

    // Total recovered count across both MUST be exactly 1
    const totalRecovered = res1.recoveredCount + res2.recoveredCount;
    expect(totalRecovered).toBe(1);

    // One must have REQUEUED, the other must have NO_OP or empty
    const allDetails = [...res1.details, ...res2.details];
    const requeued = allDetails.filter((d) => d.action === 'REQUEUED');
    expect(requeued).toHaveLength(1);

    // Verify exactly 1 attempt exists in database
    const attempts = await attemptRepo.findByJobId(jobId);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.status).toBe('FAILED');
    expect(attempts[0]?.failureReason).toBe('WORKER_LOST');
  });

  it('retry exhaustion: transitions job to FAILED and atomically inserts single DLQ record', async () => {
    const jobId = createJobId('job-exhaust-dlq');
    const job = new Job({
      id: jobId,
      pipelineRunId: testRunId,
      stepName: 'exhaust-step',
      command: 'exit 1',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 1, // Only 1 attempt allowed!
      },
    });
    await jobRepo.save(job);

    // Claim lease
    const claimRes = await leaseRepo.claim({
      jobId,
      workerId: 'worker-crash-dlq',
      durationMs: 5000,
    });
    const lease = (claimRes as { lease: WorkerLease }).lease;

    // Start attempt 1
    const attempt = job.createAttempt();
    attempt.start();
    job.start();
    await jobRepo.save(job);

    // Expire lease
    await pool.query(
      "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1;",
      [lease.id],
    );

    // Run recovery
    const recoveryResult = await recoveryService.recoverExpiredLeases();
    expect(recoveryResult.recoveredCount).toBe(1);
    expect(recoveryResult.details[0]?.action).toBe('DEAD_LETTERED');
    expect(recoveryResult.details[0]?.deadLetterReason).toBe('WORKER_LOSS_RETRY_EXHAUSTED');

    // Verify job is permanently FAILED
    const updatedJob = await jobRepo.findById(jobId);
    expect(updatedJob?.status).toBe('FAILED');
    expect(updatedJob?.nextAttemptAt).toBeUndefined();

    // Verify durable DLQ record
    const dlqRecord = await dlqRepo.findByJobId(jobId);
    expect(dlqRecord).not.toBeNull();
    expect(dlqRecord?.jobId).toBe(jobId);
    expect(dlqRecord?.pipelineRunId).toBe(testRunId);
    expect(dlqRecord?.reason).toBe('WORKER_LOSS_RETRY_EXHAUSTED');
    expect(dlqRecord?.failedAttemptCount).toBe(1);
    expect(dlqRecord?.lastAttemptId).toBe(attempt.id);
    expect(dlqRecord?.lastWorkerId).toBe('worker-crash-dlq');

    // Verify duplicate recovery produces no duplicate DLQ records
    const repeatedRecovery = await recoveryService.recoverExpiredLeases();
    expect(repeatedRecovery.recoveredCount).toBe(0);
    const totalDLQ = await dlqRepo.count();
    expect(totalDLQ).toBe(1);
  });

  it('rejects stale owner renewal after lease has been expired and recovered', async () => {
    const jobId = createJobId('job-stale-owner');
    const job = new Job({
      id: jobId,
      pipelineRunId: testRunId,
      stepName: 'stale-step',
      command: 'echo 1',
      initialStatus: 'QUEUED',
      retryPolicy: { maxAttempts: 2 },
    });
    await jobRepo.save(job);

    // Worker A claims lease
    const claimRes = await leaseRepo.claim({
      jobId,
      workerId: 'worker-A',
      durationMs: 5000,
    });
    const leaseA = (claimRes as { lease: WorkerLease }).lease;

    // Expire lease A and recover
    await pool.query(
      "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1;",
      [leaseA.id],
    );
    await recoveryService.recoverExpiredLeases();

    // Worker A wakes up and attempts to renew expired lease
    const renewResult = await leaseRepo.renew({
      leaseId: leaseA.id,
      jobId,
      workerId: 'worker-A',
    });
    expect(renewResult.status).toBe('REJECTED');
    if (renewResult.status === 'REJECTED') {
      expect(renewResult.reason).toBe('LEASE_EXPIRED');
    }

    // Worker B claims the recovered job with a fresh lease
    const claimResB = await leaseRepo.claim({
      jobId,
      workerId: 'worker-B',
      durationMs: 5000,
    });
    expect(claimResB.status).toBe('ACQUIRED');
    const leaseB = (claimResB as { lease: WorkerLease }).lease;
    expect(leaseB.id).not.toBe(leaseA.id);

    // Worker A attempts to renew Worker B's lease
    const staleAttack = await leaseRepo.renew({
      leaseId: leaseB.id,
      jobId,
      workerId: 'worker-A', // Old owner!
    });
    expect(staleAttack.status).toBe('REJECTED');
    if (staleAttack.status === 'REJECTED') {
      expect(staleAttack.reason).toBe('LEASE_OWNER_MISMATCH');
    }
  });

  it('never resurrects terminal jobs that were cancelled during worker execution', async () => {
    const jobId = createJobId('job-cancel-term');
    const job = new Job({
      id: jobId,
      pipelineRunId: testRunId,
      stepName: 'cancel-step',
      command: 'sleep 100',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    const claimRes = await leaseRepo.claim({
      jobId,
      workerId: 'worker-cancelled',
      durationMs: 5000,
    });
    const lease = (claimRes as { lease: WorkerLease }).lease;

    // Start attempt, then cancel job while worker is running
    const attempt = job.createAttempt();
    attempt.start();
    job.start();
    job.cancel(); // Operator cancelled!
    await jobRepo.save(job);

    // Lease expires
    await pool.query(
      "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1;",
      [lease.id],
    );

    // Run recovery
    const recoveryResult = await recoveryService.recoverExpiredLeases();
    expect(recoveryResult.recoveredCount).toBe(0);
    expect(recoveryResult.details[0]?.action).toBe('SKIPPED_TERMINAL');

    // Job MUST remain CANCELLED
    const refreshedJob = await jobRepo.findById(jobId);
    expect(refreshedJob?.status).toBe('CANCELLED');
  });
});
