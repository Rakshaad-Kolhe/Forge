import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { WorkerLease } from '@forge/contracts';
import {
  createDatabasePool,
  DEFAULT_DATABASE_URL,
  LeaseRecoveryService,
  PgDeadLetterRepository,
  PgJobAttemptRepository,
  PgJobRepository,
  PgPipelineRepository,
  PgPipelineRunRepository,
  PgWorkerLeaseRepository,
  resetDatabase,
  runMigrations,
  type DatabasePool,
} from '@forge/database';
import { DockerExecutor } from '@forge/executor';
import {
  createJobId,
  createPipelineId,
  createPipelineRunId,
  Job,
  Pipeline,
  PipelineRun,
} from '@forge/pipeline';
import { startWorker, type WorkerShell } from './index.js';

describe('Worker Loss Recovery & Graceful Drain Integration (Real PostgreSQL + Real Docker)', () => {
  const databaseUrl = process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL;
  const dockerHost = process.env.DOCKER_HOST ?? 'tcp://172.31.91.254:2375';

  let pool: DatabasePool;
  let leaseRepo: PgWorkerLeaseRepository;
  let jobRepo: PgJobRepository;
  let attemptRepo: PgJobAttemptRepository;
  let pipelineRepo: PgPipelineRepository;
  let runRepo: PgPipelineRunRepository;
  let dlqRepo: PgDeadLetterRepository;
  let recoveryService: LeaseRecoveryService;
  let executor: DockerExecutor;
  let workerA: WorkerShell;
  let workerB: WorkerShell;
  let dockerAvailable = false;

  const testPipelineId = createPipelineId('pipe-loss-recovery');
  const testRunId = createPipelineRunId('run-loss-recovery');

  beforeAll(async () => {
    pool = createDatabasePool({ connectionString: databaseUrl, maxConnections: 10 });
    await resetDatabase(pool);
    await runMigrations(pool);

    leaseRepo = new PgWorkerLeaseRepository(pool);
    jobRepo = new PgJobRepository(pool);
    attemptRepo = new PgJobAttemptRepository(pool);
    pipelineRepo = new PgPipelineRepository(pool);
    runRepo = new PgPipelineRunRepository(pool);
    dlqRepo = new PgDeadLetterRepository(pool);
    recoveryService = new LeaseRecoveryService(pool);

    executor = new DockerExecutor({
      dockerHost,
      defaultImage: 'alpine:3.19',
      defaultTimeoutMs: 30000,
      user: '1000:1000',
    });

    dockerAvailable = await executor.isAvailable();
    if (!dockerAvailable) {
      console.warn('Docker daemon not available at', dockerHost, '- tests may be skipped');
    }

    workerA = startWorker({
      workerId: 'worker-node-alpha',
      leaseRepository: leaseRepo,
      jobRepository: jobRepo,
      pool,
      executor,
      defaultLeaseDurationMs: 15000,
      defaultLeaseRenewalIntervalMs: 2000,
    });

    workerB = startWorker({
      workerId: 'worker-node-beta',
      leaseRepository: leaseRepo,
      jobRepository: jobRepo,
      pool,
      executor,
      defaultLeaseDurationMs: 15000,
      defaultLeaseRenewalIntervalMs: 2000,
    });
  });

  afterAll(async () => {
    await workerA.stop();
    await workerB.stop();
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
      name: 'Loss Recovery Integration Pipeline',
      steps: [{ name: 'step1', command: 'echo test' }],
    });
    await pipelineRepo.save(pipeline);

    const run = new PipelineRun({
      id: testRunId,
      pipelineId: testPipelineId,
      pipelineName: 'Loss Recovery Integration Pipeline',
    });
    run.markQueued();
    run.start();
    await runRepo.save(run);
  });

  it('recovers lost worker executing Docker container, requeues with backoff, and Worker B succeeds on next attempt', async () => {
    if (!dockerAvailable) return;

    const jobId = createJobId('job-worker-lost-retry');
    const job = new Job({
      id: jobId,
      pipelineRunId: testRunId,
      stepName: 'crash-recovery-step',
      command: 'echo "hello from worker"',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 3,
        backoff: { baseDelayMs: 200, factor: 1, maxDelayMs: 1000 },
        retryOn: ['FAILED'],
      },
    });
    await jobRepo.save(job);

    // 1. Worker A claims job
    const claimResA = await workerA.claimJob(jobId, 10000);
    expect(claimResA.status).toBe('ACQUIRED');
    const leaseA = (claimResA as { lease: WorkerLease }).lease;

    // 2. Worker A executes job
    const execPromiseA = workerA.executeJob({ job, leaseId: leaseA.id });
    await execPromiseA;

    // Verify attempt 1 succeeded
    let savedJob = await jobRepo.findById(jobId);
    expect(savedJob?.status).toBe('SUCCEEDED');

    // Now test simulated crash during execution with a fresh job:
    const jobId2 = createJobId('job-worker-crash-in-flight');
    const job2 = new Job({
      id: jobId2,
      pipelineRunId: testRunId,
      stepName: 'in-flight-crash-step',
      command: 'echo "attempt 2 will succeed"',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 3,
        backoff: { baseDelayMs: 100, factor: 1, maxDelayMs: 1000 },
        retryOn: ['FAILED'],
      },
    });
    await jobRepo.save(job2);

    // Worker A claims job 2
    const claimRes2 = await workerA.claimJob(jobId2, 10000);
    expect(claimRes2.status).toBe('ACQUIRED');
    const lease2 = (claimRes2 as { lease: WorkerLease }).lease;

    // Simulate Worker A starting attempt 1 in DB
    const attempt1 = job2.createAttempt();
    attempt1.start();
    job2.start();
    await jobRepo.save(job2);
    await attemptRepo.save(attempt1);

    // Simulate Worker A abrupt loss: lease expires without renewal
    await pool.query(
      "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1;",
      [lease2.id],
    );

    // Run recovery service
    const recoveryResult = await recoveryService.recoverExpiredLeases();
    expect(recoveryResult.recoveredCount).toBe(1);
    expect(recoveryResult.details[0]?.action).toBe('REQUEUED');
    expect(recoveryResult.details[0]?.leaseId).toBe(lease2.id);

    // Verify lease is now EXPIRED
    const expiredLease = await leaseRepo.findById(lease2.id);
    expect(expiredLease?.status).toBe('EXPIRED');

    // Verify attempt 1 is marked FAILED with WORKER_LOST
    const attempts = await attemptRepo.findByJobId(jobId2);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.status).toBe('FAILED');
    expect(attempts[0]?.failureReason).toBe('WORKER_LOST');

    // Verify job is reset to QUEUED
    savedJob = await jobRepo.findById(jobId2);
    expect(savedJob?.status).toBe('QUEUED');

    // 3. Worker B claims the recovered job with a fresh lease
    const claimResB = await workerB.claimJob(jobId2, 10000);
    expect(claimResB.status).toBe('ACQUIRED');
    const leaseB = (claimResB as { lease: WorkerLease }).lease;
    expect(leaseB.id).not.toBe(lease2.id);
    expect(leaseB.workerId).toBe('worker-node-beta');

    // 4. Worker B executes job successfully in Docker
    const execResultB = await workerB.executeJob({ job: savedJob!, leaseId: leaseB.id });
    expect(execResultB.result.status).toBe('SUCCEEDED');
    expect(execResultB.result.exitCode).toBe(0);

    // Verify attempt history: Attempt 1 (FAILED: WORKER_LOST) + Attempt 2 (SUCCEEDED)
    const finalAttempts = await attemptRepo.findByJobId(jobId2);
    expect(finalAttempts).toHaveLength(2);
    expect(finalAttempts[0]?.status).toBe('FAILED');
    expect(finalAttempts[0]?.failureReason).toBe('WORKER_LOST');
    expect(finalAttempts[1]?.status).toBe('SUCCEEDED');
    expect(finalAttempts[1]?.exitCode).toBe(0);

    // Job is now SUCCEEDED
    const finalJob = await jobRepo.findById(jobId2);
    expect(finalJob?.status).toBe('SUCCEEDED');
  });

  it('sends job to DLQ when worker is lost and retry policy is exhausted', async () => {
    if (!dockerAvailable) return;

    const jobId = createJobId('job-exhausted-to-dlq');
    const job = new Job({
      id: jobId,
      pipelineRunId: testRunId,
      stepName: 'exhausted-step',
      command: 'echo "should not retry"',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 1, // Only 1 attempt
      },
    });
    await jobRepo.save(job);

    // Worker A claims job
    const claimRes = await workerA.claimJob(jobId, 5000);
    expect(claimRes.status).toBe('ACQUIRED');
    const lease = (claimRes as { lease: WorkerLease }).lease;

    // Worker A creates and starts attempt 1
    const attempt1 = job.createAttempt();
    attempt1.start();
    job.start();
    await jobRepo.save(job);
    await attemptRepo.save(attempt1);

    // Simulate Worker A abrupt loss
    await pool.query(
      "UPDATE worker_leases SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1;",
      [lease.id],
    );

    // Run recovery
    const recoveryResult = await recoveryService.recoverExpiredLeases();
    expect(recoveryResult.recoveredCount).toBe(1);
    expect(recoveryResult.details[0]?.action).toBe('DEAD_LETTERED');
    expect(recoveryResult.details[0]?.deadLetterReason).toBe('WORKER_LOSS_RETRY_EXHAUSTED');

    // Job is permanently FAILED
    const finalJob = await jobRepo.findById(jobId);
    expect(finalJob?.status).toBe('FAILED');

    // DLQ record created with authoritative metadata
    const dlqRecord = await dlqRepo.findByJobId(jobId);
    expect(dlqRecord).not.toBeNull();
    expect(dlqRecord?.jobId).toBe(jobId);
    expect(dlqRecord?.reason).toBe('WORKER_LOSS_RETRY_EXHAUSTED');
    expect(dlqRecord?.failedAttemptCount).toBe(1);
    expect(dlqRecord?.lastWorkerId).toBe('worker-node-alpha');

    // Worker B cannot claim the job
    const claimResB = await workerB.claimJob(jobId, 5000);
    expect(claimResB.status).toBe('NOT_CLAIMABLE');
  });

  it('gracefully drains in-flight Docker execution: completes active container, releases lease, and rejects new work', async () => {
    if (!dockerAvailable) return;

    // Create a standalone worker for drain testing
    const drainWorker = startWorker({
      workerId: 'worker-node-drain',
      leaseRepository: leaseRepo,
      jobRepository: jobRepo,
      pool,
      executor,
      defaultLeaseDurationMs: 15000,
    });

    const jobId = createJobId('job-drain-test');
    const job = new Job({
      id: jobId,
      pipelineRunId: testRunId,
      stepName: 'drain-step',
      command: 'sleep 1 && echo "drain container finished"',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    // Claim job
    const claimRes = await drainWorker.claimJob(jobId, 10000);
    expect(claimRes.status).toBe('ACQUIRED');
    const lease = (claimRes as { lease: WorkerLease }).lease;

    // Start executing the Docker container in the background
    const execPromise = drainWorker.executeJob({ job, leaseId: lease.id });

    // Give container a moment to spin up and register as active task
    await new Promise((r) => setTimeout(r, 100));

    // Initiate graceful stop with 5s drain timeout
    const stopPromise = drainWorker.stop({ timeoutMs: 5000 });

    // Verify worker status is now DRAINING
    expect(drainWorker.getStatus()).toBe('DRAINING');

    // Verify worker rejects new claims immediately
    const claimWhileDraining = await drainWorker.claimJob('job-another');
    expect(claimWhileDraining.status).toBe('NOT_CLAIMABLE');

    // Await both execution and stop completion
    const [execResult] = await Promise.all([execPromise, stopPromise]);
    expect(execResult.result.status).toBe('SUCCEEDED');
    expect(execResult.result.exitCode).toBe(0);

    // Verify worker is now OFFLINE
    expect(drainWorker.getStatus()).toBe('OFFLINE');

    // Verify job in database is SUCCEEDED
    const finishedJob = await jobRepo.findById(jobId);
    expect(finishedJob?.status).toBe('SUCCEEDED');
  });
});
