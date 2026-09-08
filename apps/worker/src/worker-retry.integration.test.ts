import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createDatabasePool,
  PgJobAttemptRepository,
  PgJobRepository,
  PgPipelineRepository,
  PgPipelineRunRepository,
  PgWorkerLeaseRepository,
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
import { createJobSourceFromRepository, ForgeScheduler } from '@forge/scheduler';
import { startWorker, type WorkerShell } from './index.js';

describe('Worker Retry & Attempt Orchestration Integration (Real PostgreSQL + Real Docker)', () => {
  const databaseUrl = process.env.DATABASE_URL ?? 'postgresql://forge:forge@127.0.0.1:5432/forge';
  const dockerHost = process.env.DOCKER_HOST ?? 'tcp://172.31.91.254:2375';

  let pool: DatabasePool;
  let leaseRepo: PgWorkerLeaseRepository;
  let jobRepo: PgJobRepository;
  let attemptRepo: PgJobAttemptRepository;
  let pipelineRepo: PgPipelineRepository;
  let runRepo: PgPipelineRunRepository;
  let executor: DockerExecutor;
  let workerA: WorkerShell;
  let workerB: WorkerShell;
  let dockerAvailable = false;

  beforeAll(async () => {
    pool = createDatabasePool({ connectionString: databaseUrl, maxConnections: 10 });
    await runMigrations(pool);

    leaseRepo = new PgWorkerLeaseRepository(pool);
    jobRepo = new PgJobRepository(pool);
    attemptRepo = new PgJobAttemptRepository(pool);
    pipelineRepo = new PgPipelineRepository(pool);
    runRepo = new PgPipelineRunRepository(pool);

    executor = new DockerExecutor({
      dockerHost,
      defaultImage: 'alpine:3.19',
      defaultTimeoutMs: 30000,
      user: '1000:1000',
    });

    dockerAvailable = await executor.isAvailable();

    workerA = startWorker({
      workerId: 'worker-node-alpha',
      leaseRepository: leaseRepo,
      pool,
      executor,
      defaultLeaseDurationMs: 30000,
    });

    workerB = startWorker({
      workerId: 'worker-node-beta',
      leaseRepository: leaseRepo,
      pool,
      executor,
      defaultLeaseDurationMs: 30000,
    });
  });

  afterAll(async () => {
    await workerA.stop();
    await workerB.stop();
    await pool.close();
  });

  async function createTestPipelineAndRun(prefix: string) {
    const pipelineId = createPipelineId(`pipe-retry-${prefix}-${Date.now()}`);
    const pipeline = new Pipeline({
      id: pipelineId,
      name: `Retry Test Pipeline ${prefix}`,
      steps: [{ name: 'step-1', command: 'echo test' }],
    });
    await pipelineRepo.save(pipeline);

    const runId = createPipelineRunId(`run-retry-${prefix}-${Date.now()}`);
    const run = new PipelineRun({
      id: runId,
      pipelineId,
      pipelineName: pipeline.name,
      initialStatus: 'RUNNING',
    });
    await runRepo.save(run);

    return { pipeline, run };
  }

  it('verifies Docker daemon availability for retry integration testing', () => {
    expect(dockerAvailable).toBe(true);
  });

  it('Test 1: Single attempt (maxAttempts = 1) permanently fails on non-zero exit with no retry', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('single');
    const jobId = createJobId(`job-single-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'build',
      command: 'echo "Failing single attempt" && exit 42',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 1,
      },
    });
    await jobRepo.save(job);

    // Worker A claims lease
    const claimRes = await workerA.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    // Worker A executes
    const execRes = await workerA.executeJob({
      job,
      leaseId: claimRes.lease.id,
    });

    expect(execRes.result.status).toBe('FAILED');
    expect(execRes.job.status).toBe('FAILED');
    expect(execRes.retryDecision?.action).toBe('FINAL_FAILURE');
    expect(execRes.retryDecision?.reason).toBe('MAX_ATTEMPTS_EXHAUSTED');

    // Authoritative state in PostgreSQL
    const persisted = await jobRepo.findById(job.id);
    expect(persisted?.status).toBe('FAILED');
    expect(persisted?.nextAttemptAt).toBeUndefined();

    const attempts = await attemptRepo.findByJobId(job.id);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.attemptNumber).toBe(1);
    expect(attempts[0]?.status).toBe('FAILED');
    expect(attempts[0]?.exitCode).toBe(42);

    // Lease released
    const activeLease = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease).toBeNull();
  });

  it('Test 2: Fail then succeed across attempts with backoff delay and fresh leases', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('recover');
    const jobId = createJobId(`job-recover-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'test',
      command: 'exit 1', // will be overridden in attempt 2
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 2,
        backoff: {
          baseDelayMs: 200,
          factor: 2,
          maxDelayMs: 1000,
        },
        retryOn: ['FAILED'],
      },
    });
    await jobRepo.save(job);

    // --- ATTEMPT 1: Fails ---
    const claim1 = await workerA.claimJob(job.id);
    expect(claim1.status).toBe('ACQUIRED');
    if (claim1.status !== 'ACQUIRED') return;

    const exec1 = await workerA.executeJob({
      job,
      leaseId: claim1.lease.id,
    });

    expect(exec1.result.status).toBe('FAILED');
    expect(exec1.job.status).toBe('QUEUED');
    expect(exec1.retryDecision?.action).toBe('RETRY');
    if (exec1.retryDecision?.action === 'RETRY') {
      expect(exec1.retryDecision.nextAttemptNumber).toBe(2);
      expect(exec1.retryDecision.delayMs).toBe(200);
    }

    // Verify DB state after attempt 1
    const jobAfterAtt1 = await jobRepo.findById(job.id);
    expect(jobAfterAtt1?.status).toBe('QUEUED');
    expect(jobAfterAtt1?.nextAttemptAt).toBeDefined();

    const attemptsAfter1 = await attemptRepo.findByJobId(job.id);
    expect(attemptsAfter1).toHaveLength(1);
    expect(attemptsAfter1[0]?.attemptNumber).toBe(1);
    expect(attemptsAfter1[0]?.status).toBe('FAILED');

    // Lease 1 must be released
    const activeLease1 = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease1).toBeNull();

    // --- Wait for backoff delay (200ms) ---
    await new Promise((resolve) => setTimeout(resolve, 250));

    // --- ATTEMPT 2: Succeeds ---
    // Update command for attempt 2 to succeed
    const jobForAtt2 = (await jobRepo.findById(job.id))!;
    (jobForAtt2 as { command: string }).command = 'echo "Attempt 2 succeeded" && exit 0';

    const claim2 = await workerA.claimJob(job.id);
    expect(claim2.status).toBe('ACQUIRED');
    if (claim2.status !== 'ACQUIRED') return;

    // Verify fresh lease invariant: claim2 lease id != claim1 lease id
    expect(claim2.lease.id).not.toBe(claim1.lease.id);

    const exec2 = await workerA.executeJob({
      job: jobForAtt2,
      leaseId: claim2.lease.id,
    });

    expect(exec2.result.status).toBe('SUCCEEDED');
    expect(exec2.job.status).toBe('SUCCEEDED');
    expect(exec2.attempt.attemptNumber).toBe(2);

    // Verify final DB state
    const jobFinal = await jobRepo.findById(job.id);
    expect(jobFinal?.status).toBe('SUCCEEDED');
    expect(jobFinal?.nextAttemptAt).toBeUndefined();

    const attemptsFinal = await attemptRepo.findByJobId(job.id);
    expect(attemptsFinal).toHaveLength(2);
    expect(attemptsFinal[0]?.attemptNumber).toBe(1);
    expect(attemptsFinal[0]?.status).toBe('FAILED');
    expect(attemptsFinal[1]?.attemptNumber).toBe(2);
    expect(attemptsFinal[1]?.status).toBe('SUCCEEDED');

    // Both leases released
    const activeLeaseFinal = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLeaseFinal).toBeNull();
  });

  it('Test 3: Retry exhaustion: job fails 3 consecutive times and transitions to terminal FAILED', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('exhaust');
    const jobId = createJobId(`job-exhaust-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'test',
      command: 'exit 99',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 3,
        backoff: {
          baseDelayMs: 50,
          factor: 2,
          maxDelayMs: 200,
        },
        retryOn: ['FAILED'],
      },
    });
    await jobRepo.save(job);

    const leaseIds: string[] = [];

    // Execute attempts 1 through 3
    for (let attemptNum = 1; attemptNum <= 3; attemptNum++) {
      if (attemptNum > 1) {
        await new Promise((resolve) => setTimeout(resolve, 150));
      }

      const freshJob = (await jobRepo.findById(job.id))!;
      const claim = await workerA.claimJob(job.id);
      expect(claim.status).toBe('ACQUIRED');
      if (claim.status !== 'ACQUIRED') return;
      leaseIds.push(claim.lease.id);

      const exec = await workerA.executeJob({
        job: freshJob,
        leaseId: claim.lease.id,
      });

      expect(exec.result.status).toBe('FAILED');
      expect(exec.attempt.attemptNumber).toBe(attemptNum);

      if (attemptNum < 3) {
        expect(exec.job.status).toBe('QUEUED');
        expect(exec.retryDecision?.action).toBe('RETRY');
      } else {
        expect(exec.job.status).toBe('FAILED');
        expect(exec.retryDecision?.action).toBe('FINAL_FAILURE');
        expect(exec.retryDecision?.reason).toBe('MAX_ATTEMPTS_EXHAUSTED');
      }

      // Verify lease is released after every attempt
      const active = await leaseRepo.findActiveByJobId(job.id);
      expect(active).toBeNull();
    }

    // Verify all lease IDs were unique
    expect(new Set(leaseIds).size).toBe(3);

    // Verify authoritative DB state
    const finalJob = await jobRepo.findById(job.id);
    expect(finalJob?.status).toBe('FAILED');
    expect(finalJob?.nextAttemptAt).toBeUndefined();

    const allAttempts = await attemptRepo.findByJobId(job.id);
    expect(allAttempts).toHaveLength(3);
    expect(allAttempts.map((a) => a.attemptNumber)).toEqual([1, 2, 3]);
    expect(allAttempts.every((a) => a.status === 'FAILED')).toBe(true);
  });

  it('Test 4: Backoff timing verification — nextAttemptAt reflects calculated delay', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('timing');
    const jobId = createJobId(`job-timing-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'timing-step',
      command: 'exit 1',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 2,
        backoff: {
          baseDelayMs: 5000,
          factor: 2,
          maxDelayMs: 20000,
        },
        retryOn: ['FAILED'],
      },
    });
    await jobRepo.save(job);

    const beforeExecution = Date.now();
    const claim = await workerA.claimJob(job.id);
    expect(claim.status).toBe('ACQUIRED');
    if (claim.status !== 'ACQUIRED') return;

    const exec = await workerA.executeJob({
      job,
      leaseId: claim.lease.id,
    });

    expect(exec.job.status).toBe('QUEUED');
    const persisted = await jobRepo.findById(job.id);
    expect(persisted?.nextAttemptAt).toBeDefined();

    const scheduledTime = persisted!.nextAttemptAt!.getTime();
    const expectedTime = beforeExecution + 5000;
    // Difference should be within 1500ms (container execution duration tolerance)
    expect(Math.abs(scheduledTime - expectedTime)).toBeLessThan(1500);
  });

  it('Test 5: Retry on different worker (worker hopping between attempts)', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('hop');
    const jobId = createJobId(`job-hop-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'hop-step',
      command: 'exit 1',
      initialStatus: 'QUEUED',
      retryPolicy: {
        maxAttempts: 2,
        backoff: {
          baseDelayMs: 100,
          factor: 1,
          maxDelayMs: 1000,
        },
        retryOn: ['FAILED'],
      },
    });
    await jobRepo.save(job);

    // Attempt 1 on Worker Node Alpha
    const claimA = await workerA.claimJob(job.id);
    expect(claimA.status).toBe('ACQUIRED');
    if (claimA.status !== 'ACQUIRED') return;

    await workerA.executeJob({
      job,
      leaseId: claimA.lease.id,
    });

    // Wait for backoff
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Attempt 2 on Worker Node Beta
    const jobForB = (await jobRepo.findById(job.id))!;
    (jobForB as { command: string }).command = 'echo "Worker B succeeded" && exit 0';

    const claimB = await workerB.claimJob(job.id);
    expect(claimB.status).toBe('ACQUIRED');
    if (claimB.status !== 'ACQUIRED') return;
    expect(claimB.lease.workerId).toBe('worker-node-beta');

    const execB = await workerB.executeJob({
      job: jobForB,
      leaseId: claimB.lease.id,
    });

    expect(execB.job.status).toBe('SUCCEEDED');
    expect(execB.attempt.attemptNumber).toBe(2);

    const attempts = await attemptRepo.findByJobId(job.id);
    expect(attempts).toHaveLength(2);
    expect(attempts[0]?.attemptNumber).toBe(1);
    expect(attempts[0]?.status).toBe('FAILED');
    expect(attempts[1]?.attemptNumber).toBe(2);
    expect(attempts[1]?.status).toBe('SUCCEEDED');
  });

  it('Test 6: Scheduler discovers and prioritizes due retry jobs over lower priority jobs', async () => {
    // Clean up any lingering queued test jobs to guarantee test isolation
    await pool.query("DELETE FROM jobs WHERE status = 'QUEUED'");

    const { run } = await createTestPipelineAndRun('sched');

    // High-priority job due for retry
    const highRetryJob = new Job({
      id: createJobId(`job-sched-high-${Date.now()}`),
      pipelineRunId: run.id,
      stepName: 'high-step',
      command: 'echo "high"',
      priority: 90,
      initialStatus: 'QUEUED',
      nextAttemptAt: new Date(Date.now() - 5000), // due 5 seconds ago
    });
    await jobRepo.save(highRetryJob);

    // Low-priority job due for retry
    const lowRetryJob = new Job({
      id: createJobId(`job-sched-low-${Date.now()}`),
      pipelineRunId: run.id,
      stepName: 'low-step',
      command: 'echo "low"',
      priority: 10,
      initialStatus: 'QUEUED',
      nextAttemptAt: new Date(Date.now() - 2000),
    });
    await jobRepo.save(lowRetryJob);

    // Future-retry job (NOT due yet)
    const futureJob = new Job({
      id: createJobId(`job-sched-future-${Date.now()}`),
      pipelineRunId: run.id,
      stepName: 'future-step',
      command: 'echo "future"',
      priority: 100,
      initialStatus: 'QUEUED',
      nextAttemptAt: new Date(Date.now() + 60000), // 1 minute in future
    });
    await jobRepo.save(futureJob);

    const jobSource = createJobSourceFromRepository(jobRepo);
    const mockWorkerSource = {
      listWorkers: async () => [
        {
          workerId: 'worker-sched-1',
          capabilities: { executors: ['docker'] },
          resources: { cpuCores: 8, memoryBytes: 16384 },
          status: 'READY' as const,
          liveness: 'ALIVE' as const,
        },
      ],
    };

    const scheduler = new ForgeScheduler({
      jobSource,
      workerSource: mockWorkerSource,
      leaseRepository: leaseRepo,
    });

    const scheduleResult = await scheduler.scheduleDueJobs();

    // Only highRetryJob and lowRetryJob should be returned, NOT futureJob
    const relevantDecisions = scheduleResult.orderedDecisions.filter(
      (d) => d.jobId === highRetryJob.id || d.jobId === lowRetryJob.id || d.jobId === futureJob.id,
    );
    expect(relevantDecisions.map((d) => d.jobId)).toEqual([highRetryJob.id, lowRetryJob.id]);

    // Clean up leases claimed by scheduler
    for (const dec of scheduleResult.scheduledDecisions) {
      if (dec.lease) {
        await leaseRepo.release({
          leaseId: dec.lease.id,
          jobId: dec.jobId,
          workerId: 'worker-sched-1',
        });
      }
    }

    // Clean up test jobs
    await pool.query('DELETE FROM jobs WHERE id IN ($1, $2, $3)', [
      highRetryJob.id,
      lowRetryJob.id,
      futureJob.id,
    ]);
  });

  it('Test 7: User cancellation overrides scheduled retry state', async () => {
    const { run } = await createTestPipelineAndRun('cancel');
    const jobId = createJobId(`job-cancel-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'cancel-step',
      command: 'echo "test"',
      initialStatus: 'QUEUED',
      nextAttemptAt: new Date(Date.now() + 10000),
    });
    await jobRepo.save(job);

    // User cancels the job
    const jobToCancel = (await jobRepo.findById(job.id))!;
    jobToCancel.cancel();
    jobToCancel.clearNextAttemptAt();
    await jobRepo.save(jobToCancel);

    // Verify DB state
    const cancelledJob = await jobRepo.findById(job.id);
    expect(cancelledJob?.status).toBe('CANCELLED');
    expect(cancelledJob?.nextAttemptAt).toBeUndefined();

    // Query findSchedulableJobs: CANCELLED job must NOT be returned
    const schedulable = await jobRepo.findSchedulableJobs();
    expect(schedulable.some((j) => j.id === job.id)).toBe(false);
  });
});
