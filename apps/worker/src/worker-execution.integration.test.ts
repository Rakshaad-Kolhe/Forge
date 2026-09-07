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
import { startWorker, type WorkerShell } from './index.js';

describe('Worker Execution & Lease Integration (Real PostgreSQL + Real Docker)', () => {
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
    const pipelineId = createPipelineId(`pipe-${prefix}-${Date.now()}`);
    const pipeline = new Pipeline({
      id: pipelineId,
      name: `Test Pipeline ${prefix}`,
      steps: [{ name: 'step-1', command: 'echo test' }],
    });
    await pipelineRepo.save(pipeline);

    const runId = createPipelineRunId(`run-${prefix}-${Date.now()}`);
    const run = new PipelineRun({
      id: runId,
      pipelineId,
      pipelineName: pipeline.name,
      initialStatus: 'RUNNING',
    });
    await runRepo.save(run);

    return { pipeline, run };
  }

  it('verifies Docker daemon availability for integration testing', () => {
    expect(dockerAvailable).toBe(true);
  });

  it('claims and executes a successful job, persisting SUCCEEDED state and releasing lease', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('succ');
    const jobId = createJobId(`job-succ-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'build',
      command: 'echo "Integration build step completed successfully"',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    // 1. Worker A claims the job
    const claimRes = await workerA.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    // 2. Worker A executes the job
    const execRes = await workerA.executeJob({
      job,
      leaseId: claimRes.lease.id,
    });

    expect(execRes.result.status).toBe('SUCCEEDED');
    expect(execRes.result.exitCode).toBe(0);
    expect(execRes.result.stdout).toContain('Integration build step completed successfully');
    expect(execRes.job.status).toBe('SUCCEEDED');
    expect(execRes.attempt.status).toBe('SUCCEEDED');
    expect(execRes.attempt.exitCode).toBe(0);

    // 3. Verify persistent state in PostgreSQL
    const persistedJob = await jobRepo.findById(job.id);
    expect(persistedJob).not.toBeNull();
    expect(persistedJob?.status).toBe('SUCCEEDED');

    const attempts = await attemptRepo.findByJobId(job.id);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.status).toBe('SUCCEEDED');
    expect(attempts[0]?.exitCode).toBe(0);

    // 4. Verify lease was released in PostgreSQL
    const activeLease = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease).toBeNull();

    const releasedLease = await leaseRepo.findById(claimRes.lease.id);
    expect(releasedLease?.status).toBe('RELEASED');
  });

  it('claims and executes a failing job, persisting FAILED state and releasing lease', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('fail');
    const jobId = createJobId(`job-fail-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'test',
      command: 'echo "Failing integration test..." && exit 7',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    // 1. Worker A claims the job
    const claimRes = await workerA.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    // 2. Worker A executes the job
    const execRes = await workerA.executeJob({
      job,
      leaseId: claimRes.lease.id,
    });

    expect(execRes.result.status).toBe('FAILED');
    expect(execRes.result.exitCode).toBe(7);
    expect(execRes.job.status).toBe('FAILED');
    expect(execRes.attempt.status).toBe('FAILED');
    expect(execRes.attempt.exitCode).toBe(7);

    // 3. Verify persistent state in PostgreSQL
    const persistedJob = await jobRepo.findById(job.id);
    expect(persistedJob?.status).toBe('FAILED');

    const attempts = await attemptRepo.findByJobId(job.id);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.status).toBe('FAILED');
    expect(attempts[0]?.exitCode).toBe(7);

    // 4. Lease must still be released after failure
    const activeLease = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease).toBeNull();
  });

  it('claims and executes a timed-out job, persisting TIMED_OUT state and releasing lease', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('tout');
    const jobId = createJobId(`job-tout-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'sleep',
      command: 'sleep 10',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    // 1. Worker A claims the job
    const claimRes = await workerA.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    // 2. Worker A executes the job with 1500ms timeout
    const execRes = await workerA.executeJob({
      job,
      leaseId: claimRes.lease.id,
      timeoutMs: 1500,
    });

    expect(execRes.result.status).toBe('TIMED_OUT');
    expect(execRes.job.status).toBe('TIMED_OUT');
    expect(execRes.attempt.status).toBe('TIMED_OUT');

    // 3. Verify persistent state in PostgreSQL
    const persistedJob = await jobRepo.findById(job.id);
    expect(persistedJob?.status).toBe('TIMED_OUT');

    const attempts = await attemptRepo.findByJobId(job.id);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.status).toBe('TIMED_OUT');

    // 4. Lease released
    const activeLease = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease).toBeNull();
  });

  it('prevents a competing worker from executing a job held by another worker', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('comp');
    const jobId = createJobId(`job-comp-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'compile',
      command: 'echo "compiling"',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    // 1. Worker A claims the job
    const claimResA = await workerA.claimJob(job.id);
    expect(claimResA.status).toBe('ACQUIRED');
    if (claimResA.status !== 'ACQUIRED') return;

    // 2. Worker B attempts to execute the job using Worker A's lease ID
    await expect(
      workerB.executeJob({
        job,
        leaseId: claimResA.lease.id,
      }),
    ).rejects.toThrow(/does not hold active lease/);

    // 3. Verify lease is still actively held by Worker A
    const activeLease = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease?.workerId).toBe('worker-node-alpha');

    // Clean up lease
    await workerA.releaseLease(claimResA.lease.id, job.id);
  });
});
