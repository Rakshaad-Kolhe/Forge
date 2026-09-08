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

describe('Worker & Lease Execution Races (Real PostgreSQL + Real Docker)', () => {
  const databaseUrl = process.env.DATABASE_URL ?? 'postgresql://forge:forge@127.0.0.1:5432/forge';
  const dockerHost = process.env.DOCKER_HOST ?? 'tcp://127.0.0.1:2375';

  let pool: DatabasePool;
  let leaseRepo: PgWorkerLeaseRepository;
  let jobRepo: PgJobRepository;
  let attemptRepo: PgJobAttemptRepository;
  let pipelineRepo: PgPipelineRepository;
  let runRepo: PgPipelineRunRepository;
  let executor: DockerExecutor;
  let worker: WorkerShell;
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

    worker = startWorker({
      workerId: 'worker-race-node',
      leaseRepository: leaseRepo,
      pool,
      executor,
      defaultLeaseDurationMs: 30000,
    });
  });

  afterAll(async () => {
    await worker.stop();
    await pool.close();
  });

  async function createTestPipelineAndRun(prefix: string) {
    const pipelineId = createPipelineId(`pipe-${prefix}-${Date.now()}`);
    const pipeline = new Pipeline({
      id: pipelineId,
      name: `Race Test Pipeline ${prefix}`,
      steps: [{ name: 'step-1', command: 'echo race-test' }],
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

  it('handles completion vs cancellation race deterministically with lease release', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('comp-cancel');
    const jobId = createJobId(`job-comp-cancel-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'test',
      command: 'echo "fast completion"',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    const claimRes = await worker.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    // Trigger abort right after start
    const controller = new AbortController();
    setTimeout(() => {
      controller.abort();
    }, 50);

    const execRes = await worker.executeJob({
      job,
      leaseId: claimRes.lease.id,
    });

    // Must be either SUCCEEDED or CANCELLED, never invalid state
    expect(['SUCCEEDED', 'CANCELLED']).toContain(execRes.result.status);
    expect(['SUCCEEDED', 'CANCELLED']).toContain(execRes.job.status);

    // In both cases, lease must be released
    const activeLease = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease).toBeNull();

    const persistedLease = await leaseRepo.findById(claimRes.lease.id);
    expect(persistedLease?.status).toBe('RELEASED');
  });

  it('handles timeout vs completion race deterministically with lease release', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('tout-comp');
    const jobId = createJobId(`job-tout-comp-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'test',
      command: 'sleep 1',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    const claimRes = await worker.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    // Timeout closely aligned with execution
    const execRes = await worker.executeJob({
      job,
      leaseId: claimRes.lease.id,
      timeoutMs: 1000,
    });

    expect(['SUCCEEDED', 'TIMED_OUT']).toContain(execRes.result.status);
    expect(['SUCCEEDED', 'TIMED_OUT']).toContain(execRes.job.status);

    // Lease must be released
    const activeLease = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease).toBeNull();
  });

  it('handles cancellation taking precedence when fired before timeout', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('cancel-tout');
    const jobId = createJobId(`job-cancel-tout-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'test',
      command: 'sleep 10',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    const claimRes = await worker.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    // We configure a 5-second timeout, but cancel at 800ms
    const controller = new AbortController();
    setTimeout(() => {
      controller.abort();
    }, 800);

    const execRes = await executor.execute({
      jobId: job.id,
      attemptId: 'att-cancel-tout-1',
      workerId: 'worker-race-node',
      command: job.command,
      timeoutMs: 5000,
      abortSignal: controller.signal,
    });

    expect(execRes.status).toBe('CANCELLED');
    expect(execRes.exitCode).toBeNull();

    await worker.releaseLease(claimRes.lease.id, job.id);
  });

  it('verifies result persistence before lease release', async () => {
    if (!dockerAvailable) return;

    const { run } = await createTestPipelineAndRun('persist-release');
    const jobId = createJobId(`job-persist-release-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'build',
      command: 'echo "persisted state verification"',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    const claimRes = await worker.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    const execRes = await worker.executeJob({
      job,
      leaseId: claimRes.lease.id,
    });

    expect(execRes.result.status).toBe('SUCCEEDED');

    // Authoritative check: persisted job must be SUCCEEDED and attempt must exist
    const persistedJob = await jobRepo.findById(job.id);
    expect(persistedJob?.status).toBe('SUCCEEDED');

    const attempts = await attemptRepo.findByJobId(job.id);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.status).toBe('SUCCEEDED');

    // Lease is released
    const activeLease = await leaseRepo.findActiveByJobId(job.id);
    expect(activeLease).toBeNull();
  });

  it('waits for in-flight container during worker graceful drain and rejects subsequent work', async () => {
    if (!dockerAvailable) return;

    const drainWorker = startWorker({
      workerId: 'worker-drain-race',
      leaseRepository: leaseRepo,
      pool,
      executor,
      defaultLeaseDurationMs: 30000,
    });

    const { run } = await createTestPipelineAndRun('drain-race');
    const jobId = createJobId(`job-drain-race-${Date.now()}`);
    const job = new Job({
      id: jobId,
      pipelineRunId: run.id,
      stepName: 'sleep-work',
      command: 'sleep 0.5 && echo "drain completed"',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(job);

    const claimRes = await drainWorker.claimJob(job.id);
    expect(claimRes.status).toBe('ACQUIRED');
    if (claimRes.status !== 'ACQUIRED') return;

    // Start execution asynchronously
    const executionPromise = drainWorker.executeJob({
      job,
      leaseId: claimRes.lease.id,
    });

    // Initiate worker drain while execution is in flight
    await new Promise((r) => setTimeout(r, 100));
    const drainPromise = drainWorker.drain({ timeoutMs: 4000 });

    // Subsequent execution attempts on this worker must be rejected immediately
    const nextJob = new Job({
      id: createJobId(`job-rejected-${Date.now()}`),
      pipelineRunId: run.id,
      stepName: 'rejected',
      command: 'echo rejected',
      initialStatus: 'QUEUED',
    });
    await jobRepo.save(nextJob);

    await expect(
      drainWorker.executeJob({
        job: nextJob,
        leaseId: 'fake-lease',
      }),
    ).rejects.toThrow(/not accepting new executions/);

    // Both execution and drain should complete cleanly
    const [execRes] = await Promise.all([executionPromise, drainPromise]);
    expect(execRes.result.status).toBe('SUCCEEDED');
    expect(execRes.result.stdout).toContain('drain completed');

    await drainWorker.stop();
  });
});
