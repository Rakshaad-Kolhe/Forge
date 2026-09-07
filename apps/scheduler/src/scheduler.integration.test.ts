import {
  createDatabasePool,
  DEFAULT_DATABASE_URL,
  PgJobRepository,
  PgPipelineRepository,
  PgPipelineRunRepository,
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
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createJobSourceFromRepository, ForgeScheduler } from './scheduler.js';

describe('Real PostgreSQL, Redis & Queue Scheduler Integration Tests', () => {
  let pool: DatabasePool;
  let redisClient: RedisClient;
  let pipelineRepo: PgPipelineRepository;
  let jobRepo: PgJobRepository;
  let pipelineRunRepo: PgPipelineRunRepository;
  let workerRepo: PgWorkerRepository;
  let workerRegistry: WorkerRegistry;
  let queue: JobQueue;
  let scheduler: ForgeScheduler;

  const queueName = 'test-scheduler-queue';

  beforeAll(async () => {
    // 1. Setup PostgreSQL pool & migrations
    pool = createDatabasePool({ connectionString: DEFAULT_DATABASE_URL });
    await runMigrations(pool);
    pipelineRepo = new PgPipelineRepository(pool);
    jobRepo = new PgJobRepository(pool);
    pipelineRunRepo = new PgPipelineRunRepository(pool);
    workerRepo = new PgWorkerRepository(pool);

    // 2. Setup Redis client
    redisClient = createRedisClient({
      url: DEFAULT_REDIS_URL,
      connectTimeoutMillis: 5000,
      maxRetriesPerRequest: 2,
    });
    await redisClient.connect();

    // 3. Setup WorkerRegistry with 2s heartbeat TTL for quick expiry testing
    const heartbeatStore = createWorkerHeartbeatStore(redisClient, { defaultTtlSeconds: 2 });
    workerRegistry = createWorkerRegistry(workerRepo, heartbeatStore, { heartbeatTtlSeconds: 2 });

    // 4. Setup FIFO JobQueue
    queue = createJobQueue(redisClient, {
      queueName,
      defaultVisibilityTimeoutSeconds: 5,
    });

    // 5. Setup ForgeScheduler with real dependencies
    scheduler = new ForgeScheduler({
      workerSource: workerRegistry,
      jobSource: createJobSourceFromRepository(jobRepo),
    });
  });

  afterAll(async () => {
    await queue.close();
    await redisClient.close();
    await pool.close();
  });

  beforeEach(async () => {
    await resetDatabase(pool);
    await runMigrations(pool);

    // Clean queue keys in Redis
    const raw = redisClient.getRawClient();
    const queueKeys = await raw.keys(`forge:queue:${queueName}:*`);
    if (queueKeys.length > 0) {
      await raw.del(...queueKeys);
    }
    const workerKeys = await raw.keys('forge:worker:*');
    if (workerKeys.length > 0) {
      await raw.del(...workerKeys);
    }
  });

  afterEach(async () => {
    const raw = redisClient.getRawClient();
    const queueKeys = await raw.keys(`forge:queue:${queueName}:*`);
    if (queueKeys.length > 0) {
      await raw.del(...queueKeys);
    }
    const workerKeys = await raw.keys('forge:worker:*');
    if (workerKeys.length > 0) {
      await raw.del(...workerKeys);
    }
  });

  it('schedules a persistent PostgreSQL job against live registered workers', async () => {
    // Register two real workers in PostgreSQL + Redis
    await workerRegistry.register({
      workerId: 'worker-node-02',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    });

    await workerRegistry.register({
      workerId: 'worker-node-01',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    });

    // Create and persist parent Pipeline + PipelineRun + Job in PostgreSQL
    const pipeId = createPipelineId('pipe-sched-01');
    const pipeline = new Pipeline({
      id: pipeId,
      name: 'deploy-pipeline',
      steps: [{ name: 'test', command: 'docker run test' }],
    });
    await pipelineRepo.save(pipeline);

    const runId = createPipelineRunId('run-sched-01');
    const pipelineRun = new PipelineRun({
      id: runId,
      pipelineId: pipeId,
      pipelineName: 'deploy-pipeline',
    });
    await pipelineRunRepo.save(pipelineRun);

    const jobId = createJobId('job-sched-01');
    const job = new Job({
      id: jobId,
      pipelineRunId: runId,
      stepName: 'test',
      command: 'docker run test',
      requirements: { executor: 'docker', cpuCores: 2 },
    });
    await jobRepo.save(job);

    // Schedule using job ID — ForgeScheduler loads job from PostgreSQL and workers from WorkerRegistry
    const decision = await scheduler.schedule(jobId);

    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.jobId).toBe(jobId);
      // Canonical deterministic selection: worker-node-01 < worker-node-02
      expect(decision.workerId).toBe('worker-node-01');
      expect(decision.candidateWorkerCount).toBe(2);
      expect(decision.eligibleWorkerCount).toBe(2);
    }

    // Verify job status in database remains untouched (no premature RUNNING state)
    const storedJob = await jobRepo.findById(jobId);
    expect(storedJob?.status).toBe('PENDING');
  });

  it('evaluates queued job message without acknowledging, preserving queue recoverability', async () => {
    await workerRegistry.register({
      workerId: 'worker-solo',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 8, memoryBytes: 16384 },
    });

    const pipeId = createPipelineId('pipe-queue-01');
    const pipeline = new Pipeline({
      id: pipeId,
      name: 'ci-pipeline',
      steps: [{ name: 'build', command: 'make build' }],
    });
    await pipelineRepo.save(pipeline);

    const runId = createPipelineRunId('run-queue-01');
    const pipelineRun = new PipelineRun({
      id: runId,
      pipelineId: pipeId,
      pipelineName: 'ci-pipeline',
    });
    await pipelineRunRepo.save(pipelineRun);

    const jobId = createJobId('job-queue-01');
    const job = new Job({
      id: jobId,
      pipelineRunId: runId,
      stepName: 'build',
      command: 'make build',
      requirements: { executor: 'docker', cpuCores: 4 },
    });
    await jobRepo.save(job);

    // Enqueue message
    await queue.enqueue({
      jobId,
      pipelineRunId: runId,
      stepName: 'build',
    });

    expect(await queue.depth()).toBe(1);

    // Dequeue and schedule via scheduleNext
    const scheduledItem = await scheduler.scheduleNext(queue, { visibilityTimeoutSeconds: 1 });
    expect(scheduledItem).not.toBeNull();
    expect(scheduledItem?.delivery.message.jobId).toBe(jobId);
    expect(scheduledItem?.decision.status).toBe('SCHEDULED');
    if (scheduledItem?.decision.status === 'SCHEDULED') {
      expect(scheduledItem.decision.workerId).toBe('worker-solo');
    }

    // CRITICAL: Verify message was NOT acknowledged and remains in-flight in Redis
    expect(await queue.inFlightCount()).toBe(1);
    expect(await queue.depth()).toBe(0);

    // Wait 1.1s for visibility timeout to expire
    await new Promise((resolve) => setTimeout(resolve, 1100));

    // Reclaim expired messages — verifies the unacknowledged job is completely recoverable!
    const reclaimed = await queue.reclaimExpired();
    expect(reclaimed).toBe(1);
    expect(await queue.depth()).toBe(1);
    expect(await queue.inFlightCount()).toBe(0);
  });

  it('excludes crashed/stale workers whose Redis heartbeat TTL has expired', async () => {
    // Register worker-a with a 1-second TTL
    await workerRegistry.register({
      workerId: 'worker-a',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    });

    // Register worker-b
    await workerRegistry.register({
      workerId: 'worker-b',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    });

    const pipeId = createPipelineId('pipe-stale-01');
    await pipelineRepo.save(
      new Pipeline({
        id: pipeId,
        name: 'test',
        steps: [{ name: 'test', command: 'echo test' }],
      }),
    );

    const runId = createPipelineRunId('run-stale-01');
    await pipelineRunRepo.save(
      new PipelineRun({ id: runId, pipelineId: pipeId, pipelineName: 'test' }),
    );

    const jobId = createJobId('job-stale-01');
    await jobRepo.save(
      new Job({
        id: jobId,
        pipelineRunId: runId,
        stepName: 'test',
        command: 'echo test',
        requirements: { executor: 'docker', cpuCores: 2 },
      }),
    );

    // Wait 2.2s for worker-a & worker-b heartbeats to expire in Redis (simulating crash)
    await new Promise((resolve) => setTimeout(resolve, 2200));

    // Refresh ONLY worker-b's heartbeat
    await workerRegistry.heartbeat(createWorkerId('worker-b'));

    // Schedule: worker-a is STALE, so scheduler MUST select worker-b even though worker-a is alphabetically first
    const decision = await scheduler.schedule(jobId);

    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.workerId).toBe('worker-b');
      expect(decision.candidateWorkerCount).toBe(1); // Only ALIVE worker returned by registry
    }
  });

  it('returns UNSCHEDULABLE when no live workers satisfy job requirements', async () => {
    await workerRegistry.register({
      workerId: 'worker-small',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 2, memoryBytes: 4096 },
    });

    const pipeId = createPipelineId('pipe-unsched-01');
    await pipelineRepo.save(
      new Pipeline({
        id: pipeId,
        name: 'test',
        steps: [{ name: 'test', command: 'echo test' }],
      }),
    );

    const runId = createPipelineRunId('run-unsched-01');
    await pipelineRunRepo.save(
      new PipelineRun({ id: runId, pipelineId: pipeId, pipelineName: 'test' }),
    );

    const jobId = createJobId('job-high-cpu');
    await jobRepo.save(
      new Job({
        id: jobId,
        pipelineRunId: runId,
        stepName: 'test',
        command: 'echo test',
        requirements: { executor: 'docker', cpuCores: 16 }, // Requires 16 cores
      }),
    );

    const decision = await scheduler.schedule(jobId);

    expect(decision.status).toBe('UNSCHEDULABLE');
    if (decision.status === 'UNSCHEDULABLE') {
      expect(decision.reason).toBe('NO_ELIGIBLE_WORKER');
      expect(decision.failureReasons).toContain('INSUFFICIENT_CPU');
      expect(decision.candidateWorkerCount).toBe(1);
      expect(decision.eligibleWorkerCount).toBe(0);
    }
  });
});
