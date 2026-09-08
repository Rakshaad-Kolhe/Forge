import { evaluatePlacement, orderJobsByPriority, orderJobsWithFairAging } from '@forge/scheduler';
import {
  createDatabasePool,
  PgJobRepository,
  PgPipelineRepository,
  PgPipelineRunRepository,
  PgWorkerLeaseRepository,
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
import { createRedisClient, type RedisClient } from '@forge/redis';
import { createJobQueue, type JobQueue } from '@forge/queue';
import { runBenchmark, type BenchmarkRunResult } from '../utils/timer.js';
import {
  generateBenchmarkJobs,
  generateBenchmarkWorkers,
  createJobCandidate,
} from '../utils/fixtures.js';
import { benchmarkConfig } from '../config.js';

export async function runComponentBenchmarks(): Promise<BenchmarkRunResult[]> {
  const results: BenchmarkRunResult[] = [];
  const warmup = benchmarkConfig.warmup.component;
  const measured = benchmarkConfig.iterations.component;
  const seed = benchmarkConfig.seed;

  // --------------------------------------------------------------------------
  // 1. evaluatePlacement (Pure Component) Across Worker Scales
  // --------------------------------------------------------------------------
  const targetJob = createJobCandidate('bench-job-target', 75, {
    requirements: { executor: 'docker', cpuCores: 2, memoryBytes: 2048 * 1024 * 1024 },
  });

  for (const workerCount of [1, 10, 50, 100]) {
    const workers = generateBenchmarkWorkers(workerCount, { seed: seed + workerCount });

    results.push(
      await runBenchmark({
        name: `component:evaluatePlacement (workers=${workerCount})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => evaluatePlacement(targetJob, workers),
      }),
    );
  }

  // --------------------------------------------------------------------------
  // 2. Head-to-Head Policy Comparison: HPF vs FairAging Sorting
  // --------------------------------------------------------------------------
  const agingConfig = {
    agingIntervalMs: benchmarkConfig.fairness.agingIntervalMs,
    ageBonusStep: 1,
    maxAgeBonus: benchmarkConfig.fairness.maxBonus,
  };
  const evalDate = new Date();

  for (const count of benchmarkConfig.scales.inMemoryJobCounts) {
    const jobs = generateBenchmarkJobs(count, { seed: seed + 1000 + count });

    results.push(
      await runBenchmark({
        name: `component:policy:HPF (jobs=${count})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => orderJobsByPriority(jobs),
      }),
    );

    results.push(
      await runBenchmark({
        name: `component:policy:FairAging (jobs=${count})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => orderJobsWithFairAging(jobs, evalDate, agingConfig),
      }),
    );
  }

  // --------------------------------------------------------------------------
  // 3. Persistent Database & Redis Benchmarks (With Clean Teardown)
  // --------------------------------------------------------------------------
  let pool: DatabasePool | null = null;
  let redisClient: RedisClient | null = null;
  let queue: JobQueue | null = null;

  try {
    pool = createDatabasePool({ connectionString: benchmarkConfig.databaseUrl });
    const jobRepo = new PgJobRepository(pool);
    const pipelineRepo = new PgPipelineRepository(pool);
    const pipelineRunRepo = new PgPipelineRunRepository(pool);
    const leaseRepo = new PgWorkerLeaseRepository(pool);

    redisClient = createRedisClient({
      url: benchmarkConfig.redisUrl,
      connectTimeoutMillis: 5000,
      maxRetriesPerRequest: 2,
    });
    await redisClient.connect();

    queue = createJobQueue(redisClient, {
      queueName: benchmarkConfig.queueName,
      defaultVisibilityTimeoutSeconds: 5,
    });

    const prefix = `${benchmarkConfig.prefix}comp-`;
    const pipeId = createPipelineId(`${prefix}pipe-01`);
    const runId = createPipelineRunId(`${prefix}run-01`);

    // Clean any previous runs
    await pool.query('DELETE FROM worker_leases WHERE job_id LIKE $1', [`${prefix}%`]);
    await pool.query('DELETE FROM jobs WHERE id LIKE $1', [`${prefix}%`]);
    await pool.query('DELETE FROM pipeline_runs WHERE id = $1', [runId]);
    await pool.query('DELETE FROM pipelines WHERE id = $1', [pipeId]);

    // Insert benchmark pipeline & run
    await pipelineRepo.save(
      new Pipeline({
        id: pipeId,
        name: 'benchmark-comp-pipeline',
        steps: [{ name: 'step1', command: 'echo test' }],
      }),
    );
    await pipelineRunRepo.save(
      new PipelineRun({
        id: runId,
        pipelineId: pipeId,
        pipelineName: 'benchmark-comp-pipeline',
      }),
    );

    // Seed 50 queued jobs for DB query benchmarking
    const dbJobCount = 50;
    for (let i = 0; i < dbJobCount; i++) {
      const jobId = createJobId(`${prefix}job-${String(i).padStart(3, '0')}`);
      await jobRepo.save(
        new Job({
          id: jobId,
          pipelineRunId: runId,
          stepName: `step-${i}`,
          command: 'echo test',
          priority: 50 + (i % 20),
          initialStatus: 'QUEUED',
        }),
      );
    }

    // Benchmark: PgJobRepository.findSchedulableJobs
    results.push(
      await runBenchmark({
        name: `component:db:findSchedulableJobs (table_jobs=${dbJobCount}, limit=50)`,
        warmupIterations: 5,
        measuredIterations: measured,
        fn: async () => {
          return await jobRepo.findSchedulableJobs({ now: new Date(), limit: 50 });
        },
      }),
    );

    // Benchmark: Redis JobQueue Enqueue & Dequeue
    const rawRedis = redisClient.getRawClient();
    const cleanQueueKeys = async () => {
      const keys = await rawRedis.keys(`forge:queue:${benchmarkConfig.queueName}:*`);
      if (keys.length > 0) {
        await rawRedis.del(...keys);
      }
    };

    await cleanQueueKeys();

    let enqueueCounter = 0;
    results.push(
      await runBenchmark({
        name: 'component:queue:enqueue',
        warmupIterations: 5,
        measuredIterations: measured,
        fn: async () => {
          enqueueCounter++;
          return await queue!.enqueue({
            messageId: `${prefix}msg-${enqueueCounter}`,
            payload: { jobId: `${prefix}job-${enqueueCounter}` },
          });
        },
      }),
    );

    results.push(
      await runBenchmark({
        name: 'component:queue:dequeue',
        warmupIterations: 5,
        measuredIterations: measured,
        fn: async () => {
          return await queue!.dequeue();
        },
      }),
    );

    await cleanQueueKeys();

    // Benchmark: Worker Lease Acquisition (Uncontended vs Conflict)
    const leaseJobId = createJobId(`${prefix}lease-job-01`);
    await jobRepo.save(
      new Job({
        id: leaseJobId,
        pipelineRunId: runId,
        stepName: 'step-lease-test',
        command: 'echo test',
        priority: 100,
        initialStatus: 'QUEUED',
      }),
    );

    // Uncontended claim (release after each iteration)
    results.push(
      await runBenchmark({
        name: 'component:lease:claim (uncontended)',
        warmupIterations: 5,
        measuredIterations: measured,
        fn: async (iter) => {
          const workerId = `bench-worker-${iter % 5}`;
          const claimRes = await leaseRepo.claim({
            jobId: leaseJobId,
            workerId,
            durationMs: 30000,
          });
          if (claimRes.status === 'ACQUIRED') {
            await leaseRepo.release({
              leaseId: claimRes.lease.id,
              jobId: leaseJobId,
              workerId,
            });
          }
          return claimRes;
        },
      }),
    );

    // Contended / conflict claim (job remains actively leased to worker-prime)
    const primeClaim = await leaseRepo.claim({
      jobId: leaseJobId,
      workerId: 'worker-prime',
      durationMs: 30000,
    });
    if (primeClaim.status !== 'ACQUIRED') {
      throw new Error(`Failed to establish prime lease: ${primeClaim.reason}`);
    }

    results.push(
      await runBenchmark({
        name: 'component:lease:claim (contended conflict)',
        warmupIterations: 5,
        measuredIterations: measured,
        fn: async (iter) => {
          return await leaseRepo.claim({
            jobId: leaseJobId,
            workerId: `worker-contender-${iter}`,
            durationMs: 30000,
          });
        },
      }),
    );

    // Clean up prime lease
    await leaseRepo.release({
      leaseId: primeClaim.lease.id,
      jobId: leaseJobId,
      workerId: 'worker-prime',
    });

    // Clean up DB records
    await pool.query('DELETE FROM worker_leases WHERE job_id LIKE $1', [`${prefix}%`]);
    await pool.query('DELETE FROM jobs WHERE id LIKE $1', [`${prefix}%`]);
    await pool.query('DELETE FROM pipeline_runs WHERE id = $1', [runId]);
    await pool.query('DELETE FROM pipelines WHERE id = $1', [pipeId]);
  } finally {
    if (queue) {
      try {
        await queue.close();
      } catch {
        // ignore close errors
      }
    }
    if (redisClient) {
      try {
        await redisClient.close();
      } catch {
        // ignore close errors
      }
    }
    if (pool) {
      try {
        await pool.close();
      } catch {
        // ignore close errors
      }
    }
  }

  return results;
}
