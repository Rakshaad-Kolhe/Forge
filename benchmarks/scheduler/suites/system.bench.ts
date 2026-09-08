import {
  evaluatePrioritizedWork,
  fairAgingPriorityPolicy,
  ForgeScheduler,
  highestPriorityFirstPolicy,
  type WorkerSource,
} from '@forge/scheduler';
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
import { runBenchmark, type BenchmarkRunResult } from '../utils/timer.js';
import {
  generateBenchmarkJobs,
  generateBenchmarkWorkers,
  createJobCandidate,
} from '../utils/fixtures.js';
import { benchmarkConfig } from '../config.js';

export async function runSystemBenchmarks(): Promise<BenchmarkRunResult[]> {
  const results: BenchmarkRunResult[] = [];
  const warmup = benchmarkConfig.warmup.system;
  const measured = benchmarkConfig.iterations.system;
  const seed = benchmarkConfig.seed;

  // --------------------------------------------------------------------------
  // 1. In-Memory Batch Placement Scaling (evaluatePrioritizedWork)
  // --------------------------------------------------------------------------
  for (const jobCount of benchmarkConfig.scales.jobCounts) {
    for (const workerCount of [1, 10, 50]) {
      const jobs = generateBenchmarkJobs(jobCount, { seed: seed + jobCount });
      const workers = generateBenchmarkWorkers(workerCount, { seed: seed + workerCount });
      const evalDate = new Date();

      // HPF Policy
      results.push(
        await runBenchmark({
          name: `system:batch:HPF (jobs=${jobCount}, workers=${workerCount})`,
          warmupIterations: warmup,
          measuredIterations: measured,
          fn: () =>
            evaluatePrioritizedWork(
              jobs,
              workers,
              highestPriorityFirstPolicy,
              undefined,
              undefined,
              evalDate,
            ),
        }),
      );

      // FairAging Policy
      results.push(
        await runBenchmark({
          name: `system:batch:FairAging (jobs=${jobCount}, workers=${workerCount})`,
          warmupIterations: warmup,
          measuredIterations: measured,
          fn: () =>
            evaluatePrioritizedWork(
              jobs,
              workers,
              fairAgingPriorityPolicy,
              undefined,
              undefined,
              evalDate,
            ),
        }),
      );
    }
  }

  // --------------------------------------------------------------------------
  // 2. Persistent End-to-End Scheduling with Database & Leases
  // --------------------------------------------------------------------------
  let pool: DatabasePool | null = null;
  try {
    pool = createDatabasePool({ connectionString: benchmarkConfig.databaseUrl });
    const jobRepo = new PgJobRepository(pool);
    const pipelineRepo = new PgPipelineRepository(pool);
    const pipelineRunRepo = new PgPipelineRunRepository(pool);
    const leaseRepo = new PgWorkerLeaseRepository(pool);

    const prefix = `${benchmarkConfig.prefix}sys-`;
    const pipeId = createPipelineId(`${prefix}pipe-01`);
    const runId = createPipelineRunId(`${prefix}run-01`);

    // Clean any previous artifacts
    await pool.query('DELETE FROM worker_leases WHERE job_id LIKE $1', [`${prefix}%`]);
    await pool.query('DELETE FROM jobs WHERE id LIKE $1', [`${prefix}%`]);
    await pool.query('DELETE FROM pipeline_runs WHERE id = $1', [runId]);
    await pool.query('DELETE FROM pipelines WHERE id = $1', [pipeId]);

    // Create pipeline and run
    await pipelineRepo.save(
      new Pipeline({
        id: pipeId,
        name: 'benchmark-sys-pipeline',
        steps: [{ name: 'step1', command: 'echo test' }],
      }),
    );
    await pipelineRunRepo.save(
      new PipelineRun({
        id: runId,
        pipelineId: pipeId,
        pipelineName: 'benchmark-sys-pipeline',
      }),
    );

    // Prepare worker pool (10 candidate workers)
    const workerCandidates = generateBenchmarkWorkers(10, { seed: seed + 555 });
    const staticWorkerSource: WorkerSource = {
      listWorkers: async () => workerCandidates,
    };

    const persistentScheduler = new ForgeScheduler({
      workerSource: staticWorkerSource,
      leaseRepository: leaseRepo,
      leaseDurationMs: 60000,
    });

    const sequentialLeaseRepo = {
      claim: (opts: Parameters<typeof leaseRepo.claim>[0]) => leaseRepo.claim(opts),
      renew: (opts: Parameters<typeof leaseRepo.renew>[0]) => leaseRepo.renew(opts),
      release: (opts: Parameters<typeof leaseRepo.release>[0]) => leaseRepo.release(opts),
      findActiveByJobId: (id: string) => leaseRepo.findActiveByJobId(id),
      findById: (id: string) => leaseRepo.findById(id),
      findByWorkerId: (wId: string) => leaseRepo.findByWorkerId(wId),
      reclaimExpiredLeases: () => leaseRepo.reclaimExpiredLeases(),
    };

    const sequentialScheduler = new ForgeScheduler({
      workerSource: staticWorkerSource,
      leaseRepository: sequentialLeaseRepo,
      leaseDurationMs: 60000,
    });

    // Test persistent batch scheduling for sizes 10, 25, 50: Sequential vs Batched
    for (const batchSize of [10, 25, 50]) {
      // 1. Sequential baseline
      results.push(
        await runBenchmark({
          name: `system:persistent:schedulePrioritized:sequential (batch=${batchSize}, workers=10)`,
          warmupIterations: 2,
          measuredIterations: Math.min(measured, 10),
          beforeIteration: async (iter) => {
            for (let i = 0; i < batchSize; i++) {
              const jId = createJobId(`${prefix}seq-b${batchSize}-i${iter}-j${i}`);
              await jobRepo.save(
                new Job({
                  id: jId,
                  pipelineRunId: runId,
                  stepName: `step-${i}`,
                  command: 'echo test',
                  priority: 50 + (i % 20),
                  initialStatus: 'QUEUED',
                }),
              );
            }
          },
          fn: async (iter) => {
            const batchJobs: Job[] = [];
            for (let i = 0; i < batchSize; i++) {
              const jId = createJobId(`${prefix}seq-b${batchSize}-i${iter}-j${i}`);
              const job = await jobRepo.findById(jId);
              if (job) batchJobs.push(job);
            }
            return await sequentialScheduler.schedulePrioritized(batchJobs);
          },
          afterIteration: async (iter) => {
            await pool!.query('DELETE FROM worker_leases WHERE job_id LIKE $1', [
              `${prefix}seq-b${batchSize}-i${iter}-%`,
            ]);
            await pool!.query('DELETE FROM jobs WHERE id LIKE $1', [
              `${prefix}seq-b${batchSize}-i${iter}-%`,
            ]);
          },
        }),
      );

      // 2. Batched optimized (PR 18)
      results.push(
        await runBenchmark({
          name: `system:persistent:schedulePrioritized (batch=${batchSize}, workers=10)`,
          warmupIterations: 2,
          measuredIterations: Math.min(measured, 10),
          beforeIteration: async (iter) => {
            for (let i = 0; i < batchSize; i++) {
              const jId = createJobId(`${prefix}b${batchSize}-i${iter}-j${i}`);
              await jobRepo.save(
                new Job({
                  id: jId,
                  pipelineRunId: runId,
                  stepName: `step-${i}`,
                  command: 'echo test',
                  priority: 50 + (i % 20),
                  initialStatus: 'QUEUED',
                }),
              );
            }
          },
          fn: async (iter) => {
            const batchJobs: Job[] = [];
            for (let i = 0; i < batchSize; i++) {
              const jId = createJobId(`${prefix}b${batchSize}-i${iter}-j${i}`);
              const job = await jobRepo.findById(jId);
              if (job) batchJobs.push(job);
            }
            return await persistentScheduler.schedulePrioritized(batchJobs);
          },
          afterIteration: async (iter) => {
            await pool!.query('DELETE FROM worker_leases WHERE job_id LIKE $1', [
              `${prefix}b${batchSize}-i${iter}-%`,
            ]);
            await pool!.query('DELETE FROM jobs WHERE id LIKE $1', [
              `${prefix}b${batchSize}-i${iter}-%`,
            ]);
          },
        }),
      );
    }

    // Clean up pipeline and run
    await pool.query('DELETE FROM worker_leases WHERE job_id LIKE $1', [`${prefix}%`]);
    await pool.query('DELETE FROM jobs WHERE id LIKE $1', [`${prefix}%`]);
    await pool.query('DELETE FROM pipeline_runs WHERE id = $1', [runId]);
    await pool.query('DELETE FROM pipelines WHERE id = $1', [pipeId]);
  } finally {
    if (pool) {
      try {
        await pool.close();
      } catch {
        // ignore close error
      }
    }
  }

  // --------------------------------------------------------------------------
  // 3. Boundary & Negative Edge Conditions
  // --------------------------------------------------------------------------
  const normalJob = createJobCandidate('bench-normal-job', 50);
  const gpuJob = createJobCandidate('bench-gpu-job', 50, {
    requirements: { capabilities: ['gpu-high-tier'] },
  });
  const backoffJob = createJobCandidate('bench-backoff-job', 50, {
    nextAttemptAt: new Date(Date.now() + 60000), // Backoff active for 60s
  });
  const standardWorkers = generateBenchmarkWorkers(10, { seed: seed + 777 });

  // 3a. Zero Available Workers
  results.push(
    await runBenchmark({
      name: 'system:boundary:zeroWorkers',
      warmupIterations: warmup,
      measuredIterations: measured,
      fn: () => evaluatePrioritizedWork([normalJob], [], highestPriorityFirstPolicy),
    }),
  );

  // 3b. Incompatible Requirements
  results.push(
    await runBenchmark({
      name: 'system:boundary:incompatibleRequirements',
      warmupIterations: warmup,
      measuredIterations: measured,
      fn: () => evaluatePrioritizedWork([gpuJob], standardWorkers, highestPriorityFirstPolicy),
    }),
  );

  // 3c. Active Retry Backoff (Future nextAttemptAt)
  results.push(
    await runBenchmark({
      name: 'system:boundary:activeBackoff',
      warmupIterations: warmup,
      measuredIterations: measured,
      fn: () => evaluatePrioritizedWork([backoffJob], standardWorkers, highestPriorityFirstPolicy),
    }),
  );

  return results;
}
