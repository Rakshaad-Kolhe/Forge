import {
  calculateAgeBonus,
  calculateEffectivePriority,
  compareJobPriority,
  deterministicFirstEligiblePolicy,
  orderJobsByPriority,
  orderJobsWithFairAging,
} from '@forge/scheduler';
import { filterEligibleWorkers } from '@forge/pipeline';
import { runBenchmark, type BenchmarkRunResult } from '../utils/timer.js';
import {
  generateBenchmarkJobs,
  generateBenchmarkWorkers,
  createJobCandidate,
  createWorkerCandidateFixture,
} from '../utils/fixtures.js';
import { benchmarkConfig } from '../config.js';

export async function runMicroBenchmarks(): Promise<BenchmarkRunResult[]> {
  const results: BenchmarkRunResult[] = [];
  const warmup = benchmarkConfig.warmup.micro;
  const measured = benchmarkConfig.iterations.micro;
  const seed = benchmarkConfig.seed;

  // --------------------------------------------------------------------------
  // 1. Raw Priority Comparison & Aging Calculations
  // --------------------------------------------------------------------------
  const jobA = createJobCandidate('job-1', 100, { queuedAt: new Date(Date.now() - 60000) });
  const jobB = createJobCandidate('job-2', 50, { queuedAt: new Date(Date.now() - 10000) });
  const agingConfig = {
    agingIntervalMs: benchmarkConfig.fairness.agingIntervalMs,
    ageBonusStep: 1,
    maxAgeBonus: benchmarkConfig.fairness.maxBonus,
  };

  results.push(
    await runBenchmark({
      name: 'micro:compareJobPriority',
      warmupIterations: warmup,
      measuredIterations: measured,
      fn: () => compareJobPriority(jobA, jobB),
    }),
  );

  results.push(
    await runBenchmark({
      name: 'micro:calculateAgeBonus',
      warmupIterations: warmup,
      measuredIterations: measured,
      fn: () => calculateAgeBonus(35000, agingConfig),
    }),
  );

  const evalDate = new Date();
  results.push(
    await runBenchmark({
      name: 'micro:calculateEffectivePriority',
      warmupIterations: warmup,
      measuredIterations: measured,
      fn: () => calculateEffectivePriority(jobA, evalDate, agingConfig),
    }),
  );

  // --------------------------------------------------------------------------
  // 2. In-Memory Job Ordering at Scale (HPF vs FairAging)
  // --------------------------------------------------------------------------
  for (const count of [10, 50, 100, 500, 1000]) {
    const jobs = generateBenchmarkJobs(count, { seed: seed + count });

    results.push(
      await runBenchmark({
        name: `micro:orderJobsByPriority (N=${count})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => orderJobsByPriority(jobs),
      }),
    );

    results.push(
      await runBenchmark({
        name: `micro:orderJobsWithFairAging (N=${count})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => orderJobsWithFairAging(jobs, evalDate, agingConfig),
      }),
    );
  }

  // --------------------------------------------------------------------------
  // 3. Worker Eligibility Filtering (filterEligibleWorkers)
  // --------------------------------------------------------------------------
  for (const workerCount of [1, 10, 50, 100]) {
    const workers = generateBenchmarkWorkers(workerCount, { seed: seed + workerCount });
    const simpleReqs = { executor: 'docker', cpuCores: 1, memoryBytes: 512 * 1024 * 1024 };
    const heavyReqs = { executor: 'docker', cpuCores: 8, memoryBytes: 16 * 1024 * 1024 * 1024 };
    const gpuReqs = { executor: 'docker', capabilities: ['gpu'] };

    results.push(
      await runBenchmark({
        name: `micro:filterEligibleWorkers:simple (workers=${workerCount})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => filterEligibleWorkers(simpleReqs, workers),
      }),
    );

    results.push(
      await runBenchmark({
        name: `micro:filterEligibleWorkers:heavy (workers=${workerCount})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => filterEligibleWorkers(heavyReqs, workers),
      }),
    );

    results.push(
      await runBenchmark({
        name: `micro:filterEligibleWorkers:gpu (workers=${workerCount})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => filterEligibleWorkers(gpuReqs, workers),
      }),
    );
  }

  // --------------------------------------------------------------------------
  // 4. Deterministic Worker Selection Policy
  // --------------------------------------------------------------------------
  for (const candidateCount of [1, 10, 50, 100]) {
    const candidates = Array.from({ length: candidateCount }, (_, i) =>
      createWorkerCandidateFixture(`worker-${String(i).padStart(4, '0')}`),
    );

    results.push(
      await runBenchmark({
        name: `micro:deterministicWorkerSelection (candidates=${candidateCount})`,
        warmupIterations: warmup,
        measuredIterations: measured,
        fn: () => deterministicFirstEligiblePolicy.selectWorker(candidates),
      }),
    );
  }

  return results;
}
