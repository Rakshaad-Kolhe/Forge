import { createJobId, createPipelineRunId, Job, type WorkerCandidate } from '@forge/pipeline';
import type { JobRequirements } from '@forge/contracts';
import { SeededPRNG } from './prng.js';

export interface JobFixtureOptions {
  readonly priorityDistribution?: 'uniform' | 'bimodal' | 'fixed' | 'sequential';
  readonly fixedPriority?: number;
  readonly requirementsDistribution?: 'simple' | 'heavy' | 'gpu' | 'mixed';
  readonly baseTime?: Date;
  readonly maxAgeMinutes?: number;
  readonly withRetries?: boolean;
}

/**
 * Deterministically generates an array of candidate worker fixtures.
 */
export function generateCandidateWorkers(count: number, prng: SeededPRNG): WorkerCandidate[] {
  const workers: WorkerCandidate[] = [];

  const cpuOptions = [2, 4, 8, 16];
  const memoryOptions = [4096, 8192, 16384, 32768];

  for (let i = 0; i < count; i++) {
    const workerId = `worker-bench-${(i + 1).toString().padStart(4, '0')}`;
    const cpuCores = prng.nextElement(cpuOptions);
    const memoryBytes = prng.nextElement(memoryOptions);
    const hasGpu = prng.next() < 0.2; // 20% workers have GPU

    workers.push({
      workerId,
      status: 'READY',
      liveness: 'ALIVE',
      capabilities: {
        executors: ['docker', 'shell'],
        custom: {
          os: 'linux',
          arch: 'x86_64',
          zone: prng.nextElement(['us-east-1a', 'us-east-1b', 'us-west-2a']),
        },
      },
      resources: {
        cpuCores,
        memoryBytes,
        gpuCount: hasGpu ? 1 : 0,
      },
    });
  }

  return workers;
}

/**
 * Deterministically generates an array of domain Job fixtures.
 */
export function generateJobs(
  count: number,
  prng: SeededPRNG,
  options: JobFixtureOptions = {},
): Job[] {
  const {
    priorityDistribution = 'uniform',
    fixedPriority = 0,
    requirementsDistribution = 'simple',
    baseTime = new Date('2026-09-08T12:00:00.000Z'),
    maxAgeMinutes = 60,
    withRetries = false,
  } = options;

  const jobs: Job[] = [];

  for (let i = 0; i < count; i++) {
    const jobId = createJobId(`job-bench-${(i + 1).toString().padStart(5, '0')}`);
    const pipelineRunId = createPipelineRunId(`run-bench-${Math.floor(i / 10) + 1}`);

    // Determine priority
    let priority = 0;
    if (priorityDistribution === 'fixed') {
      priority = fixedPriority;
    } else if (priorityDistribution === 'sequential') {
      priority = (i % 2000) - 1000;
    } else if (priorityDistribution === 'bimodal') {
      priority = prng.next() < 0.7 ? prng.nextInt(0, 50) : prng.nextInt(500, 900);
    } else {
      // uniform
      priority = prng.nextInt(-500, 500);
    }

    // Determine requirements
    let requirements: JobRequirements = { executor: 'docker', cpuCores: 1, memoryBytes: 1024 };
    if (requirementsDistribution === 'heavy') {
      requirements = { executor: 'docker', cpuCores: 4, memoryBytes: 8192 };
    } else if (requirementsDistribution === 'gpu') {
      requirements = { executor: 'docker', cpuCores: 4, memoryBytes: 8192, gpuCount: 1 };
    } else if (requirementsDistribution === 'mixed') {
      const type = prng.next();
      if (type < 0.6) {
        requirements = { executor: 'docker', cpuCores: 1, memoryBytes: 1024 };
      } else if (type < 0.85) {
        requirements = { executor: 'docker', cpuCores: 4, memoryBytes: 8192 };
      } else {
        requirements = { executor: 'docker', cpuCores: 8, memoryBytes: 16384, gpuCount: 1 };
      }
    }

    // Determine timestamps
    const ageMs = prng.nextInt(0, maxAgeMinutes * 60 * 1000);
    const createdAt = new Date(baseTime.getTime() - ageMs);
    const queuedAt = new Date(createdAt.getTime() + prng.nextInt(0, 5000));

    let nextAttemptAt: Date | undefined;
    if (withRetries && prng.next() < 0.3) {
      // 30% are retried jobs
      const isDue = prng.next() < 0.5;
      nextAttemptAt = isDue
        ? new Date(baseTime.getTime() - prng.nextInt(1000, 60000))
        : new Date(baseTime.getTime() + prng.nextInt(1000, 60000));
    }

    const job = new Job({
      id: jobId,
      pipelineRunId,
      stepName: `step-${(i % 5) + 1}`,
      command: 'echo benchmark-task',
      priority,
      requirements,
      createdAt,
      queuedAt,
      nextAttemptAt,
    });
    job.markQueued(queuedAt);

    jobs.push(job);
  }

  return jobs;
}

/**
 * Convenience generator for benchmark jobs with automatic PRNG instantiation.
 */
export function generateBenchmarkJobs(
  count: number,
  options?: { seed?: number } & JobFixtureOptions,
): Job[] {
  const prng = new SeededPRNG(options?.seed ?? 42);
  return generateJobs(count, prng, options);
}

/**
 * Convenience generator for benchmark workers with automatic PRNG instantiation.
 */
export function generateBenchmarkWorkers(
  count: number,
  options?: { seed?: number },
): WorkerCandidate[] {
  const prng = new SeededPRNG(options?.seed ?? 42);
  return generateCandidateWorkers(count, prng);
}

/**
 * Creates a single candidate job for targeted microbenchmarks.
 */
export function createJobCandidate(
  id: string,
  priority = 0,
  overrides?: Partial<ConstructorParameters<typeof Job>[0]>,
): Job {
  const now = new Date('2026-09-08T12:00:00.000Z');
  const job = new Job({
    id: createJobId(id),
    pipelineRunId: createPipelineRunId('run-default'),
    stepName: 'test-step',
    command: 'echo test',
    priority,
    requirements: { executor: 'docker', cpuCores: 1, memoryBytes: 1024 },
    createdAt: now,
    queuedAt: now,
    ...overrides,
  });
  if (job.status === 'PENDING') {
    job.markQueued(overrides?.queuedAt ?? now);
  }
  return job;
}

/**
 * Creates a single candidate worker for targeted microbenchmarks.
 */
export function createWorkerCandidateFixture(
  workerId: string,
  overrides?: Partial<WorkerCandidate>,
): WorkerCandidate {
  return {
    workerId,
    status: 'READY',
    liveness: 'ALIVE',
    capabilities: {
      executors: ['docker', 'shell'],
      custom: { os: 'linux', arch: 'x86_64' },
      ...overrides?.capabilities,
    },
    resources: {
      cpuCores: 4,
      memoryBytes: 8192,
      gpuCount: 0,
      ...overrides?.resources,
    },
    ...overrides,
  };
}
