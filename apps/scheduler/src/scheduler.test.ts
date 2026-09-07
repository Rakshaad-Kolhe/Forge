import { createLogger } from '@forge/logging';
import { createJobId, createPipelineRunId, Job, type WorkerCandidate } from '@forge/pipeline';
import { describe, expect, it, vi } from 'vitest';
import { JobNotFoundError, JobSourceError, WorkerSourceError } from './errors.js';
import {
  evaluatePlacement,
  evaluatePrioritizedWork,
  ForgeScheduler,
  isOperationallyEligible,
} from './scheduler.js';
import type { JobSource, WorkerSource } from './types.js';

describe('Scheduler — isOperationallyEligible', () => {
  it('identifies READY + ALIVE as operationally eligible', () => {
    const worker: WorkerCandidate = {
      workerId: 'worker-1',
      ...({ status: 'READY', liveness: 'ALIVE' } as unknown as WorkerCandidate),
    };
    expect(isOperationallyEligible(worker)).toBe(true);
  });

  it('identifies nested worker metadata with READY + ALIVE', () => {
    const worker: WorkerCandidate = {
      worker: {
        capabilities: { executors: ['shell'] },
        resources: { cpuCores: 4, memoryBytes: 4096 },
      },
      ...({
        worker: { status: 'READY', workerId: 'w-1' },
        liveness: 'ALIVE',
      } as unknown as WorkerCandidate),
    };
    expect(isOperationallyEligible(worker)).toBe(true);
  });

  it('rejects candidates that are not READY (DRAINING, OFFLINE, STARTING)', () => {
    expect(
      isOperationallyEligible({
        workerId: 'w-draining',
        ...({ status: 'DRAINING', liveness: 'ALIVE' } as unknown as WorkerCandidate),
      }),
    ).toBe(false);

    expect(
      isOperationallyEligible({
        workerId: 'w-offline',
        ...({ status: 'OFFLINE', liveness: 'ALIVE' } as unknown as WorkerCandidate),
      }),
    ).toBe(false);

    expect(
      isOperationallyEligible({
        workerId: 'w-starting',
        ...({ status: 'STARTING', liveness: 'ALIVE' } as unknown as WorkerCandidate),
      }),
    ).toBe(false);
  });

  it('rejects candidates that are STALE even if status is READY', () => {
    expect(
      isOperationallyEligible({
        workerId: 'w-stale',
        ...({ status: 'READY', liveness: 'STALE' } as unknown as WorkerCandidate),
      }),
    ).toBe(false);
  });
});

describe('Scheduler — evaluatePlacement (Pure)', () => {
  const dummyJob = {
    id: 'job-123',
    requirements: { executor: 'docker', cpuCores: 2, memoryBytes: 4096 },
  };

  it('schedules successfully when one compatible worker exists', () => {
    const worker: WorkerCandidate = {
      workerId: 'worker-1',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    };

    const decision = evaluatePlacement(dummyJob, [worker]);

    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.jobId).toBe('job-123');
      expect(decision.workerId).toBe('worker-1');
      expect(decision.candidateWorkerCount).toBe(1);
      expect(decision.eligibleWorkerCount).toBe(1);
      expect(decision.reason).toContain('worker-1');
    }
  });

  it('returns UNSCHEDULABLE when candidate worker list is empty', () => {
    const decision = evaluatePlacement(dummyJob, []);

    expect(decision.status).toBe('UNSCHEDULABLE');
    if (decision.status === 'UNSCHEDULABLE') {
      expect(decision.jobId).toBe('job-123');
      expect(decision.reason).toBe('NO_ELIGIBLE_WORKER');
      expect(decision.candidateWorkerCount).toBe(0);
      expect(decision.eligibleWorkerCount).toBe(0);
    }
  });

  it('returns UNSCHEDULABLE when candidates exist but none are eligible', () => {
    const shellWorker: WorkerCandidate = {
      workerId: 'worker-shell',
      capabilities: { executors: ['shell'] },
      resources: { cpuCores: 8, memoryBytes: 16384 },
    };

    const lowCpuWorker: WorkerCandidate = {
      workerId: 'worker-low-cpu',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 1, memoryBytes: 8192 },
    };

    const decision = evaluatePlacement(dummyJob, [shellWorker, lowCpuWorker]);

    expect(decision.status).toBe('UNSCHEDULABLE');
    if (decision.status === 'UNSCHEDULABLE') {
      expect(decision.jobId).toBe('job-123');
      expect(decision.reason).toBe('NO_ELIGIBLE_WORKER');
      expect(decision.candidateWorkerCount).toBe(2);
      expect(decision.eligibleWorkerCount).toBe(0);
      expect(decision.failureReasons).toBeDefined();
      expect(decision.failureReasons).toContain('EXECUTOR_UNSUPPORTED');
      expect(decision.failureReasons).toContain('INSUFFICIENT_CPU');
    }
  });

  it('returns UNSCHEDULABLE with INVALID_JOB_REQUIREMENTS when requirements are malformed', () => {
    const invalidJob = {
      id: 'job-bad-req',
      requirements: {
        cpuCores: -4,
      },
    };

    const worker: WorkerCandidate = {
      workerId: 'worker-1',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 8, memoryBytes: 16384 },
    };

    const decision = evaluatePlacement(invalidJob, [worker]);

    expect(decision.status).toBe('UNSCHEDULABLE');
    if (decision.status === 'UNSCHEDULABLE') {
      expect(decision.jobId).toBe('job-bad-req');
      expect(decision.reason).toBe('INVALID_JOB_REQUIREMENTS');
      expect(decision.failureReasons).toContain('INVALID_REQUIREMENTS');
    }
  });

  it('deterministically selects first eligible worker in ascending workerId order', () => {
    const workerB: WorkerCandidate = {
      workerId: 'worker-b',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    };

    const workerA: WorkerCandidate = {
      workerId: 'worker-a',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    };

    const workerC: WorkerCandidate = {
      workerId: 'worker-c',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    };

    // Shuffled input order: [B, C, A]
    const decision = evaluatePlacement(dummyJob, [workerB, workerC, workerA]);

    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.workerId).toBe('worker-a');
      expect(decision.candidateWorkerCount).toBe(3);
      expect(decision.eligibleWorkerCount).toBe(3);
    }
  });

  it('integrates PR 09 capability and resource matching correctly', () => {
    // Docker requirement: shell worker must be skipped
    const dockerJob = {
      id: 'job-docker',
      requirements: { executor: 'docker' },
    };

    const shellWorker: WorkerCandidate = {
      workerId: 'worker-01-shell',
      capabilities: { executors: ['shell'] },
      resources: { cpuCores: 16, memoryBytes: 32768 },
    };

    const dockerWorker: WorkerCandidate = {
      workerId: 'worker-02-docker',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 2, memoryBytes: 2048 },
    };

    const decision = evaluatePlacement(dockerJob, [shellWorker, dockerWorker]);
    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.workerId).toBe('worker-02-docker');
    }

    // High CPU requirement: worker with 4 cores must be skipped
    const cpuJob = {
      id: 'job-cpu',
      requirements: { cpuCores: 8 },
    };

    const lowCpuWorker: WorkerCandidate = {
      workerId: 'worker-cpu-4',
      capabilities: { executors: ['shell'] },
      resources: { cpuCores: 4, memoryBytes: 32768 },
    };

    const highCpuWorker: WorkerCandidate = {
      workerId: 'worker-cpu-8',
      capabilities: { executors: ['shell'] },
      resources: { cpuCores: 8, memoryBytes: 32768 },
    };

    const cpuDecision = evaluatePlacement(cpuJob, [lowCpuWorker, highCpuWorker]);
    expect(cpuDecision.status).toBe('SCHEDULED');
    if (cpuDecision.status === 'SCHEDULED') {
      expect(cpuDecision.workerId).toBe('worker-cpu-8');
    }

    // GPU requirement: worker without GPU must be skipped
    const gpuJob = {
      id: 'job-gpu',
      requirements: { gpuCount: 1 },
    };

    const noGpuWorker: WorkerCandidate = {
      workerId: 'worker-gpu-0',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 8, memoryBytes: 16384, gpuCount: 0 },
    };

    const gpuWorker: WorkerCandidate = {
      workerId: 'worker-gpu-1',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 8, memoryBytes: 16384, gpuCount: 2 },
    };

    const gpuDecision = evaluatePlacement(gpuJob, [noGpuWorker, gpuWorker]);
    expect(gpuDecision.status).toBe('SCHEDULED');
    if (gpuDecision.status === 'SCHEDULED') {
      expect(gpuDecision.workerId).toBe('worker-gpu-1');
    }
  });

  it('excludes non-operational workers (STALE, DRAINING, OFFLINE)', () => {
    const readyAliveWorker: WorkerCandidate = {
      workerId: 'worker-ready-alive',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
      ...({ status: 'READY', liveness: 'ALIVE' } as unknown as WorkerCandidate),
    };

    const readyStaleWorker: WorkerCandidate = {
      workerId: 'worker-a-stale', // would be first alphabetically, but is STALE
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
      ...({ status: 'READY', liveness: 'STALE' } as unknown as WorkerCandidate),
    };

    const drainingWorker: WorkerCandidate = {
      workerId: 'worker-a-draining',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
      ...({ status: 'DRAINING', liveness: 'ALIVE' } as unknown as WorkerCandidate),
    };

    const decision = evaluatePlacement(dummyJob, [
      readyStaleWorker,
      drainingWorker,
      readyAliveWorker,
    ]);

    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.workerId).toBe('worker-ready-alive');
      expect(decision.candidateWorkerCount).toBe(3);
      expect(decision.eligibleWorkerCount).toBe(1);
    }
  });
});

describe('ForgeScheduler — Service Class with Dependency Injection', () => {
  const testJob = new Job({
    id: createJobId('job-test-01'),
    pipelineRunId: createPipelineRunId('run-test-01'),
    stepName: 'build',
    command: 'npm run build',
    requirements: { executor: 'docker', cpuCores: 2 },
  });

  it('schedules job using injected WorkerSource and JobSource', async () => {
    const mockWorkerSource: WorkerSource = {
      listWorkers: vi.fn().mockResolvedValue([
        {
          workerId: 'worker-alpha',
          capabilities: { executors: ['docker'] },
          resources: { cpuCores: 4, memoryBytes: 8192 },
          status: 'READY',
          liveness: 'ALIVE',
        },
      ]),
    };

    const mockJobSource: JobSource = {
      getJob: vi.fn().mockResolvedValue(testJob),
    };

    const logs: string[] = [];
    const logger = createLogger({
      service: 'scheduler',
      environment: 'test',
      writeFn: (msg) => logs.push(msg),
    });

    const scheduler = new ForgeScheduler({
      workerSource: mockWorkerSource,
      jobSource: mockJobSource,
      logger,
    });

    const decision = await scheduler.schedule('job-test-01');

    expect(mockJobSource.getJob).toHaveBeenCalledWith('job-test-01');
    expect(mockWorkerSource.listWorkers).toHaveBeenCalledWith({
      status: 'READY',
      liveness: 'ALIVE',
    });
    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.workerId).toBe('worker-alpha');
    }
    expect(logs.some((l) => l.includes('Scheduler placement evaluated'))).toBe(true);

    // CRITICAL: Ensure scheduler does NOT mutate job state
    expect(testJob.status).toBe('PENDING');
  });

  it('throws JobNotFoundError when job does not exist in JobSource', async () => {
    const mockJobSource: JobSource = {
      getJob: vi.fn().mockResolvedValue(null),
    };

    const scheduler = new ForgeScheduler({ jobSource: mockJobSource });

    await expect(scheduler.schedule('nonexistent-job')).rejects.toThrow(JobNotFoundError);
  });

  it('throws JobSourceError when jobSource retrieval fails', async () => {
    const mockJobSource: JobSource = {
      getJob: vi.fn().mockRejectedValue(new Error('PostgreSQL connection timeout')),
    };

    const scheduler = new ForgeScheduler({ jobSource: mockJobSource });

    await expect(scheduler.schedule('job-fail')).rejects.toThrow(JobSourceError);
  });

  it('throws WorkerSourceError when workerSource retrieval fails', async () => {
    const mockWorkerSource: WorkerSource = {
      listWorkers: vi.fn().mockRejectedValue(new Error('Redis cluster down')),
    };

    const scheduler = new ForgeScheduler({ workerSource: mockWorkerSource });

    await expect(scheduler.schedule(testJob)).rejects.toThrow(WorkerSourceError);
  });

  it('allows scheduling directly with Job object without JobSource configured', async () => {
    const mockWorkerSource: WorkerSource = {
      listWorkers: vi.fn().mockResolvedValue([]),
    };

    const scheduler = new ForgeScheduler({ workerSource: mockWorkerSource });
    const decision = await scheduler.schedule(testJob);

    expect(decision.status).toBe('UNSCHEDULABLE');
    expect(decision.reason).toBe('NO_ELIGIBLE_WORKER');
  });

  it('includes job priority in single schedule decision', async () => {
    const jobWithPriority = new Job({
      id: createJobId('job-prio-42'),
      pipelineRunId: createPipelineRunId('run-prio'),
      stepName: 'build',
      command: 'echo build',
      priority: 42,
    });

    const mockWorkerSource: WorkerSource = {
      listWorkers: vi.fn().mockResolvedValue([
        {
          workerId: 'worker-1',
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: 4, memoryBytes: 4096 },
          status: 'READY',
          liveness: 'ALIVE',
        },
      ]),
    };

    const scheduler = new ForgeScheduler({ workerSource: mockWorkerSource });
    const decision = await scheduler.schedule(jobWithPriority);

    expect(decision.status).toBe('SCHEDULED');
    expect(decision.priority).toBe(42);
  });
});

describe('Scheduler — evaluatePrioritizedWork & schedulePrioritized', () => {
  const workerNormal: WorkerCandidate = {
    workerId: 'worker-normal',
    capabilities: { executors: ['docker'] },
    resources: { cpuCores: 4, memoryBytes: 8192 },
    ...({ status: 'READY', liveness: 'ALIVE' } as unknown as WorkerCandidate),
  };

  const jobHighUnsatisfiable = new Job({
    id: createJobId('job-high-unschedulable'),
    pipelineRunId: createPipelineRunId('run-batch'),
    stepName: 'huge-task',
    command: 'echo huge',
    priority: 500,
    requirements: { executor: 'docker', cpuCores: 64, memoryBytes: 128 * 1024 * 1024 * 1024 },
  });

  const jobMediumEligible = new Job({
    id: createJobId('job-medium-eligible'),
    pipelineRunId: createPipelineRunId('run-batch'),
    stepName: 'med-task',
    command: 'echo med',
    priority: 100,
    requirements: { executor: 'docker', cpuCores: 2, memoryBytes: 4096 },
  });

  const jobLowEligible = new Job({
    id: createJobId('job-low-eligible'),
    pipelineRunId: createPipelineRunId('run-batch'),
    stepName: 'low-task',
    command: 'echo low',
    priority: -50,
    requirements: { executor: 'docker', cpuCores: 1, memoryBytes: 1024 },
  });

  it('demonstrates non-blocking unschedulable semantics: unsatisfiable high-priority job never blocks eligible lower-priority jobs', () => {
    // Input order is intentionally shuffled
    const batch = [jobLowEligible, jobHighUnsatisfiable, jobMediumEligible];

    const result = evaluatePrioritizedWork(batch, [workerNormal]);

    // Evaluation sequence must be strictly priority descending: 500 -> 100 -> -50
    expect(result.orderedDecisions).toHaveLength(3);
    expect(result.orderedDecisions[0]?.jobId).toBe('job-high-unschedulable');
    expect(result.orderedDecisions[0]?.status).toBe('UNSCHEDULABLE');
    expect(result.orderedDecisions[0]?.priority).toBe(500);

    // Job medium (100) was NOT blocked by job high (500) being unschedulable
    expect(result.orderedDecisions[1]?.jobId).toBe('job-medium-eligible');
    expect(result.orderedDecisions[1]?.status).toBe('SCHEDULED');
    expect(result.orderedDecisions[1]?.priority).toBe(100);

    // Job low (-50) was also evaluated and scheduled
    expect(result.orderedDecisions[2]?.jobId).toBe('job-low-eligible');
    expect(result.orderedDecisions[2]?.status).toBe('SCHEDULED');
    expect(result.orderedDecisions[2]?.priority).toBe(-50);

    expect(result.scheduledDecisions).toHaveLength(2);
    expect(result.unschedulableDecisions).toHaveLength(1);
    expect(result.unschedulableDecisions[0]?.reason).toBe('NO_ELIGIBLE_WORKER');
  });

  it('schedules prioritized batch via ForgeScheduler.schedulePrioritized with mixed Job and IDs', async () => {
    const mockWorkerSource: WorkerSource = {
      listWorkers: vi.fn().mockResolvedValue([workerNormal]),
    };

    const mockJobSource: JobSource = {
      getJob: vi.fn().mockImplementation(async (id: string) => {
        if (id === jobMediumEligible.id) return jobMediumEligible;
        return null;
      }),
    };

    const scheduler = new ForgeScheduler({
      workerSource: mockWorkerSource,
      jobSource: mockJobSource,
    });

    const result = await scheduler.schedulePrioritized([
      jobLowEligible,
      jobMediumEligible.id, // ID string
    ]);

    expect(result.orderedDecisions).toHaveLength(2);
    // 100 > -50
    expect(result.orderedDecisions[0]?.jobId).toBe(jobMediumEligible.id);
    expect(result.orderedDecisions[0]?.status).toBe('SCHEDULED');
    expect(result.orderedDecisions[1]?.jobId).toBe(jobLowEligible.id);
    expect(result.orderedDecisions[1]?.status).toBe('SCHEDULED');
  });

  it('scheduleNextBatch dequeues batch, orders by priority, and evaluates without acknowledging messages', async () => {
    const mockQueue = {
      dequeue: vi
        .fn()
        .mockResolvedValueOnce({
          message: { messageId: 'msg-1', jobId: jobLowEligible.id },
        })
        .mockResolvedValueOnce({
          message: { messageId: 'msg-2', jobId: jobMediumEligible.id },
        })
        .mockResolvedValueOnce(null),
      acknowledge: vi.fn(),
    };

    const mockJobSource: JobSource = {
      getJob: vi.fn().mockImplementation(async (id: string) => {
        if (id === jobLowEligible.id) return jobLowEligible;
        if (id === jobMediumEligible.id) return jobMediumEligible;
        return null;
      }),
    };

    const mockWorkerSource: WorkerSource = {
      listWorkers: vi.fn().mockResolvedValue([workerNormal]),
    };

    const scheduler = new ForgeScheduler({
      workerSource: mockWorkerSource,
      jobSource: mockJobSource,
    });

    const batchResult = await scheduler.scheduleNextBatch(
      mockQueue as unknown as Parameters<typeof scheduler.scheduleNextBatch>[0],
      5,
    );

    expect(batchResult.deliveries).toHaveLength(2);
    expect(batchResult.result.orderedDecisions).toHaveLength(2);

    // Evaluated in priority order: jobMediumEligible (100) first, then jobLowEligible (-50)
    expect(batchResult.result.orderedDecisions[0]?.jobId).toBe(jobMediumEligible.id);
    expect(batchResult.result.orderedDecisions[1]?.jobId).toBe(jobLowEligible.id);

    // CRITICAL: queue.acknowledge must NEVER be called by scheduler
    expect(mockQueue.acknowledge).not.toHaveBeenCalled();
  });
});
