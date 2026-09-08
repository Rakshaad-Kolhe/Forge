import type { UnschedulableDecision } from '@forge/contracts';
import type { LeaseRecoveryService } from '@forge/database';
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

  describe('Distributed Worker Lease Claiming Integration', () => {
    it('claims lease when placement succeeds and leaseRepository is configured', async () => {
      const mockLease = {
        id: 'lease-test-123',
        jobId: jobMediumEligible.id,
        workerId: 'worker-normal',
        status: 'ACTIVE' as const,
        durationMs: 30000,
        acquiredAt: new Date(),
        renewedAt: new Date(),
        expiresAt: new Date(Date.now() + 30000),
        createdAt: new Date(),
      };

      const mockLeaseRepo = {
        claim: vi.fn().mockResolvedValue({
          status: 'ACQUIRED',
          lease: mockLease,
          isIdempotent: false,
        }),
        renew: vi.fn(),
        release: vi.fn(),
        findActiveByJobId: vi.fn(),
        findById: vi.fn(),
        findByWorkerId: vi.fn(),
        reclaimExpiredLeases: vi.fn(),
      };

      const scheduler = new ForgeScheduler({
        workerSource: { listWorkers: vi.fn().mockResolvedValue([workerNormal]) },
        leaseRepository: mockLeaseRepo,
        leaseDurationMs: 30000,
      });

      const decision = await scheduler.schedule(jobMediumEligible);

      expect(decision.status).toBe('SCHEDULED');
      if (decision.status === 'SCHEDULED') {
        expect(decision.workerId).toBe('worker-normal');
        expect(decision.lease).toBeDefined();
        expect(decision.lease?.id).toBe('lease-test-123');
      }
      expect(mockLeaseRepo.claim).toHaveBeenCalledWith({
        jobId: jobMediumEligible.id,
        workerId: 'worker-normal',
        durationMs: 30000,
      });
    });

    it('returns UNSCHEDULABLE with LEASE_CONFLICT when claim encounters active lease conflict', async () => {
      const mockLeaseRepo = {
        claim: vi.fn().mockResolvedValue({
          status: 'CONFLICT',
          reason: 'LEASE_ALREADY_HELD',
          currentOwnerId: 'worker-other',
          expiresAt: new Date(Date.now() + 20000),
        }),
        renew: vi.fn(),
        release: vi.fn(),
        findActiveByJobId: vi.fn(),
        findById: vi.fn(),
        findByWorkerId: vi.fn(),
        reclaimExpiredLeases: vi.fn(),
      };

      const scheduler = new ForgeScheduler({
        workerSource: { listWorkers: vi.fn().mockResolvedValue([workerNormal]) },
        leaseRepository: mockLeaseRepo,
      });

      const decision = await scheduler.schedule(jobMediumEligible);

      expect(decision.status).toBe('UNSCHEDULABLE');
      if (decision.status === 'UNSCHEDULABLE') {
        expect(decision.reason).toBe('LEASE_CONFLICT');
        expect(decision.failureReasons?.[0]).toContain('worker-other');
      }
    });

    it('claims leases in schedulePrioritized for successfully scheduled jobs', async () => {
      const mockLeaseRepo = {
        claim: vi.fn().mockImplementation((opts: { jobId: string; workerId: string }) =>
          Promise.resolve({
            status: 'ACQUIRED',
            lease: {
              id: `lease-${opts.jobId}`,
              jobId: opts.jobId,
              workerId: opts.workerId,
              status: 'ACTIVE' as const,
              durationMs: 30000,
              acquiredAt: new Date(),
              renewedAt: new Date(),
              expiresAt: new Date(Date.now() + 30000),
              createdAt: new Date(),
            },
          }),
        ),
        renew: vi.fn(),
        release: vi.fn(),
        findActiveByJobId: vi.fn(),
        findById: vi.fn(),
        findByWorkerId: vi.fn(),
        reclaimExpiredLeases: vi.fn(),
      };

      const scheduler = new ForgeScheduler({
        workerSource: { listWorkers: vi.fn().mockResolvedValue([workerNormal]) },
        leaseRepository: mockLeaseRepo,
      });

      const prioritizedResult = await scheduler.schedulePrioritized([
        jobMediumEligible,
        jobLowEligible,
      ]);

      expect(prioritizedResult.scheduledDecisions).toHaveLength(2);
      expect(prioritizedResult.scheduledDecisions[0]?.lease?.id).toBe(
        `lease-${jobMediumEligible.id}`,
      );
      expect(prioritizedResult.scheduledDecisions[1]?.lease?.id).toBe(`lease-${jobLowEligible.id}`);
      expect(mockLeaseRepo.claim).toHaveBeenCalledTimes(2);
    });
  });

  describe('Scheduler — Retry Backoff Awareness', () => {
    const worker: WorkerCandidate = {
      workerId: 'worker-1',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192 },
    };

    it('returns UNSCHEDULABLE with RETRY_BACKOFF_ACTIVE when nextAttemptAt is in the future', () => {
      const futureDate = new Date(Date.now() + 10000);
      const backoffJob = {
        id: 'job-backoff',
        requirements: { executor: 'docker', cpuCores: 2, memoryBytes: 4096 },
        nextAttemptAt: futureDate,
      };

      const decision = evaluatePlacement(backoffJob, [worker]);

      expect(decision.status).toBe('UNSCHEDULABLE');
      if (decision.status === 'UNSCHEDULABLE') {
        expect(decision.jobId).toBe('job-backoff');
        expect(decision.reason).toBe('RETRY_BACKOFF_ACTIVE');
        expect(decision.failureReasons?.[0]).toContain('Retry backoff active until');
      }
    });

    it('schedules successfully when nextAttemptAt is in the past (due for retry)', () => {
      const pastDate = new Date(Date.now() - 5000);
      const dueJob = {
        id: 'job-due',
        requirements: { executor: 'docker', cpuCores: 2, memoryBytes: 4096 },
        nextAttemptAt: pastDate,
      };

      const decision = evaluatePlacement(dueJob, [worker]);

      expect(decision.status).toBe('SCHEDULED');
      if (decision.status === 'SCHEDULED') {
        expect(decision.jobId).toBe('job-due');
        expect(decision.workerId).toBe('worker-1');
      }
    });

    it('respects explicitly passed now parameter in evaluatePlacement', () => {
      const referenceDate = new Date('2026-09-07T12:00:00Z');
      const targetDate = new Date('2026-09-07T12:05:00Z');

      const futureJob = {
        id: 'job-param',
        requirements: { executor: 'docker', cpuCores: 2, memoryBytes: 4096 },
        nextAttemptAt: targetDate,
      };

      // When now is before targetDate -> UNSCHEDULABLE
      const decisionBefore = evaluatePlacement(futureJob, [worker], referenceDate);
      expect(decisionBefore.status).toBe('UNSCHEDULABLE');
      if (decisionBefore.status === 'UNSCHEDULABLE') {
        expect(decisionBefore.reason).toBe('RETRY_BACKOFF_ACTIVE');
      }

      // When now is after targetDate -> SCHEDULED
      const decisionAfter = evaluatePlacement(
        futureJob,
        [worker],
        new Date('2026-09-07T12:06:00Z'),
      );
      expect(decisionAfter.status).toBe('SCHEDULED');
    });

    it('scheduleDueJobs queries findSchedulableJobs and schedules due jobs in priority order', async () => {
      const dueHighJob = new Job({
        id: createJobId('job-due-high'),
        pipelineRunId: createPipelineRunId('run-1'),
        stepName: 'build-high',
        command: 'echo high',
        requirements: { executor: 'docker', cpuCores: 2, memoryBytes: 4096 },
        priority: 100,
        nextAttemptAt: new Date(Date.now() - 1000),
      });

      const dueLowJob = new Job({
        id: createJobId('job-due-low'),
        pipelineRunId: createPipelineRunId('run-1'),
        stepName: 'build-low',
        command: 'echo low',
        requirements: { executor: 'docker', cpuCores: 2, memoryBytes: 4096 },
        priority: 10,
        nextAttemptAt: new Date(Date.now() - 2000),
      });

      const mockJobSource: JobSource = {
        findSchedulableJobs: vi.fn().mockResolvedValue([dueHighJob, dueLowJob]),
        getJob: vi.fn(),
      };

      const scheduler = new ForgeScheduler({
        workerSource: { listWorkers: vi.fn().mockResolvedValue([worker]) },
        jobSource: mockJobSource,
      });

      const result = await scheduler.scheduleDueJobs({ limit: 5 });

      expect(mockJobSource.findSchedulableJobs).toHaveBeenCalled();
      expect(result.processedCount).toBe(2);
      expect(result.scheduledCount).toBe(2);
      expect(result.scheduledDecisions[0]?.jobId).toBe(dueHighJob.id);
      expect(result.scheduledDecisions[1]?.jobId).toBe(dueLowJob.id);
    });

    it('scheduleDueJobs throws JobSourceError if findSchedulableJobs is not implemented on JobSource', async () => {
      const mockJobSource: JobSource = {
        getJob: vi.fn(),
      };

      const scheduler = new ForgeScheduler({
        workerSource: { listWorkers: vi.fn().mockResolvedValue([worker]) },
        jobSource: mockJobSource,
      });

      await expect(scheduler.scheduleDueJobs()).rejects.toThrow(JobSourceError);
    });
  });

  describe('Scheduler — Worker Draining & Lease Recovery Integration', () => {
    it('strictly excludes DRAINING + ALIVE candidate workers from job placement', async () => {
      const drainingWorker: WorkerCandidate = {
        workerId: 'worker-draining',
        ...({
          status: 'DRAINING',
          liveness: 'ALIVE',
          worker: {
            status: 'DRAINING',
            workerId: 'worker-draining',
            capabilities: { executors: ['docker'] },
            resources: { cpuCores: 4, memoryBytes: 8192 },
          },
        } as unknown as WorkerCandidate),
      };

      const job = new Job({
        id: createJobId('job-drain-test'),
        pipelineRunId: createPipelineRunId('run-1'),
        stepName: 'test',
        command: 'echo 1',
        requirements: { executor: 'docker' },
      });

      // Pure evaluatePlacement check
      const placementDecision = evaluatePlacement(job, [drainingWorker]);
      expect(placementDecision.status).toBe('UNSCHEDULABLE');
      expect((placementDecision as UnschedulableDecision).reason).toBe('NO_ELIGIBLE_WORKER');

      // Full scheduler.schedule check
      const scheduler = new ForgeScheduler({
        workerSource: { listWorkers: vi.fn().mockResolvedValue([drainingWorker]) },
      });

      const decision = await scheduler.schedule(job);
      expect(decision.status).toBe('UNSCHEDULABLE');
      expect((decision as UnschedulableDecision).reason).toBe('NO_ELIGIBLE_WORKER');
    });

    it('delegates recoverExpiredLeases to configured recoveryService', async () => {
      const mockRecoveryService = {
        recoverExpiredLeases: vi.fn().mockResolvedValue({
          recoveredCount: 1,
          details: [
            {
              leaseId: 'lease-1',
              jobId: 'job-1',
              workerId: 'worker-crashed',
              action: 'REQUEUED' as const,
              nextAttemptAt: new Date(),
            },
          ],
        }),
      };

      const scheduler = new ForgeScheduler({
        recoveryService: mockRecoveryService as unknown as LeaseRecoveryService,
      });

      const result = await scheduler.recoverExpiredLeases({ batchSize: 5 });
      expect(mockRecoveryService.recoverExpiredLeases).toHaveBeenCalledWith({ batchSize: 5 });
      expect(result.recoveredCount).toBe(1);
      expect(result.details[0]?.action).toBe('REQUEUED');
    });

    it('throws if recoverExpiredLeases is called without recoveryService', async () => {
      const scheduler = new ForgeScheduler();
      await expect(scheduler.recoverExpiredLeases()).rejects.toThrow(
        /LeaseRecoveryService is required to recover expired leases/,
      );
    });

    it('starts and cleanly stops periodic recovery loop without overlapping sweeps', async () => {
      let sweepCount = 0;
      const mockRecoveryService = {
        recoverExpiredLeases: vi.fn(async () => {
          sweepCount++;
          // Simulate 20ms sweep delay
          await new Promise((resolve) => setTimeout(resolve, 20));
          return { recoveredCount: 0, details: [] };
        }),
      };

      const scheduler = new ForgeScheduler({
        recoveryService: mockRecoveryService as unknown as LeaseRecoveryService,
      });

      // Start loop with 30ms interval
      scheduler.startRecoveryLoop(30);

      // Calling startRecoveryLoop again is a no-op
      scheduler.startRecoveryLoop(30);

      // Wait 100ms for sweeps to fire
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Stop loop cleanly
      await scheduler.stopRecoveryLoop();
      const finalCount = sweepCount;
      expect(finalCount).toBeGreaterThanOrEqual(1);

      // Wait another 50ms and verify no further sweeps fired
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(sweepCount).toBe(finalCount);
    });
  });
});
