import { describe, it, expect, vi } from 'vitest';
import type { Executor } from '@forge/contracts';
import type { JobRepository } from '@forge/database';
import { InProcessEventBus, type ForgeEvent } from '@forge/events';
import type { Job } from '@forge/pipeline';
import { startWorker } from './index.js';
import { createLogger } from '@forge/logging';
import {
  type WorkerRegistry,
  type WorkerId,
  type RegisterWorkerInput,
  type WorkerMetadata,
  type WorkerStatus,
  createWorkerId,
} from '@forge/worker-registry';

describe('Worker Service Shell', () => {
  it('starts successfully and logs startup event in standalone mode', async () => {
    const logs: string[] = [];
    const testLogger = createLogger({
      service: 'worker',
      environment: 'development',
      writeFn: (msg) => logs.push(msg),
    });

    const worker = startWorker({ logger: testLogger });
    expect(logs.some((l) => l.includes('Forge Worker service shell started'))).toBe(true);
    expect(worker.workerId).toBeDefined();
    expect(worker.getStatus()).toBe('READY');

    await worker.stop();
    expect(logs.some((l) => l.includes('Forge Worker service shell stopped'))).toBe(true);
  });

  it('orchestrates registration, periodic heartbeat, and deregistration with registry', async () => {
    const registered: RegisterWorkerInput[] = [];
    const heartbeats: { id: WorkerId; status?: WorkerStatus }[] = [];
    const deregistered: WorkerId[] = [];

    const mockRegistry: WorkerRegistry = {
      register: vi.fn(async (input: RegisterWorkerInput): Promise<WorkerMetadata> => {
        registered.push(input);
        const wId = createWorkerId(input.workerId ?? 'w1');
        return {
          workerId: wId,
          status: input.status ?? 'READY',
          hostname: input.hostname,
          capabilities: input.capabilities,
          resources: input.resources,
          registeredAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        };
      }),
      heartbeat: vi.fn(async (id: WorkerId, status?: WorkerStatus): Promise<void> => {
        heartbeats.push({ id, status });
      }),
      deregister: vi.fn(async (id: WorkerId): Promise<void> => {
        deregistered.push(id);
      }),
      getWorker: vi.fn(),
      listWorkers: vi.fn(),
    };

    const worker = startWorker({
      workerId: 'test-worker-1',
      registry: mockRegistry,
      heartbeatIntervalMs: 20,
    });

    // Wait for initial registration promise to resolve
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(mockRegistry.register).toHaveBeenCalledWith(
      expect.objectContaining({
        workerId: 'test-worker-1',
        status: 'READY',
        capabilities: expect.objectContaining({
          executors: ['docker'],
        }),
      }),
    );
    expect(registered.length).toBe(1);
    expect(registered[0]?.workerId).toBe('test-worker-1');

    // Wait for at least one heartbeat tick
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(mockRegistry.heartbeat).toHaveBeenCalled();
    expect(heartbeats.length).toBeGreaterThanOrEqual(1);
    expect(heartbeats[0]?.id).toBe('test-worker-1');

    // Stop the worker
    await worker.stop();

    expect(mockRegistry.deregister).toHaveBeenCalledWith('test-worker-1');
    expect(deregistered).toContain('test-worker-1');
    expect(worker.getStatus()).toBe('OFFLINE');
  });

  it('manages distributed leases: claims, renews, releases, and gracefully releases on stop', async () => {
    const mockLease = {
      id: 'lease-worker-001',
      jobId: 'job-worker-001',
      workerId: 'worker-leased',
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
      renew: vi.fn().mockResolvedValue({
        status: 'RENEWED',
        lease: { ...mockLease, renewedAt: new Date(), expiresAt: new Date(Date.now() + 45000) },
      }),
      release: vi.fn().mockResolvedValue({
        status: 'RELEASED',
        leaseId: 'lease-worker-001',
        jobId: 'job-worker-001',
      }),
      findActiveByJobId: vi.fn(),
      findById: vi.fn(),
      findByWorkerId: vi.fn(),
      reclaimExpiredLeases: vi.fn(),
    };

    const worker = startWorker({
      workerId: 'worker-leased',
      leaseRepository: mockLeaseRepo,
      defaultLeaseDurationMs: 30000,
    });

    // 1. Claim job
    const claimRes = await worker.claimJob('job-worker-001');
    expect(claimRes.status).toBe('ACQUIRED');
    expect(mockLeaseRepo.claim).toHaveBeenCalledWith({
      jobId: 'job-worker-001',
      workerId: 'worker-leased',
      durationMs: 30000,
    });
    expect(worker.getActiveLeases()).toHaveLength(1);
    expect(worker.getActiveLeases()[0]?.id).toBe('lease-worker-001');

    // 2. Renew lease
    const renewRes = await worker.renewLease('lease-worker-001', 'job-worker-001', 45000);
    expect(renewRes.status).toBe('RENEWED');
    expect(mockLeaseRepo.renew).toHaveBeenCalledWith({
      leaseId: 'lease-worker-001',
      jobId: 'job-worker-001',
      workerId: 'worker-leased',
      durationMs: 45000,
    });

    // 3. Graceful stop releases active lease
    await worker.stop();
    expect(mockLeaseRepo.release).toHaveBeenCalledWith({
      leaseId: 'lease-worker-001',
      jobId: 'job-worker-001',
      workerId: 'worker-leased',
    });
    expect(worker.getActiveLeases()).toHaveLength(0);
  });

  it('executeJob validates active lease before executing; rejects if lease not held', async () => {
    const mockLeaseRepo = {
      claim: vi.fn(),
      renew: vi.fn(),
      release: vi.fn(),
      findActiveByJobId: vi.fn().mockResolvedValue(null), // No active lease
      findById: vi.fn(),
      findByWorkerId: vi.fn(),
      reclaimExpiredLeases: vi.fn(),
    };

    const worker = startWorker({
      workerId: 'worker-1',
      leaseRepository: mockLeaseRepo,
    });

    const mockJob = {
      id: 'job-unowned',
      command: 'echo test',
      status: 'QUEUED',
      createAttempt: vi.fn(),
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
    };

    await expect(
      worker.executeJob({
        job: mockJob as unknown as Job,
        leaseId: 'lease-unowned',
      }),
    ).rejects.toThrow(/does not hold active lease/);
  });

  it('executeJob orchestrates full lifecycle: starts attempt, executes, persists, and releases lease', async () => {
    const mockLease = {
      id: 'lease-exec-1',
      jobId: 'job-exec-1',
      workerId: 'worker-exec',
      status: 'ACTIVE' as const,
      durationMs: 30000,
      acquiredAt: new Date(),
      renewedAt: new Date(),
      expiresAt: new Date(Date.now() + 30000),
      createdAt: new Date(),
    };

    const mockLeaseRepo = {
      claim: vi.fn(),
      renew: vi.fn(),
      release: vi.fn().mockResolvedValue({
        status: 'RELEASED',
        leaseId: 'lease-exec-1',
        jobId: 'job-exec-1',
      }),
      findActiveByJobId: vi.fn().mockResolvedValue(mockLease),
      findById: vi.fn(),
      findByWorkerId: vi.fn(),
      reclaimExpiredLeases: vi.fn(),
    };

    const savedJobs: unknown[] = [];
    const mockJobRepo = {
      save: vi.fn(async (j) => {
        savedJobs.push(j);
      }),
      findById: vi.fn(),
      findByPipelineRunId: vi.fn(),
    };

    const mockExecutor = {
      name: 'mock',
      isAvailable: vi.fn().mockResolvedValue(true),
      execute: vi.fn().mockResolvedValue({
        status: 'SUCCEEDED',
        exitCode: 0,
        startedAt: new Date(),
        finishedAt: new Date(),
        durationMs: 250,
        stdout: 'Success output',
        stderr: '',
        truncated: false,
      }),
    };

    const worker = startWorker({
      workerId: 'worker-exec',
      leaseRepository: mockLeaseRepo,
      jobRepository: mockJobRepo as unknown as JobRepository,
      executor: mockExecutor,
    });

    const mockAttempt = {
      id: 'att-1',
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
      timeout: vi.fn(),
      cancel: vi.fn(),
    };

    const mockJob = {
      id: 'job-exec-1',
      command: 'echo "hello"',
      status: 'QUEUED',
      createAttempt: vi.fn().mockReturnValue(mockAttempt),
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
      timeout: vi.fn(),
      cancel: vi.fn(),
    };

    const { result, attempt, job } = await worker.executeJob({
      job: mockJob as unknown as Job,
      leaseId: 'lease-exec-1',
    });

    expect(attempt).toBeDefined();
    expect(result.status).toBe('SUCCEEDED');
    expect(mockAttempt.start).toHaveBeenCalled();
    expect(mockJob.start).toHaveBeenCalled();
    expect(mockExecutor.execute).toHaveBeenCalled();
    expect(mockAttempt.succeed).toHaveBeenCalledWith(0, expect.any(String));
    expect(mockJob.succeed).toHaveBeenCalled();
    expect(mockJobRepo.save).toHaveBeenCalledWith(job);
    expect(mockLeaseRepo.release).toHaveBeenCalledWith({
      leaseId: 'lease-exec-1',
      jobId: 'job-exec-1',
      workerId: 'worker-exec',
    });
  });

  it('executeJob handles execution timeout: marks TIMED_OUT, persists, and releases lease', async () => {
    const mockLease = {
      id: 'lease-exec-timeout',
      jobId: 'job-exec-timeout',
      workerId: 'worker-exec',
      status: 'ACTIVE' as const,
      durationMs: 30000,
      acquiredAt: new Date(),
      renewedAt: new Date(),
      expiresAt: new Date(Date.now() + 30000),
      createdAt: new Date(),
    };

    const mockLeaseRepo = {
      claim: vi.fn(),
      renew: vi.fn(),
      release: vi.fn().mockResolvedValue({
        status: 'RELEASED',
        leaseId: 'lease-exec-timeout',
        jobId: 'job-exec-timeout',
      }),
      findActiveByJobId: vi.fn().mockResolvedValue(mockLease),
      findById: vi.fn(),
      findByWorkerId: vi.fn(),
      reclaimExpiredLeases: vi.fn(),
    };

    const mockJobRepo = {
      save: vi.fn(),
      findById: vi.fn(),
      findByPipelineRunId: vi.fn(),
    };

    const mockExecutor = {
      name: 'mock',
      isAvailable: vi.fn().mockResolvedValue(true),
      execute: vi.fn().mockResolvedValue({
        status: 'TIMED_OUT',
        exitCode: null,
        startedAt: new Date(),
        finishedAt: new Date(),
        durationMs: 1500,
        stdout: '',
        stderr: '',
        truncated: false,
        failureReason: 'Execution timed out',
      }),
    };

    const worker = startWorker({
      workerId: 'worker-exec',
      leaseRepository: mockLeaseRepo,
      jobRepository: mockJobRepo as unknown as JobRepository,
      executor: mockExecutor,
    });

    const mockAttempt = {
      id: 'att-timeout',
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
      timeout: vi.fn(),
      cancel: vi.fn(),
    };

    const mockJob = {
      id: 'job-exec-timeout',
      command: 'sleep 100',
      status: 'QUEUED',
      createAttempt: vi.fn().mockReturnValue(mockAttempt),
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
      timeout: vi.fn(),
      cancel: vi.fn(),
    };

    const { result } = await worker.executeJob({
      job: mockJob as unknown as Job,
      leaseId: 'lease-exec-timeout',
    });

    expect(result.status).toBe('TIMED_OUT');
    expect(mockAttempt.timeout).toHaveBeenCalledWith(expect.any(String));
    expect(mockJob.timeout).toHaveBeenCalled();
    expect(mockJobRepo.save).toHaveBeenCalled();
    expect(mockLeaseRepo.release).toHaveBeenCalled();
  });

  it('executeJob detects definitive lease loss during renewal and aborts execution', async () => {
    const mockLease = {
      id: 'lease-loss',
      jobId: 'job-loss',
      workerId: 'worker-exec',
      status: 'ACTIVE' as const,
      durationMs: 30000,
      acquiredAt: new Date(),
      renewedAt: new Date(),
      expiresAt: new Date(Date.now() + 30000),
      createdAt: new Date(),
    };

    const mockLeaseRepo = {
      claim: vi.fn(),
      renew: vi.fn().mockResolvedValue({
        status: 'REJECTED',
        reason: 'LEASE_EXPIRED',
      }),
      release: vi.fn(),
      findActiveByJobId: vi.fn().mockResolvedValue(mockLease),
      findById: vi.fn(),
      findByWorkerId: vi.fn(),
      reclaimExpiredLeases: vi.fn(),
    };

    const mockJobRepo = {
      save: vi.fn(),
      findById: vi.fn(),
      findByPipelineRunId: vi.fn(),
    };

    // Executor that simulates long execution and listens to abortSignal
    const mockExecutor = {
      name: 'mock',
      isAvailable: vi.fn().mockResolvedValue(true),
      execute: vi.fn(async (ctx) => {
        return new Promise((resolve) => {
          ctx.abortSignal?.addEventListener('abort', () => {
            resolve({
              status: 'CANCELLED',
              exitCode: null,
              startedAt: new Date(),
              finishedAt: new Date(),
              durationMs: 50,
              stdout: '',
              stderr: '',
              truncated: false,
              failureReason: 'Execution cancelled via abort signal',
            });
          });
        });
      }),
    };

    const worker = startWorker({
      workerId: 'worker-exec',
      leaseRepository: mockLeaseRepo,
      jobRepository: mockJobRepo as unknown as JobRepository,
      executor: mockExecutor as unknown as Executor,
      defaultLeaseRenewalIntervalMs: 20, // 20ms interval
    });

    const mockAttempt = {
      id: 'att-loss',
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
      timeout: vi.fn(),
      cancel: vi.fn(),
    };

    const mockJob = {
      id: 'job-loss',
      command: 'sleep 50',
      status: 'QUEUED',
      createAttempt: vi.fn().mockReturnValue(mockAttempt),
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
      timeout: vi.fn(),
      cancel: vi.fn(),
    };

    const { result } = await worker.executeJob({
      job: mockJob as unknown as Job,
      leaseId: 'lease-loss',
    });

    expect(result).toBeDefined();
    expect(mockAttempt.fail).toHaveBeenCalledWith(
      1,
      expect.stringContaining('Lease ownership lost'),
      expect.any(String),
    );
    expect(mockJob.fail).toHaveBeenCalled();
    // Lease release should NOT be attempted when ownership was definitively lost
    expect(mockLeaseRepo.release).not.toHaveBeenCalled();
  });

  describe('Worker Retry Attempt Orchestration', () => {
    it('schedules retry when attempt fails and policy permits further attempts', async () => {
      const mockLease = {
        id: 'lease-retry-1',
        jobId: 'job-retry-1',
        workerId: 'worker-retry',
        status: 'ACTIVE' as const,
        durationMs: 30000,
        acquiredAt: new Date(),
        renewedAt: new Date(),
        expiresAt: new Date(Date.now() + 30000),
        createdAt: new Date(),
      };

      const mockLeaseRepo = {
        claim: vi.fn(),
        renew: vi.fn(),
        release: vi.fn().mockResolvedValue({ status: 'RELEASED' }),
        findActiveByJobId: vi.fn().mockResolvedValue(mockLease),
        findById: vi.fn(),
        findByWorkerId: vi.fn(),
        reclaimExpiredLeases: vi.fn(),
      };

      const mockJobRepo = {
        save: vi.fn().mockResolvedValue(undefined),
        findById: vi.fn(),
        findByPipelineRunId: vi.fn(),
      };

      const mockExecutor = {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi.fn().mockResolvedValue({
          status: 'FAILED',
          exitCode: 42,
          failureReason: 'Process exited with code 42',
          startedAt: new Date(),
          finishedAt: new Date(),
          durationMs: 100,
          stdout: '',
          stderr: 'Process exited with code 42',
          truncated: false,
        }),
      };

      const worker = startWorker({
        workerId: 'worker-retry',
        leaseRepository: mockLeaseRepo,
        jobRepository: mockJobRepo as unknown as JobRepository,
        executor: mockExecutor as unknown as Executor,
      });

      const mockAttempt = {
        id: 'att-1',
        attemptNumber: 1,
        status: 'RUNNING' as 'RUNNING' | 'FAILED',
        start: vi.fn(),
        succeed: vi.fn(),
        fail: vi.fn().mockImplementation(() => {
          mockAttempt.status = 'FAILED';
        }),
        timeout: vi.fn(),
        cancel: vi.fn(),
      };

      const mockJob = {
        id: 'job-retry-1',
        command: 'exit 42',
        status: 'QUEUED',
        retryPolicy: {
          maxAttempts: 3,
          backoff: {
            baseDelayMs: 1000,
            factor: 2,
            maxDelayMs: 10000,
          },
          retryOn: ['FAILED' as const],
        },
        createAttempt: vi.fn().mockReturnValue(mockAttempt),
        start: vi.fn(),
        succeed: vi.fn(),
        fail: vi.fn(),
        timeout: vi.fn(),
        cancel: vi.fn(),
        transitionTo: vi.fn(),
        setNextAttemptAt: vi.fn(),
        clearNextAttemptAt: vi.fn(),
      };

      const execResult = await worker.executeJob({
        job: mockJob as unknown as Job,
        leaseId: 'lease-retry-1',
      });

      expect(execResult.result.status).toBe('FAILED');
      expect(mockAttempt.fail).toHaveBeenCalledWith(
        42,
        'Process exited with code 42',
        expect.any(String),
      );
      expect(execResult.retryDecision).toBeDefined();
      expect(execResult.retryDecision?.action).toBe('RETRY');
      if (execResult.retryDecision?.action === 'RETRY') {
        expect(execResult.retryDecision.nextAttemptNumber).toBe(2);
        expect(execResult.retryDecision.delayMs).toBe(1000); // 1000 * 2^(1-1)
      }

      // Verifies state transitions and scheduling
      expect(mockJob.transitionTo).toHaveBeenCalledWith('QUEUED');
      expect(mockJob.setNextAttemptAt).toHaveBeenCalledWith(expect.any(Date));
      expect(mockJob.fail).not.toHaveBeenCalled();

      // Verifies persistence and lease release (fresh lease required per attempt)
      expect(mockJobRepo.save).toHaveBeenCalledWith(mockJob);
      expect(mockLeaseRepo.release).toHaveBeenCalledWith({
        leaseId: 'lease-retry-1',
        jobId: 'job-retry-1',
        workerId: 'worker-retry',
      });
    });

    it('marks job FAILED permanently when retry policy attempts are exhausted', async () => {
      const mockLease = {
        id: 'lease-exhausted-1',
        jobId: 'job-exhausted-1',
        workerId: 'worker-retry',
        status: 'ACTIVE' as const,
        durationMs: 30000,
        acquiredAt: new Date(),
        renewedAt: new Date(),
        expiresAt: new Date(Date.now() + 30000),
        createdAt: new Date(),
      };

      const mockLeaseRepo = {
        claim: vi.fn(),
        renew: vi.fn(),
        release: vi.fn().mockResolvedValue({ status: 'RELEASED' }),
        findActiveByJobId: vi.fn().mockResolvedValue(mockLease),
        findById: vi.fn(),
        findByWorkerId: vi.fn(),
        reclaimExpiredLeases: vi.fn(),
      };

      const mockJobRepo = {
        save: vi.fn().mockResolvedValue(undefined),
        findById: vi.fn(),
        findByPipelineRunId: vi.fn(),
      };

      const mockExecutor = {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi.fn().mockResolvedValue({
          status: 'FAILED',
          exitCode: 1,
          failureReason: 'Process failed',
          startedAt: new Date(),
          finishedAt: new Date(),
          durationMs: 100,
          stdout: '',
          stderr: 'error',
          truncated: false,
        }),
      };

      const worker = startWorker({
        workerId: 'worker-retry',
        leaseRepository: mockLeaseRepo,
        jobRepository: mockJobRepo as unknown as JobRepository,
        executor: mockExecutor as unknown as Executor,
      });

      // Attempt 3 of 3
      const mockAttempt = {
        id: 'att-3',
        attemptNumber: 3,
        status: 'RUNNING' as 'RUNNING' | 'FAILED',
        start: vi.fn(),
        succeed: vi.fn(),
        fail: vi.fn().mockImplementation(() => {
          mockAttempt.status = 'FAILED';
        }),
        timeout: vi.fn(),
        cancel: vi.fn(),
      };

      const mockJob = {
        id: 'job-exhausted-1',
        command: 'exit 1',
        status: 'QUEUED',
        retryPolicy: {
          maxAttempts: 3,
          backoff: {
            baseDelayMs: 1000,
            factor: 2,
            maxDelayMs: 10000,
          },
          retryOn: ['FAILED' as const],
        },
        createAttempt: vi.fn().mockReturnValue(mockAttempt),
        start: vi.fn(),
        succeed: vi.fn(),
        fail: vi.fn(),
        timeout: vi.fn(),
        cancel: vi.fn(),
        transitionTo: vi.fn(),
        setNextAttemptAt: vi.fn(),
        clearNextAttemptAt: vi.fn(),
      };

      const execResult = await worker.executeJob({
        job: mockJob as unknown as Job,
        leaseId: 'lease-exhausted-1',
      });

      expect(execResult.result.status).toBe('FAILED');
      expect(mockAttempt.fail).toHaveBeenCalled();
      expect(execResult.retryDecision?.action).toBe('FINAL_FAILURE');
      expect(execResult.retryDecision?.reason).toBe('MAX_ATTEMPTS_EXHAUSTED');

      // Job transitions to terminal FAILED and clears nextAttemptAt
      expect(mockJob.fail).toHaveBeenCalled();
      expect(mockJob.clearNextAttemptAt).toHaveBeenCalled();
      expect(mockJob.transitionTo).not.toHaveBeenCalledWith('QUEUED');

      // Lease is released
      expect(mockLeaseRepo.release).toHaveBeenCalled();
    });
  });

  describe('Graceful Shutdown & Drain Lifecycle', () => {
    it('transitions READY -> DRAINING and rejects new claims and executions', async () => {
      const heartbeats: { id: WorkerId; status?: WorkerStatus }[] = [];
      const mockRegistry = {
        register: vi.fn().mockResolvedValue({}),
        heartbeat: vi.fn(async (id: WorkerId, status?: WorkerStatus) => {
          heartbeats.push({ id, status });
        }),
        deregister: vi.fn().mockResolvedValue({}),
        getWorker: vi.fn(),
        listWorkers: vi.fn(),
      } as unknown as WorkerRegistry;

      const mockLeaseRepo = {
        claim: vi.fn(),
        renew: vi.fn(),
        release: vi.fn(),
        findActiveByJobId: vi.fn(),
        findById: vi.fn(),
        findByWorkerId: vi.fn(),
        reclaimExpiredLeases: vi.fn(),
      };

      const worker = startWorker({
        workerId: 'drain-worker-1',
        registry: mockRegistry,
        leaseRepository: mockLeaseRepo,
      });

      expect(worker.getStatus()).toBe('READY');

      // Initiate drain
      await worker.drain();
      expect(worker.getStatus()).toBe('DRAINING');
      expect(mockRegistry.heartbeat).toHaveBeenCalledWith('drain-worker-1', 'DRAINING');

      // Reject new job claim during DRAINING
      const claimResult = await worker.claimJob('job-rejected-1');
      expect(claimResult.status).toBe('NOT_CLAIMABLE');
      if (claimResult.status === 'NOT_CLAIMABLE') {
        expect(claimResult.reason).toBe('JOB_NOT_CLAIMABLE');
        expect(claimResult.details).toContain('is in DRAINING state');
      }
      expect(mockLeaseRepo.claim).not.toHaveBeenCalled();

      // Reject new job execution during DRAINING
      const mockJob = { id: 'job-rejected-2' } as Job;
      await expect(worker.executeJob({ job: mockJob, leaseId: 'lease-fake' })).rejects.toThrow(
        /is in DRAINING state and not accepting new executions/,
      );

      // Now stop the worker
      await worker.stop();
      expect(worker.getStatus()).toBe('OFFLINE');
      expect(mockRegistry.deregister).toHaveBeenCalledWith('drain-worker-1');
    });

    it('waits for in-flight executions to finish during drain before releasing leases', async () => {
      let executionFinished = false;
      const mockExecutor: Executor = {
        name: 'mock',
        execute: vi.fn(async () => {
          // Simulate 50ms execution delay
          await new Promise((resolve) => setTimeout(resolve, 50));
          executionFinished = true;
          return {
            status: 'SUCCEEDED' as const,
            exitCode: 0,
            startedAt: new Date(),
            finishedAt: new Date(),
            durationMs: 50,
            stdout: 'done',
            stderr: '',
            truncated: false,
          };
        }),
        isAvailable: vi.fn().mockResolvedValue(true),
      };

      const mockLeaseRepo = {
        claim: vi.fn(),
        renew: vi.fn().mockResolvedValue({ status: 'RENEWED' }),
        release: vi.fn().mockResolvedValue({ status: 'RELEASED', leaseId: 'l1', jobId: 'j1' }),
        findActiveByJobId: vi.fn().mockResolvedValue({
          id: 'l1',
          jobId: 'j1',
          workerId: 'drain-worker-2',
          status: 'ACTIVE',
          expiresAt: new Date(Date.now() + 60000),
        }),
        findById: vi.fn(),
        findByWorkerId: vi.fn(),
        reclaimExpiredLeases: vi.fn(),
      };

      const worker = startWorker({
        workerId: 'drain-worker-2',
        executor: mockExecutor,
        leaseRepository: mockLeaseRepo,
        drainTimeoutMs: 1000,
      });

      const mockAttempt = {
        id: 'att-1',
        attemptNumber: 1,
        status: 'PENDING',
        start: vi.fn(),
        succeed: vi.fn(),
        fail: vi.fn(),
        isTerminal: () => false,
      };

      const mockJob = {
        id: 'j1',
        command: 'echo hello',
        status: 'QUEUED',
        createAttempt: vi.fn().mockReturnValue(mockAttempt),
        start: vi.fn(),
        succeed: vi.fn(),
        fail: vi.fn(),
      };

      // Start execution
      const execPromise = worker.executeJob({
        job: mockJob as unknown as Job,
        leaseId: 'l1',
      });

      // While execution is in-flight, trigger stop() which will drain
      expect(executionFinished).toBe(false);
      const stopPromise = worker.stop();

      await Promise.all([execPromise, stopPromise]);

      expect(executionFinished).toBe(true);
      expect(worker.getStatus()).toBe('OFFLINE');
      expect(mockLeaseRepo.release).toHaveBeenCalled();
    });

    it('repeated stop and drain calls are idempotent', async () => {
      const deregisterFn = vi.fn().mockResolvedValue({});
      const mockRegistry = {
        register: vi.fn().mockResolvedValue({}),
        heartbeat: vi.fn().mockResolvedValue({}),
        deregister: deregisterFn,
        getWorker: vi.fn(),
        listWorkers: vi.fn(),
      } as unknown as WorkerRegistry;

      const worker = startWorker({
        workerId: 'idempotent-worker',
        registry: mockRegistry,
      });

      // Call drain twice, then stop twice concurrently
      await Promise.all([worker.drain(), worker.drain(), worker.stop(), worker.stop()]);

      expect(worker.getStatus()).toBe('OFFLINE');
      // Deregister should only be called once
      expect(deregisterFn).toHaveBeenCalledTimes(1);

      // Subsequent stop call is also safe
      await worker.stop();
      expect(deregisterFn).toHaveBeenCalledTimes(1);
    });
  });
});

describe('Worker Service Shell — lifecycle events (PR 20)', () => {
  const leaseRecord = (id: string, jobId: string) => ({
    id,
    jobId,
    workerId: 'worker-ev',
    status: 'ACTIVE' as const,
    durationMs: 30000,
    acquiredAt: new Date(),
    renewedAt: new Date(),
    expiresAt: new Date(Date.now() + 30000),
    createdAt: new Date(),
  });

  const leaseRepoFor = (lease: ReturnType<typeof leaseRecord>) => ({
    claim: vi.fn(),
    renew: vi.fn(),
    release: vi
      .fn()
      .mockResolvedValue({ status: 'RELEASED', leaseId: lease.id, jobId: lease.jobId }),
    findActiveByJobId: vi.fn().mockResolvedValue(lease),
    findById: vi.fn(),
    findByWorkerId: vi.fn(),
    reclaimExpiredLeases: vi.fn(),
  });

  const jobRepoRecording = () => {
    const saved: unknown[] = [];
    return {
      repo: {
        save: vi.fn(async (j: unknown) => {
          saved.push(j);
        }),
        findById: vi.fn(),
        findByPipelineRunId: vi.fn(),
      },
      saved,
    };
  };

  const mockAttempt = (id: string, attemptNumber = 1) => ({
    id,
    attemptNumber,
    status: 'RUNNING' as string,
    start: vi.fn(),
    succeed: vi.fn(),
    fail: vi.fn(),
    timeout: vi.fn(),
    cancel: vi.fn(),
  });

  const mockJob = (
    id: string,
    attempt: ReturnType<typeof mockAttempt>,
    extra: Record<string, unknown> = {},
  ) => ({
    id,
    pipelineRunId: 'run-ev',
    command: 'echo ev',
    priority: 0,
    status: 'QUEUED',
    createAttempt: vi.fn().mockReturnValue(attempt),
    start: vi.fn(),
    succeed: vi.fn(),
    fail: vi.fn(),
    timeout: vi.fn(),
    cancel: vi.fn(),
    transitionTo: vi.fn(),
    setNextAttemptAt: vi.fn(),
    clearNextAttemptAt: vi.fn(),
    ...extra,
  });

  const execResult = (over: Partial<Record<string, unknown>> = {}) => ({
    status: 'SUCCEEDED',
    exitCode: 0,
    startedAt: new Date(),
    finishedAt: new Date(),
    durationMs: 120,
    stdout: '',
    stderr: '',
    truncated: false,
    ...over,
  });

  const collectorBus = () => {
    const bus = new InProcessEventBus();
    const events: ForgeEvent[] = [];
    bus.subscribe((e) => void events.push(e));
    return { bus, events };
  };

  // PR 21: JobStarted / terminal / JobQueued are co-committed to the transactional outbox
  // (they require a `pool`); only JobLogChunk stays on the best-effort bus. The final fix
  // wave then widened the worker `pool`-required guard: a configured `eventPublisher`
  // without a transactional `pool` is now rejected outright (previously allowed as long as
  // no `jobRepository` was also configured, which silently dropped every durable lifecycle
  // event). These pool-less scenarios therefore assert the guard rejection; the bus-vs-outbox
  // routing and the JobLogChunk payload shape are covered against real PostgreSQL + Docker
  // in `worker-execution.integration.test.ts`.
  it('rejects a successful-execution run when a publisher is configured without a transactional pool', async () => {
    const { bus } = collectorBus();
    const lease = leaseRecord('lease-ev-1', 'job-ev-1');
    const attempt = mockAttempt('job-ev-1-attempt-1');

    const worker = startWorker({
      workerId: 'worker-ev',
      leaseRepository: leaseRepoFor(lease),
      executor: {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi
          .fn()
          .mockResolvedValue(execResult({ stdout: 'build ok\n', stderr: '', durationMs: 250 })),
      } as unknown as Executor,
      eventPublisher: bus,
    });

    await expect(
      worker.executeJob({
        job: mockJob('job-ev-1', attempt) as unknown as Job,
        leaseId: 'lease-ev-1',
      }),
    ).rejects.toThrow(/transactional pool/);
    await bus.close();
  });

  it('rejects a non-retryable-failure run when a publisher is configured without a transactional pool', async () => {
    const { bus } = collectorBus();
    const lease = leaseRecord('lease-ev-2', 'job-ev-2');
    const attempt = mockAttempt('job-ev-2-attempt-1');

    const worker = startWorker({
      workerId: 'worker-ev',
      leaseRepository: leaseRepoFor(lease),
      executor: {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi.fn().mockResolvedValue(
          execResult({
            status: 'FAILED',
            exitCode: 7,
            failureReason: 'Process exited with code 7',
            stderr: 'boom',
          }),
        ),
      } as unknown as Executor,
      eventPublisher: bus,
    });

    await expect(
      worker.executeJob({
        job: mockJob('job-ev-2', attempt) as unknown as Job,
        leaseId: 'lease-ev-2',
      }),
    ).rejects.toThrow(/transactional pool/);
    await bus.close();
  });

  it('rejects a cancelled-execution run when a publisher is configured without a transactional pool', async () => {
    const { bus } = collectorBus();
    const lease = leaseRecord('lease-ev-3', 'job-ev-3');
    const attempt = mockAttempt('job-ev-3-attempt-1');
    const job = mockJob('job-ev-3', attempt);

    const worker = startWorker({
      workerId: 'worker-ev',
      leaseRepository: leaseRepoFor(lease),
      executor: {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi
          .fn()
          .mockResolvedValue(
            execResult({ status: 'CANCELLED', exitCode: null, failureReason: 'cancelled' }),
          ),
      } as unknown as Executor,
      eventPublisher: bus,
    });

    await expect(
      worker.executeJob({
        job: job as unknown as Job,
        leaseId: 'lease-ev-3',
      }),
    ).rejects.toThrow(/transactional pool/);
    await bus.close();
  });

  it('rejects a wall-clock-timeout run when a publisher is configured without a transactional pool', async () => {
    const { bus } = collectorBus();
    const lease = leaseRecord('lease-ev-4', 'job-ev-4');
    const attempt = mockAttempt('job-ev-4-attempt-1');
    const job = mockJob('job-ev-4', attempt);

    const worker = startWorker({
      workerId: 'worker-ev',
      leaseRepository: leaseRepoFor(lease),
      executor: {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi.fn().mockResolvedValue(
          execResult({
            status: 'TIMED_OUT',
            exitCode: null,
            failureReason: 'Execution timed out after 1000ms',
          }),
        ),
      } as unknown as Executor,
      eventPublisher: bus,
    });

    await expect(
      worker.executeJob({
        job: job as unknown as Job,
        leaseId: 'lease-ev-4',
      }),
    ).rejects.toThrow(/transactional pool/);
    await bus.close();
  });

  it('rejects a retry-scheduled run when a publisher is configured without a transactional pool', async () => {
    const { bus } = collectorBus();
    const lease = leaseRecord('lease-ev-5', 'job-ev-5');
    const attempt = {
      ...mockAttempt('job-ev-5-attempt-1'),
      fail: vi.fn(),
    };
    // real evaluateRetry needs attempt.status to be FAILED after fail()
    attempt.fail = vi.fn().mockImplementation(() => {
      attempt.status = 'FAILED';
    });

    const worker = startWorker({
      workerId: 'worker-ev',
      leaseRepository: leaseRepoFor(lease),
      executor: {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi.fn().mockResolvedValue(
          execResult({
            status: 'FAILED',
            exitCode: 1,
            failureReason: 'Process exited with code 1',
          }),
        ),
      } as unknown as Executor,
      eventPublisher: bus,
    });

    const job = mockJob('job-ev-5', attempt, {
      retryPolicy: {
        maxAttempts: 3,
        backoff: { baseDelayMs: 1000, factor: 2, maxDelayMs: 10000 },
        retryOn: ['FAILED' as const],
      },
      nextAttemptAt: new Date('2026-09-08T12:05:00.000Z'),
    });

    await expect(
      worker.executeJob({
        job: job as unknown as Job,
        leaseId: 'lease-ev-5',
      }),
    ).rejects.toThrow(/transactional pool/);
    await bus.close();
  });

  it('emits WorkerRegistered and WorkerHeartbeat with correct worker identity', async () => {
    const { bus, events } = collectorBus();
    const registry: WorkerRegistry = {
      register: vi.fn(async (input: RegisterWorkerInput): Promise<WorkerMetadata> => ({
        workerId: createWorkerId(input.workerId ?? 'w'),
        status: 'READY',
        hostname: input.hostname,
        capabilities: input.capabilities,
        resources: input.resources,
        registeredAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      })),
      heartbeat: vi.fn(async (): Promise<void> => undefined),
      deregister: vi.fn(async (): Promise<void> => undefined),
      getWorker: vi.fn(),
      listWorkers: vi.fn(),
    };

    const worker = startWorker({
      workerId: 'worker-ev-reg',
      registry,
      heartbeatIntervalMs: 15,
      capabilities: { executors: ['docker', 'shell'] },
      resources: { cpuCores: 8, memoryBytes: 42 },
      eventPublisher: bus,
    });

    await new Promise((resolve) => setTimeout(resolve, 60));
    await worker.stop();

    const registered = events.find((e) => e.event_type === 'WorkerRegistered');
    expect(registered).toMatchObject({
      event_type: 'WorkerRegistered',
      worker_id: 'worker-ev-reg',
      payload: {
        worker_id: 'worker-ev-reg',
        capabilities: ['docker', 'shell'],
        cpu_cores: 8,
        memory_bytes: 42,
      },
    });
    const heartbeats = events.filter((e) => e.event_type === 'WorkerHeartbeat');
    expect(heartbeats.length).toBeGreaterThanOrEqual(1);
    expect(heartbeats[0]).toMatchObject({
      worker_id: 'worker-ev-reg',
      payload: { worker_id: 'worker-ev-reg', status: 'READY' },
    });
    await bus.close();
  });

  it('rejects a publisher configured without a transactional pool before any execution begins', async () => {
    const lease = leaseRecord('lease-ev-6', 'job-ev-6');
    const attempt = mockAttempt('job-ev-6-attempt-1');
    const publish = vi.fn().mockRejectedValue(new Error('bus offline'));

    const worker = startWorker({
      workerId: 'worker-ev',
      leaseRepository: leaseRepoFor(lease),
      executor: {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi.fn().mockResolvedValue(execResult({ stdout: 'ok' })),
      } as unknown as Executor,
      eventPublisher: { publish },
    });

    await expect(
      worker.executeJob({
        job: mockJob('job-ev-6', attempt) as unknown as Job,
        leaseId: 'lease-ev-6',
      }),
    ).rejects.toThrow(/transactional pool/);
    // The guard fires before any executor or publisher call.
    expect(publish).not.toHaveBeenCalled();
  });

  it('emits nothing and behaves normally when no eventPublisher is configured', async () => {
    const lease = leaseRecord('lease-ev-7', 'job-ev-7');
    const attempt = mockAttempt('job-ev-7-attempt-1');
    const worker = startWorker({
      workerId: 'worker-ev',
      leaseRepository: leaseRepoFor(lease),
      jobRepository: jobRepoRecording().repo as unknown as JobRepository,
      executor: {
        name: 'mock',
        isAvailable: vi.fn().mockResolvedValue(true),
        execute: vi.fn().mockResolvedValue(execResult({ stdout: 'ok' })),
      } as unknown as Executor,
    });

    const { result } = await worker.executeJob({
      job: mockJob('job-ev-7', attempt) as unknown as Job,
      leaseId: 'lease-ev-7',
    });
    expect(result.status).toBe('SUCCEEDED');
  });
});

describe('Worker durable events guard (PR 21)', () => {
  const fakeJobRepo = () => ({
    save: vi.fn(async () => {}),
    findById: vi.fn(),
    findByPipelineRunId: vi.fn(),
  });

  const okExecutor = () =>
    ({
      name: 'mock',
      isAvailable: vi.fn().mockResolvedValue(true),
      execute: vi.fn().mockResolvedValue({
        status: 'SUCCEEDED',
        exitCode: 0,
        startedAt: new Date(),
        finishedAt: new Date(),
        durationMs: 5,
        stdout: '',
        stderr: '',
        truncated: false,
      }),
    }) as unknown as Executor;

  const guardJob = () => {
    const attempt = {
      id: 'a1',
      attemptNumber: 1,
      status: 'RUNNING',
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
      timeout: vi.fn(),
      cancel: vi.fn(),
    };
    return {
      id: 'job-guard',
      pipelineRunId: 'run-guard',
      command: 'echo hi',
      priority: 0,
      status: 'QUEUED',
      createAttempt: vi.fn().mockReturnValue(attempt),
      start: vi.fn(),
      succeed: vi.fn(),
      fail: vi.fn(),
      timeout: vi.fn(),
      cancel: vi.fn(),
      transitionTo: vi.fn(),
      setNextAttemptAt: vi.fn(),
      clearNextAttemptAt: vi.fn(),
    };
  };

  it('executeJob throws when a publisher is configured without a transactional pool', async () => {
    const shell = startWorker({
      workerId: 'worker-guard',
      jobRepository: fakeJobRepo() as unknown as JobRepository,
      eventPublisher: { publish: async () => {} },
      executor: okExecutor(),
    });

    await expect(
      shell.executeJob({ job: guardJob() as unknown as Job, leaseId: 'l1' }),
    ).rejects.toThrow(/transactional pool/);
  });

  it('executeJob does not fire the guard for jobRepository-only persistence with no publisher', async () => {
    const shell = startWorker({
      workerId: 'worker-guard-2',
      jobRepository: fakeJobRepo() as unknown as JobRepository,
      executor: okExecutor(),
    });

    const { result } = await shell.executeJob({
      job: guardJob() as unknown as Job,
      leaseId: 'l1',
    });
    expect(result.status).toBe('SUCCEEDED');
  });

  it('executeJob throws for a publisher with no transactional pool even when no other persistence is configured', async () => {
    const shell = startWorker({
      workerId: 'worker-guard-3',
      eventPublisher: { publish: async () => {} },
      executor: okExecutor(),
    });

    await expect(
      shell.executeJob({ job: guardJob() as unknown as Job, leaseId: 'l1' }),
    ).rejects.toThrow(/transactional pool/);
  });
});
