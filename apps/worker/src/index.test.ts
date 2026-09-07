import { describe, it, expect, vi } from 'vitest';
import type { Executor } from '@forge/contracts';
import type { JobRepository } from '@forge/database';
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
  it('starts successfully and logs startup event in standalone mode', () => {
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

    worker.stop();
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
});
