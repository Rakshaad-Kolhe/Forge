import { describe, it, expect, vi } from 'vitest';
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
          executors: ['shell'],
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
});
