import { randomUUID } from 'node:crypto';
import type { WorkerRecord, WorkerRepository } from '@forge/database';
import type { Logger } from '@forge/logging';
import { WorkerRegistrationError, WorkerValidationError } from './errors.js';
import {
  createWorkerId,
  type RegisterWorkerInput,
  type WorkerHeartbeatStore,
  type WorkerId,
  type WorkerInfo,
  type WorkerLiveness,
  type WorkerMetadata,
  type WorkerRegistry,
  type WorkerRegistryOptions,
  type WorkerStatus,
} from './types.js';

const DEFAULT_HEARTBEAT_TTL_SECONDS = 15;

const VALID_STATUSES = new Set<WorkerStatus>(['STARTING', 'READY', 'DRAINING', 'OFFLINE']);

class DefaultWorkerRegistry implements WorkerRegistry {
  private readonly heartbeatTtlSeconds: number;

  constructor(
    private readonly workerRepository: WorkerRepository,
    private readonly heartbeatStore: WorkerHeartbeatStore,
    options?: WorkerRegistryOptions,
    private readonly logger?: Logger,
  ) {
    this.heartbeatTtlSeconds = options?.heartbeatTtlSeconds ?? DEFAULT_HEARTBEAT_TTL_SECONDS;
  }

  public async register(input: RegisterWorkerInput): Promise<WorkerMetadata> {
    this.validateRegistrationInput(input);

    const workerId = createWorkerId(
      input.workerId && input.workerId.trim() ? input.workerId.trim() : `worker_${randomUUID()}`,
    );

    const status: WorkerStatus = input.status ?? 'READY';
    const now = new Date();

    const workerRecord: WorkerRecord = {
      id: workerId,
      status,
      hostname: input.hostname?.trim() || null,
      executors: [...input.capabilities.executors],
      resources: {
        cpuCores: input.resources.cpuCores,
        memoryBytes: input.resources.memoryBytes,
        gpuCount: input.resources.gpuCount ?? 0,
      },
      registeredAt: now,
      updatedAt: now,
    };

    try {
      // 1. Authoritative durable write in PostgreSQL
      await this.workerRepository.save(workerRecord);

      // 2. Initial transient heartbeat in Redis
      await this.heartbeatStore.recordHeartbeat(workerId, status, this.heartbeatTtlSeconds);

      this.logger?.info('Worker successfully registered', {
        workerId,
        status,
        hostname: workerRecord.hostname,
        executors: workerRecord.executors,
      });

      return {
        workerId,
        status,
        hostname: workerRecord.hostname,
        capabilities: { executors: workerRecord.executors },
        resources: workerRecord.resources,
        registeredAt: workerRecord.registeredAt.toISOString(),
        updatedAt: workerRecord.updatedAt.toISOString(),
      };
    } catch (err) {
      this.logger?.error('Worker registration failed', {
        workerId,
        error: (err as Error).message,
      });
      throw new WorkerRegistrationError(
        `Failed to register worker "${workerId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async heartbeat(workerId: WorkerId, status?: WorkerStatus): Promise<void> {
    const targetStatus = status ?? 'READY';
    await this.heartbeatStore.recordHeartbeat(workerId, targetStatus, this.heartbeatTtlSeconds);
  }

  public async deregister(workerId: WorkerId): Promise<void> {
    try {
      // 1. Remove transient heartbeat key from Redis
      await this.heartbeatStore.removeHeartbeat(workerId);

      // 2. Mark worker status as OFFLINE in PostgreSQL (preserving historical identity)
      await this.workerRepository.updateStatus(workerId, 'OFFLINE');

      this.logger?.info('Worker gracefully deregistered', { workerId });
    } catch (err) {
      this.logger?.error('Failed to deregister worker', {
        workerId,
        error: (err as Error).message,
      });
      throw err;
    }
  }

  public async getWorker(workerId: WorkerId): Promise<WorkerInfo | null> {
    const record = await this.workerRepository.findById(workerId);
    if (!record) {
      return null;
    }

    const heartbeat = await this.heartbeatStore.getHeartbeat(workerId);
    const liveness: WorkerLiveness = heartbeat !== null ? 'ALIVE' : 'STALE';

    return {
      worker: {
        workerId: createWorkerId(record.id),
        status: record.status,
        hostname: record.hostname,
        capabilities: { executors: record.executors },
        resources: record.resources,
        registeredAt: record.registeredAt.toISOString(),
        updatedAt: record.updatedAt.toISOString(),
      },
      liveness,
      lastHeartbeatAt: heartbeat?.timestamp ?? null,
    };
  }

  public async listWorkers(filter?: {
    status?: WorkerStatus;
    liveness?: WorkerLiveness;
  }): Promise<WorkerInfo[]> {
    const records = await this.workerRepository.list(
      filter?.status ? { status: filter.status } : undefined,
    );

    const workerInfos: WorkerInfo[] = await Promise.all(
      records.map(async (record) => {
        const id = createWorkerId(record.id);
        const heartbeat = await this.heartbeatStore.getHeartbeat(id);
        const liveness: WorkerLiveness = heartbeat !== null ? 'ALIVE' : 'STALE';

        return {
          worker: {
            workerId: id,
            status: record.status,
            hostname: record.hostname,
            capabilities: { executors: record.executors },
            resources: record.resources,
            registeredAt: record.registeredAt.toISOString(),
            updatedAt: record.updatedAt.toISOString(),
          },
          liveness,
          lastHeartbeatAt: heartbeat?.timestamp ?? null,
        };
      }),
    );

    if (filter?.liveness) {
      return workerInfos.filter((info) => info.liveness === filter.liveness);
    }

    return workerInfos;
  }

  private validateRegistrationInput(input: RegisterWorkerInput): void {
    if (!input || typeof input !== 'object') {
      throw new WorkerValidationError('RegisterWorkerInput must be a valid object');
    }

    if (input.workerId !== undefined) {
      if (typeof input.workerId !== 'string' || !input.workerId.trim()) {
        throw new WorkerValidationError('workerId must be a non-empty string');
      }
    }

    if (input.status !== undefined && !VALID_STATUSES.has(input.status)) {
      throw new WorkerValidationError(
        `Invalid worker status "${input.status}". Allowed statuses: ${Array.from(VALID_STATUSES).join(', ')}`,
      );
    }

    if (
      !input.capabilities ||
      !Array.isArray(input.capabilities.executors) ||
      input.capabilities.executors.length === 0
    ) {
      throw new WorkerValidationError(
        'Worker capabilities must specify at least one valid executor name',
      );
    }

    for (const executor of input.capabilities.executors) {
      if (!executor || typeof executor !== 'string' || !executor.trim()) {
        throw new WorkerValidationError('Executor name in capabilities cannot be empty');
      }
    }

    if (!input.resources || typeof input.resources !== 'object') {
      throw new WorkerValidationError('Worker resources must be specified');
    }

    if (
      typeof input.resources.cpuCores !== 'number' ||
      !Number.isInteger(input.resources.cpuCores) ||
      input.resources.cpuCores < 1
    ) {
      throw new WorkerValidationError('cpuCores must be a positive integer >= 1');
    }

    if (
      typeof input.resources.memoryBytes !== 'number' ||
      !Number.isFinite(input.resources.memoryBytes) ||
      input.resources.memoryBytes < 1
    ) {
      throw new WorkerValidationError('memoryBytes must be a positive number >= 1');
    }

    if (input.resources.gpuCount !== undefined) {
      if (
        typeof input.resources.gpuCount !== 'number' ||
        !Number.isInteger(input.resources.gpuCount) ||
        input.resources.gpuCount < 0
      ) {
        throw new WorkerValidationError('gpuCount must be a non-negative integer >= 0');
      }
    }
  }
}

/**
 * Creates a WorkerRegistry coordinating PostgreSQL and Redis.
 */
export function createWorkerRegistry(
  workerRepository: WorkerRepository,
  heartbeatStore: WorkerHeartbeatStore,
  options?: WorkerRegistryOptions,
  logger?: Logger,
): WorkerRegistry {
  return new DefaultWorkerRegistry(workerRepository, heartbeatStore, options, logger);
}
