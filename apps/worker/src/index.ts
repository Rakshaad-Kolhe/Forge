import os from 'node:os';
import crypto from 'node:crypto';
import { loadConfig } from '@forge/config';
import type {
  ClaimJobResult,
  ReleaseLeaseResult,
  RenewLeaseResult,
  WorkerLease,
} from '@forge/contracts';
import type { WorkerLeaseRepository } from '@forge/database';
import { createLogger, type Logger } from '@forge/logging';
import {
  type WorkerRegistry,
  type WorkerId,
  type WorkerStatus,
  type WorkerResources,
  type WorkerCapabilities,
  createWorkerId,
} from '@forge/worker-registry';

export interface StartWorkerOptions {
  logger?: Logger;
  workerId?: string;
  registry?: WorkerRegistry;
  leaseRepository?: WorkerLeaseRepository;
  defaultLeaseDurationMs?: number;
  hostname?: string;
  capabilities?: WorkerCapabilities;
  resources?: WorkerResources;
  heartbeatIntervalMs?: number;
}

export interface WorkerShell {
  workerId: WorkerId;
  getStatus: () => WorkerStatus;
  claimJob: (jobId: string, durationMs?: number) => Promise<ClaimJobResult>;
  renewLease: (leaseId: string, jobId: string, durationMs?: number) => Promise<RenewLeaseResult>;
  releaseLease: (leaseId: string, jobId: string) => Promise<ReleaseLeaseResult>;
  getActiveLeases: () => readonly WorkerLease[];
  stop: () => Promise<void>;
}

/**
 * Starts the Forge Worker service shell.
 * Coordinates worker identity, registration, and periodic heartbeat with the WorkerRegistry.
 */
export function startWorker(options?: StartWorkerOptions): WorkerShell {
  const config = loadConfig();
  const logger =
    options?.logger ??
    createLogger({
      service: 'worker',
      environment: config.nodeEnv,
      minLevel: config.logLevel,
    });

  const workerId = createWorkerId(options?.workerId ?? crypto.randomUUID());
  let currentStatus: WorkerStatus = 'READY';
  let heartbeatTimer: NodeJS.Timeout | undefined;
  const activeLeases = new Map<string, WorkerLease>();

  logger.info('Forge Worker service shell started', {
    status: 'running',
    service: 'worker',
    workerId,
    environment: config.nodeEnv,
  });

  if (options?.registry) {
    const capabilities: WorkerCapabilities = options.capabilities ?? {
      executors: ['shell'],
    };
    const resources: WorkerResources = options.resources ?? {
      cpuCores: os.cpus().length,
      memoryBytes: os.totalmem(),
    };
    const hostname = options.hostname ?? os.hostname();

    // Register worker durably and send initial heartbeat
    options.registry
      .register({
        workerId,
        status: 'READY',
        hostname,
        capabilities,
        resources,
      })
      .then(() => {
        logger.info('Worker registered with registry', {
          workerId,
          hostname,
          capabilities,
          resources,
        });
      })
      .catch((err: unknown) => {
        logger.error('Failed to register worker with registry', {
          workerId,
          error: err instanceof Error ? err.message : String(err),
        });
      });

    const intervalMs = options.heartbeatIntervalMs ?? config.workerHeartbeatIntervalMs;
    heartbeatTimer = setInterval(async () => {
      try {
        await options.registry!.heartbeat(workerId, currentStatus);
      } catch (err: unknown) {
        logger.error('Worker heartbeat renewal failed', {
          workerId,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }, intervalMs);

    heartbeatTimer.unref();
  }

  const claimJob = async (jobId: string, durationMs?: number): Promise<ClaimJobResult> => {
    if (!options?.leaseRepository) {
      throw new Error('Worker cannot claim job without configured leaseRepository');
    }
    const leaseDuration =
      durationMs ?? options.defaultLeaseDurationMs ?? config.workerJobLeaseDurationMs;
    const result = await options.leaseRepository.claim({
      jobId,
      workerId,
      durationMs: leaseDuration,
    });
    if (result.status === 'ACQUIRED') {
      activeLeases.set(result.lease.id, result.lease);
      logger.info('Worker claimed job lease', {
        workerId,
        jobId,
        leaseId: result.lease.id,
        expiresAt: result.lease.expiresAt,
      });
    }
    return result;
  };

  const renewLease = async (
    leaseId: string,
    jobId: string,
    durationMs?: number,
  ): Promise<RenewLeaseResult> => {
    if (!options?.leaseRepository) {
      throw new Error('Worker cannot renew lease without configured leaseRepository');
    }
    const result = await options.leaseRepository.renew({
      leaseId,
      jobId,
      workerId,
      durationMs,
    });
    if (result.status === 'RENEWED') {
      activeLeases.set(result.lease.id, result.lease);
      logger.info('Worker renewed job lease', {
        workerId,
        jobId,
        leaseId,
        expiresAt: result.lease.expiresAt,
      });
    } else {
      activeLeases.delete(leaseId);
      logger.warn('Worker failed to renew job lease', {
        workerId,
        jobId,
        leaseId,
        reason: result.reason,
      });
    }
    return result;
  };

  const releaseLease = async (leaseId: string, jobId: string): Promise<ReleaseLeaseResult> => {
    if (!options?.leaseRepository) {
      throw new Error('Worker cannot release lease without configured leaseRepository');
    }
    const result = await options.leaseRepository.release({
      leaseId,
      jobId,
      workerId,
    });
    activeLeases.delete(leaseId);
    if (result.status === 'RELEASED') {
      logger.info('Worker released job lease', {
        workerId,
        jobId,
        leaseId,
      });
    }
    return result;
  };

  const getActiveLeases = (): readonly WorkerLease[] => {
    return Object.freeze(Array.from(activeLeases.values()));
  };

  return {
    workerId,
    getStatus: () => currentStatus,
    claimJob,
    renewLease,
    releaseLease,
    getActiveLeases,
    stop: async () => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = undefined;
      }
      logger.info('Forge Worker service shell stopped', { workerId });

      // Gracefully release all actively held leases
      if (options?.leaseRepository && activeLeases.size > 0) {
        for (const [leaseId, lease] of Array.from(activeLeases.entries())) {
          try {
            await options.leaseRepository.release({
              leaseId,
              jobId: lease.jobId,
              workerId,
            });
            logger.info('Worker released lease during graceful stop', {
              workerId,
              leaseId,
              jobId: lease.jobId,
            });
          } catch (err: unknown) {
            logger.error('Failed to release lease during graceful stop', {
              workerId,
              leaseId,
              error: err instanceof Error ? err.message : String(err),
            });
          }
        }
        activeLeases.clear();
      }

      if (options?.registry) {
        try {
          currentStatus = 'OFFLINE';
          await options.registry.deregister(workerId);
        } catch (err: unknown) {
          logger.error('Worker deregistration failed', {
            workerId,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }
    },
  };
}

const normalizedArgv1 = process.argv[1]?.replace(/\\/g, '/') ?? '';
const isDirectRun =
  Boolean(normalizedArgv1) &&
  (normalizedArgv1.endsWith('worker/dist/index.js') ||
    normalizedArgv1.endsWith('worker/src/index.ts'));

if (isDirectRun) {
  const shell = startWorker();

  const shutdown = async (_signal: string) => {
    await shell.stop();
    process.exit(0);
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}
