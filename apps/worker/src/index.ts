import os from 'node:os';
import crypto from 'node:crypto';
import { loadConfig } from '@forge/config';
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
  hostname?: string;
  capabilities?: WorkerCapabilities;
  resources?: WorkerResources;
  heartbeatIntervalMs?: number;
}

export interface WorkerShell {
  workerId: WorkerId;
  getStatus: () => WorkerStatus;
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

  return {
    workerId,
    getStatus: () => currentStatus,
    stop: async () => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = undefined;
      }
      logger.info('Forge Worker service shell stopped', { workerId });

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
