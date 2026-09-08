import { loadConfig } from '@forge/config';
import { createLogger, type Logger } from '@forge/logging';

export * from './types.js';
export * from './errors.js';
export * from './policy.js';
export * from './job-policy.js';
export * from './fairness-policy.js';
export * from './scheduler.js';

export interface SchedulerShell {
  stop: () => void;
}

/**
 * Starts the minimal Forge Scheduler service shell.
 * PR 01 provides the process and logging skeleton only.
 */
export function startScheduler(options?: { logger?: Logger }): SchedulerShell {
  const config = loadConfig();
  const logger =
    options?.logger ??
    createLogger({
      service: 'scheduler',
      environment: config.nodeEnv,
      minLevel: config.logLevel,
    });

  logger.info('Forge Scheduler service shell started', {
    status: 'running',
    service: 'scheduler',
    environment: config.nodeEnv,
  });

  return {
    stop: () => {
      logger.info('Forge Scheduler service shell stopped');
    },
  };
}

const normalizedArgv1 = process.argv[1]?.replace(/\\/g, '/') ?? '';
const isDirectRun =
  Boolean(normalizedArgv1) &&
  (normalizedArgv1.endsWith('scheduler/dist/index.js') ||
    normalizedArgv1.endsWith('scheduler/src/index.ts'));

if (isDirectRun) {
  const shell = startScheduler();

  const shutdown = (_signal: string) => {
    shell.stop();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}
