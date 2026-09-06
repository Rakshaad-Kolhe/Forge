import { loadConfig } from '@forge/config';
import { createLogger, type Logger } from '@forge/logging';

export interface WorkerShell {
  stop: () => void;
}

/**
 * Starts the minimal Forge Worker service shell.
 * PR 01 provides the process and logging skeleton only.
 */
export function startWorker(options?: { logger?: Logger }): WorkerShell {
  const config = loadConfig();
  const logger =
    options?.logger ??
    createLogger({
      service: 'worker',
      environment: config.nodeEnv,
      minLevel: config.logLevel,
    });

  logger.info('Forge Worker service shell started', {
    status: 'running',
    service: 'worker',
    environment: config.nodeEnv,
  });

  return {
    stop: () => {
      logger.info('Forge Worker service shell stopped');
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

  const shutdown = (_signal: string) => {
    shell.stop();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}
