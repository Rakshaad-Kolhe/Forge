import { loadConfig } from '@forge/config';
import { PgOutboxRepository, type DatabasePool } from '@forge/database';
import type { EventPublisher } from '@forge/events';
import { createLogger, type Logger } from '@forge/logging';
import { OutboxDispatcher, type OutboxDispatcherConfig } from '@forge/outbox';

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
 *
 * PR 01 provides the process and logging skeleton. PR 21 adds an optional
 * {@link OutboxDispatcher}: when BOTH a database `pool` and an event `publisher` are
 * supplied, the shell owns a dispatcher that drains the transactional outbox and stops it
 * on `stop()`. With neither supplied, behaviour is unchanged — a log-only shell.
 */
export function startScheduler(options?: {
  logger?: Logger;
  pool?: DatabasePool;
  publisher?: EventPublisher;
}): SchedulerShell {
  const config = loadConfig();
  const logger =
    options?.logger ??
    createLogger({
      service: 'scheduler',
      environment: config.nodeEnv,
      minLevel: config.logLevel,
    });

  let dispatcher: OutboxDispatcher | undefined;
  if (options?.pool && options?.publisher) {
    const dispatcherConfig: OutboxDispatcherConfig = {
      pollIntervalMs: config.outboxDispatchPollIntervalMs,
      batchSize: config.outboxDispatchBatchSize,
      claimTimeoutMs: config.outboxClaimTimeoutMs,
      publishTimeoutMs: config.outboxPublishTimeoutMs,
      maxDeliveryAttempts: config.outboxMaxDeliveryAttempts,
      baseBackoffMs: config.outboxDeliveryBaseBackoffMs,
      maxBackoffMs: config.outboxDeliveryMaxBackoffMs,
      retentionMaxAgeMs: config.outboxRetentionMaxAgeMs,
      retentionBatchSize: config.outboxRetentionBatchSize,
      retentionEveryNTicks: config.outboxRetentionEveryNTicks,
    };
    dispatcher = new OutboxDispatcher({
      repository: new PgOutboxRepository(options.pool, {
        maxPayloadBytes: config.outboxMaxPayloadBytes,
      }),
      publisher: options.publisher,
      logger,
      config: dispatcherConfig,
    });
    dispatcher.start();
    logger.info('Outbox dispatcher started', { dispatcherId: dispatcher.dispatcherId });
  }

  logger.info('Forge Scheduler service shell started', {
    status: 'running',
    service: 'scheduler',
    environment: config.nodeEnv,
  });

  return {
    stop: () => {
      void dispatcher?.stop();
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
