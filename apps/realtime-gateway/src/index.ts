/**
 * `@forge/realtime-gateway` — dedicated WebSocket gateway process.
 *
 *   Redis Pub/Sub → RedisEventSubscriber → RealtimeGateway → browser / CLI clients
 *
 * Transient by design. PostgreSQL + the PR 21 outbox stay authoritative; a Redis outage
 * only delays realtime delivery, and a disconnected client recovers via the API.
 */
import { loadConfig } from '@forge/config';
import { createLogger } from '@forge/logging';
import { RedisEventSubscriber } from '@forge/realtime';
import { createRedisPubSub } from '@forge/redis';
import { AllowAuthenticatedAuthorizer } from './authorization.js';
import { gatewayConfigFromAppConfig } from './config.js';
import { createGateway } from './gateway.js';

export * from './protocol.js';
export * from './subscription.js';
export * from './origin.js';
export * from './auth.js';
export * from './authorization.js';
export * from './config.js';
export * from './metrics.js';
export * from './connection.js';
export * from './registry.js';
export * from './gateway.js';

async function main(): Promise<void> {
  const appConfig = loadConfig();
  const gatewayConfig = gatewayConfigFromAppConfig(appConfig);
  const logger = createLogger({
    service: 'realtime-gateway',
    environment: appConfig.nodeEnv,
    minLevel: appConfig.logLevel,
  });

  const pubsub = createRedisPubSub({ url: appConfig.redisUrl }, logger);
  const subscriber = new RedisEventSubscriber({
    pubsub,
    channel: gatewayConfig.channel,
    logger,
  });

  const gateway = createGateway({
    config: gatewayConfig,
    subscriber,
    authorizer: new AllowAuthenticatedAuthorizer(logger),
    logger,
  });

  pubsub.onConnectionChange((state) => {
    if (state === 'disconnected') {
      gateway.getMetrics().incr('redisDisconnects');
      logger.warn('realtime.redis_disconnected', {});
    } else {
      logger.info('realtime.redis_connected', {});
    }
  });

  try {
    await pubsub.connect();
  } catch (err) {
    // Degraded start: keep serving WebSocket handshakes; ioredis keeps retrying and
    // re-subscribes on recovery. No events flow until Redis is back — never a false
    // "job failed" and never a loss of durable state.
    logger.error('realtime.redis_connect_failed_degraded_start', {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  await gateway.start();

  const shutdown = async (signal: string): Promise<void> => {
    logger.info('realtime-gateway.shutdown_signal', { signal });
    await gateway.stop();
    await pubsub.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

const normalizedArgv1 = process.argv[1]?.replace(/\\/g, '/') ?? '';
const isDirectRun =
  Boolean(normalizedArgv1) &&
  (normalizedArgv1.endsWith('realtime-gateway/dist/index.js') ||
    normalizedArgv1.endsWith('realtime-gateway/src/index.ts'));

if (isDirectRun) {
  main().catch((err: unknown) => {
    console.error('[forge-realtime-gateway] fatal', err);
    process.exit(1);
  });
}
