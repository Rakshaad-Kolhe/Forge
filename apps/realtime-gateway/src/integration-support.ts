/**
 * Helpers for the gateway's live integration tests (real Redis + a real `ws` client).
 * Test-only; not exported from the package entrypoint.
 */
import { once } from 'node:events';
import { WebSocket } from 'ws';
import { loadConfig } from '@forge/config';
import { createLogger, type Logger } from '@forge/logging';
import { RedisEventPublisher, RedisEventSubscriber, serializeForgeEvent } from '@forge/realtime';
import {
  createRedisPubSub,
  type RedisPubSub,
  type RedisPubSubConnectionState,
  type RedisPubSubMessageHandler,
} from '@forge/redis';
import type { ForgeEvent } from '@forge/events';
import { AllowAuthenticatedAuthorizer, type SubscriptionAuthorizer } from './authorization.js';
import { gatewayConfigFromAppConfig } from './config.js';
import { createGateway, type RealtimeGateway } from './gateway.js';

export const TEST_REDIS_URL = 'redis://127.0.0.1:6379';
export const TEST_TOKEN = 'integration-secret-token';

/**
 * Synchronous in-process {@link RedisPubSub} for tests that need deterministic delivery
 * timing (e.g. bursting many events in one tick to exercise gateway backpressure). The
 * Redis transport itself is covered separately by the live pubsub/realtime integration
 * tests — here we only need the gateway's fan-out + connection code to run for real.
 */
export class SyncPubSub implements RedisPubSub {
  public status = 'ready';
  private readonly handlers = new Map<string, Set<RedisPubSubMessageHandler>>();

  public async connect(): Promise<void> {}
  public async close(): Promise<void> {
    this.handlers.clear();
  }
  public async publish(channel: string, message: string): Promise<number> {
    const set = this.handlers.get(channel);
    if (!set) return 0;
    for (const h of Array.from(set)) h(message, channel);
    return set.size;
  }
  public async subscribe(channel: string, handler: RedisPubSubMessageHandler): Promise<void> {
    let set = this.handlers.get(channel);
    if (!set) {
      set = new Set();
      this.handlers.set(channel, set);
    }
    set.add(handler);
  }
  public async unsubscribe(channel: string): Promise<void> {
    this.handlers.delete(channel);
  }
  public onConnectionChange(_l: (s: RedisPubSubConnectionState) => void): () => void {
    return () => {};
  }
  /** Test helper: publish a validated ForgeEvent straight onto a channel. */
  public emit(channel: string, event: ForgeEvent): void {
    void this.publish(channel, serializeForgeEvent(event));
  }
}

export interface TestGateway {
  readonly gateway: RealtimeGateway;
  readonly port: number;
  readonly channel: string;
  readonly pubsub: RedisPubSub;
  readonly logger: Logger;
  stop(): Promise<void>;
}

let channelCounter = 0;

export async function startTestGateway(
  overrides: {
    channel?: string;
    authToken?: string;
    originAllowlist?: string;
    maxConnections?: number;
    maxSubscriptionsPerConnection?: number;
    maxPendingMessages?: number;
    maxMessageBytes?: number;
    heartbeatIntervalMs?: number;
    authorizer?: SubscriptionAuthorizer;
    silent?: boolean;
    /** Inject a transport (e.g. {@link SyncPubSub}); defaults to a live Redis connection. */
    pubsub?: RedisPubSub;
  } = {},
): Promise<TestGateway> {
  channelCounter += 1;
  const channel =
    overrides.channel ??
    `forge:test:gw:${Date.now()}_${channelCounter}_${Math.random().toString(36).slice(2, 8)}`;

  const appConfig = loadConfig({
    NODE_ENV: 'test',
    WEBSOCKET_AUTH_TOKEN: overrides.authToken ?? TEST_TOKEN,
    WEBSOCKET_ORIGIN_ALLOWLIST: overrides.originAllowlist ?? '',
    WEBSOCKET_MAX_CONNECTIONS: String(overrides.maxConnections ?? 50),
    WEBSOCKET_MAX_SUBSCRIPTIONS_PER_CONNECTION: String(
      overrides.maxSubscriptionsPerConnection ?? 10,
    ),
    WEBSOCKET_MAX_PENDING_MESSAGES: String(overrides.maxPendingMessages ?? 20),
    WEBSOCKET_MAX_MESSAGE_BYTES: String(overrides.maxMessageBytes ?? 4096),
    WEBSOCKET_HEARTBEAT_INTERVAL_MS: String(overrides.heartbeatIntervalMs ?? 30000),
    REALTIME_REDIS_CHANNEL: channel,
  });

  const logger = createLogger({
    service: 'realtime-gateway',
    environment: 'test',
    minLevel: overrides.silent === false ? 'debug' : 'error',
  });

  const pubsub =
    overrides.pubsub ?? createRedisPubSub({ url: TEST_REDIS_URL, maxRetriesPerRequest: 2 }, logger);
  await pubsub.connect();
  const subscriber = new RedisEventSubscriber({ pubsub, channel, logger });

  const gateway = createGateway({
    // Port 0 lets the OS pick a free port — a test-only override, not a config value.
    config: { ...gatewayConfigFromAppConfig(appConfig), port: 0 },
    subscriber,
    authorizer: overrides.authorizer ?? new AllowAuthenticatedAuthorizer(),
    logger,
  });
  await gateway.start();

  const port = gateway.address()?.port;
  if (!port) {
    throw new Error('test gateway did not bind a port');
  }

  return {
    gateway,
    port,
    channel,
    pubsub,
    logger,
    async stop() {
      await gateway.stop();
      await pubsub.close();
    },
  };
}

/** A publisher that writes straight to the gateway's Redis channel (stands in for the outbox path). */
export async function makeChannelPublisher(channel: string): Promise<{
  publisher: RedisEventPublisher;
  close(): Promise<void>;
}> {
  const pubsub = createRedisPubSub({ url: TEST_REDIS_URL, maxRetriesPerRequest: 2 });
  await pubsub.connect();
  return {
    publisher: new RedisEventPublisher({ pubsub, channel }),
    close: () => pubsub.close(),
  };
}

export interface TestClientOptions {
  readonly headers?: Record<string, string>;
  readonly protocols?: string | string[];
  readonly origin?: string;
}

/**
 * Opens a `ws` client, attaches a {@link MessageCollector} *before* the socket opens (so
 * the server's immediate `ready` frame is never missed), and resolves once open.
 */
export async function openClient(
  port: number,
  options: TestClientOptions = {},
): Promise<WebSocket & { rx: MessageCollector }> {
  const headers: Record<string, string> = {
    authorization: `Bearer ${TEST_TOKEN}`,
    ...options.headers,
  };
  if (options.origin) {
    headers['origin'] = options.origin;
  }
  const ws = new WebSocket(`ws://127.0.0.1:${port}`, options.protocols, {
    headers,
  }) as WebSocket & { rx: MessageCollector };
  ws.rx = new MessageCollector(ws);
  await once(ws, 'open');
  return ws;
}

/** Collects parsed server messages; `waitFor` resolves when `predicate` matches one. */
export class MessageCollector {
  public readonly messages: Array<Record<string, unknown>> = [];

  constructor(ws: WebSocket) {
    ws.on('message', (data) => {
      this.messages.push(JSON.parse(String(data)) as Record<string, unknown>);
    });
  }

  public async waitFor(
    predicate: (m: Record<string, unknown>) => boolean,
    timeoutMs = 3000,
  ): Promise<Record<string, unknown>> {
    const started = Date.now();
    for (;;) {
      const hit = this.messages.find(predicate);
      if (hit) {
        return hit;
      }
      if (Date.now() - started > timeoutMs) {
        throw new Error(
          `timed out waiting for message; saw: ${JSON.stringify(this.messages.map((m) => m['type']))}`,
        );
      }
      await new Promise((r) => setTimeout(r, 15));
    }
  }

  public count(type: string): number {
    return this.messages.filter((m) => m['type'] === type).length;
  }
}
