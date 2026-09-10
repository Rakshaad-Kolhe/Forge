/**
 * Redis Pub/Sub adapter — the transient cross-process transport seam for Forge V2.
 *
 *   RedisPubSub  ≠  a durable message log.
 *
 * Pub/Sub is fire-and-forget: a message published while no subscriber is connected is
 * gone. It never replaces the PostgreSQL transactional outbox (PR 21) as the durable
 * event buffer — it only moves already-committed events between processes in real time.
 *
 * The adapter keeps a **dedicated subscriber connection** (ioredis forbids regular
 * commands once a connection enters subscriber mode) separate from the publisher
 * connection. Both `connect()` and `close()` are idempotent. There is no global
 * singleton — callers construct, own, and `close()` an instance. On a dropped
 * connection ioredis reconnects and **automatically re-subscribes** the channels it
 * held, so a Redis blip does not require the caller to re-subscribe; the adapter only
 * surfaces the transition via {@link RedisPubSub.onConnectionChange} and structured logs.
 */
import { Redis } from 'ioredis';
import type { Logger } from '@forge/logging';
import type { RedisConfig } from './config.js';
import { RedisCommandError, RedisConnectionError, RedisError, sanitizeRedisUrl } from './errors.js';

/** Invoked for every message received on a subscribed channel. Must not throw. */
export type RedisPubSubMessageHandler = (message: string, channel: string) => void;

/** Lifecycle transitions of the underlying subscriber connection. */
export type RedisPubSubConnectionState = 'connected' | 'disconnected';

export interface RedisPubSub {
  /** Status of the subscriber connection ('wait' | 'connecting' | 'ready' | 'close' | 'end' | ...). */
  readonly status: string;
  /** Connects both the publisher and subscriber connections. Idempotent. */
  connect(): Promise<void>;
  /** Publishes `message` on `channel`. Resolves with the number of receiving subscribers. */
  publish(channel: string, message: string): Promise<number>;
  /** Registers `handler` for `channel`, subscribing the underlying connection on first use. */
  subscribe(channel: string, handler: RedisPubSubMessageHandler): Promise<void>;
  /** Drops every handler for `channel` and unsubscribes the underlying connection. */
  unsubscribe(channel: string): Promise<void>;
  /** Closes both connections gracefully. Idempotent. */
  close(): Promise<void>;
  /** Observes subscriber-connection lifecycle transitions. Returns an idempotent unsubscribe. */
  onConnectionChange(listener: (state: RedisPubSubConnectionState) => void): () => void;
}

function buildConnection(config: RedisConfig): Redis {
  return new Redis(config.url, {
    lazyConnect: config.lazyConnect ?? true,
    maxRetriesPerRequest:
      config.maxRetriesPerRequest !== undefined ? config.maxRetriesPerRequest : 3,
    connectTimeout: config.connectTimeoutMillis ?? 5000,
    enableReadyCheck: config.enableReadyCheck ?? true,
    retryStrategy: config.retryStrategy ?? ((times: number) => Math.min(times * 100, 2000)),
  });
}

class ManagedRedisPubSub implements RedisPubSub {
  private readonly publisher: Redis;
  private readonly subscriber: Redis;
  private readonly sanitizedUrl: string;
  private readonly handlers = new Map<string, Set<RedisPubSubMessageHandler>>();
  private readonly connectionListeners = new Set<(state: RedisPubSubConnectionState) => void>();
  private messageListenerBound = false;
  private closed = false;

  constructor(
    config: RedisConfig,
    private readonly logger?: Logger,
  ) {
    this.sanitizedUrl = sanitizeRedisUrl(config.url);
    this.publisher = buildConnection(config);
    this.subscriber = buildConnection(config);
    this.registerLifecycleEvents();
  }

  private registerLifecycleEvents(): void {
    this.subscriber.on('ready', () => {
      this.logger?.info('realtime.redis_connected', { url: this.sanitizedUrl });
      this.emitConnectionState('connected');
    });
    this.subscriber.on('end', () => {
      this.logger?.warn('realtime.redis_disconnected', { url: this.sanitizedUrl });
      this.emitConnectionState('disconnected');
    });
    this.subscriber.on('error', (err: Error) => {
      this.logger?.error('realtime.redis_subscriber_error', {
        url: this.sanitizedUrl,
        error: err.message,
      });
    });
    this.publisher.on('error', (err: Error) => {
      this.logger?.error('realtime.redis_publisher_error', {
        url: this.sanitizedUrl,
        error: err.message,
      });
    });
  }

  private emitConnectionState(state: RedisPubSubConnectionState): void {
    for (const listener of this.connectionListeners) {
      try {
        listener(state);
      } catch (err) {
        this.logger?.error('realtime.redis_connection_listener_failed', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }

  public get status(): string {
    return this.subscriber.status;
  }

  public async connect(): Promise<void> {
    if (this.closed) {
      throw new RedisConnectionError('Cannot connect a closed RedisPubSub');
    }
    await Promise.all([
      this.connectOne(this.publisher, 'publisher'),
      this.connectOne(this.subscriber, 'subscriber'),
    ]);
  }

  private async connectOne(conn: Redis, role: string): Promise<void> {
    if (conn.status === 'ready' || conn.status === 'connecting') {
      return;
    }
    try {
      await conn.connect();
    } catch (err) {
      throw new RedisConnectionError(
        `Failed to connect RedisPubSub ${role} to ${this.sanitizedUrl}: ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async publish(channel: string, message: string): Promise<number> {
    if (this.closed) {
      throw new RedisConnectionError('Cannot publish on a closed RedisPubSub');
    }
    try {
      return await this.publisher.publish(channel, message);
    } catch (err) {
      throw this.wrapError(err, `PUBLISH "${channel}"`);
    }
  }

  public async subscribe(channel: string, handler: RedisPubSubMessageHandler): Promise<void> {
    if (this.closed) {
      throw new RedisConnectionError('Cannot subscribe on a closed RedisPubSub');
    }
    this.bindMessageListener();

    let channelHandlers = this.handlers.get(channel);
    const isFirstForChannel = channelHandlers === undefined;
    if (!channelHandlers) {
      channelHandlers = new Set();
      this.handlers.set(channel, channelHandlers);
    }
    channelHandlers.add(handler);

    if (isFirstForChannel) {
      try {
        await this.subscriber.subscribe(channel);
      } catch (err) {
        // Roll back the handler bookkeeping so a retry re-attempts the SUBSCRIBE.
        channelHandlers.delete(handler);
        if (channelHandlers.size === 0) {
          this.handlers.delete(channel);
        }
        throw this.wrapError(err, `SUBSCRIBE "${channel}"`);
      }
    }
  }

  public async unsubscribe(channel: string): Promise<void> {
    if (!this.handlers.has(channel)) {
      return;
    }
    this.handlers.delete(channel);
    if (this.closed) {
      return;
    }
    try {
      await this.subscriber.unsubscribe(channel);
    } catch (err) {
      throw this.wrapError(err, `UNSUBSCRIBE "${channel}"`);
    }
  }

  public async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.handlers.clear();
    await Promise.all([this.quietQuit(this.publisher), this.quietQuit(this.subscriber)]);
  }

  private async quietQuit(conn: Redis): Promise<void> {
    if (conn.status === 'end') {
      return;
    }
    try {
      await conn.quit();
    } catch {
      conn.disconnect();
    }
  }

  public onConnectionChange(listener: (state: RedisPubSubConnectionState) => void): () => void {
    this.connectionListeners.add(listener);
    let active = true;
    return () => {
      if (active) {
        active = false;
        this.connectionListeners.delete(listener);
      }
    };
  }

  private bindMessageListener(): void {
    if (this.messageListenerBound) {
      return;
    }
    this.messageListenerBound = true;
    this.subscriber.on('message', (channel: string, message: string) => {
      const channelHandlers = this.handlers.get(channel);
      if (!channelHandlers) {
        return;
      }
      for (const handler of Array.from(channelHandlers)) {
        try {
          handler(message, channel);
        } catch (err) {
          this.logger?.error('realtime.redis_message_handler_failed', {
            channel,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }
    });
  }

  private wrapError(err: unknown, operation: string): RedisError {
    if (err instanceof RedisError) {
      return err;
    }
    const message = (err as Error).message ?? String(err);
    if (
      message.includes('Connection is closed') ||
      message.includes('ECONNREFUSED') ||
      message.includes('ENOTFOUND') ||
      message.includes('ETIMEDOUT')
    ) {
      return new RedisConnectionError(
        `RedisPubSub ${operation} failed due to connection error: ${message}`,
        err as Error,
      );
    }
    return new RedisCommandError(`RedisPubSub ${operation} failed: ${message}`, err as Error);
  }
}

/**
 * Creates a managed Redis Pub/Sub adapter. The connection is lazy by default — call
 * {@link RedisPubSub.connect} (or rely on the first `publish` auto-connecting the
 * publisher) before expecting delivery.
 */
export function createRedisPubSub(config: RedisConfig, logger?: Logger): RedisPubSub {
  return new ManagedRedisPubSub(config, logger);
}
