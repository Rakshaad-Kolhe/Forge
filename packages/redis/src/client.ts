import { Redis } from 'ioredis';
import type { Logger } from '@forge/logging';
import type { RedisConfig } from './config.js';
import { RedisCommandError, RedisConnectionError, RedisError, sanitizeRedisUrl } from './errors.js';
import { deserializeJson, serializeJson } from './serialization.js';
import type { RedisClient, SetOptions } from './types.js';

class ManagedRedisClient implements RedisClient {
  private readonly client: Redis;
  private readonly sanitizedUrl: string;

  constructor(
    config: RedisConfig,
    private readonly logger?: Logger,
  ) {
    this.sanitizedUrl = sanitizeRedisUrl(config.url);

    this.client = new Redis(config.url, {
      lazyConnect: config.lazyConnect ?? true,
      maxRetriesPerRequest:
        config.maxRetriesPerRequest !== undefined ? config.maxRetriesPerRequest : 3,
      connectTimeout: config.connectTimeoutMillis ?? 5000,
      enableReadyCheck: config.enableReadyCheck ?? true,
      retryStrategy:
        config.retryStrategy ??
        ((times: number) => {
          // Bounded exponential backoff with max 2s delay
          return Math.min(times * 100, 2000);
        }),
    });

    this.registerLifecycleEvents();
  }

  private registerLifecycleEvents(): void {
    this.client.on('connect', () => {
      this.logger?.debug('Redis socket connected', { url: this.sanitizedUrl });
    });

    this.client.on('ready', () => {
      this.logger?.info('Redis connection ready', { url: this.sanitizedUrl });
    });

    this.client.on('error', (err: Error) => {
      this.logger?.error('Redis client error', {
        error: err.message,
        url: this.sanitizedUrl,
      });
    });

    this.client.on('close', () => {
      this.logger?.debug('Redis connection closed', { url: this.sanitizedUrl });
    });

    this.client.on('reconnecting', (time: number) => {
      this.logger?.warn('Redis reconnecting', {
        delayMs: time,
        url: this.sanitizedUrl,
      });
    });

    this.client.on('end', () => {
      this.logger?.debug('Redis connection ended', { url: this.sanitizedUrl });
    });
  }

  public get status(): string {
    return this.client.status;
  }

  public async connect(): Promise<void> {
    if (this.client.status === 'ready' || this.client.status === 'connecting') {
      return;
    }

    try {
      await this.client.connect();
    } catch (err) {
      throw new RedisConnectionError(
        `Failed to connect to Redis at ${this.sanitizedUrl}: ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async close(): Promise<void> {
    if (this.client.status === 'end') {
      return;
    }

    try {
      await this.client.quit();
    } catch {
      this.client.disconnect();
    }
  }

  public async disconnect(): Promise<void> {
    this.client.disconnect();
  }

  public async healthCheck(): Promise<boolean> {
    try {
      const res = await this.client.ping();
      return res === 'PONG';
    } catch {
      return false;
    }
  }

  public getRawClient(): Redis {
    return this.client;
  }

  // --- Low-Level Key-Value Primitives ---

  public async get(key: string): Promise<string | null> {
    try {
      return await this.client.get(key);
    } catch (err) {
      throw this.wrapError(err, `GET "${key}"`);
    }
  }

  public async set(key: string, value: string, options?: SetOptions): Promise<boolean> {
    try {
      const args: (string | number)[] = [key, value];

      if (options?.ttlSeconds !== undefined) {
        args.push('EX', options.ttlSeconds);
      } else if (options?.ttlMillis !== undefined) {
        args.push('PX', options.ttlMillis);
      }

      if (options?.ifNotExists) {
        args.push('NX');
      } else if (options?.ifExists) {
        args.push('XX');
      }

      const res = await (
        this.client.set as unknown as (...a: (string | number)[]) => Promise<string | null>
      )(...args);
      return res === 'OK';
    } catch (err) {
      throw this.wrapError(err, `SET "${key}"`);
    }
  }

  public async del(...keys: string[]): Promise<number> {
    if (keys.length === 0) {
      return 0;
    }
    try {
      return await this.client.del(...keys);
    } catch (err) {
      throw this.wrapError(err, `DEL [${keys.join(', ')}]`);
    }
  }

  public async exists(...keys: string[]): Promise<number> {
    if (keys.length === 0) {
      return 0;
    }
    try {
      return await this.client.exists(...keys);
    } catch (err) {
      throw this.wrapError(err, `EXISTS [${keys.join(', ')}]`);
    }
  }

  public async expire(key: string, seconds: number): Promise<boolean> {
    try {
      const res = await this.client.expire(key, seconds);
      return res === 1;
    } catch (err) {
      throw this.wrapError(err, `EXPIRE "${key}" ${seconds}s`);
    }
  }

  public async ttl(key: string): Promise<number> {
    try {
      return await this.client.ttl(key);
    } catch (err) {
      throw this.wrapError(err, `TTL "${key}"`);
    }
  }

  // --- Structured JSON Helpers ---

  public async getJson<T>(key: string): Promise<T | null> {
    const raw = await this.get(key);
    if (raw === null) {
      return null;
    }
    return deserializeJson<T>(raw);
  }

  public async setJson<T>(key: string, value: T, options?: SetOptions): Promise<boolean> {
    const serialized = serializeJson(value);
    return await this.set(key, serialized, options);
  }

  // --- Atomic Coordination Primitives ---

  public async setNx(key: string, value: string, ttlSeconds?: number): Promise<boolean> {
    try {
      if (ttlSeconds !== undefined) {
        const res = await this.client.set(key, value, 'EX', ttlSeconds, 'NX');
        return res === 'OK';
      }
      const res = await this.client.set(key, value, 'NX');
      return res === 'OK';
    } catch (err) {
      throw this.wrapError(err, `SETNX "${key}"`);
    }
  }

  public async incr(key: string): Promise<number> {
    try {
      return await this.client.incr(key);
    } catch (err) {
      throw this.wrapError(err, `INCR "${key}"`);
    }
  }

  public async decr(key: string): Promise<number> {
    try {
      return await this.client.decr(key);
    } catch (err) {
      throw this.wrapError(err, `DECR "${key}"`);
    }
  }

  public async eval(
    script: string,
    numkeys: number,
    ...args: (string | number)[]
  ): Promise<unknown> {
    try {
      return await this.client.eval(script, numkeys, ...args);
    } catch (err) {
      throw this.wrapError(err, 'EVAL');
    }
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
        `Redis ${operation} failed due to connection error: ${message}`,
        err as Error,
      );
    }

    return new RedisCommandError(`Redis ${operation} failed: ${message}`, err as Error);
  }
}

/**
 * Creates and initializes a managed RedisClient instance.
 */
export function createRedisClient(config: RedisConfig, logger?: Logger): RedisClient {
  return new ManagedRedisClient(config, logger);
}
