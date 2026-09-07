import type { Logger } from '@forge/logging';
import type { RedisClient } from '@forge/redis';
import { WorkerHeartbeatError } from './errors.js';
import { getWorkerHeartbeatKey } from './keys.js';
import type {
  HeartbeatStoreOptions,
  WorkerHeartbeat,
  WorkerHeartbeatStore,
  WorkerId,
  WorkerStatus,
} from './types.js';

const DEFAULT_HEARTBEAT_TTL_SECONDS = 15;

class RedisWorkerHeartbeatStore implements WorkerHeartbeatStore {
  private readonly defaultTtlSeconds: number;

  constructor(
    private readonly redisClient: RedisClient,
    options?: HeartbeatStoreOptions,
    private readonly logger?: Logger,
  ) {
    this.defaultTtlSeconds = options?.defaultTtlSeconds ?? DEFAULT_HEARTBEAT_TTL_SECONDS;
  }

  public async recordHeartbeat(
    workerId: WorkerId,
    status: WorkerStatus,
    ttlSeconds?: number,
  ): Promise<void> {
    const key = getWorkerHeartbeatKey(workerId);
    const ttl = ttlSeconds ?? this.defaultTtlSeconds;

    const payload: WorkerHeartbeat = {
      workerId,
      timestamp: new Date().toISOString(),
      status,
    };

    try {
      // Atomic SET with TTL in seconds (SET ... EX)
      await this.redisClient.setJson(key, payload, { ttlSeconds: ttl });
    } catch (err) {
      this.logger?.error('Failed to record worker heartbeat in Redis', {
        workerId,
        error: (err as Error).message,
      });
      throw new WorkerHeartbeatError(
        `Failed to record heartbeat for worker "${workerId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async isAlive(workerId: WorkerId): Promise<boolean> {
    const key = getWorkerHeartbeatKey(workerId);
    try {
      const count = await this.redisClient.exists(key);
      return count > 0;
    } catch (err) {
      this.logger?.error('Failed to check worker liveness in Redis', {
        workerId,
        error: (err as Error).message,
      });
      throw new WorkerHeartbeatError(
        `Failed to check liveness for worker "${workerId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async getHeartbeat(workerId: WorkerId): Promise<WorkerHeartbeat | null> {
    const key = getWorkerHeartbeatKey(workerId);
    try {
      return await this.redisClient.getJson<WorkerHeartbeat>(key);
    } catch (err) {
      this.logger?.error('Failed to get worker heartbeat from Redis', {
        workerId,
        error: (err as Error).message,
      });
      throw new WorkerHeartbeatError(
        `Failed to get heartbeat for worker "${workerId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async getTtl(workerId: WorkerId): Promise<number> {
    const key = getWorkerHeartbeatKey(workerId);
    try {
      return await this.redisClient.ttl(key);
    } catch (err) {
      this.logger?.error('Failed to get worker heartbeat TTL from Redis', {
        workerId,
        error: (err as Error).message,
      });
      throw new WorkerHeartbeatError(
        `Failed to get TTL for worker "${workerId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async removeHeartbeat(workerId: WorkerId): Promise<void> {
    const key = getWorkerHeartbeatKey(workerId);
    try {
      await this.redisClient.del(key);
    } catch (err) {
      this.logger?.error('Failed to remove worker heartbeat from Redis', {
        workerId,
        error: (err as Error).message,
      });
      throw new WorkerHeartbeatError(
        `Failed to remove heartbeat for worker "${workerId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }
}

/**
 * Creates a Redis-backed WorkerHeartbeatStore instance.
 */
export function createWorkerHeartbeatStore(
  redisClient: RedisClient,
  options?: HeartbeatStoreOptions,
  logger?: Logger,
): WorkerHeartbeatStore {
  return new RedisWorkerHeartbeatStore(redisClient, options, logger);
}
