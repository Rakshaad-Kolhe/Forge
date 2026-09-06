export const DEFAULT_REDIS_URL = 'redis://127.0.0.1:6379';

/**
 * Configuration options for creating a managed Redis connection.
 */
export interface RedisConfig {
  /**
   * Redis connection URI (e.g. redis://127.0.0.1:6379 or redis://:password@127.0.0.1:6379/0).
   */
  url: string;

  /**
   * Maximum command retries before failing.
   * Default: 3. Set to null for unlimited.
   */
  maxRetriesPerRequest?: number | null;

  /**
   * Connection timeout in milliseconds.
   * Default: 5000ms.
   */
  connectTimeoutMillis?: number;

  /**
   * Custom retry strategy for reconnecting to Redis.
   * Receives retry attempt count, returns delay in milliseconds or null to abort.
   */
  retryStrategy?: (times: number) => number | void | null;

  /**
   * Whether to check if Redis is ready after connecting.
   * Default: true.
   */
  enableReadyCheck?: boolean;

  /**
   * Whether to defer connection until explicit connect() call.
   * Default: true.
   */
  lazyConnect?: boolean;
}
