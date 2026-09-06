import type { Redis } from 'ioredis';

/**
 * Options for generic Redis SET commands.
 */
export interface SetOptions {
  /**
   * Time-to-live in seconds (translates to Redis EX).
   */
  ttlSeconds?: number;

  /**
   * Time-to-live in milliseconds (translates to Redis PX).
   */
  ttlMillis?: number;

  /**
   * Only set the key if it does not already exist (translates to Redis NX).
   */
  ifNotExists?: boolean;

  /**
   * Only set the key if it already exists (translates to Redis XX).
   */
  ifExists?: boolean;
}

/**
 * High-level Redis client interface providing lifecycle management,
 * generic coordination primitives, TTL support, and atomic operations.
 */
export interface RedisClient {
  /**
   * Current connection status string reported by the underlying driver
   * ('wait' | 'connecting' | 'connect' | 'ready' | 'close' | 'reconnecting' | 'end').
   */
  readonly status: string;

  /**
   * Establishes a connection to the Redis server.
   * Idempotent: does nothing if already connected or ready.
   */
  connect(): Promise<void>;

  /**
   * Closes the connection gracefully, allowing in-flight commands to finish.
   */
  close(): Promise<void>;

  /**
   * Immediately closes the client connection, abandoning queued commands.
   */
  disconnect(): Promise<void>;

  /**
   * Performs an active health check using an explicit PING operation.
   * Returns true if Redis responds with PONG; false if unreachable.
   */
  healthCheck(): Promise<boolean>;

  /**
   * Returns the underlying driver instance for specialized operations.
   */
  getRawClient(): Redis;

  // --- Low-Level Key-Value Primitives ---

  /**
   * Gets the string value of a key.
   */
  get(key: string): Promise<string | null>;

  /**
   * Sets a string key-value pair with optional TTL or conditional flags.
   * Returns true if key was set; false if conditions (e.g. NX) were not met.
   */
  set(key: string, value: string, options?: SetOptions): Promise<boolean>;

  /**
   * Deletes one or more keys. Returns the count of keys that were removed.
   */
  del(...keys: string[]): Promise<number>;

  /**
   * Checks whether one or more keys exist. Returns the count of existing keys.
   */
  exists(...keys: string[]): Promise<number>;

  /**
   * Sets a timeout on a key in seconds.
   * Returns true if timeout was set; false if key does not exist.
   */
  expire(key: string, seconds: number): Promise<boolean>;

  /**
   * Returns the remaining time to live of a key in seconds.
   * Returns -1 if the key has no associated expiration, -2 if the key does not exist.
   */
  ttl(key: string): Promise<number>;

  // --- Structured JSON Helpers ---

  /**
   * Retrieves and deserializes a JSON value stored at key.
   * Returns null if key does not exist.
   */
  getJson<T>(key: string): Promise<T | null>;

  /**
   * Serializes and stores a JSON value at key with optional TTL and conditions.
   */
  setJson<T>(key: string, value: T, options?: SetOptions): Promise<boolean>;

  // --- Atomic Coordination Primitives ---

  /**
   * Sets a key only if it does not already exist, with an optional TTL in seconds.
   * Returns true if the key was set (acquired); false if already held.
   * Note: This is an atomic primitive, NOT a full distributed lock implementation.
   */
  setNx(key: string, value: string, ttlSeconds?: number): Promise<boolean>;

  /**
   * Atomically increments the numeric value of a key by 1.
   * Returns the value of key after the increment.
   */
  incr(key: string): Promise<number>;

  /**
   * Atomically decrements the numeric value of a key by 1.
   * Returns the value of key after the decrement.
   */
  decr(key: string): Promise<number>;

  /**
   * Atomically executes an arbitrary Lua script on the Redis server.
   */
  eval(script: string, numkeys: number, ...args: (string | number)[]): Promise<unknown>;
}
