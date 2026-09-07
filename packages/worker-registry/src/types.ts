/**
 * Type-safe branded Worker identifier.
 */
declare const __brand: unique symbol;
export type Brand<T, B> = T & { readonly [__brand]: B };

export type WorkerId = Brand<string, 'WorkerId'>;

/**
 * Creates and validates a branded WorkerId.
 */
export function createWorkerId(id: string): WorkerId {
  const trimmed = id?.trim();
  if (!trimmed) {
    throw new Error('WorkerId cannot be empty');
  }
  return trimmed as WorkerId;
}

/**
 * Explicit worker lifecycle statuses.
 *
 * STARTING: Worker process is booting and initializing local executors/environment.
 * READY: Worker is fully registered, sending heartbeats, and eligible for future work.
 * DRAINING: Worker is gracefully finishing in-flight work and not accepting new jobs.
 * OFFLINE: Worker process has gracefully shut down or been explicitly deregistered.
 */
export type WorkerStatus = 'STARTING' | 'READY' | 'DRAINING' | 'OFFLINE';

/**
 * Worker execution capabilities advertised to the cluster.
 */
export interface WorkerCapabilities {
  readonly executors: readonly string[];
}

/**
 * Hardware capacity and execution resource limits of the worker node.
 */
export interface WorkerResources {
  readonly cpuCores: number;
  readonly memoryBytes: number;
  readonly gpuCount?: number;
}

/**
 * Full durable worker record shape persisted in PostgreSQL.
 */
export interface WorkerMetadata {
  readonly workerId: WorkerId;
  readonly status: WorkerStatus;
  readonly hostname?: string | null;
  readonly capabilities: WorkerCapabilities;
  readonly resources: WorkerResources;
  readonly registeredAt: string;
  readonly updatedAt: string;
}

/**
 * Input arguments for registering a worker in the cluster.
 */
export interface RegisterWorkerInput {
  readonly workerId?: string;
  readonly status?: WorkerStatus;
  readonly hostname?: string;
  readonly capabilities: WorkerCapabilities;
  readonly resources: WorkerResources;
}

/**
 * Transient liveness state of a worker derived from Redis heartbeat expiration.
 *
 * ALIVE: Recent heartbeat exists in Redis within the configured TTL.
 * STALE: Durable worker record exists in PostgreSQL, but Redis heartbeat has expired or is absent.
 */
export type WorkerLiveness = 'ALIVE' | 'STALE';

/**
 * Ephemeral heartbeat payload stored in Redis.
 */
export interface WorkerHeartbeat {
  readonly workerId: WorkerId;
  readonly timestamp: string;
  readonly status: WorkerStatus;
}

/**
 * Unified view of durable worker metadata combined with transient Redis liveness state.
 */
export interface WorkerInfo {
  readonly worker: WorkerMetadata;
  readonly liveness: WorkerLiveness;
  readonly lastHeartbeatAt: string | null;
}

/**
 * Configuration options for the Redis-backed WorkerHeartbeatStore.
 */
export interface HeartbeatStoreOptions {
  /**
   * Default time-to-live in seconds for worker heartbeat keys.
   * Default: 15 seconds.
   */
  readonly defaultTtlSeconds?: number;
}

/**
 * Abstraction for managing transient worker heartbeats in Redis.
 */
export interface WorkerHeartbeatStore {
  /**
   * Atomically records a heartbeat for the worker, setting or refreshing the expiration TTL.
   */
  recordHeartbeat(workerId: WorkerId, status: WorkerStatus, ttlSeconds?: number): Promise<void>;

  /**
   * Checks whether the worker has an active, unexpired heartbeat in Redis.
   */
  isAlive(workerId: WorkerId): Promise<boolean>;

  /**
   * Retrieves the current heartbeat payload from Redis, or null if absent/expired.
   */
  getHeartbeat(workerId: WorkerId): Promise<WorkerHeartbeat | null>;

  /**
   * Returns remaining TTL in seconds for the worker's heartbeat key (-2 if missing).
   */
  getTtl(workerId: WorkerId): Promise<number>;

  /**
   * Immediately deletes the worker's heartbeat key (e.g. during graceful shutdown).
   */
  removeHeartbeat(workerId: WorkerId): Promise<void>;
}

/**
 * Configuration options for the WorkerRegistry.
 */
export interface WorkerRegistryOptions {
  /**
   * Heartbeat TTL in seconds applied during worker heartbeat renewals.
   * Default: 15 seconds.
   */
  readonly heartbeatTtlSeconds?: number;
}

/**
 * High-level registry coordinating durable PostgreSQL worker metadata
 * with transient Redis worker liveness.
 */
export interface WorkerRegistry {
  /**
   * Registers worker metadata in PostgreSQL and establishes initial Redis heartbeat.
   * Idempotent: repeated registration of the same workerId updates metadata without duplication.
   */
  register(input: RegisterWorkerInput): Promise<WorkerMetadata>;

  /**
   * Refreshes transient heartbeat in Redis. Does NOT write to PostgreSQL.
   */
  heartbeat(workerId: WorkerId, status?: WorkerStatus): Promise<void>;

  /**
   * Gracefully deregisters a worker: removes Redis heartbeat key and marks PostgreSQL status as OFFLINE.
   * Historical metadata remains in PostgreSQL.
   */
  deregister(workerId: WorkerId): Promise<void>;

  /**
   * Retrieves worker metadata from PostgreSQL combined with transient Redis liveness state.
   */
  getWorker(workerId: WorkerId): Promise<WorkerInfo | null>;

  /**
   * Lists workers from PostgreSQL with their respective real-time liveness state,
   * optionally filtered by status or liveness.
   */
  listWorkers(filter?: { status?: WorkerStatus; liveness?: WorkerLiveness }): Promise<WorkerInfo[]>;
}
