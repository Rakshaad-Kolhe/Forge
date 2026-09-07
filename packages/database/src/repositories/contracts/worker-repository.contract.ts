/**
 * Allowed worker lifecycle statuses.
 */
export type WorkerLifecycleStatus = 'STARTING' | 'READY' | 'DRAINING' | 'OFFLINE';

/**
 * Worker hardware and capacity resources persisted in PostgreSQL.
 */
export interface WorkerResourcesRecord {
  cpuCores: number;
  memoryBytes: number;
  gpuCount?: number;
}

/**
 * Authoritative worker metadata persisted in PostgreSQL.
 */
export interface WorkerRecord {
  id: string;
  status: WorkerLifecycleStatus;
  hostname?: string | null;
  executors: string[];
  resources: WorkerResourcesRecord;
  registeredAt: Date;
  updatedAt: Date;
}

/**
 * Repository interface for durable Worker metadata in PostgreSQL.
 */
export interface WorkerRepository {
  /**
   * Persists a worker record. If a record with the same ID already exists,
   * updates status, hostname, executors, resources, and updatedAt (idempotent upsert).
   */
  save(worker: WorkerRecord): Promise<void>;

  /**
   * Finds a worker by its unique identifier.
   */
  findById(id: string): Promise<WorkerRecord | null>;

  /**
   * Lists all workers, optionally filtered by status, ordered by registeredAt DESC.
   */
  list(filter?: { status?: WorkerLifecycleStatus }): Promise<WorkerRecord[]>;

  /**
   * Updates a worker's lifecycle status. Returns true if the worker existed and was updated.
   */
  updateStatus(id: string, status: WorkerLifecycleStatus): Promise<boolean>;

  /**
   * Deletes a worker record. Returns true if removed.
   */
  delete(id: string): Promise<boolean>;
}
