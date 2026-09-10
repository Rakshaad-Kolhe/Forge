import type {
  BatchClaimOptions,
  BatchClaimResult,
  ClaimJobOptions,
  ClaimJobResult,
  JobLeaseStatus,
  ReleaseLeaseOptions,
  ReleaseLeaseResult,
  RenewLeaseOptions,
  RenewLeaseResult,
  WorkerLease,
} from '@forge/contracts';

/**
 * Repository interface for durable, distributed worker job leases in PostgreSQL.
 */
export interface WorkerLeaseRepository {
  /**
   * Atomically claims a job for a worker with a renewable, time-bounded lease.
   *
   * Guarantees:
   * - At most one ACTIVE lease can exist for a job at any given time.
   * - Row lock on jobs table (`FOR UPDATE`) serializes concurrent claim requests.
   * - If an active lease exists and is held by the same worker, returns idempotent ACQUIRED.
   * - If an active lease exists and is held by another worker and unexpired, returns CONFLICT.
   * - If an active lease exists but has expired according to DB NOW(), it is marked EXPIRED and the new lease is ACQUIRED.
   * - If the job does not exist or is not QUEUED, returns NOT_CLAIMABLE.
   */
  claim(options: ClaimJobOptions): Promise<ClaimJobResult>;

  /**
   * Atomically claims multiple job leases in a single batched database transaction (PR 18).
   *
   * Guarantees:
   * - Deadlock-free row locking via canonical ascending ID ordering (`ORDER BY id ASC FOR UPDATE`).
   * - At most one ACTIVE lease per job (`uq_worker_leases_active_job`).
   * - Safe partial success: returns individual outcomes (ACQUIRED, CONFLICT, NOT_CLAIMABLE)
   *   for each attempted job matching the input order.
   * - Replaces expired active leases in bulk.
   * - Reduces database round-trips and transaction overhead from O(N) to O(1).
   * - PR 21: when `options.pendingOutbox` is supplied, one outbox row per FRESH
   *   acquisition (`status === 'ACQUIRED' && isIdempotent !== true`) is co-committed
   *   in the same claim transaction as the lease INSERT; an enqueue failure rolls the
   *   whole batch back and no lease is granted.
   */
  claimBatch?(options: BatchClaimOptions): Promise<BatchClaimResult>;

  /**
   * Renews an existing active lease held by a specific worker.
   *
   * Guarantees:
   * - Only the recorded lease owner can renew.
   * - Cannot renew an already EXPIRED or RELEASED lease.
   * - Atomically updates renewed_at and extends expires_at by durationMs from DB NOW().
   */
  renew(options: RenewLeaseOptions): Promise<RenewLeaseResult>;

  /**
   * Releases an active lease explicitly (e.g. when worker finishes or graceful shutdown).
   *
   * Guarantees:
   * - Only the recorded lease owner can release.
   * - Transitions lease status from ACTIVE to RELEASED.
   */
  release(options: ReleaseLeaseOptions): Promise<ReleaseLeaseResult>;

  /**
   * Finds the active lease for a job, if one exists and has not expired.
   */
  findActiveByJobId(jobId: string): Promise<WorkerLease | null>;

  /**
   * Finds a lease record by its unique ID.
   */
  findById(leaseId: string): Promise<WorkerLease | null>;

  /**
   * Lists leases for a given worker, optionally filtered by status.
   */
  findByWorkerId(workerId: string, filter?: { status?: JobLeaseStatus }): Promise<WorkerLease[]>;

  /**
   * Scans and updates all ACTIVE leases whose expires_at <= NOW() to EXPIRED.
   * Returns the number of reclaimed leases.
   */
  reclaimExpiredLeases(): Promise<number>;
}
