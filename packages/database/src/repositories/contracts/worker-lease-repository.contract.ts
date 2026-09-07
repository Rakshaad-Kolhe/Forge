import type {
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
