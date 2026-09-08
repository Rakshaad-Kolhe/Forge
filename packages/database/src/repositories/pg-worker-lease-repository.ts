import { randomUUID } from 'node:crypto';
import type {
  BatchClaimItemResult,
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
import { PersistenceError } from '../errors.js';
import type { DatabaseClient, WorkerLeaseRow } from '../types.js';
import type { WorkerLeaseRepository } from './contracts/worker-lease-repository.contract.js';

interface JobLockRow {
  id: string;
  status: string;
}

interface LeaseCheckRow extends WorkerLeaseRow {
  is_expired: boolean;
}

export class PgWorkerLeaseRepository implements WorkerLeaseRepository {
  constructor(private readonly client: DatabaseClient) {}

  public async claim(options: ClaimJobOptions): Promise<ClaimJobResult> {
    try {
      const batchResult = await this.claimBatch({
        items: [
          {
            jobId: options.jobId,
            workerId: options.workerId,
            durationMs: options.durationMs,
          },
        ],
      });

      const itemRes = batchResult.results[0];
      if (!itemRes) {
        throw new Error(`Batch claim returned empty results for job "${options.jobId}"`);
      }

      if (itemRes.status === 'ACQUIRED') {
        return {
          status: 'ACQUIRED',
          lease: itemRes.lease,
          isIdempotent: itemRes.isIdempotent,
        };
      }

      if (itemRes.status === 'CONFLICT') {
        return {
          status: 'CONFLICT',
          reason: itemRes.reason,
          currentOwnerId: itemRes.currentOwnerId,
          expiresAt: itemRes.expiresAt,
        };
      }

      return {
        status: 'NOT_CLAIMABLE',
        reason: itemRes.reason === 'DUPLICATE_IN_BATCH' ? 'JOB_NOT_CLAIMABLE' : itemRes.reason,
        details: itemRes.details,
      };
    } catch (err) {
      const cause = (err as PersistenceError).cause as
        { code?: string; message?: string } | undefined;
      const pgErr = cause ?? (err as { code?: string; message?: string });
      // Handle race condition on partial unique index uq_worker_leases_active_job
      if (pgErr?.code === '23505') {
        const active = await this.findActiveByJobId(options.jobId);
        if (active) {
          if (active.workerId === options.workerId) {
            return {
              status: 'ACQUIRED',
              lease: active,
              isIdempotent: true,
            };
          }
          return {
            status: 'CONFLICT',
            reason: 'LEASE_ALREADY_HELD',
            currentOwnerId: active.workerId,
            expiresAt: active.expiresAt,
          };
        }
      }

      throw err instanceof PersistenceError
        ? err
        : new PersistenceError(
            `Failed to claim job lease for job "${options.jobId}": ${(err as Error).message}`,
            err as Error,
          );
    }
  }

  public async claimBatch(options: BatchClaimOptions): Promise<BatchClaimResult> {
    if (!options.items || options.items.length === 0) {
      return {
        results: [],
        acquiredCount: 0,
        conflictCount: 0,
        notClaimableCount: 0,
      };
    }

    try {
      return await this.withTransactionClient(async (txClient) => {
        const items = options.items;
        const results: (BatchClaimItemResult | null)[] = new Array(items.length).fill(null);

        // Track first index of each job ID in this batch to handle duplicates
        const firstIndexForJob = new Map<string, number>();
        const jobIdsToQuery: string[] = [];

        for (let i = 0; i < items.length; i++) {
          const item = items[i]!;
          if (firstIndexForJob.has(item.jobId)) {
            continue;
          }
          firstIndexForJob.set(item.jobId, i);
          jobIdsToQuery.push(item.jobId);
        }

        // Canonical ascending sort on unique job IDs to prevent deadlocks across concurrent transactions
        const sortedJobIds = [...jobIdsToQuery].sort();

        // 1. Lock job rows in canonical ascending order
        const jobRes = await txClient.query<JobLockRow>(
          `SELECT id, status FROM jobs WHERE id = ANY($1::text[]) ORDER BY id ASC FOR UPDATE;`,
          [sortedJobIds],
        );

        const jobStatusMap = new Map<string, string>();
        for (const row of jobRes.rows) {
          jobStatusMap.set(row.id, row.status);
        }

        // 2. Lock active leases for these jobs in canonical ascending order
        const leaseRes = await txClient.query<LeaseCheckRow>(
          `
          SELECT id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at,
                 (expires_at <= NOW()) AS is_expired
          FROM worker_leases
          WHERE job_id = ANY($1::text[]) AND status = 'ACTIVE'
          ORDER BY id ASC
          FOR UPDATE;
          `,
          [sortedJobIds],
        );

        const activeLeaseMap = new Map<string, LeaseCheckRow>();
        for (const row of leaseRes.rows) {
          activeLeaseMap.set(row.job_id, row);
        }

        const expiredLeaseIdsToUpdate: string[] = [];
        interface InsertCandidate {
          id: string;
          jobId: string;
          workerId: string;
          durationMs: number;
          primaryIndex: number;
        }
        const leasesToInsert: InsertCandidate[] = [];

        // Evaluate primary items for each unique job ID
        for (const [jobId, primaryIdx] of firstIndexForJob.entries()) {
          const item = items[primaryIdx]!;
          const status = jobStatusMap.get(jobId);

          if (!status) {
            results[primaryIdx] = {
              jobId: item.jobId,
              workerId: item.workerId,
              status: 'NOT_CLAIMABLE',
              reason: 'JOB_NOT_FOUND',
              details: `Job "${item.jobId}" does not exist.`,
            };
            continue;
          }

          if (status !== 'QUEUED') {
            results[primaryIdx] = {
              jobId: item.jobId,
              workerId: item.workerId,
              status: 'NOT_CLAIMABLE',
              reason: 'JOB_NOT_CLAIMABLE',
              details: `Job "${item.jobId}" is in status "${status}", expected "QUEUED".`,
            };
            continue;
          }

          const currentLease = activeLeaseMap.get(jobId);
          if (currentLease) {
            if (!currentLease.is_expired) {
              if (currentLease.worker_id === item.workerId) {
                results[primaryIdx] = {
                  jobId: item.jobId,
                  workerId: item.workerId,
                  status: 'ACQUIRED',
                  lease: this.mapRow(currentLease),
                  isIdempotent: true,
                };
              } else {
                results[primaryIdx] = {
                  jobId: item.jobId,
                  workerId: item.workerId,
                  status: 'CONFLICT',
                  reason: 'LEASE_ALREADY_HELD',
                  currentOwnerId: currentLease.worker_id,
                  expiresAt: new Date(currentLease.expires_at),
                };
              }
              continue;
            }

            // Existing lease is expired - mark for batch update
            expiredLeaseIdsToUpdate.push(currentLease.id);
          }

          const durationMs = item.durationMs ?? options.defaultDurationMs ?? 30000;
          leasesToInsert.push({
            id: `lease_${randomUUID()}`,
            jobId: item.jobId,
            workerId: item.workerId,
            durationMs,
            primaryIndex: primaryIdx,
          });
        }

        // Expire leases in bulk
        if (expiredLeaseIdsToUpdate.length > 0) {
          await txClient.query(
            `UPDATE worker_leases SET status = 'EXPIRED' WHERE id = ANY($1::text[]);`,
            [expiredLeaseIdsToUpdate],
          );
        }

        // Insert new leases in bulk
        if (leasesToInsert.length > 0) {
          const ids = leasesToInsert.map((l) => l.id);
          const jobIds = leasesToInsert.map((l) => l.jobId);
          const workerIds = leasesToInsert.map((l) => l.workerId);
          const durations = leasesToInsert.map((l) => l.durationMs);

          const insertRes = await txClient.query<WorkerLeaseRow>(
            `
            INSERT INTO worker_leases (
              id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at
            )
            SELECT
              v.id,
              v.job_id,
              v.worker_id,
              'ACTIVE',
              v.duration_ms,
              NOW(),
              NOW(),
              NOW() + (v.duration_ms * INTERVAL '1 millisecond'),
              NOW()
            FROM (
              SELECT
                unnest($1::text[]) AS id,
                unnest($2::text[]) AS job_id,
                unnest($3::text[]) AS worker_id,
                unnest($4::int[]) AS duration_ms
            ) AS v
            RETURNING id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at;
            `,
            [ids, jobIds, workerIds, durations],
          );

          const insertedByJobId = new Map<string, WorkerLease>();
          for (const row of insertRes.rows) {
            insertedByJobId.set(row.job_id, this.mapRow(row));
          }

          for (const insertItem of leasesToInsert) {
            const lease = insertedByJobId.get(insertItem.jobId);
            if (lease) {
              results[insertItem.primaryIndex] = {
                jobId: insertItem.jobId,
                workerId: insertItem.workerId,
                status: 'ACQUIRED',
                lease,
                isIdempotent: false,
              };
            }
          }
        }

        // Resolve intra-batch duplicate items
        for (let i = 0; i < items.length; i++) {
          if (results[i] !== null) continue;
          const item = items[i]!;
          const primaryIdx = firstIndexForJob.get(item.jobId)!;
          const primaryResult = results[primaryIdx]!;

          if (primaryResult.status === 'ACQUIRED') {
            if (primaryResult.workerId === item.workerId) {
              results[i] = {
                jobId: item.jobId,
                workerId: item.workerId,
                status: 'ACQUIRED',
                lease: primaryResult.lease,
                isIdempotent: true,
              };
            } else {
              results[i] = {
                jobId: item.jobId,
                workerId: item.workerId,
                status: 'CONFLICT',
                reason: 'LEASE_ALREADY_HELD',
                currentOwnerId: primaryResult.workerId,
                expiresAt: primaryResult.lease.expiresAt,
              };
            }
          } else {
            results[i] = {
              jobId: item.jobId,
              workerId: item.workerId,
              status: 'NOT_CLAIMABLE',
              reason: 'DUPLICATE_IN_BATCH',
              details: `Duplicate request in batch for job "${item.jobId}" where primary attempt resulted in ${primaryResult.status}.`,
            };
          }
        }

        const finalResults = results as BatchClaimItemResult[];
        let acquiredCount = 0;
        let conflictCount = 0;
        let notClaimableCount = 0;

        for (const res of finalResults) {
          if (res.status === 'ACQUIRED') acquiredCount++;
          else if (res.status === 'CONFLICT') conflictCount++;
          else if (res.status === 'NOT_CLAIMABLE') notClaimableCount++;
        }

        return {
          results: finalResults,
          acquiredCount,
          conflictCount,
          notClaimableCount,
        };
      });
    } catch (err) {
      throw new PersistenceError(
        `Failed to execute batch lease claim: ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async renew(options: RenewLeaseOptions): Promise<RenewLeaseResult> {
    try {
      const res = await this.client.query<WorkerLeaseRow>(
        `
        UPDATE worker_leases
        SET renewed_at = NOW(),
            expires_at = NOW() + (COALESCE($4, duration_ms) * INTERVAL '1 millisecond'),
            duration_ms = COALESCE($4, duration_ms)
        WHERE id = $1
          AND job_id = $2
          AND worker_id = $3
          AND status = 'ACTIVE'
          AND expires_at > NOW()
        RETURNING id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at;
        `,
        [options.leaseId, options.jobId, options.workerId, options.durationMs ?? null],
      );

      if (res.rows.length > 0) {
        return {
          status: 'RENEWED',
          lease: this.mapRow(res.rows[0]!),
        };
      }

      // Query to determine rejection reason
      const inspectRes = await this.client.query<LeaseCheckRow>(
        `
        SELECT id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at,
               (expires_at <= NOW()) AS is_expired
        FROM worker_leases
        WHERE id = $1;
        `,
        [options.leaseId],
      );

      if (inspectRes.rows.length === 0) {
        return {
          status: 'REJECTED',
          reason: 'LEASE_NOT_FOUND',
          details: `Lease "${options.leaseId}" does not exist.`,
        };
      }

      const existing = inspectRes.rows[0]!;
      if (existing.job_id !== options.jobId) {
        return {
          status: 'REJECTED',
          reason: 'LEASE_NOT_FOUND',
          details: `Lease "${options.leaseId}" is for job "${existing.job_id}", not "${options.jobId}".`,
        };
      }

      if (existing.worker_id !== options.workerId) {
        return {
          status: 'REJECTED',
          reason: 'LEASE_OWNER_MISMATCH',
          details: `Lease owner mismatch: held by "${existing.worker_id}", attempted by "${options.workerId}".`,
        };
      }

      if (existing.status !== 'ACTIVE' || existing.is_expired) {
        return {
          status: 'REJECTED',
          reason: 'LEASE_EXPIRED',
          details: `Lease "${options.leaseId}" has expired or is no longer active (status: ${existing.status}).`,
        };
      }

      return {
        status: 'REJECTED',
        reason: 'LEASE_NOT_FOUND',
        details: 'Lease could not be renewed.',
      };
    } catch (err) {
      throw new PersistenceError(
        `Failed to renew lease "${options.leaseId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async release(options: ReleaseLeaseOptions): Promise<ReleaseLeaseResult> {
    try {
      const res = await this.client.query<WorkerLeaseRow>(
        `
        UPDATE worker_leases
        SET status = 'RELEASED'
        WHERE id = $1
          AND job_id = $2
          AND worker_id = $3
          AND status = 'ACTIVE'
        RETURNING id, job_id, worker_id;
        `,
        [options.leaseId, options.jobId, options.workerId],
      );

      if (res.rows.length > 0) {
        return {
          status: 'RELEASED',
          leaseId: options.leaseId,
          jobId: options.jobId,
        };
      }

      // Check reason for release rejection
      const inspectRes = await this.client.query<WorkerLeaseRow>(
        `SELECT id, job_id, worker_id, status FROM worker_leases WHERE id = $1;`,
        [options.leaseId],
      );

      if (inspectRes.rows.length === 0) {
        return {
          status: 'REJECTED',
          reason: 'LEASE_NOT_FOUND',
          details: `Lease "${options.leaseId}" does not exist.`,
        };
      }

      const existing = inspectRes.rows[0]!;
      if (existing.job_id !== options.jobId) {
        return {
          status: 'REJECTED',
          reason: 'LEASE_NOT_FOUND',
          details: `Lease "${options.leaseId}" is for job "${existing.job_id}", not "${options.jobId}".`,
        };
      }

      if (existing.worker_id !== options.workerId) {
        return {
          status: 'REJECTED',
          reason: 'LEASE_OWNER_MISMATCH',
          details: `Lease owner mismatch: held by "${existing.worker_id}", attempted by "${options.workerId}".`,
        };
      }

      if (existing.status !== 'ACTIVE') {
        return {
          status: 'REJECTED',
          reason: 'LEASE_ALREADY_INACTIVE',
          details: `Lease "${options.leaseId}" is already inactive (${existing.status}).`,
        };
      }

      return {
        status: 'REJECTED',
        reason: 'LEASE_NOT_FOUND',
        details: 'Lease could not be released.',
      };
    } catch (err) {
      throw new PersistenceError(
        `Failed to release lease "${options.leaseId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findActiveByJobId(jobId: string): Promise<WorkerLease | null> {
    try {
      const res = await this.client.query<WorkerLeaseRow>(
        `
        SELECT id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at
        FROM worker_leases
        WHERE job_id = $1
          AND status = 'ACTIVE'
          AND expires_at > NOW();
        `,
        [jobId],
      );

      if (res.rows.length === 0) {
        return null;
      }

      return this.mapRow(res.rows[0]!);
    } catch (err) {
      throw new PersistenceError(
        `Failed to find active lease for job "${jobId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findById(leaseId: string): Promise<WorkerLease | null> {
    try {
      const res = await this.client.query<WorkerLeaseRow>(
        `
        SELECT id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at
        FROM worker_leases
        WHERE id = $1;
        `,
        [leaseId],
      );

      if (res.rows.length === 0) {
        return null;
      }

      return this.mapRow(res.rows[0]!);
    } catch (err) {
      throw new PersistenceError(
        `Failed to find lease "${leaseId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findByWorkerId(
    workerId: string,
    filter?: { status?: JobLeaseStatus },
  ): Promise<WorkerLease[]> {
    try {
      let query = `
        SELECT id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at
        FROM worker_leases
        WHERE worker_id = $1
      `;
      const params: unknown[] = [workerId];

      if (filter?.status) {
        params.push(filter.status);
        query += ` AND status = $${params.length}`;
      }

      query += ' ORDER BY created_at DESC;';

      const res = await this.client.query<WorkerLeaseRow>(query, params);
      return res.rows.map((row) => this.mapRow(row));
    } catch (err) {
      throw new PersistenceError(
        `Failed to find leases for worker "${workerId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async reclaimExpiredLeases(): Promise<number> {
    try {
      const res = await this.client.query(
        `
        UPDATE worker_leases
        SET status = 'EXPIRED'
        WHERE status = 'ACTIVE'
          AND expires_at <= NOW();
        `,
      );

      return res.rowCount ?? 0;
    } catch (err) {
      throw new PersistenceError(
        `Failed to reclaim expired leases: ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  private mapRow(row: WorkerLeaseRow): WorkerLease {
    return {
      id: row.id,
      jobId: row.job_id,
      workerId: row.worker_id,
      status: row.status as JobLeaseStatus,
      durationMs: Number(row.duration_ms),
      acquiredAt: new Date(row.acquired_at),
      renewedAt: new Date(row.renewed_at),
      expiresAt: new Date(row.expires_at),
      createdAt: new Date(row.created_at),
    };
  }

  private async withTransactionClient<T>(fn: (client: DatabaseClient) => Promise<T>): Promise<T> {
    const isPool =
      'connect' in this.client &&
      typeof (this.client as { connect?: unknown }).connect === 'function';

    const client: DatabaseClient = isPool
      ? await (this.client as { connect: () => Promise<DatabaseClient> }).connect()
      : this.client;
    const isDedicatedClient = isPool;

    try {
      if (isDedicatedClient) {
        await client.query('BEGIN');
      } else {
        await client.query('SAVEPOINT claim_sp');
      }

      const result = await fn(client);

      if (isDedicatedClient) {
        await client.query('COMMIT');
      } else {
        await client.query('RELEASE SAVEPOINT claim_sp');
      }

      return result;
    } catch (err) {
      if (isDedicatedClient) {
        try {
          await client.query('ROLLBACK');
        } catch {
          // ignore rollback failure
        }
      } else {
        try {
          await client.query('ROLLBACK TO SAVEPOINT claim_sp');
        } catch {
          // ignore savepoint rollback failure
        }
      }
      throw err;
    } finally {
      if (
        isDedicatedClient &&
        'release' in client &&
        typeof (client as { release?: unknown }).release === 'function'
      ) {
        (client as { release: () => void }).release();
      }
    }
  }
}
