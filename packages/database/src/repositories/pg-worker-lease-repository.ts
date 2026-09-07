import { randomUUID } from 'node:crypto';
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
      return await this.withTransactionClient(async (txClient) => {
        // 1. Lock job row to serialize concurrent claims
        const jobRes = await txClient.query<JobLockRow>(
          'SELECT id, status FROM jobs WHERE id = $1 FOR UPDATE;',
          [options.jobId],
        );

        if (jobRes.rows.length === 0) {
          return {
            status: 'NOT_CLAIMABLE',
            reason: 'JOB_NOT_FOUND',
            details: `Job "${options.jobId}" does not exist.`,
          };
        }

        const job = jobRes.rows[0]!;
        if (job.status !== 'QUEUED') {
          return {
            status: 'NOT_CLAIMABLE',
            reason: 'JOB_NOT_CLAIMABLE',
            details: `Job "${options.jobId}" is in status "${job.status}", expected "QUEUED".`,
          };
        }

        // 2. Check for an existing ACTIVE lease
        const leaseRes = await txClient.query<LeaseCheckRow>(
          `
          SELECT id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at,
                 (expires_at <= NOW()) AS is_expired
          FROM worker_leases
          WHERE job_id = $1 AND status = 'ACTIVE'
          FOR UPDATE;
          `,
          [options.jobId],
        );

        if (leaseRes.rows.length > 0) {
          const currentLease = leaseRes.rows[0]!;

          // If lease is still valid and unexpired
          if (!currentLease.is_expired) {
            if (currentLease.worker_id === options.workerId) {
              // Idempotent re-claim by the same worker
              return {
                status: 'ACQUIRED',
                lease: this.mapRow(currentLease),
                isIdempotent: true,
              };
            }

            // Held by another worker
            return {
              status: 'CONFLICT',
              reason: 'LEASE_ALREADY_HELD',
              currentOwnerId: currentLease.worker_id,
              expiresAt: new Date(currentLease.expires_at),
            };
          }

          // Lease is expired - mark it EXPIRED so the new lease can be acquired
          await txClient.query(`UPDATE worker_leases SET status = 'EXPIRED' WHERE id = $1;`, [
            currentLease.id,
          ]);
        }

        // 3. Create new ACTIVE lease
        const leaseId = `lease_${randomUUID()}`;
        const insertRes = await txClient.query<WorkerLeaseRow>(
          `
          INSERT INTO worker_leases (
            id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at
          )
          VALUES (
            $1, $2, $3, 'ACTIVE', $4, NOW(), NOW(), NOW() + ($5 * INTERVAL '1 millisecond'), NOW()
          )
          RETURNING id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at;
          `,
          [leaseId, options.jobId, options.workerId, options.durationMs, options.durationMs],
        );

        return {
          status: 'ACQUIRED',
          lease: this.mapRow(insertRes.rows[0]!),
          isIdempotent: false,
        };
      });
    } catch (err) {
      const pgErr = err as { code?: string; message?: string };
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

      throw new PersistenceError(
        `Failed to claim job lease for job "${options.jobId}": ${(err as Error).message}`,
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
