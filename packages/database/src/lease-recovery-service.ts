import crypto from 'node:crypto';
import type {
  DeadLetterJob,
  DeadLetterReason,
  LeaseRecoveryOptions,
  RecoveredLeaseRecord,
  RecoverExpiredLeasesResult,
} from '@forge/contracts';
import { createJobId, evaluateRetry } from '@forge/pipeline';
import { withTransaction } from './transaction.js';
import type { DatabasePool } from './types.js';

export interface RecoveryLogger {
  info(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, context?: Record<string, unknown>): void;
  debug?(message: string, context?: Record<string, unknown>): void;
}

export class LeaseRecoveryService {
  constructor(
    private readonly pool: DatabasePool,
    private readonly logger?: RecoveryLogger,
  ) {}

  /**
   * Sweeps and recovers all expired active worker leases.
   *
   * Idempotent and concurrency-safe:
   * - Uses PostgreSQL row locking (`FOR UPDATE SKIP LOCKED`) to prevent race conditions.
   * - If multiple workers or schedulers recover concurrently, exactly one will process
   *   each expired lease; the other will receive a harmless NO_OP.
   * - Never resurrects terminal jobs (SUCCEEDED, FAILED, CANCELLED, TIMED_OUT).
   * - Transitions retryable jobs to QUEUED with calculated backoff nextAttemptAt.
   * - Transitions exhausted/non-retryable jobs to FAILED and inserts a durable DLQ record.
   */
  public async recoverExpiredLeases(
    options?: LeaseRecoveryOptions,
  ): Promise<RecoverExpiredLeasesResult> {
    const batchSize = Math.max(1, Math.min(100, options?.batchSize ?? 10));
    const now = options?.now ?? new Date();

    // 1. Identify candidate expired active leases
    const candidateClient = await this.pool.connect();
    let candidateLeases: { id: string; job_id: string; worker_id: string }[] = [];
    try {
      const res = await candidateClient.query<{ id: string; job_id: string; worker_id: string }>(
        `
        SELECT id, job_id, worker_id
        FROM worker_leases
        WHERE status = 'ACTIVE'
          AND expires_at <= $1
        ORDER BY expires_at ASC
        LIMIT $2;
        `,
        [now, batchSize],
      );
      candidateLeases = res.rows;
    } finally {
      candidateClient.release();
    }

    if (candidateLeases.length === 0) {
      return {
        recoveredCount: 0,
        details: Object.freeze([]),
      };
    }

    const details: RecoveredLeaseRecord[] = [];
    let recoveredCount = 0;

    // 2. Process each candidate lease in an isolated ACID transaction
    for (const candidate of candidateLeases) {
      const record = await this.recoverSingleLease(candidate.id, now);
      details.push(record);
      if (record.action === 'REQUEUED' || record.action === 'DEAD_LETTERED') {
        recoveredCount++;
      }
    }

    return {
      recoveredCount,
      details: Object.freeze(details),
    };
  }

  /**
   * Atomically recovers a single expired lease within a managed transaction.
   */
  public async recoverSingleLease(leaseId: string, now: Date): Promise<RecoveredLeaseRecord> {
    return withTransaction(this.pool, async (tx) => {
      // Step A: Lock and verify lease row
      const leaseRes = await tx.client.query<{
        id: string;
        job_id: string;
        worker_id: string;
        status: string;
        expires_at: Date;
      }>(
        `
        SELECT id, job_id, worker_id, status, expires_at
        FROM worker_leases
        WHERE id = $1
          AND status = 'ACTIVE'
        FOR UPDATE SKIP LOCKED;
        `,
        [leaseId],
      );

      if (leaseRes.rows.length === 0) {
        return {
          leaseId,
          jobId: 'unknown',
          workerId: 'unknown',
          action: 'NO_OP',
          details: 'Lease already expired, released, renewed, or locked by concurrent recovery',
        };
      }

      const leaseRow = leaseRes.rows[0]!;
      const jobId = leaseRow.job_id;
      const workerId = leaseRow.worker_id;

      // Double-check expiry against DB now
      if (new Date(leaseRow.expires_at) > now) {
        return {
          leaseId,
          jobId,
          workerId,
          action: 'NO_OP',
          details: 'Lease is not expired at specified timestamp',
        };
      }

      // Step B: Atomically mark lease as EXPIRED
      await tx.client.query(
        `
        UPDATE worker_leases
        SET status = 'EXPIRED'
        WHERE id = $1 AND status = 'ACTIVE';
        `,
        [leaseId],
      );

      this.logger?.info('Marked expired lease as EXPIRED', {
        leaseId,
        jobId,
        workerId,
      });

      // Step C: Lock and fetch Job
      const job = await tx.jobs.findById(createJobId(jobId));
      if (!job) {
        this.logger?.warn('Orphaned lease reference: job not found during recovery', {
          leaseId,
          jobId,
        });
        return {
          leaseId,
          jobId,
          workerId,
          action: 'NO_OP',
          details: `Referenced job "${jobId}" does not exist`,
        };
      }

      // Step D: Check if Job is already terminal. Never resurrect terminal jobs!
      if (job.isTerminal()) {
        this.logger?.info('Job is already terminal, skipping recovery resurrection', {
          leaseId,
          jobId,
          status: job.status,
        });
        return {
          leaseId,
          jobId,
          workerId,
          action: 'SKIPPED_TERMINAL',
          details: `Job is already in terminal state "${job.status}"`,
        };
      }

      // Step E: Reconcile current attempt if one was active
      const attempts = job.attempts;
      const latestAttempt = job.currentAttempt;

      if (latestAttempt && !latestAttempt.isTerminal()) {
        latestAttempt.fail(1, 'WORKER_LOST', now.toISOString());
        await tx.jobAttempts.save(latestAttempt);
        this.logger?.info('Reconciled active attempt as failed due to worker loss', {
          leaseId,
          jobId,
          attemptId: latestAttempt.id,
          attemptNumber: latestAttempt.attemptNumber,
        });
      }

      // Step F: Evaluate retry policy
      if (!latestAttempt) {
        // No attempt had actually started before the worker crashed/lost lease.
        // Job returns to QUEUED for another worker to pick up attempt 1.
        job.markQueued();
        job.clearNextAttemptAt();
        await tx.jobs.save(job);

        this.logger?.info('Job requeued with zero prior attempts after lease expiry', {
          leaseId,
          jobId,
        });

        return {
          leaseId,
          jobId,
          workerId,
          action: 'REQUEUED',
          details: 'Requeued cleanly without prior attempt',
        };
      }

      const retryDecision = evaluateRetry(latestAttempt, job.retryPolicy);

      if (retryDecision.action === 'RETRY') {
        const nextAttemptAt = new Date(now.getTime() + retryDecision.delayMs);
        job.markQueued();
        job.setNextAttemptAt(nextAttemptAt);
        await tx.jobs.save(job);

        this.logger?.info('Job requeued with backoff following worker loss', {
          leaseId,
          jobId,
          nextAttemptNumber: retryDecision.nextAttemptNumber,
          delayMs: retryDecision.delayMs,
          nextAttemptAt: nextAttemptAt.toISOString(),
        });

        return {
          leaseId,
          jobId,
          workerId,
          action: 'REQUEUED',
          nextAttemptAt,
          details: retryDecision.reason,
        };
      }

      // Step G: Retry exhausted or non-retryable -> Job becomes FAILED and enters DLQ
      job.fail();
      job.clearNextAttemptAt();
      await tx.jobs.save(job);

      let deadLetterReason: DeadLetterReason = 'NON_RETRYABLE_FAILURE';
      if (retryDecision.action === 'FINAL_FAILURE') {
        if (retryDecision.reason === 'MAX_ATTEMPTS_EXHAUSTED') {
          deadLetterReason = 'WORKER_LOSS_RETRY_EXHAUSTED';
        }
      }

      const dlqEntry: DeadLetterJob = {
        id: crypto.randomUUID(),
        jobId: job.id,
        pipelineRunId: job.pipelineRunId,
        reason: deadLetterReason,
        failedAttemptCount: attempts.length,
        lastAttemptId: latestAttempt.id,
        lastWorkerId: workerId,
        errorDetails: `Worker lease expired (worker loss). Retry decision: ${retryDecision.details}`,
        metadata: {
          leaseId,
          workerId,
          retryDecision,
        },
        createdAt: now,
      };

      await tx.deadLetterJobs.save(dlqEntry);

      // Step H: Update parent pipeline run completion if terminal
      try {
        const pipelineRun = await tx.pipelineRuns.findById(job.pipelineRunId);
        if (pipelineRun && pipelineRun.status === 'RUNNING' && !pipelineRun.isTerminal()) {
          pipelineRun.evaluateCompletion();
          if (pipelineRun.isTerminal()) {
            await tx.pipelineRuns.save(pipelineRun);
          }
        }
      } catch (err: unknown) {
        this.logger?.warn('Could not update parent pipeline run completion during recovery', {
          pipelineRunId: job.pipelineRunId,
          error: err instanceof Error ? err.message : String(err),
        });
      }

      this.logger?.warn('Job dead-lettered after worker loss and retry exhaustion', {
        leaseId,
        jobId,
        pipelineRunId: job.pipelineRunId,
        reason: deadLetterReason,
        attempts: attempts.length,
      });

      return {
        leaseId,
        jobId,
        workerId,
        action: 'DEAD_LETTERED',
        deadLetterReason,
        details: retryDecision.details,
      };
    });
  }
}
