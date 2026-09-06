import {
  createJobAttemptId,
  createJobId,
  JobAttempt,
  type JobAttemptId,
  type JobAttemptStatus,
  type JobId,
} from '@forge/pipeline';
import { ConstraintViolationError, PersistenceError } from '../errors.js';
import type { DatabaseClient, JobAttemptRow } from '../types.js';
import type { JobAttemptRepository } from './contracts/job-attempt-repository.contract.js';

const VALID_ATTEMPT_STATUSES = new Set<string>([
  'PENDING',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'CANCELLED',
  'TIMED_OUT',
]);

export class PgJobAttemptRepository implements JobAttemptRepository {
  constructor(private readonly client: DatabaseClient) {}

  public async save(attempt: JobAttempt): Promise<void> {
    try {
      await this.client.query(
        `
        INSERT INTO job_attempts (
          id, job_id, attempt_number, status, started_at, finished_at, exit_code, failure_reason, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (job_id, attempt_number) DO UPDATE
        SET status = EXCLUDED.status,
            started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at,
            exit_code = EXCLUDED.exit_code,
            failure_reason = EXCLUDED.failure_reason;
      `,
        [
          attempt.id,
          attempt.jobId,
          attempt.attemptNumber,
          attempt.status,
          attempt.startedAt ? new Date(attempt.startedAt) : null,
          attempt.finishedAt ? new Date(attempt.finishedAt) : null,
          attempt.exitCode ?? null,
          attempt.failureReason ?? null,
        ],
      );
    } catch (err: unknown) {
      const dbErr = err as { code?: string; constraint?: string; detail?: string };
      if (dbErr?.code === '23505') {
        throw new ConstraintViolationError(
          `Unique constraint violation for job attempt "${attempt.id}": ${dbErr.detail ?? ''}`,
          dbErr?.constraint,
          dbErr?.detail,
        );
      }
      if (dbErr?.code === '23503') {
        throw new ConstraintViolationError(
          `Job "${attempt.jobId}" does not exist for attempt "${attempt.id}"`,
          dbErr?.constraint,
          dbErr?.detail,
        );
      }
      throw new PersistenceError(
        `Failed to save job attempt "${attempt.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findById(id: JobAttemptId): Promise<JobAttempt | null> {
    try {
      const res = await this.client.query<JobAttemptRow>(
        `
        SELECT id, job_id, attempt_number, status, started_at, finished_at, exit_code, failure_reason, created_at
        FROM job_attempts
        WHERE id = $1;
      `,
        [id],
      );

      const row = res.rows[0];
      if (!row) {
        return null;
      }

      return this.mapRowToDomain(row);
    } catch (err) {
      if (err instanceof PersistenceError) throw err;
      throw new PersistenceError(
        `Failed to find job attempt "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findByJobId(jobId: JobId): Promise<JobAttempt[]> {
    try {
      const res = await this.client.query<JobAttemptRow>(
        `
        SELECT id, job_id, attempt_number, status, started_at, finished_at, exit_code, failure_reason, created_at
        FROM job_attempts
        WHERE job_id = $1
        ORDER BY attempt_number ASC;
      `,
        [jobId],
      );

      return res.rows.map((row) => this.mapRowToDomain(row));
    } catch (err) {
      if (err instanceof PersistenceError) throw err;
      throw new PersistenceError(
        `Failed to find attempts for job "${jobId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  private mapRowToDomain(row: JobAttemptRow): JobAttempt {
    if (!VALID_ATTEMPT_STATUSES.has(row.status)) {
      throw new PersistenceError(
        `Invalid JobAttemptStatus "${row.status}" encountered in persistent row "${row.id}"`,
      );
    }

    try {
      return new JobAttempt({
        id: createJobAttemptId(row.id),
        jobId: createJobId(row.job_id),
        attemptNumber: row.attempt_number,
        initialStatus: row.status as JobAttemptStatus,
        startedAt: row.started_at ? row.started_at.toISOString() : undefined,
        finishedAt: row.finished_at ? row.finished_at.toISOString() : undefined,
        exitCode: row.exit_code !== null ? row.exit_code : undefined,
        failureReason: row.failure_reason !== null ? row.failure_reason : undefined,
      });
    } catch (err) {
      throw new PersistenceError(
        `Failed to reconstruct JobAttempt domain model for ID "${row.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }
}
