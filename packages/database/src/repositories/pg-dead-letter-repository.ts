import type { DeadLetterJob, DeadLetterReason } from '@forge/contracts';
import { PersistenceError } from '../errors.js';
import type { DatabaseClient, DeadLetterJobRow } from '../types.js';
import type {
  DeadLetterFilter,
  DeadLetterRepository,
} from './contracts/dead-letter-repository.contract.js';

export class PgDeadLetterRepository implements DeadLetterRepository {
  constructor(private readonly client: DatabaseClient) {}

  public async save(entry: DeadLetterJob): Promise<void> {
    try {
      await this.client.query(
        `
        INSERT INTO dead_letter_jobs (
          id, job_id, pipeline_run_id, reason, failed_attempt_count,
          last_attempt_id, last_worker_id, error_details, metadata, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (job_id) DO UPDATE
        SET reason = EXCLUDED.reason,
            failed_attempt_count = EXCLUDED.failed_attempt_count,
            last_attempt_id = EXCLUDED.last_attempt_id,
            last_worker_id = EXCLUDED.last_worker_id,
            error_details = EXCLUDED.error_details,
            metadata = EXCLUDED.metadata;
        `,
        [
          entry.id,
          entry.jobId,
          entry.pipelineRunId,
          entry.reason,
          entry.failedAttemptCount,
          entry.lastAttemptId ?? null,
          entry.lastWorkerId ?? null,
          entry.errorDetails ?? null,
          entry.metadata ? JSON.stringify(entry.metadata) : null,
          entry.createdAt,
        ],
      );
    } catch (err) {
      throw new PersistenceError(
        `Failed to save dead-letter record for job "${entry.jobId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findByJobId(jobId: string): Promise<DeadLetterJob | null> {
    try {
      const res = await this.client.query<DeadLetterJobRow>(
        `
        SELECT id, job_id, pipeline_run_id, reason, failed_attempt_count,
               last_attempt_id, last_worker_id, error_details, metadata, created_at
        FROM dead_letter_jobs
        WHERE job_id = $1;
        `,
        [jobId],
      );

      if (res.rows.length === 0) {
        return null;
      }

      return this.mapRow(res.rows[0]!);
    } catch (err) {
      throw new PersistenceError(
        `Failed to find dead-letter record for job "${jobId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findByPipelineRunId(pipelineRunId: string): Promise<DeadLetterJob[]> {
    try {
      const res = await this.client.query<DeadLetterJobRow>(
        `
        SELECT id, job_id, pipeline_run_id, reason, failed_attempt_count,
               last_attempt_id, last_worker_id, error_details, metadata, created_at
        FROM dead_letter_jobs
        WHERE pipeline_run_id = $1
        ORDER BY created_at DESC;
        `,
        [pipelineRunId],
      );

      return res.rows.map((row) => this.mapRow(row));
    } catch (err) {
      throw new PersistenceError(
        `Failed to find dead-letter records for pipeline run "${pipelineRunId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async list(filter?: DeadLetterFilter): Promise<DeadLetterJob[]> {
    try {
      let query = `
        SELECT id, job_id, pipeline_run_id, reason, failed_attempt_count,
               last_attempt_id, last_worker_id, error_details, metadata, created_at
        FROM dead_letter_jobs
      `;
      const params: unknown[] = [];

      if (filter?.reason) {
        params.push(filter.reason);
        query += ` WHERE reason = $${params.length}`;
      }

      query += ' ORDER BY created_at DESC';

      if (filter?.limit !== undefined) {
        params.push(filter.limit);
        query += ` LIMIT $${params.length}`;
      }

      if (filter?.offset !== undefined) {
        params.push(filter.offset);
        query += ` OFFSET $${params.length}`;
      }

      query += ';';

      const res = await this.client.query<DeadLetterJobRow>(query, params);
      return res.rows.map((row) => this.mapRow(row));
    } catch (err) {
      throw new PersistenceError(
        `Failed to list dead-letter records: ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async count(): Promise<number> {
    try {
      const res = await this.client.query<{ count: string }>(
        'SELECT COUNT(*) as count FROM dead_letter_jobs;',
      );
      return parseInt(res.rows[0]?.count ?? '0', 10);
    } catch (err) {
      throw new PersistenceError(
        `Failed to count dead-letter records: ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  private mapRow(row: DeadLetterJobRow): DeadLetterJob {
    let metadata: Record<string, unknown> | undefined;
    if (row.metadata) {
      metadata =
        typeof row.metadata === 'string'
          ? JSON.parse(row.metadata)
          : (row.metadata as Record<string, unknown>);
    }

    return {
      id: row.id,
      jobId: row.job_id,
      pipelineRunId: row.pipeline_run_id,
      reason: row.reason as DeadLetterReason,
      failedAttemptCount: Number(row.failed_attempt_count),
      lastAttemptId: row.last_attempt_id ?? undefined,
      lastWorkerId: row.last_worker_id ?? undefined,
      errorDetails: row.error_details ?? undefined,
      metadata: metadata ? Object.freeze(metadata) : undefined,
      createdAt: new Date(row.created_at),
    };
  }
}
