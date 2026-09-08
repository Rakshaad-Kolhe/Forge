import type { DeadLetterJob, DeadLetterReason } from '@forge/contracts';

/**
 * Filter options when querying dead-lettered jobs.
 */
export interface DeadLetterFilter {
  readonly reason?: DeadLetterReason;
  readonly limit?: number;
  readonly offset?: number;
}

/**
 * Authoritative repository contract for managing Dead-Letter Queue (DLQ) records in PostgreSQL.
 */
export interface DeadLetterRepository {
  /**
   * Persists a dead-letter job record.
   * Idempotent: if a record for the job already exists, preserves or updates it safely.
   */
  save(entry: DeadLetterJob): Promise<void>;

  /**
   * Finds a dead-letter record by its unique job ID.
   */
  findByJobId(jobId: string): Promise<DeadLetterJob | null>;

  /**
   * Finds all dead-letter records belonging to a pipeline run.
   */
  findByPipelineRunId(pipelineRunId: string): Promise<DeadLetterJob[]>;

  /**
   * Lists dead-letter records matching optional criteria in reverse chronological order.
   */
  list(filter?: DeadLetterFilter): Promise<DeadLetterJob[]>;

  /**
   * Returns the total count of dead-letter records in the database.
   */
  count(): Promise<number>;
}
