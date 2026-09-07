import type { Job, JobId, PipelineRunId } from '@forge/pipeline';

/**
 * Repository interface for Job aggregates.
 */
export interface JobRepository {
  /**
   * Persists a new job or updates an existing one.
   * Enforces domain state machine transition rules and terminal state immutability.
   * If the job contains attempts, persists them as well.
   */
  save(job: Job): Promise<void>;

  /**
   * Finds a job by its unique ID, reconstructing constituent execution attempts.
   */
  findById(id: JobId): Promise<Job | null>;

  /**
   * Finds all jobs for a given pipeline run ordered by creation.
   */
  findByPipelineRunId(pipelineRunId: PipelineRunId): Promise<Job[]>;

  /**
   * Finds jobs in QUEUED status whose retry backoff (if any) has matured (next_attempt_at IS NULL OR next_attempt_at <= now).
   * Ordered by priority descending, then creation time ascending.
   */
  findSchedulableJobs(options?: { now?: Date; limit?: number }): Promise<Job[]>;
}
