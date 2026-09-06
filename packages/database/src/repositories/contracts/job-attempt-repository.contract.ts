import type { JobAttempt, JobAttemptId, JobId } from '@forge/pipeline';

/**
 * Repository interface for JobAttempt entities.
 */
export interface JobAttemptRepository {
  /**
   * Persists a new job attempt or updates an existing attempt.
   */
  save(attempt: JobAttempt): Promise<void>;

  /**
   * Finds a specific job attempt by its unique ID.
   */
  findById(id: JobAttemptId): Promise<JobAttempt | null>;

  /**
   * Finds all attempts for a given job ordered chronologically by attempt number.
   */
  findByJobId(jobId: JobId): Promise<JobAttempt[]>;
}
