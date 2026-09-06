import {
  createJobId,
  createPipelineRunId,
  Job,
  type JobId,
  type JobStatus,
  type PipelineRunId,
} from '@forge/pipeline';
import { ConstraintViolationError, EntityNotFoundError, PersistenceError } from '../errors.js';
import type { DatabaseClient, JobRow } from '../types.js';
import type { JobRepository } from './contracts/job-repository.contract.js';
import { PgJobAttemptRepository } from './pg-job-attempt-repository.js';

const VALID_JOB_STATUSES = new Set<string>([
  'PENDING',
  'QUEUED',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'CANCELLED',
  'TIMED_OUT',
]);

export class PgJobRepository implements JobRepository {
  private readonly attemptRepo: PgJobAttemptRepository;

  constructor(private readonly client: DatabaseClient) {
    this.attemptRepo = new PgJobAttemptRepository(client);
  }

  public async save(job: Job): Promise<void> {
    const dependsOnJson = JSON.stringify([...job.dependsOn]);

    try {
      await this.client.query(
        `
        INSERT INTO jobs (id, pipeline_run_id, step_name, command, depends_on, status, created_at)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, NOW())
        ON CONFLICT (id) DO UPDATE
        SET status = EXCLUDED.status;
      `,
        [job.id, job.pipelineRunId, job.stepName, job.command, dependsOnJson, job.status],
      );

      // Persist any constituent attempts
      for (const attempt of job.attempts) {
        await this.attemptRepo.save(attempt);
      }
    } catch (err: unknown) {
      if (err instanceof ConstraintViolationError || err instanceof PersistenceError) {
        throw err;
      }
      const dbErr = err as { code?: string; constraint?: string; detail?: string };
      if (dbErr?.code === '23505') {
        throw new ConstraintViolationError(
          `Unique constraint violation for job "${job.id}": ${dbErr.detail ?? ''}`,
          dbErr?.constraint,
          dbErr?.detail,
        );
      }
      if (dbErr?.code === '23503') {
        throw new ConstraintViolationError(
          `Pipeline run "${job.pipelineRunId}" does not exist for job "${job.id}"`,
          dbErr?.constraint,
          dbErr?.detail,
        );
      }
      throw new PersistenceError(
        `Failed to save job "${job.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findById(id: JobId): Promise<Job | null> {
    try {
      const res = await this.client.query<JobRow>(
        `
        SELECT id, pipeline_run_id, step_name, command, depends_on, status, created_at
        FROM jobs
        WHERE id = $1;
      `,
        [id],
      );

      const row = res.rows[0];
      if (!row) {
        return null;
      }

      const attempts = await this.attemptRepo.findByJobId(id);

      return this.mapRowToDomain(row, attempts);
    } catch (err) {
      if (err instanceof PersistenceError) throw err;
      throw new PersistenceError(
        `Failed to find job "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findByPipelineRunId(pipelineRunId: PipelineRunId): Promise<Job[]> {
    try {
      const res = await this.client.query<JobRow>(
        `
        SELECT id, pipeline_run_id, step_name, command, depends_on, status, created_at
        FROM jobs
        WHERE pipeline_run_id = $1
        ORDER BY created_at ASC;
      `,
        [pipelineRunId],
      );

      const jobs: Job[] = [];
      for (const row of res.rows) {
        const jobId = createJobId(row.id);
        const attempts = await this.attemptRepo.findByJobId(jobId);
        jobs.push(this.mapRowToDomain(row, attempts));
      }

      return jobs;
    } catch (err) {
      if (err instanceof PersistenceError) throw err;
      throw new PersistenceError(
        `Failed to find jobs for pipeline run "${pipelineRunId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async updateStatus(id: JobId, status: JobStatus): Promise<void> {
    if (!VALID_JOB_STATUSES.has(status)) {
      throw new PersistenceError(`Invalid JobStatus "${status}" provided for update`);
    }

    try {
      const res = await this.client.query('UPDATE jobs SET status = $1 WHERE id = $2;', [
        status,
        id,
      ]);

      if ((res.rowCount ?? 0) === 0) {
        throw new EntityNotFoundError('Job', id);
      }
    } catch (err) {
      if (err instanceof EntityNotFoundError || err instanceof PersistenceError) throw err;
      throw new PersistenceError(
        `Failed to update status for job "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  private mapRowToDomain(
    row: JobRow,
    attempts: readonly InstanceType<typeof import('@forge/pipeline').JobAttempt>[],
  ): Job {
    if (!VALID_JOB_STATUSES.has(row.status)) {
      throw new PersistenceError(
        `Invalid JobStatus "${row.status}" encountered in persistent row "${row.id}"`,
      );
    }

    try {
      const dependsOn = (
        typeof row.depends_on === 'string' ? JSON.parse(row.depends_on) : row.depends_on
      ) as string[];

      return new Job({
        id: createJobId(row.id),
        pipelineRunId: createPipelineRunId(row.pipeline_run_id),
        stepName: row.step_name,
        command: row.command,
        dependsOn,
        initialStatus: row.status as JobStatus,
        attempts,
      });
    } catch (err) {
      throw new PersistenceError(
        `Failed to reconstruct Job domain model for ID "${row.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }
}
