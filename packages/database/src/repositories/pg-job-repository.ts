import {
  createJobId,
  createJobStateMachine,
  createPipelineRunId,
  InvalidStateTransitionError,
  Job,
  type JobId,
  type JobStatus,
  type PipelineRunId,
} from '@forge/pipeline';
import { ConstraintViolationError, PersistenceError } from '../errors.js';
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
    // Pre-save state machine transition validation
    const existingRes = await this.client.query<{ status: string }>(
      'SELECT status FROM jobs WHERE id = $1;',
      [job.id],
    );

    const existingRow = existingRes.rows[0];
    if (existingRow) {
      const existingStatus = existingRow.status as JobStatus;
      if (existingStatus !== job.status) {
        const sm = createJobStateMachine(job.id, existingStatus);
        sm.transitionTo(job.status);
      }
    }

    const dependsOnJson = JSON.stringify([...job.dependsOn]);
    const requirementsJson = JSON.stringify(job.requirements ?? {});
    const retryPolicyJson = job.retryPolicy ? JSON.stringify(job.retryPolicy) : null;
    const nextAttemptAt = job.nextAttemptAt ?? null;

    try {
      await this.client.query(
        `
        INSERT INTO jobs (
          id, pipeline_run_id, step_name, command, depends_on, requirements, priority, retry_policy, next_attempt_at, status, created_at
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8::jsonb, $9, $10, NOW())
        ON CONFLICT (id) DO UPDATE
        SET status = EXCLUDED.status,
            requirements = EXCLUDED.requirements,
            priority = EXCLUDED.priority,
            retry_policy = EXCLUDED.retry_policy,
            next_attempt_at = EXCLUDED.next_attempt_at;
      `,
        [
          job.id,
          job.pipelineRunId,
          job.stepName,
          job.command,
          dependsOnJson,
          requirementsJson,
          job.priority,
          retryPolicyJson,
          nextAttemptAt,
          job.status,
        ],
      );

      // Persist any constituent attempts
      for (const attempt of job.attempts) {
        await this.attemptRepo.save(attempt);
      }
    } catch (err: unknown) {
      if (
        err instanceof ConstraintViolationError ||
        err instanceof PersistenceError ||
        err instanceof InvalidStateTransitionError
      ) {
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
        SELECT id, pipeline_run_id, step_name, command, depends_on, requirements, priority, retry_policy, next_attempt_at, status, created_at
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
        SELECT id, pipeline_run_id, step_name, command, depends_on, requirements, priority, retry_policy, next_attempt_at, status, created_at
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

  public async findSchedulableJobs(options?: { now?: Date; limit?: number }): Promise<Job[]> {
    const now = options?.now ?? new Date();
    const limit = options?.limit ?? 50;

    try {
      const res = await this.client.query<JobRow>(
        `
        SELECT id, pipeline_run_id, step_name, command, depends_on, requirements, priority, retry_policy, next_attempt_at, status, created_at
        FROM jobs
        WHERE status = 'QUEUED'
          AND (next_attempt_at IS NULL OR next_attempt_at <= $1)
        ORDER BY priority DESC, created_at ASC
        LIMIT $2;
      `,
        [now, limit],
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
        `Failed to find schedulable jobs: ${(err as Error).message}`,
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

      const requirements = row.requirements
        ? typeof row.requirements === 'string'
          ? JSON.parse(row.requirements)
          : row.requirements
        : undefined;

      const retryPolicy = row.retry_policy
        ? typeof row.retry_policy === 'string'
          ? JSON.parse(row.retry_policy)
          : row.retry_policy
        : undefined;

      return new Job({
        id: createJobId(row.id),
        pipelineRunId: createPipelineRunId(row.pipeline_run_id),
        stepName: row.step_name,
        command: row.command,
        dependsOn,
        requirements,
        priority: row.priority ?? 0,
        retryPolicy,
        nextAttemptAt: row.next_attempt_at ?? undefined,
        createdAt: row.created_at,
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
