import {
  createPipelineId,
  createPipelineRunId,
  createPipelineRunStateMachine,
  InvalidStateTransitionError,
  PipelineRun,
  type PipelineId,
  type PipelineRunId,
  type PipelineRunStatus,
} from '@forge/pipeline';
import { ConstraintViolationError, PersistenceError } from '../errors.js';
import type { DatabaseClient, DatabasePool, PipelineRunRow } from '../types.js';
import type { PipelineRunRepository } from './contracts/pipeline-run-repository.contract.js';
import { PgJobRepository } from './pg-job-repository.js';

const VALID_RUN_STATUSES = new Set<string>([
  'PENDING',
  'QUEUED',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'CANCELLED',
  'TIMED_OUT',
]);

export class PgPipelineRunRepository implements PipelineRunRepository {
  private readonly jobRepo: PgJobRepository;

  constructor(private readonly client: DatabaseClient) {
    this.jobRepo = new PgJobRepository(client);
  }

  public async save(run: PipelineRun): Promise<void> {
    const isPool =
      'connect' in this.client &&
      typeof (this.client as DatabasePool).connect === 'function' &&
      typeof (this.client as unknown as { release?: unknown }).release !== 'function';

    if (isPool) {
      const client = await (this.client as DatabasePool).connect();
      try {
        await client.query('BEGIN');
        const txRepo = new PgPipelineRunRepository(client);
        await txRepo.save(run);
        await client.query('COMMIT');
        return;
      } catch (err) {
        try {
          await client.query('ROLLBACK');
        } catch {
          // Ignore rollback error to allow original error to surface
        }
        throw err;
      } finally {
        client.release();
      }
    }

    // Pre-save state machine transition validation
    const existingRes = await this.client.query<{ status: string }>(
      'SELECT status FROM pipeline_runs WHERE id = $1;',
      [run.id],
    );

    const existingRow = existingRes.rows[0];
    if (existingRow) {
      const existingStatus = existingRow.status as PipelineRunStatus;
      if (existingStatus !== run.status) {
        const sm = createPipelineRunStateMachine(run.id, existingStatus);
        sm.transitionTo(run.status);
      }
    }

    try {
      await this.client.query(
        `
        INSERT INTO pipeline_runs (
          id, pipeline_id, pipeline_name, status, created_at, started_at, finished_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE
        SET status = EXCLUDED.status,
            started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at;
      `,
        [
          run.id,
          run.pipelineId,
          run.pipelineName,
          run.status,
          new Date(run.createdAt),
          run.startedAt ? new Date(run.startedAt) : null,
          run.finishedAt ? new Date(run.finishedAt) : null,
        ],
      );

      // Persist constituent jobs
      for (const job of run.getJobs()) {
        await this.jobRepo.save(job);
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
      if (dbErr?.code === '23503') {
        throw new ConstraintViolationError(
          `Pipeline "${run.pipelineId}" does not exist for run "${run.id}"`,
          dbErr?.constraint,
          dbErr?.detail,
        );
      }
      throw new PersistenceError(
        `Failed to save pipeline run "${run.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findById(id: PipelineRunId): Promise<PipelineRun | null> {
    try {
      const res = await this.client.query<PipelineRunRow>(
        `
        SELECT id, pipeline_id, pipeline_name, status, created_at, started_at, finished_at
        FROM pipeline_runs
        WHERE id = $1;
      `,
        [id],
      );

      const row = res.rows[0];
      if (!row) {
        return null;
      }

      const jobs = await this.jobRepo.findByPipelineRunId(id);

      return this.mapRowToDomain(row, jobs);
    } catch (err) {
      if (err instanceof PersistenceError) throw err;
      throw new PersistenceError(
        `Failed to find pipeline run "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findByPipelineId(pipelineId: PipelineId): Promise<PipelineRun[]> {
    try {
      const res = await this.client.query<PipelineRunRow>(
        `
        SELECT id, pipeline_id, pipeline_name, status, created_at, started_at, finished_at
        FROM pipeline_runs
        WHERE pipeline_id = $1
        ORDER BY created_at ASC;
      `,
        [pipelineId],
      );

      const runs: PipelineRun[] = [];
      for (const row of res.rows) {
        const runId = createPipelineRunId(row.id);
        const jobs = await this.jobRepo.findByPipelineRunId(runId);
        runs.push(this.mapRowToDomain(row, jobs));
      }

      return runs;
    } catch (err) {
      if (err instanceof PersistenceError) throw err;
      throw new PersistenceError(
        `Failed to find runs for pipeline "${pipelineId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  private mapRowToDomain(
    row: PipelineRunRow,
    jobs: readonly InstanceType<typeof import('@forge/pipeline').Job>[],
  ): PipelineRun {
    if (!VALID_RUN_STATUSES.has(row.status)) {
      throw new PersistenceError(
        `Invalid PipelineRunStatus "${row.status}" encountered in persistent row "${row.id}"`,
      );
    }

    try {
      const run = new PipelineRun({
        id: createPipelineRunId(row.id),
        pipelineId: createPipelineId(row.pipeline_id),
        pipelineName: row.pipeline_name,
        initialStatus: row.status as PipelineRunStatus,
        createdAt: row.created_at.toISOString(),
        startedAt: row.started_at ? row.started_at.toISOString() : undefined,
        finishedAt: row.finished_at ? row.finished_at.toISOString() : undefined,
      });

      for (const job of jobs) {
        run.addJob(job);
      }

      return run;
    } catch (err) {
      throw new PersistenceError(
        `Failed to reconstruct PipelineRun domain model for ID "${row.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }
}
