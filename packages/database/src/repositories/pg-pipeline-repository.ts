import { createPipelineId, Pipeline, type PipelineId, type StepDefinition } from '@forge/pipeline';
import { ConstraintViolationError, PersistenceError } from '../errors.js';
import type { DatabaseClient, PipelineRow } from '../types.js';
import type { PipelineRepository } from './contracts/pipeline-repository.contract.js';

export class PgPipelineRepository implements PipelineRepository {
  constructor(private readonly client: DatabaseClient) {}

  public async save(pipeline: Pipeline): Promise<void> {
    const stepsJson = JSON.stringify(
      pipeline.getSteps().map((s) => ({
        name: s.name,
        command: s.command,
        dependsOn: [...s.dependsOn],
      })),
    );

    try {
      await this.client.query(
        `
        INSERT INTO pipelines (id, name, steps, created_at, updated_at)
        VALUES ($1, $2, $3::jsonb, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE
        SET name = EXCLUDED.name,
            steps = EXCLUDED.steps,
            updated_at = NOW();
      `,
        [pipeline.id, pipeline.name, stepsJson],
      );
    } catch (err) {
      throw new PersistenceError(
        `Failed to save pipeline "${pipeline.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findById(id: PipelineId): Promise<Pipeline | null> {
    try {
      const res = await this.client.query<PipelineRow>(
        'SELECT id, name, steps, created_at, updated_at FROM pipelines WHERE id = $1;',
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
        `Failed to find pipeline "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async list(): Promise<Pipeline[]> {
    try {
      const res = await this.client.query<PipelineRow>(
        'SELECT id, name, steps, created_at, updated_at FROM pipelines ORDER BY created_at ASC;',
      );

      return res.rows.map((row) => this.mapRowToDomain(row));
    } catch (err) {
      if (err instanceof PersistenceError) throw err;
      throw new PersistenceError(
        `Failed to list pipelines: ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async delete(id: PipelineId): Promise<boolean> {
    try {
      const res = await this.client.query('DELETE FROM pipelines WHERE id = $1;', [id]);
      return (res.rowCount ?? 0) > 0;
    } catch (err: unknown) {
      const dbErr = err as { code?: string; constraint?: string; detail?: string };
      if (dbErr?.code === '23503' || dbErr?.code === '23001') {
        throw new ConstraintViolationError(
          `Cannot delete pipeline "${id}" because pipeline runs reference it`,
          dbErr?.constraint,
          dbErr?.detail,
        );
      }
      throw new PersistenceError(
        `Failed to delete pipeline "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  private mapRowToDomain(row: PipelineRow): Pipeline {
    try {
      const steps = (
        typeof row.steps === 'string' ? JSON.parse(row.steps) : row.steps
      ) as StepDefinition[];
      return new Pipeline({
        id: createPipelineId(row.id),
        name: row.name,
        steps,
      });
    } catch (err) {
      throw new PersistenceError(
        `Failed to reconstruct Pipeline domain model for ID "${row.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }
}
