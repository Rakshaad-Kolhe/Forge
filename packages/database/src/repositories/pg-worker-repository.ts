import { PersistenceError } from '../errors.js';
import type { DatabaseClient } from '../types.js';
import type {
  WorkerLifecycleStatus,
  WorkerRecord,
  WorkerRepository,
  WorkerResourcesRecord,
} from './contracts/worker-repository.contract.js';

interface WorkerRow {
  id: string;
  status: string;
  hostname: string | null;
  executors: string[] | string;
  resources: WorkerResourcesRecord | string;
  registered_at: string | Date;
  updated_at: string | Date;
}

export class PgWorkerRepository implements WorkerRepository {
  constructor(private readonly client: DatabaseClient) {}

  public async save(worker: WorkerRecord): Promise<void> {
    try {
      await this.client.query(
        `
        INSERT INTO workers (id, status, hostname, executors, resources, registered_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET
          status = EXCLUDED.status,
          hostname = EXCLUDED.hostname,
          executors = EXCLUDED.executors,
          resources = EXCLUDED.resources,
          updated_at = NOW();
        `,
        [
          worker.id,
          worker.status,
          worker.hostname ?? null,
          JSON.stringify(worker.executors),
          JSON.stringify(worker.resources),
          worker.registeredAt,
          worker.updatedAt,
        ],
      );
    } catch (err) {
      throw new PersistenceError(
        `Failed to save worker "${worker.id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async findById(id: string): Promise<WorkerRecord | null> {
    try {
      const res = await this.client.query<WorkerRow>(
        `
        SELECT id, status, hostname, executors, resources, registered_at, updated_at
        FROM workers
        WHERE id = $1;
        `,
        [id],
      );

      if (res.rows.length === 0) {
        return null;
      }

      return this.mapRow(res.rows[0]!);
    } catch (err) {
      throw new PersistenceError(
        `Failed to find worker "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async list(filter?: { status?: WorkerLifecycleStatus }): Promise<WorkerRecord[]> {
    try {
      let query = `
        SELECT id, status, hostname, executors, resources, registered_at, updated_at
        FROM workers
      `;
      const params: unknown[] = [];

      if (filter?.status) {
        params.push(filter.status);
        query += ` WHERE status = $${params.length}`;
      }

      query += ' ORDER BY registered_at DESC;';

      const res = await this.client.query<WorkerRow>(query, params);
      return res.rows.map((row) => this.mapRow(row));
    } catch (err) {
      throw new PersistenceError(`Failed to list workers: ${(err as Error).message}`, err as Error);
    }
  }

  public async updateStatus(id: string, status: WorkerLifecycleStatus): Promise<boolean> {
    try {
      const res = await this.client.query(
        `
        UPDATE workers
        SET status = $1, updated_at = NOW()
        WHERE id = $2;
        `,
        [status, id],
      );

      return (res.rowCount ?? 0) > 0;
    } catch (err) {
      throw new PersistenceError(
        `Failed to update status for worker "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async delete(id: string): Promise<boolean> {
    try {
      const res = await this.client.query('DELETE FROM workers WHERE id = $1;', [id]);

      return (res.rowCount ?? 0) > 0;
    } catch (err) {
      throw new PersistenceError(
        `Failed to delete worker "${id}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  private mapRow(row: WorkerRow): WorkerRecord {
    const executors = Array.isArray(row.executors)
      ? row.executors
      : (JSON.parse(row.executors as string) as string[]);

    const resources =
      typeof row.resources === 'object' && row.resources !== null
        ? (row.resources as WorkerResourcesRecord)
        : (JSON.parse(row.resources as string) as WorkerResourcesRecord);

    return {
      id: row.id,
      status: row.status as WorkerLifecycleStatus,
      hostname: row.hostname,
      executors,
      resources,
      registeredAt: new Date(row.registered_at),
      updatedAt: new Date(row.updated_at),
    };
  }
}
