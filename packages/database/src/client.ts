import pg from 'pg';
import { DatabaseConnectionError, sanitizeConnectionString } from './errors.js';
import type { DatabaseConfig, DatabasePool } from './types.js';

const { Pool } = pg;

/**
 * Creates and initializes a managed PostgreSQL connection pool.
 */
export function createDatabasePool(config: DatabaseConfig): DatabasePool {
  const pool = new Pool({
    connectionString: config.connectionString,
    max: config.maxConnections ?? 10,
    idleTimeoutMillis: config.idleTimeoutMillis ?? 10000,
    connectionTimeoutMillis: config.connectionTimeoutMillis ?? 5000,
  });

  const sanitizedUrl = sanitizeConnectionString(config.connectionString);

  pool.on('error', (err) => {
    // Prevent unhandled errors from crashing the Node.js process
    console.error(
      `[forge-database] Unexpected error on idle client (${sanitizedUrl}):`,
      err.message,
    );
  });

  return {
    async query<R extends pg.QueryResultRow = pg.QueryResultRow>(
      queryText: string,
      values?: unknown[],
    ): Promise<pg.QueryResult<R>> {
      try {
        return await pool.query<R>(queryText, values as Parameters<typeof pool.query>[1]);
      } catch (err) {
        const errorRecord = err as { code?: string; message?: string };
        if (errorRecord?.code === 'ECONNREFUSED' || errorRecord?.code === 'ENOTFOUND') {
          throw new DatabaseConnectionError(
            `Failed to execute query due to connection failure: ${errorRecord.message ?? ''}`,
            err as Error,
          );
        }
        throw err;
      }
    },

    async connect(): Promise<pg.PoolClient> {
      try {
        return await pool.connect();
      } catch (err) {
        throw new DatabaseConnectionError(
          `Failed to acquire client from pool (${sanitizedUrl}): ${(err as Error).message}`,
          err as Error,
        );
      }
    },

    async healthCheck(): Promise<boolean> {
      try {
        const res = await pool.query('SELECT 1 as healthy;');
        return res.rows.length > 0 && res.rows[0].healthy === 1;
      } catch {
        return false;
      }
    },

    async close(): Promise<void> {
      await pool.end();
    },

    getPool(): pg.Pool {
      return pool;
    },
  };
}
