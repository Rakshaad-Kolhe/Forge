import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createDatabasePool } from '../client.js';
import { DEFAULT_DATABASE_URL } from '../config.js';
import type { DatabasePool } from '../types.js';
import { resetDatabase, runMigrations } from './migrator.js';

describe('Database Migrations', () => {
  let pool: DatabasePool;

  beforeAll(async () => {
    pool = createDatabasePool({
      connectionString: DEFAULT_DATABASE_URL,
    });
    await resetDatabase(pool);
  });

  afterAll(async () => {
    await resetDatabase(pool);
    await pool.close();
  });

  it('executes migrations and applies schema on clean database', async () => {
    const newlyApplied = await runMigrations(pool);
    expect(newlyApplied).toContain('001_initial_schema');

    // Verify tables exist in PostgreSQL
    const res = await pool.query<{ table_name: string }>(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      ORDER BY table_name;
    `);

    const tableNames = res.rows.map((r) => r.table_name);
    expect(tableNames).toContain('forge_migrations');
    expect(tableNames).toContain('pipelines');
    expect(tableNames).toContain('pipeline_runs');
    expect(tableNames).toContain('jobs');
    expect(tableNames).toContain('job_attempts');
  });

  it('is idempotent when run repeatedly', async () => {
    const secondRun = await runMigrations(pool);
    expect(secondRun).toHaveLength(0);
  });
});
