import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createDatabasePool } from '../client.js';
import { DEFAULT_DATABASE_URL } from '../config.js';
import type { DatabasePool } from '../types.js';
import { MIGRATIONS, resetDatabase, runMigrations } from './migrator.js';

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

  it('includes the 008_outbox_events migration with the outbox table and partial indexes', () => {
    const m = MIGRATIONS.find((x) => x.name === '008_outbox_events');
    expect(m).toBeDefined();
    expect(m!.sql).toContain('CREATE TABLE IF NOT EXISTS outbox_events');
    expect(m!.sql).toContain('CONSTRAINT uq_outbox_events_event_id UNIQUE (event_id)');
    expect(m!.sql).toContain("CHECK (status IN ('PENDING','CLAIMED','PUBLISHED','DEAD'))");
    expect(m!.sql).toContain("WHERE status = 'PENDING'");
    expect(m!.sql).toContain('claim_token');
    expect(m!.sql).toContain('delivery_attempt_count');
    expect(m!.sql).toContain('dispatch_count');
  });

  it('creates a usable outbox_events table', async () => {
    await runMigrations(pool);
    const res = await pool.query<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns WHERE table_name = 'outbox_events' ORDER BY column_name;`,
    );
    const cols = res.rows.map((r) => r.column_name);
    expect(cols).toEqual(
      expect.arrayContaining([
        'available_at',
        'claim_token',
        'claimed_at',
        'claimed_by',
        'created_at',
        'delivery_attempt_count',
        'dispatch_count',
        'event_id',
        'event_type',
        'id',
        'last_error',
        'occurred_at',
        'payload',
        'published_at',
        'status',
        'version',
      ]),
    );
  });
});
