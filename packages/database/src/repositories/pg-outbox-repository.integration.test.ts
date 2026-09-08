import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDatabasePool } from '../client.js';
import { DEFAULT_DATABASE_URL } from '../config.js';
import { ConstraintViolationError, OutboxPayloadError } from '../errors.js';
import { resetDatabase, runMigrations } from '../migrations/migrator.js';
import type { DatabasePool } from '../types.js';
import { PgOutboxRepository } from './pg-outbox-repository.js';

function input(overrides: Partial<Parameters<PgOutboxRepository['enqueue']>[0]> = {}) {
  const eventId = overrides.eventId ?? randomUUID();
  return {
    id: `outbox_${randomUUID()}`,
    eventId,
    eventType: 'JobStarted',
    version: 1,
    occurredAt: new Date().toISOString(),
    correlation: { jobId: 'job1', attemptId: 'att1', workerId: 'w1' },
    payload: {
      event_id: eventId,
      event_type: 'JobStarted',
      version: 1,
      payload: { job_id: 'job1' },
    },
    ...overrides,
  };
}

describe('PgOutboxRepository — enqueue', () => {
  let pool: DatabasePool;
  let repo: PgOutboxRepository;

  beforeAll(async () => {
    pool = createDatabasePool({ connectionString: DEFAULT_DATABASE_URL });
    await resetDatabase(pool);
    await runMigrations(pool);
    repo = new PgOutboxRepository(pool);
  });
  afterAll(async () => {
    await resetDatabase(pool);
    await pool.close();
  });
  beforeEach(async () => {
    await pool.query('DELETE FROM outbox_events;');
  });

  it('inserts a PENDING row with zeroed counters', async () => {
    const i = input();
    await repo.enqueue(i);
    const row = await repo.findByEventId(i.eventId);
    expect(row).not.toBeNull();
    expect(row!.status).toBe('PENDING');
    expect(row!.deliveryAttemptCount).toBe(0);
    expect(row!.dispatchCount).toBe(0);
    expect(row!.payload).toEqual(i.payload);
  });

  it('rejects a duplicate event_id with ConstraintViolationError', async () => {
    const eventId = randomUUID();
    await repo.enqueue(input({ eventId }));
    await expect(repo.enqueue(input({ eventId }))).rejects.toBeInstanceOf(ConstraintViolationError);
  });

  it('rejects a non-object payload', async () => {
    await expect(
      repo.enqueue(input({ payload: 'nope' as unknown as Record<string, unknown> })),
    ).rejects.toBeInstanceOf(OutboxPayloadError);
  });

  it('rejects a non-UUID-shaped eventId', async () => {
    await expect(repo.enqueue(input({ eventId: 'not-a-uuid' }))).rejects.toBeInstanceOf(
      OutboxPayloadError,
    );
  });

  it('rejects an oversized payload', async () => {
    const small = new PgOutboxRepository(pool, { maxPayloadBytes: 64 });
    await expect(
      small.enqueue(input({ payload: { blob: 'x'.repeat(500) } })),
    ).rejects.toBeInstanceOf(OutboxPayloadError);
  });

  it('enqueueMany inserts all rows atomically per statement', async () => {
    await repo.enqueueMany([input(), input(), input()]);
    const rows = await repo.listByStatus('PENDING', 10);
    expect(rows).toHaveLength(3);
  });
});
