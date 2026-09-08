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

describe('PgOutboxRepository — claimBatch', () => {
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

  it('claims due PENDING rows, sets a distinct token per row, bumps dispatch_count only', async () => {
    await repo.enqueueMany([input(), input()]);
    const claimed = await repo.claimBatch({
      dispatcherId: 'disp-A',
      limit: 10,
      staleClaimBefore: new Date(Date.now() - 60000),
    });
    expect(claimed).toHaveLength(2);
    expect(new Set(claimed.map((c) => c.claimToken)).size).toBe(2);
    for (const c of claimed) {
      expect(c.status).toBe('CLAIMED');
      expect(c.dispatchCount).toBe(1);
      expect(c.deliveryAttemptCount).toBe(0);
      expect(c.claimedBy).toBe('disp-A');
    }
  });

  it('does not claim rows whose available_at is in the future', async () => {
    const i = input();
    await repo.enqueue(i);
    await pool.query(
      `UPDATE outbox_events SET available_at = NOW() + INTERVAL '1 hour' WHERE id = $1;`,
      [i.id],
    );
    const claimed = await repo.claimBatch({
      dispatcherId: 'disp-A',
      limit: 10,
      staleClaimBefore: new Date(Date.now() - 60000),
    });
    expect(claimed).toHaveLength(0);
  });

  it('reclaims a CLAIMED row older than staleClaimBefore and bumps dispatch_count again', async () => {
    const i = input();
    await repo.enqueue(i);
    await repo.claimBatch({
      dispatcherId: 'disp-A',
      limit: 10,
      staleClaimBefore: new Date(Date.now() - 60000),
    });
    await pool.query(
      `UPDATE outbox_events SET claimed_at = NOW() - INTERVAL '10 minutes' WHERE id = $1;`,
      [i.id],
    );
    const reclaimed = await repo.claimBatch({
      dispatcherId: 'disp-B',
      limit: 10,
      staleClaimBefore: new Date(Date.now() - 60000),
    });
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]!.dispatchCount).toBe(2);
    expect(reclaimed[0]!.deliveryAttemptCount).toBe(0);
    expect(reclaimed[0]!.claimedBy).toBe('disp-B');
  });

  it('respects the batch limit and occurred_at ordering', async () => {
    const old = input({ occurredAt: new Date(Date.now() - 10000).toISOString() });
    const recent = input({ occurredAt: new Date().toISOString() });
    await repo.enqueue(recent);
    await repo.enqueue(old);
    const claimed = await repo.claimBatch({
      dispatcherId: 'disp-A',
      limit: 1,
      staleClaimBefore: new Date(Date.now() - 60000),
    });
    expect(claimed).toHaveLength(1);
    expect(claimed[0]!.eventId).toBe(old.eventId);
  });

  it('does not claim PUBLISHED or DEAD rows', async () => {
    const i = input();
    await repo.enqueue(i);
    await pool.query(
      `UPDATE outbox_events SET status = 'PUBLISHED', published_at = NOW() WHERE id = $1;`,
      [i.id],
    );
    const claimed = await repo.claimBatch({
      dispatcherId: 'disp-A',
      limit: 10,
      staleClaimBefore: new Date(Date.now() - 60000),
    });
    expect(claimed).toHaveLength(0);
  });
});

describe('PgOutboxRepository — markPublished / markRetry (fenced)', () => {
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

  async function claimOne() {
    const i = input();
    await repo.enqueue(i);
    const [row] = await repo.claimBatch({
      dispatcherId: 'disp-A',
      limit: 1,
      staleClaimBefore: new Date(Date.now() - 60000),
    });
    return { i, row: row! };
  }

  it('markPublished with the correct token → OK and terminal PUBLISHED', async () => {
    const { i, row } = await claimOne();
    expect(await repo.markPublished(row.id, row.claimToken)).toBe('OK');
    const after = await repo.findByEventId(i.eventId);
    expect(after!.status).toBe('PUBLISHED');
    expect(after!.publishedAt).toBeInstanceOf(Date);
    expect(after!.deliveryAttemptCount).toBe(0); // publish success burns no delivery budget
  });

  it('markPublished with a stale token → CLAIM_LOST and no mutation', async () => {
    const { row } = await claimOne();
    // simulate a reclaim by a newer owner
    await pool.query(`UPDATE outbox_events SET claim_token = 'newer-token' WHERE id = $1;`, [
      row.id,
    ]);
    expect(await repo.markPublished(row.id, row.claimToken)).toBe('CLAIM_LOST');
    const res = await pool.query(`SELECT status, claim_token FROM outbox_events WHERE id = $1;`, [
      row.id,
    ]);
    expect(res.rows[0]!.status).toBe('CLAIMED');
    expect(res.rows[0]!.claim_token).toBe('newer-token');
  });

  it('markRetry(exhausted=false) → PENDING, backoff, delivery_attempt_count++', async () => {
    const { i, row } = await claimOne();
    const availableAt = new Date(Date.now() + 5000);
    expect(
      await repo.markRetry({
        id: row.id,
        claimToken: row.claimToken,
        availableAt,
        lastError: 'boom',
        exhausted: false,
      }),
    ).toBe('OK');
    const after = await repo.findByEventId(i.eventId);
    expect(after!.status).toBe('PENDING');
    expect(after!.deliveryAttemptCount).toBe(1);
    expect(after!.dispatchCount).toBe(1);
    expect(after!.lastError).toBe('boom');
    expect(after!.availableAt.getTime()).toBeGreaterThan(Date.now() + 1000);
  });

  it('markRetry(exhausted=true) → DEAD, retains payload + last_error + attempts', async () => {
    const { i, row } = await claimOne();
    expect(
      await repo.markRetry({
        id: row.id,
        claimToken: row.claimToken,
        availableAt: new Date(),
        lastError: 'final',
        exhausted: true,
      }),
    ).toBe('OK');
    const after = await repo.findByEventId(i.eventId);
    expect(after!.status).toBe('DEAD');
    expect(after!.deliveryAttemptCount).toBe(1);
    expect(after!.lastError).toBe('final');
    expect(after!.payload).toEqual(i.payload);
  });

  it('markRetry with a stale token → CLAIM_LOST, no PUBLISHED→PENDING resurrection', async () => {
    const { i, row } = await claimOne();
    await repo.markPublished(row.id, row.claimToken); // row is now PUBLISHED, token cleared
    const outcome = await repo.markRetry({
      id: row.id,
      claimToken: row.claimToken,
      availableAt: new Date(),
      lastError: 'late',
      exhausted: false,
    });
    expect(outcome).toBe('CLAIM_LOST');
    expect((await repo.findByEventId(i.eventId))!.status).toBe('PUBLISHED');
  });

  it('truncates last_error to 2000 chars', async () => {
    const { i, row } = await claimOne();
    await repo.markRetry({
      id: row.id,
      claimToken: row.claimToken,
      availableAt: new Date(),
      lastError: 'x'.repeat(5000),
      exhausted: false,
    });
    expect((await repo.findByEventId(i.eventId))!.lastError!.length).toBe(2000);
  });
});

describe('PgOutboxRepository — retention & stats', () => {
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

  it('deletePublishedBefore removes only old PUBLISHED rows, bounded by limit', async () => {
    const keepPending = input();
    const oldPublished = input();
    const recentPublished = input();
    await repo.enqueueMany([keepPending, oldPublished, recentPublished]);
    await pool.query(
      `UPDATE outbox_events SET status='PUBLISHED', published_at = NOW() - INTERVAL '10 days' WHERE id = $1;`,
      [oldPublished.id],
    );
    await pool.query(
      `UPDATE outbox_events SET status='PUBLISHED', published_at = NOW() WHERE id = $1;`,
      [recentPublished.id],
    );
    const deleted = await repo.deletePublishedBefore(new Date(Date.now() - 7 * 86400000), 100);
    expect(deleted).toBe(1);
    expect(await repo.findByEventId(oldPublished.eventId)).toBeNull();
    expect(await repo.findByEventId(keepPending.eventId)).not.toBeNull();
    expect(await repo.findByEventId(recentPublished.eventId)).not.toBeNull();
  });

  it('deletePublishedBefore never touches PENDING/CLAIMED/DEAD', async () => {
    const dead = input();
    await repo.enqueue(dead);
    await pool.query(
      `UPDATE outbox_events SET status='DEAD', published_at = NOW() - INTERVAL '30 days' WHERE id = $1;`,
      [dead.id],
    );
    expect(await repo.deletePublishedBefore(new Date(), 100)).toBe(0);
    expect((await repo.findByEventId(dead.eventId))!.status).toBe('DEAD');
  });

  it('stats reports per-status counts and oldest pending age', async () => {
    await repo.enqueueMany([input(), input()]);
    const s = await repo.stats();
    expect(s.pending).toBe(2);
    expect(s.claimed).toBe(0);
    expect(s.published).toBe(0);
    expect(s.dead).toBe(0);
    expect(s.oldestPendingAgeMs).toBeGreaterThanOrEqual(0);
  });
});
