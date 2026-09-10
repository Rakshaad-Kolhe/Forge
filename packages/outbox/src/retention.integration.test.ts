import {
  createDatabasePool,
  DEFAULT_DATABASE_URL,
  PgOutboxRepository,
  resetDatabase,
  runMigrations,
  type DatabasePool,
} from '@forge/database';
import { createForgeEvent, toOutboxEnqueueInput, type EventPublisher } from '@forge/events';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { OutboxDispatcher, type OutboxDispatcherConfig } from './dispatcher.js';

const CFG: OutboxDispatcherConfig = {
  pollIntervalMs: 10_000,
  batchSize: 10,
  claimTimeoutMs: 60_000,
  publishTimeoutMs: 5_000,
  maxDeliveryAttempts: 5,
  baseBackoffMs: 50,
  maxBackoffMs: 200,
  retentionMaxAgeMs: 7 * 24 * 60 * 60 * 1000, // 7 days
  retentionBatchSize: 100,
  retentionEveryNTicks: 1, // sweep on every runOnce
};

const NOOP_PUBLISHER: EventPublisher = { publish: async () => {} };

function heartbeatInput() {
  return toOutboxEnqueueInput(
    createForgeEvent('WorkerHeartbeat', {
      correlation: { worker_id: 'w1' },
      payload: { worker_id: 'w1', status: 'READY' },
    }),
  );
}

describe('OutboxDispatcher — retention sweep (real PG)', () => {
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

  it('one runOnce deletes only the aged PUBLISHED row; PENDING + DEAD untouched', async () => {
    const oldPublished = heartbeatInput();
    const pending = heartbeatInput();
    const dead = heartbeatInput();
    await repo.enqueue(oldPublished);
    await repo.enqueue(pending);
    await repo.enqueue(dead);

    await pool.query(
      `UPDATE outbox_events SET status='PUBLISHED', published_at = NOW() - INTERVAL '10 days' WHERE id = $1;`,
      [oldPublished.id],
    );
    // Keep the PENDING row out of the claim window so runOnce only exercises retention.
    await pool.query(
      `UPDATE outbox_events SET available_at = NOW() + INTERVAL '1 hour' WHERE id = $1;`,
      [pending.id],
    );
    await pool.query(`UPDATE outbox_events SET status='DEAD' WHERE id = $1;`, [dead.id]);

    const d = new OutboxDispatcher({
      repository: new PgOutboxRepository(pool),
      publisher: NOOP_PUBLISHER,
      config: CFG,
    });
    const summary = await d.runOnce();

    expect(summary.retentionDeleted).toBe(1);
    expect(summary.published).toBe(0);
    expect(await repo.findByEventId(oldPublished.eventId)).toBeNull();

    const pendingRow = await repo.findByEventId(pending.eventId);
    expect(pendingRow!.status).toBe('PENDING');
    const deadRow = await repo.findByEventId(dead.eventId);
    expect(deadRow!.status).toBe('DEAD');
  });

  it('does not delete PUBLISHED rows newer than the retention cutoff', async () => {
    const recent = heartbeatInput();
    await repo.enqueue(recent);
    await pool.query(
      `UPDATE outbox_events SET status='PUBLISHED', published_at = NOW() - INTERVAL '1 day' WHERE id = $1;`,
      [recent.id],
    );

    const d = new OutboxDispatcher({
      repository: new PgOutboxRepository(pool),
      publisher: NOOP_PUBLISHER,
      config: CFG,
    });
    const summary = await d.runOnce();

    expect(summary.retentionDeleted).toBe(0);
    expect(await repo.findByEventId(recent.eventId)).not.toBeNull();
  });

  it('two dispatchers sweeping concurrently do not error and delete each aged row once', async () => {
    // All aged rows share one published_at — the normal case (a batch published in
    // one transaction). deletePublishedBefore uses FOR UPDATE SKIP LOCKED, so two
    // concurrent sweeps partition the rows (each skips what the other locked)
    // instead of locking them in opposite order and deadlocking.
    const aged = [heartbeatInput(), heartbeatInput(), heartbeatInput(), heartbeatInput()];
    for (const row of aged) {
      await repo.enqueue(row);
    }
    await pool.query(
      `UPDATE outbox_events SET status='PUBLISHED', published_at = NOW() - INTERVAL '10 days'
       WHERE id = ANY($1::text[]);`,
      [aged.map((row) => row.id)],
    );

    const mk = () =>
      new OutboxDispatcher({
        repository: new PgOutboxRepository(pool),
        publisher: NOOP_PUBLISHER,
        config: CFG,
      });
    const [s1, s2] = await Promise.all([mk().runOnce(), mk().runOnce()]);

    // No exception thrown, and every aged row is removed exactly once in total
    // between the two sweeps — no double-delete, no deadlock, no miss.
    expect(s1.retentionDeleted + s2.retentionDeleted).toBe(aged.length);
    for (const row of aged) {
      expect(await repo.findByEventId(row.eventId)).toBeNull();
    }
  });
});
