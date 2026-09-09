import {
  createDatabasePool,
  DEFAULT_DATABASE_URL,
  PgOutboxRepository,
  resetDatabase,
  runMigrations,
  type DatabasePool,
} from '@forge/database';
import {
  createForgeEvent,
  toOutboxEnqueueInput,
  type EventPublisher,
  type ForgeEvent,
} from '@forge/events';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { OutboxDispatcher, type OutboxDispatcherConfig } from './dispatcher.js';

const BASE: OutboxDispatcherConfig = {
  pollIntervalMs: 10,
  batchSize: 20,
  claimTimeoutMs: 60000,
  publishTimeoutMs: 1000,
  maxDeliveryAttempts: 5,
  baseBackoffMs: 50,
  maxBackoffMs: 500,
  retentionMaxAgeMs: 0,
  retentionBatchSize: 100,
  retentionEveryNTicks: 1000,
};

describe('OutboxDispatcher — N-dispatcher concurrency (real PG)', () => {
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

  function heartbeat(): ForgeEvent {
    return createForgeEvent('WorkerHeartbeat', {
      correlation: { worker_id: 'w1' },
      payload: { worker_id: 'w1', status: 'READY' },
    });
  }

  for (const n of [2, 5, 10]) {
    it(`${n} dispatchers deliver every event at least once with no permanent CLAIMED`, async () => {
      const total = 60;
      const delivered = new Map<string, number>();
      for (let i = 0; i < total; i++) await repo.enqueue(toOutboxEnqueueInput(heartbeat()));

      const publisher: EventPublisher = {
        publish: async (e) => {
          delivered.set(e.event_id, (delivered.get(e.event_id) ?? 0) + 1);
        },
      };
      const dispatchers = Array.from(
        { length: n },
        () =>
          new OutboxDispatcher({
            repository: new PgOutboxRepository(pool),
            publisher,
            config: BASE,
          }),
      );
      dispatchers.forEach((d) => d.start());
      // wait until all rows are PUBLISHED or a timeout
      const deadline = Date.now() + 15000;
      let s = await repo.stats();
      while (s.published < total && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 100));
        s = await repo.stats();
      }
      await Promise.all(dispatchers.map((d) => d.stop()));

      expect(s.published).toBe(total);
      expect(s.pending + s.claimed + s.dead).toBe(0);
      expect(delivered.size).toBe(total); // no event lost
      for (const c of delivered.values()) expect(c).toBeGreaterThanOrEqual(1); // at-least-once
    });
  }
});
