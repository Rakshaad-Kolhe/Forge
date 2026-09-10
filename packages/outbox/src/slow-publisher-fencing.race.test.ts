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

describe('slow-publisher fencing race (mandatory)', () => {
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

  it('a stale dispatcher cannot mutate a row a newer owner reclaimed; no PUBLISHED→PENDING', async () => {
    const input = toOutboxEnqueueInput(
      createForgeEvent('WorkerHeartbeat', {
        correlation: { worker_id: 'w1' },
        payload: { worker_id: 'w1', status: 'READY' },
      }),
    );
    await repo.enqueue(input);

    // Dispatcher A: publish sleeps far longer than the (tiny) claim timeout.
    let aPublishResolved = false;
    const slowPublisher: EventPublisher = {
      publish: async () => {
        await new Promise((r) => setTimeout(r, 800));
        aPublishResolved = true;
      },
    };
    const fastPublisher: EventPublisher = { publish: async () => {} };

    const cfgA: OutboxDispatcherConfig = {
      pollIntervalMs: 10_000,
      batchSize: 10,
      claimTimeoutMs: 100,
      publishTimeoutMs: 5000,
      maxDeliveryAttempts: 5,
      baseBackoffMs: 50,
      maxBackoffMs: 200,
      retentionMaxAgeMs: 0,
      retentionBatchSize: 10,
      retentionEveryNTicks: 1000,
    };
    const A = new OutboxDispatcher({
      repository: new PgOutboxRepository(pool),
      publisher: slowPublisher,
      config: cfgA,
    });
    const B = new OutboxDispatcher({
      repository: new PgOutboxRepository(pool),
      publisher: fastPublisher,
      config: cfgA,
    });

    const aTick = A.runOnce(); // A claims (token T1), starts slow publish
    await new Promise((r) => setTimeout(r, 150)); // let A's claim age past claimTimeoutMs
    const bSummary = await B.runOnce(); // B reclaims (token T2), publishes, markPublished(T2)
    expect(bSummary.published).toBe(1);

    const aSummary = await aTick; // A wakes, markPublished(T1) → CLAIM_LOST
    expect(aPublishResolved).toBe(true);
    expect(aSummary.claimLost).toBe(1);
    expect(aSummary.published).toBe(0);

    const row = await repo.findByEventId(input.eventId);
    expect(row!.status).toBe('PUBLISHED'); // never resurrected to PENDING
  });
});
