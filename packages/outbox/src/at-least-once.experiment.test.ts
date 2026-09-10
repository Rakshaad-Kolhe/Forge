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
  claimTimeoutMs: 100,
  publishTimeoutMs: 5000,
  maxDeliveryAttempts: 5,
  baseBackoffMs: 10,
  maxBackoffMs: 50,
  retentionMaxAgeMs: 0,
  retentionBatchSize: 10,
  retentionEveryNTicks: 1000,
};

describe('at-least-once duplicate-delivery experiment (mandatory)', () => {
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

  it('publish succeeds, dispatcher "crashes" before markPublished, reclaim re-publishes the SAME event_id', async () => {
    const input = toOutboxEnqueueInput(
      createForgeEvent('JobSucceeded', {
        correlation: { job_id: 'jobX', attempt_id: 'a1', worker_id: 'w1' },
        payload: {
          job_id: 'jobX',
          attempt_id: 'a1',
          worker_id: 'w1',
          attempt_number: 1,
          duration_ms: 5,
          exit_code: 0,
        },
      }),
    );
    await repo.enqueue(input);

    const seen: string[] = [];
    // "Crash" the first dispatcher: publish OK, then throw before markPublished can run.
    let firstPass = true;
    const publisher: EventPublisher = {
      publish: async (e) => {
        seen.push(e.event_id);
        if (firstPass) {
          firstPass = false;
          throw new Error('__crash_after_publish__');
        }
      },
    };
    const d1 = new OutboxDispatcher({
      repository: new PgOutboxRepository(pool),
      publisher,
      config: CFG,
    });
    // runOnce swallows the throw as a publish failure → row goes back to PENDING (delivery_attempt_count=1)
    await d1.runOnce();

    await new Promise((r) => setTimeout(r, 150)); // age past claimTimeoutMs / backoff
    const d2 = new OutboxDispatcher({
      repository: new PgOutboxRepository(pool),
      publisher,
      config: CFG,
    });
    const s2 = await d2.runOnce();

    expect(seen).toHaveLength(2);
    expect(seen[0]).toBe(seen[1]); // SAME event_id observed twice — expected at-least-once
    expect(s2.published).toBe(1);
    const row = await repo.findByEventId(input.eventId);
    expect(row!.status).toBe('PUBLISHED');
    expect(row!.deliveryAttemptCount).toBe(1); // exactly one genuine failed attempt was counted
  });
});
