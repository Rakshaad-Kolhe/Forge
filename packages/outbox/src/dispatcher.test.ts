import type { EventPublisher, ForgeEvent } from '@forge/events';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { OutboxDispatcher } from './dispatcher.js';
import { CONFIG, evt, FakeOutboxRepo } from './test-support.js';

describe('OutboxDispatcher.runOnce', () => {
  let repo: FakeOutboxRepo;
  beforeEach(() => {
    repo = new FakeOutboxRepo();
  });

  it('publishes a claimed event and marks it PUBLISHED', async () => {
    const id = repo.seed(evt());
    const publisher: EventPublisher = { publish: vi.fn().mockResolvedValue(undefined) };
    const d = new OutboxDispatcher({ repository: repo, publisher, config: CONFIG });
    const s = await d.runOnce();
    expect(s).toMatchObject({ claimed: 1, published: 1, retried: 0, dead: 0, claimLost: 0 });
    expect(repo.rows.get(id)!.status).toBe('PUBLISHED');
    expect(publisher.publish).toHaveBeenCalledOnce();
  });

  it('reconstructs the event from the stored payload (not from job state)', async () => {
    repo.seed(evt());
    const seen: ForgeEvent[] = [];
    const publisher: EventPublisher = {
      publish: async (e) => {
        seen.push(e);
      },
    };
    await new OutboxDispatcher({ repository: repo, publisher, config: CONFIG }).runOnce();
    expect(seen[0]!.event_type).toBe('WorkerHeartbeat');
  });

  it('on publisher throw → PENDING with backoff, delivery_attempt_count++', async () => {
    const id = repo.seed(evt());
    const publisher: EventPublisher = {
      publish: vi.fn().mockRejectedValue(new Error('transport down')),
    };
    const s = await new OutboxDispatcher({ repository: repo, publisher, config: CONFIG }).runOnce();
    expect(s).toMatchObject({ claimed: 1, published: 0, retried: 1, dead: 0 });
    const r = repo.rows.get(id)!;
    expect(r.status).toBe('PENDING');
    expect(r.deliveryAttemptCount).toBe(1);
    expect(r.lastError).toContain('transport down');
    expect(r.availableAt.getTime()).toBeGreaterThan(Date.now());
  });

  it('on publisher hang beyond publishTimeoutMs → treated as failed', async () => {
    repo.seed(evt());
    const publisher: EventPublisher = { publish: () => new Promise(() => {}) }; // never resolves
    const s = await new OutboxDispatcher({ repository: repo, publisher, config: CONFIG }).runOnce();
    expect(s.retried + s.dead).toBe(1);
    expect(s.published).toBe(0);
  });

  it('transitions to DEAD after maxDeliveryAttempts genuine failures', async () => {
    const id = repo.seed(evt(), { deliveryAttemptCount: CONFIG.maxDeliveryAttempts - 1 });
    const publisher: EventPublisher = {
      publish: vi.fn().mockRejectedValue(new Error('still down')),
    };
    const s = await new OutboxDispatcher({ repository: repo, publisher, config: CONFIG }).runOnce();
    expect(s.dead).toBe(1);
    expect(repo.rows.get(id)!.status).toBe('DEAD');
    expect(repo.rows.get(id)!.deliveryAttemptCount).toBe(CONFIG.maxDeliveryAttempts);
  });

  it('on markPublished CLAIM_LOST → counts claimLost, does not touch the row', async () => {
    const id = repo.seed(evt());
    const publisher: EventPublisher = {
      publish: async () => {
        // simulate a newer owner stealing the claim between publish and markPublished
        repo.rows.get(id)!.claimToken = 'newer';
      },
    };
    const s = await new OutboxDispatcher({ repository: repo, publisher, config: CONFIG }).runOnce();
    expect(s.claimLost).toBe(1);
    expect(s.published).toBe(0);
    expect(repo.rows.get(id)!.claimToken).toBe('newer');
  });

  it('counts reclaimed rows (dispatchCount >= 2)', async () => {
    repo.seed(evt(), {
      status: 'CLAIMED',
      claimedAt: new Date(Date.now() - 120000),
      dispatchCount: 1,
      claimToken: 'old',
    });
    const publisher: EventPublisher = { publish: vi.fn().mockResolvedValue(undefined) };
    const s = await new OutboxDispatcher({ repository: repo, publisher, config: CONFIG }).runOnce();
    expect(s.reclaimed).toBe(1);
  });
});

describe('OutboxDispatcher lifecycle', () => {
  it('start is idempotent and stop leaves no timer running', async () => {
    const repo = new FakeOutboxRepo();
    repo.seed(evt());
    repo.seed(evt());
    const publisher: EventPublisher = { publish: vi.fn().mockResolvedValue(undefined) };
    const d = new OutboxDispatcher({
      repository: repo,
      publisher,
      config: { ...CONFIG, pollIntervalMs: 20 },
    });
    d.start();
    d.start(); // no throw, no second interval
    await new Promise((r) => setTimeout(r, 70));
    await d.stop();
    await d.stop(); // idempotent
    const callsAfterStop = (publisher.publish as ReturnType<typeof vi.fn>).mock.calls.length;
    await new Promise((r) => setTimeout(r, 60));
    expect((publisher.publish as ReturnType<typeof vi.fn>).mock.calls.length).toBe(callsAfterStop);
  });

  it('does not run overlapping ticks', async () => {
    const repo = new FakeOutboxRepo();
    for (let i = 0; i < 5; i++) repo.seed(evt());
    let concurrent = 0;
    let maxConcurrent = 0;
    const publisher: EventPublisher = {
      publish: async () => {
        concurrent += 1;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await new Promise((r) => setTimeout(r, 15));
        concurrent -= 1;
      },
    };
    const d = new OutboxDispatcher({
      repository: repo,
      publisher,
      config: { ...CONFIG, pollIntervalMs: 5, batchSize: 5 },
    });
    d.start();
    await new Promise((r) => setTimeout(r, 120));
    await d.stop();
    expect(maxConcurrent).toBe(1); // sequential publish within a tick; no overlapping ticks
  });
});
