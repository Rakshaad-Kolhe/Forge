import type { EventPublisher } from '@forge/events';
import { describe, expect, it } from 'vitest';
import { OutboxDispatcher } from './dispatcher.js';
import { CONFIG, evt, FakeOutboxRepo } from './test-support.js';

describe('OutboxDispatcher — publisher.publish throws', () => {
  it('row returns to PENDING with attempt++, backoff, last_error, ownership released', async () => {
    const repo = new FakeOutboxRepo();
    const id = repo.seed(evt());
    const publisher: EventPublisher = {
      publish: async () => {
        throw new Error('kaboom');
      },
    };

    const summary = await new OutboxDispatcher({
      repository: repo,
      publisher,
      config: CONFIG,
    }).runOnce();

    expect(summary).toMatchObject({ claimed: 1, published: 0, retried: 1, dead: 0, claimLost: 0 });

    const r = repo.rows.get(id)!;
    expect(r.status).toBe('PENDING');
    expect(r.deliveryAttemptCount).toBe(1);
    expect(r.lastError).toContain('kaboom');
    expect(r.claimToken).toBeNull();
    expect(r.availableAt.getTime()).toBeGreaterThan(Date.now());
  });
});
