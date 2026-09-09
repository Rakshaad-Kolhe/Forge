import { randomUUID } from 'node:crypto';
import type { EventPublisher, ForgeEvent } from '@forge/events';
import { createForgeEvent } from '@forge/events';
import type {
  OutboxClaimedRow,
  OutboxClaimOptions,
  OutboxRepository,
  OutboxRetryInput,
  OutboxStats,
  OutboxMarkOutcome,
} from '@forge/database';
import type { OutboxEventRecord, OutboxStatus } from '@forge/contracts';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { OutboxDispatcher, type OutboxDispatcherConfig } from './dispatcher.js';

const CONFIG: OutboxDispatcherConfig = {
  pollIntervalMs: 1000,
  batchSize: 10,
  claimTimeoutMs: 60000,
  publishTimeoutMs: 200,
  maxDeliveryAttempts: 3,
  baseBackoffMs: 100,
  maxBackoffMs: 1000,
  retentionMaxAgeMs: 0,
  retentionBatchSize: 100,
  retentionEveryNTicks: 60,
};

/** In-memory repository double implementing just what the dispatcher calls. */
class FakeOutboxRepo implements OutboxRepository {
  rows = new Map<string, OutboxEventRecord & { claimToken: string | null }>();

  seed(
    event: ForgeEvent,
    over: Partial<OutboxEventRecord & { claimToken: string | null }> = {},
  ): string {
    const id = `outbox_${randomUUID()}`;
    this.rows.set(id, {
      id,
      eventId: event.event_id,
      eventType: event.event_type,
      version: event.version,
      occurredAt: new Date(event.occurred_at),
      payload: event as unknown as Record<string, unknown>,
      status: 'PENDING',
      deliveryAttemptCount: 0,
      dispatchCount: 0,
      availableAt: new Date(0),
      createdAt: new Date(),
      claimToken: null,
      ...over,
    });
    return id;
  }
  async enqueue() {}
  async enqueueMany() {}
  async claimBatch(o: OutboxClaimOptions): Promise<OutboxClaimedRow[]> {
    const out: OutboxClaimedRow[] = [];
    for (const r of this.rows.values()) {
      if (out.length >= o.limit) break;
      const due = r.status === 'PENDING' && r.availableAt.getTime() <= Date.now();
      const stale =
        r.status === 'CLAIMED' && r.claimedAt !== undefined && r.claimedAt < o.staleClaimBefore;
      if (!due && !stale) continue;
      const claimToken = randomUUID();
      Object.assign(r, {
        status: 'CLAIMED',
        claimedAt: new Date(),
        claimedBy: o.dispatcherId,
        claimToken,
        dispatchCount: r.dispatchCount + 1,
      });
      out.push({ ...(r as OutboxEventRecord), claimToken });
    }
    return out;
  }
  async markPublished(id: string, token: string): Promise<OutboxMarkOutcome> {
    const r = this.rows.get(id);
    if (!r || r.status !== 'CLAIMED' || r.claimToken !== token) return 'CLAIM_LOST';
    Object.assign(r, {
      status: 'PUBLISHED',
      publishedAt: new Date(),
      claimToken: null,
      claimedAt: undefined,
      claimedBy: undefined,
    });
    return 'OK';
  }
  async markRetry(i: OutboxRetryInput): Promise<OutboxMarkOutcome> {
    const r = this.rows.get(i.id);
    if (!r || r.status !== 'CLAIMED' || r.claimToken !== i.claimToken) return 'CLAIM_LOST';
    Object.assign(r, {
      status: i.exhausted ? 'DEAD' : 'PENDING',
      deliveryAttemptCount: r.deliveryAttemptCount + 1,
      lastError: i.lastError,
      availableAt: i.availableAt,
      ...(i.exhausted ? {} : { claimToken: null, claimedAt: undefined, claimedBy: undefined }),
    });
    return 'OK';
  }
  async deletePublishedBefore() {
    return 0;
  }
  async stats(): Promise<OutboxStats> {
    return { pending: 0, claimed: 0, published: 0, dead: 0, oldestPendingAgeMs: null };
  }
  async findByEventId(eventId: string) {
    return [...this.rows.values()].find((r) => r.eventId === eventId) ?? null;
  }
  async listByStatus(status: OutboxStatus) {
    return [...this.rows.values()].filter((r) => r.status === status);
  }
}

function evt(): ForgeEvent {
  return createForgeEvent('WorkerHeartbeat', {
    correlation: { worker_id: 'w1' },
    payload: { worker_id: 'w1', status: 'READY' },
  });
}

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
