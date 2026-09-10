import { randomUUID } from 'node:crypto';
import type { OutboxEventRecord, OutboxStatus } from '@forge/contracts';
import type {
  OutboxClaimedRow,
  OutboxClaimOptions,
  OutboxMarkOutcome,
  OutboxRepository,
  OutboxRetryInput,
  OutboxStats,
} from '@forge/database';
import type { ForgeEvent } from '@forge/events';
import { createForgeEvent } from '@forge/events';
import type { OutboxDispatcherConfig } from './dispatcher.js';

/**
 * Shared test doubles + fixtures for the `@forge/outbox` dispatcher suite.
 *
 * Extracted from `dispatcher.test.ts` (Task 12) so the unit tests and the
 * publisher-failure unit test exercise the exact same in-memory repository.
 */

export const CONFIG: OutboxDispatcherConfig = {
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
export class FakeOutboxRepo implements OutboxRepository {
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

export function evt(): ForgeEvent {
  return createForgeEvent('WorkerHeartbeat', {
    correlation: { worker_id: 'w1' },
    payload: { worker_id: 'w1', status: 'READY' },
  });
}
