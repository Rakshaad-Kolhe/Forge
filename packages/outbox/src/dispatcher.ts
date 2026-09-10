import { randomUUID } from 'node:crypto';
import type { OutboxClaimedRow, OutboxRepository } from '@forge/database';
import type { EventPublisher, ForgeEvent } from '@forge/events';
import { parseForgeEvent } from '@forge/events';
import type { Logger } from '@forge/logging';
import { outboxBackoffMs } from './backoff.js';
import { OutboxPublishTimeoutError } from './errors.js';

export interface OutboxDispatcherConfig {
  readonly pollIntervalMs: number;
  readonly batchSize: number;
  readonly claimTimeoutMs: number;
  readonly publishTimeoutMs: number;
  readonly maxDeliveryAttempts: number;
  readonly baseBackoffMs: number;
  readonly maxBackoffMs: number;
  readonly retentionMaxAgeMs: number;
  readonly retentionBatchSize: number;
  readonly retentionEveryNTicks: number;
  readonly dispatcherId?: string;
}

export interface OutboxTickSummary {
  readonly claimed: number;
  readonly published: number;
  readonly retried: number;
  readonly dead: number;
  readonly claimLost: number;
  readonly reclaimed: number;
  readonly retentionDeleted: number;
}

export interface OutboxDispatcherDeps {
  readonly repository: OutboxRepository;
  readonly publisher: EventPublisher;
  readonly logger?: Logger;
  readonly config: OutboxDispatcherConfig;
}

const LAST_ERROR_MAX = 2000;

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new OutboxPublishTimeoutError(ms)), ms);
    timer.unref?.();
    p.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e) => {
        clearTimeout(timer);
        reject(e);
      },
    );
  });
}

export class OutboxDispatcher {
  private readonly repository: OutboxRepository;
  private readonly publisher: EventPublisher;
  private readonly logger?: Logger;
  private readonly config: OutboxDispatcherConfig;
  private readonly id: string;
  private ticksSinceRetention = 0;
  private timer?: NodeJS.Timeout; // Task 12 — placeholder for daemon loop
  private dispatching = false; // Task 12 — placeholder for concurrency guard
  private stopped = true; // Task 12 — placeholder for lifecycle state

  constructor(deps: OutboxDispatcherDeps) {
    this.repository = deps.repository;
    this.publisher = deps.publisher;
    this.logger = deps.logger;
    this.config = deps.config;
    this.id = deps.config.dispatcherId ?? `outbox-${randomUUID()}`;
  }

  public get dispatcherId(): string {
    return this.id;
  }

  public async runOnce(): Promise<OutboxTickSummary> {
    const summary = {
      claimed: 0,
      published: 0,
      retried: 0,
      dead: 0,
      claimLost: 0,
      reclaimed: 0,
      retentionDeleted: 0,
    };

    const rows = await this.repository.claimBatch({
      dispatcherId: this.id,
      limit: this.config.batchSize,
      staleClaimBefore: new Date(Date.now() - this.config.claimTimeoutMs),
    });
    summary.claimed = rows.length;
    summary.reclaimed = rows.filter((r) => r.dispatchCount >= 2).length;

    for (const row of rows) {
      await this.processRow(row, summary);
    }

    this.ticksSinceRetention += 1;
    if (
      this.config.retentionMaxAgeMs > 0 &&
      this.ticksSinceRetention >= this.config.retentionEveryNTicks
    ) {
      this.ticksSinceRetention = 0;
      summary.retentionDeleted = await this.repository.deletePublishedBefore(
        new Date(Date.now() - this.config.retentionMaxAgeMs),
        this.config.retentionBatchSize,
      );
      if (summary.retentionDeleted > 0) {
        this.logger?.info('outbox.retention_swept', { deleted: summary.retentionDeleted });
      }
    }
    return summary;
  }

  private async processRow(
    row: OutboxClaimedRow,
    summary: { published: number; retried: number; dead: number; claimLost: number },
  ): Promise<void> {
    let event: ForgeEvent;
    try {
      event = parseForgeEvent(row.payload);
    } catch (err) {
      const outcome = await this.repository.markRetry({
        id: row.id,
        claimToken: row.claimToken,
        availableAt: new Date(),
        lastError: `unparseable payload: ${errMsg(err)}`.slice(0, LAST_ERROR_MAX),
        exhausted: true,
      });
      if (outcome === 'CLAIM_LOST') {
        summary.claimLost += 1;
        this.logClaimLost(row);
      } else {
        summary.dead += 1;
        this.logger?.error('outbox.dead_lettered', this.ctx(row, { reason: 'unparseable' }));
      }
      return;
    }

    this.logger?.debug('outbox.publish_started', this.ctx(row));
    try {
      await withTimeout(this.publisher.publish(event), this.config.publishTimeoutMs);
    } catch (err) {
      const exhausted = row.deliveryAttemptCount + 1 >= this.config.maxDeliveryAttempts;
      const outcome = await this.repository.markRetry({
        id: row.id,
        claimToken: row.claimToken,
        availableAt: new Date(
          Date.now() +
            outboxBackoffMs(
              row.deliveryAttemptCount,
              this.config.baseBackoffMs,
              this.config.maxBackoffMs,
            ),
        ),
        lastError: errMsg(err).slice(0, LAST_ERROR_MAX),
        exhausted,
      });
      if (outcome === 'CLAIM_LOST') {
        summary.claimLost += 1;
        this.logClaimLost(row);
        return;
      }
      if (exhausted) {
        summary.dead += 1;
        this.logger?.error('outbox.dead_lettered', this.ctx(row, { error: errMsg(err) }));
      } else {
        summary.retried += 1;
        this.logger?.warn('outbox.retry_scheduled', this.ctx(row, { error: errMsg(err) }));
      }
      return;
    }

    const outcome = await this.repository.markPublished(row.id, row.claimToken);
    if (outcome === 'CLAIM_LOST') {
      summary.claimLost += 1;
      this.logClaimLost(row);
      return;
    }
    summary.published += 1;
    this.logger?.info('outbox.published', this.ctx(row));
  }

  private ctx(row: OutboxClaimedRow, extra: Record<string, unknown> = {}): Record<string, unknown> {
    return {
      event_id: row.eventId,
      event_type: row.eventType,
      delivery_attempt_count: row.deliveryAttemptCount,
      dispatch_count: row.dispatchCount,
      ...(row.jobId ? { job_id: row.jobId } : {}),
      ...(row.workerId ? { worker_id: row.workerId } : {}),
      ...extra,
    };
  }

  private logClaimLost(row: OutboxClaimedRow): void {
    this.logger?.warn('outbox.claim_lost', this.ctx(row));
  }

  private async tick(): Promise<void> {
    if (this.dispatching || this.stopped) return;
    this.dispatching = true;
    try {
      await this.runOnce();
    } catch (err) {
      this.logger?.error('outbox.tick_failed', { error: errMsg(err) });
    } finally {
      this.dispatching = false;
    }
  }

  public start(): void {
    if (this.timer) return;
    this.stopped = false;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.config.pollIntervalMs);
    this.timer.unref();
  }

  public async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    while (this.dispatching) {
      await sleep(10);
    }
  }
}
