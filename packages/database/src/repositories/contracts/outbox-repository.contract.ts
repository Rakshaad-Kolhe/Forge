import type { OutboxEnqueueInput, OutboxEventRecord, OutboxStatus } from '@forge/contracts';

/**
 * Parameters for atomically claiming a batch of dispatchable outbox events.
 */
export interface OutboxClaimOptions {
  readonly dispatcherId: string;
  readonly limit: number;
  readonly staleClaimBefore: Date;
}

/**
 * An outbox event record returned by a claim, carrying the opaque claim token that
 * a dispatcher must present to {@link OutboxRepository.markPublished} / {@link OutboxRepository.markRetry}.
 */
export interface OutboxClaimedRow extends OutboxEventRecord {
  readonly claimToken: string;
}

/**
 * Result of a mark-published / mark-retry transition.
 * `CLAIM_LOST` indicates the row was re-claimed or advanced by another dispatcher and the write was a no-op.
 */
export type OutboxMarkOutcome = 'OK' | 'CLAIM_LOST';

/**
 * Parameters for recording a failed delivery attempt against a claimed outbox event.
 */
export interface OutboxRetryInput {
  readonly id: string;
  readonly claimToken: string;
  readonly availableAt: Date;
  readonly lastError: string;
  readonly exhausted: boolean;
}

/**
 * Aggregate counts describing the current state of the outbox.
 */
export interface OutboxStats {
  readonly pending: number;
  readonly claimed: number;
  readonly published: number;
  readonly dead: number;
  readonly oldestPendingAgeMs: number | null;
}

/**
 * Authoritative repository contract for the PostgreSQL transactional outbox (PR 21).
 *
 * Enqueue runs inside the producer's state-transition transaction; dispatch (claim →
 * publish/retry → prune) runs out-of-band. Delivery is at-least-once — consumers must
 * dedupe on `eventId`. See `docs/architecture/invariants.md` and the PR 21 spec.
 */
export interface OutboxRepository {
  /**
   * Validates and inserts a single PENDING outbox event. Intended to be called
   * within the caller's transaction alongside the domain state change.
   */
  enqueue(input: OutboxEnqueueInput): Promise<void>;

  /**
   * Validates and inserts many outbox events, one INSERT statement per event.
   */
  enqueueMany(inputs: readonly OutboxEnqueueInput[]): Promise<void>;

  /**
   * Atomically claims up to `limit` dispatchable events (PENDING and due, or CLAIMED
   * with a stale claim), stamping them for the given dispatcher and returning claim tokens.
   */
  claimBatch(options: OutboxClaimOptions): Promise<OutboxClaimedRow[]>;

  /**
   * Marks a claimed event PUBLISHED. Returns `CLAIM_LOST` if the claim token no longer matches.
   */
  markPublished(id: string, claimToken: string): Promise<OutboxMarkOutcome>;

  /**
   * Records a failed delivery: reschedules the event (or dead-letters it when `exhausted`).
   * Returns `CLAIM_LOST` if the claim token no longer matches.
   */
  markRetry(input: OutboxRetryInput): Promise<OutboxMarkOutcome>;

  /**
   * Deletes up to `limit` PUBLISHED events whose `publishedAt` precedes `cutoff`. Returns the count removed.
   */
  deletePublishedBefore(cutoff: Date, limit: number): Promise<number>;

  /**
   * Returns aggregate counts and the age of the oldest PENDING event.
   */
  stats(): Promise<OutboxStats>;

  /**
   * Looks up a single outbox event by its logical `eventId` (unique).
   */
  findByEventId(eventId: string): Promise<OutboxEventRecord | null>;

  /**
   * Lists events in a given status, oldest first, capped at `limit`.
   */
  listByStatus(status: OutboxStatus, limit: number): Promise<OutboxEventRecord[]>;
}
