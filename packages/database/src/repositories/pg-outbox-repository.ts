import type { OutboxEnqueueInput, OutboxEventRecord, OutboxStatus } from '@forge/contracts';
import { DEFAULT_OUTBOX_MAX_PAYLOAD_BYTES } from '@forge/contracts';
import { ConstraintViolationError, OutboxPayloadError, PersistenceError } from '../errors.js';
import type { DatabaseClient, OutboxEventRow } from '../types.js';
import type {
  OutboxClaimedRow,
  OutboxClaimOptions,
  OutboxMarkOutcome,
  OutboxRepository,
  OutboxRetryInput,
  OutboxStats,
} from './contracts/outbox-repository.contract.js';

/**
 * Options for {@link PgOutboxRepository}.
 */
export interface PgOutboxRepositoryOptions {
  readonly maxPayloadBytes?: number;
}

const UUID_SHAPE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const SELECT_COLS = `
  id, event_id, event_type, version, occurred_at,
  pipeline_id, run_id, job_id, attempt_id, worker_id,
  payload, status, delivery_attempt_count, dispatch_count,
  available_at, claimed_at, claimed_by, claim_token, published_at, last_error, created_at
`;

/**
 * PostgreSQL-backed {@link OutboxRepository}. Enqueue paths are fully implemented here;
 * the dispatch/prune methods are wired stubs replaced in later PR 21 tasks.
 */
export class PgOutboxRepository implements OutboxRepository {
  private readonly maxPayloadBytes: number;

  constructor(
    private readonly client: DatabaseClient,
    options: PgOutboxRepositoryOptions = {},
  ) {
    this.maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_OUTBOX_MAX_PAYLOAD_BYTES;
  }

  /**
   * Structurally validates an enqueue input and returns the serialized payload.
   * Envelope semantics are the producer's responsibility — this never imports `@forge/events`.
   */
  private validate(input: OutboxEnqueueInput): string {
    if (
      typeof input.payload !== 'object' ||
      input.payload === null ||
      Array.isArray(input.payload)
    ) {
      throw new OutboxPayloadError(
        `Outbox payload for event "${input.eventId}" must be a JSON object`,
      );
    }
    if (!UUID_SHAPE.test(input.eventId)) {
      throw new OutboxPayloadError(`Outbox event_id "${input.eventId}" is not a UUID`);
    }
    const serialized = JSON.stringify(input.payload);
    const bytes = Buffer.byteLength(serialized, 'utf8');
    if (bytes > this.maxPayloadBytes) {
      throw new OutboxPayloadError(
        `Outbox payload for event "${input.eventId}" is ${bytes} bytes, exceeds ${this.maxPayloadBytes}`,
      );
    }
    return serialized;
  }

  public async enqueue(input: OutboxEnqueueInput): Promise<void> {
    const serialized = this.validate(input);
    try {
      await this.client.query(
        `
        INSERT INTO outbox_events (
          id, event_id, event_type, version, occurred_at,
          pipeline_id, run_id, job_id, attempt_id, worker_id,
          payload, status, delivery_attempt_count, dispatch_count, available_at, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, 'PENDING', 0, 0, NOW(), NOW());
        `,
        [
          input.id,
          input.eventId,
          input.eventType,
          input.version,
          input.occurredAt,
          input.correlation.pipelineId ?? null,
          input.correlation.runId ?? null,
          input.correlation.jobId ?? null,
          input.correlation.attemptId ?? null,
          input.correlation.workerId ?? null,
          serialized,
        ],
      );
    } catch (err: unknown) {
      const dbErr = err as { code?: string; constraint?: string; detail?: string };
      if (dbErr?.code === '23505') {
        throw new ConstraintViolationError(
          `Outbox already contains event_id "${input.eventId}"`,
          dbErr?.constraint,
          dbErr?.detail,
        );
      }
      throw new PersistenceError(
        `Failed to enqueue outbox event "${input.eventId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async enqueueMany(inputs: readonly OutboxEnqueueInput[]): Promise<void> {
    for (const input of inputs) {
      await this.enqueue(input);
    }
  }

  public async findByEventId(eventId: string): Promise<OutboxEventRecord | null> {
    try {
      const res = await this.client.query<OutboxEventRow>(
        `SELECT ${SELECT_COLS} FROM outbox_events WHERE event_id = $1;`,
        [eventId],
      );
      return res.rows[0] ? this.mapRow(res.rows[0]) : null;
    } catch (err) {
      throw new PersistenceError(
        `Failed to find outbox event "${eventId}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  public async listByStatus(status: OutboxStatus, limit: number): Promise<OutboxEventRecord[]> {
    try {
      const res = await this.client.query<OutboxEventRow>(
        `SELECT ${SELECT_COLS} FROM outbox_events WHERE status = $1 ORDER BY occurred_at, id LIMIT $2;`,
        [status, limit],
      );
      return res.rows.map((row) => this.mapRow(row));
    } catch (err) {
      throw new PersistenceError(
        `Failed to list outbox events with status "${status}": ${(err as Error).message}`,
        err as Error,
      );
    }
  }

  // ---------------------------------------------------------------------------
  // Dispatch / prune surface — wired with real signatures, bodies land in Tasks 6-8.
  // ---------------------------------------------------------------------------

  public claimBatch(_options: OutboxClaimOptions): Promise<OutboxClaimedRow[]> {
    throw new Error('not implemented until Task 6');
  }

  public markPublished(_id: string, _claimToken: string): Promise<OutboxMarkOutcome> {
    throw new Error('not implemented until Task 7');
  }

  public markRetry(_input: OutboxRetryInput): Promise<OutboxMarkOutcome> {
    throw new Error('not implemented until Task 7');
  }

  public deletePublishedBefore(_cutoff: Date, _limit: number): Promise<number> {
    throw new Error('not implemented until Task 8');
  }

  public stats(): Promise<OutboxStats> {
    throw new Error('not implemented until Task 8');
  }

  private mapRow(row: OutboxEventRow): OutboxEventRecord {
    return {
      id: row.id,
      eventId: row.event_id,
      eventType: row.event_type,
      version: Number(row.version),
      occurredAt: new Date(row.occurred_at),
      ...(row.pipeline_id ? { pipelineId: row.pipeline_id } : {}),
      ...(row.run_id ? { runId: row.run_id } : {}),
      ...(row.job_id ? { jobId: row.job_id } : {}),
      ...(row.attempt_id ? { attemptId: row.attempt_id } : {}),
      ...(row.worker_id ? { workerId: row.worker_id } : {}),
      payload:
        typeof row.payload === 'string'
          ? (JSON.parse(row.payload) as Record<string, unknown>)
          : (row.payload as Record<string, unknown>),
      status: row.status as OutboxStatus,
      deliveryAttemptCount: Number(row.delivery_attempt_count),
      dispatchCount: Number(row.dispatch_count),
      availableAt: new Date(row.available_at),
      ...(row.claimed_at ? { claimedAt: new Date(row.claimed_at) } : {}),
      ...(row.claimed_by ? { claimedBy: row.claimed_by } : {}),
      ...(row.published_at ? { publishedAt: new Date(row.published_at) } : {}),
      ...(row.last_error ? { lastError: row.last_error } : {}),
      createdAt: new Date(row.created_at),
    };
  }
}
