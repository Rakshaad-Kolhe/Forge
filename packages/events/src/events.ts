/**
 * Concrete Forge lifecycle events and the {@link ForgeEvent} discriminated union.
 *
 * Payload fields are populated strictly from state that already exists in the domain models
 * (`@forge/pipeline`), lease contracts (`@forge/contracts`), and the PR 19 execution result.
 * No event introduces a new state machine — events describe transitions the existing state
 * machines already decided.
 */
import type { ForgeEventEnvelope } from './envelope.js';

// ---------------------------------------------------------------------------
// Pipeline lifecycle (contract-only in this PR — no live producer yet)
// ---------------------------------------------------------------------------

export interface PipelineCreatedPayload {
  readonly pipeline_id: string;
  readonly name: string;
  readonly step_count: number;
}
export type PipelineCreatedEvent = ForgeEventEnvelope<'PipelineCreated', PipelineCreatedPayload>;

export interface PipelineQueuedPayload {
  readonly pipeline_id: string;
  readonly run_id: string;
}
export type PipelineQueuedEvent = ForgeEventEnvelope<'PipelineQueued', PipelineQueuedPayload>;

export type PipelineCompletionStatus = 'SUCCEEDED' | 'FAILED' | 'CANCELLED' | 'TIMED_OUT';

export interface PipelineCompletedPayload {
  readonly run_id: string;
  readonly status: PipelineCompletionStatus;
  readonly job_count: number;
}
export type PipelineCompletedEvent = ForgeEventEnvelope<
  'PipelineCompleted',
  PipelineCompletedPayload
>;

// ---------------------------------------------------------------------------
// Job scheduling lifecycle
// ---------------------------------------------------------------------------

/**
 * A job entered the schedulable queue. Currently produced by the worker when a failed
 * attempt is re-queued for retry (`job.transitionTo('QUEUED')`). The initial enqueue path
 * (API/ingress) does not exist yet, so first-time queueing is not published — see the
 * Event Catalog.
 */
export interface JobQueuedPayload {
  readonly job_id: string;
  readonly run_id: string;
  readonly priority: number;
  readonly attempt_number: number;
  /** Present when the job is waiting on a retry backoff (`jobs.next_attempt_at`). */
  readonly next_attempt_at?: string;
}
export type JobQueuedEvent = ForgeEventEnvelope<'JobQueued', JobQueuedPayload>;

/**
 * A worker lease was atomically acquired for a job. Authoritative producer: the scheduler
 * placement + lease-claim flow (`ForgeScheduler`). The worker's low-level `claimJob`
 * primitive does not emit this to avoid a duplicate for the same transition.
 */
export interface JobClaimedPayload {
  readonly job_id: string;
  readonly worker_id: string;
  readonly lease_id: string;
  readonly lease_expires_at: string;
}
export type JobClaimedEvent = ForgeEventEnvelope<'JobClaimed', JobClaimedPayload>;

/**
 * Execution of an attempt started (attempt + job transitioned to `RUNNING` and persisted).
 * Authoritative producer: the worker.
 */
export interface JobStartedPayload {
  readonly job_id: string;
  readonly attempt_id: string;
  readonly worker_id: string;
  readonly attempt_number: number;
}
export type JobStartedEvent = ForgeEventEnvelope<'JobStarted', JobStartedPayload>;

export type LogStream = 'stdout' | 'stderr';

/**
 * A bounded slice of an attempt's captured output.
 *
 * In this PR these are **derived after execution completes** from the bounded
 * `ExecutionResult` (PR 19 capped capture) — they are not streamed live. Live streaming
 * arrives with the log-collector / WebSocket PR. `sequence` is monotonic across a single
 * derivation; all `stdout` chunks precede all `stderr` chunks (the executor already merged
 * capture into two separate buffers, so cross-stream interleaving is not preserved).
 */
export interface JobLogChunkPayload {
  readonly job_id: string;
  readonly attempt_id: string;
  readonly sequence: number;
  readonly stream: LogStream;
  readonly chunk: string;
  /** Byte offset of this chunk within its own stream. */
  readonly byte_offset: number;
  /** True when the stream was truncated by PR 19's output cap. */
  readonly truncated: boolean;
  /** True on the last chunk of the derivation. */
  readonly final: boolean;
}
export type JobLogChunkEvent = ForgeEventEnvelope<'JobLogChunk', JobLogChunkPayload>;

export interface JobSucceededPayload {
  readonly job_id: string;
  readonly attempt_id: string;
  readonly worker_id: string;
  readonly attempt_number: number;
  readonly duration_ms: number;
  readonly exit_code: number | null;
}
export type JobSucceededEvent = ForgeEventEnvelope<'JobSucceeded', JobSucceededPayload>;

/**
 * Kind of failure, using the existing execution vocabulary consistently:
 * - `FAILED`       — non-zero process exit.
 * - `TIMED_OUT`    — wall-clock timeout (`exit_code` is `null`).
 * - `LEASE_LOST`   — lease ownership lost mid-execution; container aborted.
 * - `EXECUTOR_ERROR` — the executor threw before producing a result.
 */
export type JobFailureKind = 'FAILED' | 'TIMED_OUT' | 'LEASE_LOST' | 'EXECUTOR_ERROR';

export interface JobFailedPayload {
  readonly job_id: string;
  readonly attempt_id: string;
  readonly worker_id: string;
  readonly attempt_number: number;
  readonly failure_kind: JobFailureKind;
  readonly reason: string;
  readonly exit_code: number | null;
  /** True when `evaluateRetry` scheduled another attempt (job returned to `QUEUED`). */
  readonly retry_scheduled: boolean;
  readonly next_attempt_at?: string;
}
export type JobFailedEvent = ForgeEventEnvelope<'JobFailed', JobFailedPayload>;

export interface JobCancelledPayload {
  readonly job_id: string;
  readonly attempt_id: string;
  readonly worker_id: string;
  readonly attempt_number: number;
}
export type JobCancelledEvent = ForgeEventEnvelope<'JobCancelled', JobCancelledPayload>;

// ---------------------------------------------------------------------------
// Worker lifecycle
// ---------------------------------------------------------------------------

export interface WorkerRegisteredPayload {
  readonly worker_id: string;
  readonly hostname?: string;
  readonly capabilities: readonly string[];
  readonly cpu_cores: number;
  readonly memory_bytes: number;
}
export type WorkerRegisteredEvent = ForgeEventEnvelope<'WorkerRegistered', WorkerRegisteredPayload>;

export interface WorkerHeartbeatPayload {
  readonly worker_id: string;
  readonly status: string;
}
export type WorkerHeartbeatEvent = ForgeEventEnvelope<'WorkerHeartbeat', WorkerHeartbeatPayload>;

/**
 * A worker's lease expired and was reconciled by lease recovery. Authoritative producer:
 * the scheduler recovery sweep (`ForgeScheduler.recoverExpiredLeases`), which observes the
 * `LeaseRecoveryService` outcome. `NO_OP` reconciliations do not emit.
 */
export type WorkerLostRecoveryAction = 'REQUEUED' | 'DEAD_LETTERED' | 'SKIPPED_TERMINAL';

export interface WorkerLostPayload {
  readonly worker_id: string;
  readonly job_id: string;
  readonly lease_id: string;
  readonly recovery_action: WorkerLostRecoveryAction;
  readonly dead_letter_reason?: string;
}
export type WorkerLostEvent = ForgeEventEnvelope<'WorkerLost', WorkerLostPayload>;

// ---------------------------------------------------------------------------
// Union + payload map
// ---------------------------------------------------------------------------

/**
 * Every Forge lifecycle event. Discriminated on `event_type`. Adding a member without
 * handling it in an exhaustive `switch` is a compile error via `assertNever` (see
 * {@link './summarize.ts'}).
 */
export type ForgeEvent =
  | PipelineCreatedEvent
  | PipelineQueuedEvent
  | PipelineCompletedEvent
  | JobQueuedEvent
  | JobClaimedEvent
  | JobStartedEvent
  | JobLogChunkEvent
  | JobSucceededEvent
  | JobFailedEvent
  | JobCancelledEvent
  | WorkerRegisteredEvent
  | WorkerHeartbeatEvent
  | WorkerLostEvent;

/** Maps an `event_type` literal to its payload type. */
export interface ForgeEventPayloadMap {
  PipelineCreated: PipelineCreatedPayload;
  PipelineQueued: PipelineQueuedPayload;
  PipelineCompleted: PipelineCompletedPayload;
  JobQueued: JobQueuedPayload;
  JobClaimed: JobClaimedPayload;
  JobStarted: JobStartedPayload;
  JobLogChunk: JobLogChunkPayload;
  JobSucceeded: JobSucceededPayload;
  JobFailed: JobFailedPayload;
  JobCancelled: JobCancelledPayload;
  WorkerRegistered: WorkerRegisteredPayload;
  WorkerHeartbeat: WorkerHeartbeatPayload;
  WorkerLost: WorkerLostPayload;
}

/** Narrows {@link ForgeEvent} to the variant for a given `event_type`. */
export type ForgeEventOf<TType extends ForgeEvent['event_type']> = Extract<
  ForgeEvent,
  { event_type: TType }
>;
