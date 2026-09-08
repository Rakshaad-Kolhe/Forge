/**
 * Compile-time exhaustiveness guard for the {@link ForgeEvent} union.
 *
 * {@link summarizeForgeEvent} switches over every `event_type` and ends in
 * {@link assertNever}. Adding a member to {@link ForgeEvent} without a matching `case` here
 * is a TypeScript error — no new event type can silently bypass handling.
 */
import type { ForgeEvent } from './events.js';

/** Throws if reached; the parameter type `never` makes an unhandled union member a compile error. */
export function assertNever(value: never): never {
  throw new Error(`Unhandled ForgeEvent variant: ${JSON.stringify(value)}`);
}

/**
 * One-line human summary of an event, for structured-log `message` fields and diagnostics.
 * Deliberately excludes payload bodies (never logs full log chunks or secrets).
 */
export function summarizeForgeEvent(event: ForgeEvent): string {
  switch (event.event_type) {
    case 'PipelineCreated':
      return `pipeline ${event.payload.pipeline_id} created (${event.payload.step_count} steps)`;
    case 'PipelineQueued':
      return `pipeline ${event.payload.pipeline_id} queued as run ${event.payload.run_id}`;
    case 'PipelineCompleted':
      return `run ${event.payload.run_id} completed: ${event.payload.status}`;
    case 'JobQueued':
      return `job ${event.payload.job_id} queued (attempt ${event.payload.attempt_number})`;
    case 'JobClaimed':
      return `job ${event.payload.job_id} claimed by ${event.payload.worker_id} (lease ${event.payload.lease_id})`;
    case 'JobStarted':
      return `job ${event.payload.job_id} attempt ${event.payload.attempt_number} started on ${event.payload.worker_id}`;
    case 'JobLogChunk':
      return `job ${event.payload.job_id} log ${event.payload.stream} #${event.payload.sequence}${event.payload.final ? ' (final)' : ''}`;
    case 'JobSucceeded':
      return `job ${event.payload.job_id} succeeded (exit ${String(event.payload.exit_code)}, ${event.payload.duration_ms}ms)`;
    case 'JobFailed':
      return `job ${event.payload.job_id} failed: ${event.payload.failure_kind}${event.payload.retry_scheduled ? ' (retry scheduled)' : ''}`;
    case 'JobCancelled':
      return `job ${event.payload.job_id} attempt ${event.payload.attempt_number} cancelled`;
    case 'WorkerRegistered':
      return `worker ${event.payload.worker_id} registered`;
    case 'WorkerHeartbeat':
      return `worker ${event.payload.worker_id} heartbeat (${event.payload.status})`;
    case 'WorkerLost':
      return `worker ${event.payload.worker_id} lost; job ${event.payload.job_id} ${event.payload.recovery_action}`;
    default:
      return assertNever(event);
  }
}
