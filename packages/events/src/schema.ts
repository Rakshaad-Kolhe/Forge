/**
 * Runtime validation for {@link ForgeEvent} using the project's existing validation library
 * (`zod`, already a dependency of `@forge/config`).
 *
 * Unknown payload/envelope fields are **stripped, not rejected** — a v1 consumer safely
 * ignores fields a future producer adds (see `docs/architecture/events.md` § Versioning).
 * A `version` other than the current {@link EVENT_SCHEMA_VERSION} is rejected: consumers
 * must not silently reinterpret an unknown schema version.
 */
import { z } from 'zod';
import { EVENT_SCHEMA_VERSION, type ForgeEventType } from './envelope.js';
import { EVENT_ID_PATTERN } from './event-id.js';
import type { ForgeEvent } from './events.js';

const id = z.string().min(1);
const isoDateTime = z.string().datetime({ offset: true });

const correlationShape = {
  pipeline_id: id.optional(),
  run_id: id.optional(),
  job_id: id.optional(),
  attempt_id: id.optional(),
  worker_id: id.optional(),
};

const envelopeShape = {
  event_id: z.string().regex(EVENT_ID_PATTERN, 'event_id must be a v4 UUID'),
  occurred_at: isoDateTime,
  version: z.literal(EVENT_SCHEMA_VERSION),
  ...correlationShape,
};

function eventSchema<TType extends ForgeEventType, TShape extends z.ZodRawShape>(
  eventType: TType,
  payload: z.ZodObject<TShape>,
) {
  return z.object({
    ...envelopeShape,
    event_type: z.literal(eventType),
    payload,
  });
}

const exitCode = z.number().int().nullable();
const logStream = z.enum(['stdout', 'stderr']);

const schemasByType = {
  PipelineCreated: eventSchema(
    'PipelineCreated',
    z.object({ pipeline_id: id, name: z.string().min(1), step_count: z.number().int().min(0) }),
  ),
  PipelineQueued: eventSchema('PipelineQueued', z.object({ pipeline_id: id, run_id: id })),
  PipelineCompleted: eventSchema(
    'PipelineCompleted',
    z.object({
      run_id: id,
      status: z.enum(['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT']),
      job_count: z.number().int().min(0),
    }),
  ),
  JobQueued: eventSchema(
    'JobQueued',
    z.object({
      job_id: id,
      run_id: id,
      priority: z.number().int(),
      attempt_number: z.number().int().min(1),
      next_attempt_at: isoDateTime.optional(),
    }),
  ),
  JobClaimed: eventSchema(
    'JobClaimed',
    z.object({ job_id: id, worker_id: id, lease_id: id, lease_expires_at: isoDateTime }),
  ),
  JobStarted: eventSchema(
    'JobStarted',
    z.object({
      job_id: id,
      attempt_id: id,
      worker_id: id,
      attempt_number: z.number().int().min(1),
    }),
  ),
  JobLogChunk: eventSchema(
    'JobLogChunk',
    z.object({
      job_id: id,
      attempt_id: id,
      sequence: z.number().int().min(0),
      stream: logStream,
      chunk: z.string(),
      byte_offset: z.number().int().min(0),
      truncated: z.boolean(),
      final: z.boolean(),
    }),
  ),
  JobSucceeded: eventSchema(
    'JobSucceeded',
    z.object({
      job_id: id,
      attempt_id: id,
      worker_id: id,
      attempt_number: z.number().int().min(1),
      duration_ms: z.number().min(0),
      exit_code: exitCode,
    }),
  ),
  JobFailed: eventSchema(
    'JobFailed',
    z.object({
      job_id: id,
      attempt_id: id,
      worker_id: id,
      attempt_number: z.number().int().min(1),
      failure_kind: z.enum(['FAILED', 'TIMED_OUT', 'LEASE_LOST', 'EXECUTOR_ERROR']),
      reason: z.string(),
      exit_code: exitCode,
      retry_scheduled: z.boolean(),
      next_attempt_at: isoDateTime.optional(),
    }),
  ),
  JobCancelled: eventSchema(
    'JobCancelled',
    z.object({
      job_id: id,
      attempt_id: id,
      worker_id: id,
      attempt_number: z.number().int().min(1),
    }),
  ),
  WorkerRegistered: eventSchema(
    'WorkerRegistered',
    z.object({
      worker_id: id,
      hostname: z.string().optional(),
      capabilities: z.array(z.string()),
      cpu_cores: z.number().min(0),
      memory_bytes: z.number().min(0),
    }),
  ),
  WorkerHeartbeat: eventSchema(
    'WorkerHeartbeat',
    z.object({ worker_id: id, status: z.string().min(1) }),
  ),
  WorkerLost: eventSchema(
    'WorkerLost',
    z.object({
      worker_id: id,
      job_id: id,
      lease_id: id,
      recovery_action: z.enum(['REQUEUED', 'DEAD_LETTERED', 'SKIPPED_TERMINAL']),
      dead_letter_reason: z.string().optional(),
    }),
  ),
} as const satisfies Record<ForgeEventType, z.ZodTypeAny>;

/** Per-type zod schema lookup — used by tests and callers validating a single known type. */
export const forgeEventSchemas = schemasByType;

/**
 * Discriminated-union schema over every event type. Rejects unknown `event_type` values and
 * mismatched / missing payloads.
 */
export const forgeEventSchema = z.discriminatedUnion('event_type', [
  schemasByType.PipelineCreated,
  schemasByType.PipelineQueued,
  schemasByType.PipelineCompleted,
  schemasByType.JobQueued,
  schemasByType.JobClaimed,
  schemasByType.JobStarted,
  schemasByType.JobLogChunk,
  schemasByType.JobSucceeded,
  schemasByType.JobFailed,
  schemasByType.JobCancelled,
  schemasByType.WorkerRegistered,
  schemasByType.WorkerHeartbeat,
  schemasByType.WorkerLost,
]);

/** Parses and validates an unknown value into a {@link ForgeEvent}. Throws `ZodError`. */
export function parseForgeEvent(input: unknown): ForgeEvent {
  return forgeEventSchema.parse(input) as ForgeEvent;
}

/** Non-throwing variant of {@link parseForgeEvent}. */
export function safeParseForgeEvent(
  input: unknown,
): { success: true; data: ForgeEvent } | { success: false; error: z.ZodError } {
  const result = forgeEventSchema.safeParse(input);
  if (result.success) {
    return { success: true, data: result.data as ForgeEvent };
  }
  return { success: false, error: result.error };
}
