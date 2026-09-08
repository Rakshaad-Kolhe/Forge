import { EVENT_SCHEMA_VERSION, type EventCorrelation, type ForgeEventType } from './envelope.js';
import type { ForgeEvent, ForgeEventOf, ForgeEventPayloadMap } from './events.js';
import { generateEventId } from './event-id.js';

/**
 * Optional overrides for deterministic construction in tests. Production code never passes
 * these — ids are Forge-generated and timestamps are wall-clock.
 */
export interface CreateForgeEventOptions {
  /** Fixed timestamp source (defaults to `new Date()`). */
  readonly now?: Date;
  /** Fixed event id (defaults to {@link generateEventId}). */
  readonly eventId?: string;
}

/**
 * Builds a fully-formed, frozen {@link ForgeEvent} envelope.
 *
 * Centralises `event_id` / `occurred_at` / `version` assignment so no producer hand-rolls
 * an envelope. Creation is allocation-light and never validates — call
 * {@link './schema.ts'.parseForgeEvent} at trust boundaries / in tests when validation is
 * required.
 *
 * @param eventType   discriminant literal.
 * @param init        correlation ids (optional) + the type-specific payload.
 * @param options     deterministic overrides for tests only.
 */
export function createForgeEvent<TType extends ForgeEventType>(
  eventType: TType,
  init: { readonly correlation?: EventCorrelation; readonly payload: ForgeEventPayloadMap[TType] },
  options: CreateForgeEventOptions = {},
): ForgeEventOf<TType> {
  const correlation = init.correlation ?? {};

  const envelope = {
    event_id: options.eventId ?? generateEventId(),
    event_type: eventType,
    occurred_at: (options.now ?? new Date()).toISOString(),
    version: EVENT_SCHEMA_VERSION,
    ...(correlation.pipeline_id !== undefined ? { pipeline_id: correlation.pipeline_id } : {}),
    ...(correlation.run_id !== undefined ? { run_id: correlation.run_id } : {}),
    ...(correlation.job_id !== undefined ? { job_id: correlation.job_id } : {}),
    ...(correlation.attempt_id !== undefined ? { attempt_id: correlation.attempt_id } : {}),
    ...(correlation.worker_id !== undefined ? { worker_id: correlation.worker_id } : {}),
    payload: init.payload,
  };

  // Immutable after creation (envelope only; payload is already declared readonly).
  // The envelope is built structurally from the discriminant + payload map, so the cast
  // back to the narrowed union member is sound.
  return Object.freeze(envelope) as unknown as ForgeEventOf<TType>;
}

/** Type guard: does `value` look like a Forge event envelope (structurally, not validated)? */
export function isForgeEventShape(value: unknown): value is ForgeEvent {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate['event_id'] === 'string' &&
    typeof candidate['event_type'] === 'string' &&
    typeof candidate['occurred_at'] === 'string' &&
    candidate['version'] === EVENT_SCHEMA_VERSION &&
    typeof candidate['payload'] === 'object' &&
    candidate['payload'] !== null
  );
}
