/**
 * Pure projection of a validated {@link ForgeEvent} into the persistence-layer
 * {@link OutboxEnqueueInput} shape. Producers call {@link '../schema.ts'.parseForgeEvent} first;
 * this helper never validates. Lives in `@forge/events` (not `@forge/outbox`) so producers
 * enqueue without taking a new package dependency, and so `@forge/database` never imports events.
 */
import { randomUUID } from 'node:crypto';
import type { OutboxEnqueueInput } from '@forge/contracts';
import type { ForgeEvent } from './events.js';

export function toOutboxEnqueueInput(event: ForgeEvent): OutboxEnqueueInput {
  const correlation: OutboxEnqueueInput['correlation'] = {
    ...(event.pipeline_id !== undefined ? { pipelineId: event.pipeline_id } : {}),
    ...(event.run_id !== undefined ? { runId: event.run_id } : {}),
    ...(event.job_id !== undefined ? { jobId: event.job_id } : {}),
    ...(event.attempt_id !== undefined ? { attemptId: event.attempt_id } : {}),
    ...(event.worker_id !== undefined ? { workerId: event.worker_id } : {}),
  };

  return {
    id: `outbox_${randomUUID()}`,
    eventId: event.event_id,
    eventType: event.event_type,
    version: event.version,
    occurredAt: event.occurred_at,
    correlation,
    payload: event as unknown as Record<string, unknown>,
  };
}
