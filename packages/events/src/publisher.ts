/**
 * Transport-independent publisher / subscriber seam.
 *
 * These interfaces know nothing about Docker, PostgreSQL, Redis, React, or WebSockets.
 * They describe *what happened*, not *how it is transported*. PR 20 ships one
 * implementation ({@link '../in-process-bus.ts'.InProcessEventBus}); later PRs can add
 * Redis / WebSocket adapters behind the same seam without touching producers.
 */
import type { Logger } from '@forge/logging';
import type { ForgeEvent } from './events.js';

/** A subscriber callback. May be sync or async; a rejection is isolated by the bus. */
export type EventHandler = (event: ForgeEvent) => void | Promise<void>;

/** Cancels a subscription. Idempotent. */
export type Unsubscribe = () => void;

/**
 * Publishes typed lifecycle events. Publication is **best-effort and happens after the
 * authoritative PostgreSQL state has been committed** (see `docs/architecture/events.md`
 * § Failure Model). A rejected `publish` must never roll back or corrupt durable state.
 */
export interface EventPublisher {
  publish(event: ForgeEvent): Promise<void>;
}

/** Registers interest in every published event. */
export interface EventSubscriber {
  subscribe(handler: EventHandler): Unsubscribe;
}

/**
 * Publishes `event` without ever throwing or blocking the caller's control loop.
 *
 * This is the helper every producer uses. It is a no-op when `publisher` is undefined
 * (events are opt-in — a scheduler/worker with no configured publisher behaves exactly as
 * before). Any failure is logged with event context and swallowed, honouring the
 * observability invariant that telemetry must never disrupt primary control loops.
 */
export async function safePublish(
  publisher: EventPublisher | undefined,
  event: ForgeEvent,
  logger?: Logger,
): Promise<void> {
  if (!publisher) {
    return;
  }
  try {
    await publisher.publish(event);
  } catch (err) {
    logger?.error('Event publication failed', {
      event_id: event.event_id,
      event_type: event.event_type,
      ...(event.job_id ? { job_id: event.job_id } : {}),
      ...(event.attempt_id ? { attempt_id: event.attempt_id } : {}),
      ...(event.worker_id ? { worker_id: event.worker_id } : {}),
      error: err instanceof Error ? err.message : String(err),
    });
  }
}
