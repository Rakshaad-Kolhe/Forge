/**
 * `@forge/events` — Forge V2's typed lifecycle event architecture.
 *
 * A neutral, transport-independent contract:
 *
 *   Producer (scheduler / worker / execution)
 *       -> createForgeEvent(...)            typed, versioned, uniquely-identified envelope
 *       -> EventPublisher.publish(...)      best-effort, after PostgreSQL commit
 *       -> EventSubscriber handlers         isolated, no global ordering
 *
 * Events are notifications of committed state transitions, never the source of truth.
 * PostgreSQL stays authoritative; Redis stays transient. There is no exactly-once
 * event-delivery guarantee. See `docs/architecture/events.md`.
 */
export * from './envelope.js';
export * from './events.js';
export * from './event-id.js';
export * from './factory.js';
export * from './schema.js';
export * from './publisher.js';
export * from './in-process-bus.js';
export * from './log-chunk.js';
export * from './summarize.js';
export * from './errors.js';
export * from './outbox-input.js';
