/**
 * `@forge/realtime` — Forge V2's transient cross-process event transport.
 *
 *   PostgreSQL + outbox (durable, authoritative)
 *        → OutboxDispatcher
 *        → EventPublisher            ← RedisEventPublisher lives here
 *        → Redis Pub/Sub             ← transient fan-out
 *        → RedisEventSubscriber      ← consumed by the WebSocket gateway
 *        → clients (best-effort)
 *
 * This package composes behind the PR 20 `EventPublisher` / `EventSubscriber` seam. It
 * never becomes a source of truth, never replays, and never claims exactly-once or
 * global ordering. A committed Forge event stays durable in PostgreSQL regardless of
 * Redis availability.
 */
export * from './errors.js';
export * from './serialization.js';
export * from './redis-event-publisher.js';
export * from './redis-event-subscriber.js';
