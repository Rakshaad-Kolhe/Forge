/**
 * `@forge/outbox` — Forge V2's PostgreSQL transactional outbox.
 *
 * Events are staged in the same PostgreSQL transaction that commits the state
 * transition they describe, then delivered best-effort by a dispatcher that
 * claims rows with `FOR UPDATE SKIP LOCKED`. PostgreSQL stays authoritative;
 * delivery is at-least-once with deterministic exponential backoff.
 */
export * from './errors.js';
export * from './backoff.js';
export * from './dispatcher.js';
