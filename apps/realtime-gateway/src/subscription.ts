/**
 * Maps subscribable Forge resources to internal filter keys, and derives the keys an
 * event matches. Filtering is entirely in-memory at the gateway — a client subscribed to
 * `run:R` receives every event whose envelope carries `run_id === R`. There is no
 * per-client or per-resource Redis channel.
 */
import type { ForgeEvent } from '@forge/events';
import type { SubscriptionTarget } from './protocol.js';

/** Canonical filter key for a subscription target, e.g. `run:abc123`. */
export function subscriptionKey(target: SubscriptionTarget): string {
  return `${target.kind}:${target.id}`;
}

/**
 * The filter keys an event belongs to, from its correlation fields. An event with a
 * `run_id` and a `job_id` matches both `run:<run_id>` and `job:<job_id>` subscribers.
 */
export function keysForEvent(event: ForgeEvent): string[] {
  const keys: string[] = [];
  if (event.pipeline_id) {
    keys.push(`pipeline:${event.pipeline_id}`);
  }
  if (event.run_id) {
    keys.push(`run:${event.run_id}`);
  }
  if (event.job_id) {
    keys.push(`job:${event.job_id}`);
  }
  return keys;
}
