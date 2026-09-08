/**
 * In-process typed event bus — the single event implementation shipped in PR 20.
 *
 *   In-process bus  ≠  distributed durable event transport.
 *
 * It fans a published event out to every subscriber **in the current process only**. It is
 * not responsible for cross-process delivery, persistence, or replay. It has no hidden
 * global singleton — callers construct and own an instance, and must `close()` it.
 *
 * Ordering: `publish` invokes handlers sequentially in subscription order and resolves only
 * after the last one settles. Therefore a producer that `await`s `publish(a)` before
 * `publish(b)` guarantees every subscriber observes `a` before `b` (per-producer ordering).
 * Concurrent `publish` calls from different producers may interleave — there is no
 * cross-producer / global ordering guarantee.
 *
 * Subscriber failure isolation: a handler that throws or rejects is caught, logged, and
 * does not prevent the remaining handlers from receiving the same event. Errors are not
 * propagated to the publisher and are not retried.
 */
import type { Logger } from '@forge/logging';
import { EventBusClosedError } from './errors.js';
import type { ForgeEvent } from './events.js';
import type { EventHandler, EventPublisher, EventSubscriber, Unsubscribe } from './publisher.js';

export interface InProcessEventBusOptions {
  /** Structured logger for subscriber-failure diagnostics. Optional. */
  readonly logger?: Logger;
}

export class InProcessEventBus implements EventPublisher, EventSubscriber {
  private readonly handlers = new Set<EventHandler>();
  private readonly logger?: Logger;
  private isClosed = false;

  constructor(options: InProcessEventBusOptions = {}) {
    this.logger = options.logger;
  }

  /** True once {@link close} has been called; the bus rejects all further use. */
  public get closed(): boolean {
    return this.isClosed;
  }

  /** Current subscriber count (diagnostics / tests). */
  public get subscriberCount(): number {
    return this.handlers.size;
  }

  public subscribe(handler: EventHandler): Unsubscribe {
    if (this.isClosed) {
      throw new EventBusClosedError('subscribe');
    }
    this.handlers.add(handler);
    let active = true;
    return () => {
      if (active) {
        active = false;
        this.handlers.delete(handler);
      }
    };
  }

  public async publish(event: ForgeEvent): Promise<void> {
    if (this.isClosed) {
      throw new EventBusClosedError('publish');
    }
    // Snapshot so a handler that (un)subscribes during dispatch does not perturb this fan-out.
    const handlers = Array.from(this.handlers);
    for (const handler of handlers) {
      try {
        await handler(event);
      } catch (err) {
        this.logger?.error('Event subscriber handler failed', {
          event_id: event.event_id,
          event_type: event.event_type,
          ...(event.job_id ? { job_id: event.job_id } : {}),
          ...(event.attempt_id ? { attempt_id: event.attempt_id } : {}),
          ...(event.worker_id ? { worker_id: event.worker_id } : {}),
          error: err instanceof Error ? err.message : String(err),
        });
        // Isolated: swallow and continue to the next handler. No propagation, no retry.
      }
    }
  }

  /**
   * Closes the bus: no further `publish` or `subscribe` is accepted and all subscribers are
   * dropped. Idempotent. In-flight `publish` calls already past the closed-check run to
   * completion.
   */
  public async close(): Promise<void> {
    this.isClosed = true;
    this.handlers.clear();
  }
}
