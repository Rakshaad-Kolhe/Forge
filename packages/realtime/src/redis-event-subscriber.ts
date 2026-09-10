/**
 * `RedisEventSubscriber` — the cross-process implementation of the PR 20
 * {@link '@forge/events'.EventSubscriber} seam.
 *
 *   Redis Pub/Sub → RedisEventSubscriber → local EventHandler fan-out
 *
 * It owns one Redis subscription to the single logical event channel and fans each
 * validated event out to every locally-registered handler, mirroring
 * `InProcessEventBus` semantics: handlers run in registration order, a throwing handler
 * is caught / logged / skipped (never propagated, never retried). A payload that fails
 * `deserializeForgeEvent` is logged as `realtime.event_rejected` and dropped — the
 * subscription loop stays alive.
 *
 * There is no replay and no durability here. On a Redis reconnect ioredis
 * re-establishes the subscription automatically; events published during the outage are
 * gone (recover authoritative state via the API, per `docs/architecture/events.md`).
 */
import { FORGE_REALTIME_EVENT_CHANNEL } from '@forge/contracts';
import type { EventHandler, EventSubscriber, ForgeEvent, Unsubscribe } from '@forge/events';
import type { Logger } from '@forge/logging';
import type { RedisPubSub } from '@forge/redis';
import { RealtimeEventDecodeError } from './errors.js';
import { deserializeForgeEvent } from './serialization.js';

export interface RedisEventSubscriberOptions {
  readonly pubsub: RedisPubSub;
  /** Logical channel to subscribe to. Defaults to {@link FORGE_REALTIME_EVENT_CHANNEL}. */
  readonly channel?: string;
  readonly logger?: Logger;
}

export class RedisEventSubscriber implements EventSubscriber {
  private readonly pubsub: RedisPubSub;
  private readonly channel: string;
  private readonly logger?: Logger;
  private readonly handlers = new Set<EventHandler>();
  private started = false;
  private startPromise?: Promise<void>;

  constructor(options: RedisEventSubscriberOptions) {
    this.pubsub = options.pubsub;
    this.channel = options.channel ?? FORGE_REALTIME_EVENT_CHANNEL;
    this.logger = options.logger;
  }

  /** Live handler count (diagnostics / tests). */
  public get handlerCount(): number {
    return this.handlers.size;
  }

  /** Subscribes the underlying Redis connection to the event channel. Idempotent. */
  public async start(): Promise<void> {
    if (this.started) {
      return;
    }
    if (!this.startPromise) {
      this.startPromise = this.pubsub
        .subscribe(this.channel, (message) => {
          void this.dispatch(message);
        })
        .then(() => {
          this.started = true;
          this.logger?.info('realtime.subscriber_started', { channel: this.channel });
        })
        .finally(() => {
          this.startPromise = undefined;
        });
    }
    await this.startPromise;
  }

  /** Unsubscribes from the event channel and drops all local handlers. Idempotent. */
  public async stop(): Promise<void> {
    if (!this.started) {
      this.handlers.clear();
      return;
    }
    this.started = false;
    this.handlers.clear();
    await this.pubsub.unsubscribe(this.channel);
    this.logger?.info('realtime.subscriber_stopped', { channel: this.channel });
  }

  public subscribe(handler: EventHandler): Unsubscribe {
    this.handlers.add(handler);
    let active = true;
    return () => {
      if (active) {
        active = false;
        this.handlers.delete(handler);
      }
    };
  }

  private async dispatch(raw: string): Promise<void> {
    let event: ForgeEvent;
    try {
      event = deserializeForgeEvent(raw);
    } catch (err) {
      this.logger?.warn('realtime.event_rejected', {
        channel: this.channel,
        error: err instanceof Error ? err.message : String(err),
        ...(err instanceof RealtimeEventDecodeError ? { reason: 'decode' } : {}),
      });
      return;
    }

    for (const handler of Array.from(this.handlers)) {
      try {
        await handler(event);
      } catch (err) {
        this.logger?.error('realtime.subscriber_handler_failed', {
          event_id: event.event_id,
          event_type: event.event_type,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }
}
