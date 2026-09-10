/**
 * `RedisEventPublisher` — the cross-process implementation of the PR 20
 * {@link '@forge/events'.EventPublisher} seam.
 *
 * It composes behind the existing seam rather than replacing it: the PR 21
 * `OutboxDispatcher` keeps calling `publisher.publish(event)` exactly as before, and the
 * worker's best-effort `safePublish(...)` path is unchanged. This class only changes
 * *where the bytes go* — a single logical Redis Pub/Sub channel.
 *
 *   Producer → ForgeEvent → EventPublisher → RedisEventPublisher → Redis Pub/Sub
 *
 * Redis is transient. A `publish` that throws (Redis unreachable, timeout) propagates to
 * the caller: the outbox dispatcher records a delivery failure and retries from the
 * durable row; the best-effort path logs and drops. Nothing here touches authoritative
 * PostgreSQL state.
 */
import {
  DEFAULT_REALTIME_PUBLISH_TIMEOUT_MS,
  FORGE_REALTIME_EVENT_CHANNEL,
} from '@forge/contracts';
import type { EventPublisher, ForgeEvent } from '@forge/events';
import type { Logger } from '@forge/logging';
import type { RedisPubSub } from '@forge/redis';
import { RealtimeError } from './errors.js';
import { serializeForgeEvent } from './serialization.js';

export interface RedisEventPublisherOptions {
  readonly pubsub: RedisPubSub;
  /** Logical channel every event is published on. Defaults to {@link FORGE_REALTIME_EVENT_CHANNEL}. */
  readonly channel?: string;
  /**
   * Upper bound on a single Redis publish. Defaults to
   * {@link DEFAULT_REALTIME_PUBLISH_TIMEOUT_MS}. Guards the best-effort worker path, where
   * (unlike the outbox dispatcher) nothing else bounds the call.
   */
  readonly publishTimeoutMs?: number;
  readonly logger?: Logger;
}

export class RedisEventPublisher implements EventPublisher {
  private readonly pubsub: RedisPubSub;
  private readonly channel: string;
  private readonly publishTimeoutMs: number;
  private readonly logger?: Logger;

  constructor(options: RedisEventPublisherOptions) {
    this.pubsub = options.pubsub;
    this.channel = options.channel ?? FORGE_REALTIME_EVENT_CHANNEL;
    this.publishTimeoutMs = options.publishTimeoutMs ?? DEFAULT_REALTIME_PUBLISH_TIMEOUT_MS;
    this.logger = options.logger;
  }

  public async publish(event: ForgeEvent): Promise<void> {
    const payload = serializeForgeEvent(event);
    try {
      const receivers = await this.withTimeout(this.pubsub.publish(this.channel, payload));
      this.logger?.debug('realtime.event_published', {
        event_id: event.event_id,
        event_type: event.event_type,
        channel: this.channel,
        receivers,
      });
    } catch (err) {
      this.logger?.warn('realtime.publish_failed', {
        event_id: event.event_id,
        event_type: event.event_type,
        channel: this.channel,
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
  }

  private withTimeout(p: Promise<number>): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new RealtimeError(`realtime publish exceeded ${this.publishTimeoutMs}ms`)),
        this.publishTimeoutMs,
      );
      timer.unref?.();
      p.then(
        (v) => {
          clearTimeout(timer);
          resolve(v);
        },
        (e) => {
          clearTimeout(timer);
          reject(e instanceof Error ? e : new Error(String(e)));
        },
      );
    });
  }
}
