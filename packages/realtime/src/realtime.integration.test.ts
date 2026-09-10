import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createForgeEvent, type ForgeEvent } from '@forge/events';
import { createRedisPubSub, type RedisPubSub } from '@forge/redis';
import { RedisEventPublisher } from './redis-event-publisher.js';
import { RedisEventSubscriber } from './redis-event-subscriber.js';

/**
 * End-to-end transport check against a live Redis (`docker compose up -d`). Proves a
 * ForgeEvent published through the EventPublisher seam reaches a RedisEventSubscriber in
 * another logical process byte-for-byte, and that two independent subscribers (two
 * gateway instances) both receive it.
 */
const REDIS_URL = 'redis://127.0.0.1:6379';

describe('realtime transport (live Redis)', () => {
  let channel: string;
  let connections: RedisPubSub[] = [];

  beforeEach(() => {
    channel = `forge:test:realtime:${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    connections = [];
  });

  afterEach(async () => {
    await Promise.all(connections.map((c) => c.close()));
  });

  const newPubSub = async (): Promise<RedisPubSub> => {
    const ps = createRedisPubSub({ url: REDIS_URL, maxRetriesPerRequest: 2 });
    await ps.connect();
    connections.push(ps);
    return ps;
  };

  const sampleEvent = (): ForgeEvent =>
    createForgeEvent('JobFailed', {
      correlation: { run_id: 'run-9', job_id: 'job-9', attempt_id: 'att-9', worker_id: 'w-9' },
      payload: {
        job_id: 'job-9',
        attempt_id: 'att-9',
        worker_id: 'w-9',
        attempt_number: 2,
        failure_kind: 'FAILED',
        reason: 'non-zero exit',
        exit_code: 1,
        retry_scheduled: false,
      },
    });

  const waitFor = <T>(fn: () => T | undefined, timeoutMs = 3000): Promise<T> =>
    new Promise((resolve, reject) => {
      const started = Date.now();
      const tick = (): void => {
        const v = fn();
        if (v !== undefined) return resolve(v);
        if (Date.now() - started > timeoutMs) return reject(new Error('waitFor timed out'));
        setTimeout(tick, 10);
      };
      tick();
    });

  it('delivers a published event to a cross-process subscriber unchanged', async () => {
    const publisher = new RedisEventPublisher({ pubsub: await newPubSub(), channel });
    const subscriber = new RedisEventSubscriber({ pubsub: await newPubSub(), channel });
    await subscriber.start();

    const received: ForgeEvent[] = [];
    subscriber.subscribe((e) => {
      received.push(e);
    });

    const event = sampleEvent();
    await publisher.publish(event);

    await waitFor(() => (received.length ? received : undefined));
    expect(received[0]).toEqual(event);
  });

  it('fans one publish out to two independent gateway subscribers', async () => {
    const publisher = new RedisEventPublisher({ pubsub: await newPubSub(), channel });
    const subA = new RedisEventSubscriber({ pubsub: await newPubSub(), channel });
    const subB = new RedisEventSubscriber({ pubsub: await newPubSub(), channel });
    await subA.start();
    await subB.start();

    const a: ForgeEvent[] = [];
    const b: ForgeEvent[] = [];
    subA.subscribe((e) => {
      a.push(e);
    });
    subB.subscribe((e) => {
      b.push(e);
    });

    const event = sampleEvent();
    await publisher.publish(event);

    await waitFor(() => (a.length && b.length ? true : undefined));
    expect(a[0]).toEqual(event);
    expect(b[0]).toEqual(event);
  });
});
