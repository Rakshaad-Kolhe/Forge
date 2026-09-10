import { describe, expect, it, vi } from 'vitest';
import { FORGE_REALTIME_EVENT_CHANNEL } from '@forge/contracts';
import { createForgeEvent, type ForgeEvent } from '@forge/events';
import { RedisEventSubscriber } from './redis-event-subscriber.js';
import { serializeForgeEvent } from './serialization.js';
import { FakeRedisPubSub } from './test-support.js';

const makeEvent = (id: string): ForgeEvent =>
  createForgeEvent('JobLogChunk', {
    correlation: { job_id: 'job-1', attempt_id: 'att-1' },
    payload: {
      job_id: 'job-1',
      attempt_id: 'att-1',
      sequence: 0,
      stream: 'stdout',
      chunk: id,
      byte_offset: 0,
      truncated: false,
      final: true,
    },
  });

describe('RedisEventSubscriber', () => {
  it('subscribes the channel once even if start() is called repeatedly', async () => {
    const pubsub = new FakeRedisPubSub();
    const sub = new RedisEventSubscriber({ pubsub });

    await Promise.all([sub.start(), sub.start()]);
    await sub.start();

    expect(pubsub.subscribeCalls).toBe(1);
    expect(pubsub.handlerCountFor(FORGE_REALTIME_EVENT_CHANNEL)).toBe(1);
  });

  it('delivers a validated event to every handler in registration order', async () => {
    const pubsub = new FakeRedisPubSub();
    const sub = new RedisEventSubscriber({ pubsub });
    await sub.start();

    const order: string[] = [];
    sub.subscribe((e) => {
      order.push(`a:${(e.payload as { chunk: string }).chunk}`);
    });
    sub.subscribe((e) => {
      order.push(`b:${(e.payload as { chunk: string }).chunk}`);
    });

    await pubsub.publish(FORGE_REALTIME_EVENT_CHANNEL, serializeForgeEvent(makeEvent('x')));
    await vi.waitFor(() => expect(order).toEqual(['a:x', 'b:x']));
  });

  it('isolates a throwing handler and still runs the others', async () => {
    const pubsub = new FakeRedisPubSub();
    const sub = new RedisEventSubscriber({ pubsub });
    await sub.start();

    const seen: string[] = [];
    sub.subscribe(() => {
      throw new Error('handler boom');
    });
    sub.subscribe((e) => {
      seen.push((e.payload as { chunk: string }).chunk);
    });

    await pubsub.publish(FORGE_REALTIME_EVENT_CHANNEL, serializeForgeEvent(makeEvent('y')));
    await vi.waitFor(() => expect(seen).toEqual(['y']));
  });

  it('drops a malformed payload and keeps the subscription alive', async () => {
    const pubsub = new FakeRedisPubSub();
    const sub = new RedisEventSubscriber({ pubsub });
    await sub.start();

    const seen: string[] = [];
    sub.subscribe((e) => {
      seen.push((e.payload as { chunk: string }).chunk);
    });

    await pubsub.publish(FORGE_REALTIME_EVENT_CHANNEL, '{ not-json');
    await pubsub.publish(FORGE_REALTIME_EVENT_CHANNEL, serializeForgeEvent(makeEvent('z')));

    await vi.waitFor(() => expect(seen).toEqual(['z']));
  });

  it('unsubscribe() stops a single handler without affecting siblings', async () => {
    const pubsub = new FakeRedisPubSub();
    const sub = new RedisEventSubscriber({ pubsub });
    await sub.start();

    const a: string[] = [];
    const b: string[] = [];
    const off = sub.subscribe((e) => {
      a.push((e.payload as { chunk: string }).chunk);
    });
    sub.subscribe((e) => {
      b.push((e.payload as { chunk: string }).chunk);
    });

    off();
    await pubsub.publish(FORGE_REALTIME_EVENT_CHANNEL, serializeForgeEvent(makeEvent('m')));

    await vi.waitFor(() => expect(b).toEqual(['m']));
    expect(a).toEqual([]);
  });

  it('stop() unsubscribes and clears handlers; is idempotent', async () => {
    const pubsub = new FakeRedisPubSub();
    const sub = new RedisEventSubscriber({ pubsub });
    await sub.start();
    sub.subscribe(() => {});

    await sub.stop();
    await sub.stop();

    expect(pubsub.unsubscribeCalls).toBe(1);
    expect(sub.handlerCount).toBe(0);
  });
});
