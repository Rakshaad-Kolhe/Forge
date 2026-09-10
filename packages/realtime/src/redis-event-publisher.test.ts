import { describe, expect, it } from 'vitest';
import { FORGE_REALTIME_EVENT_CHANNEL } from '@forge/contracts';
import { createForgeEvent } from '@forge/events';
import { RealtimeEventDecodeError } from './errors.js';
import { RedisEventPublisher } from './redis-event-publisher.js';
import { deserializeForgeEvent } from './serialization.js';
import { FakeRedisPubSub } from './test-support.js';

const event = createForgeEvent('JobStarted', {
  correlation: { job_id: 'job-1', attempt_id: 'att-1', worker_id: 'w-1' },
  payload: { job_id: 'job-1', attempt_id: 'att-1', worker_id: 'w-1', attempt_number: 1 },
});

describe('RedisEventPublisher', () => {
  it('publishes a validated, serialized event on the default channel', async () => {
    const pubsub = new FakeRedisPubSub();
    const publisher = new RedisEventPublisher({ pubsub });

    await publisher.publish(event);

    expect(pubsub.published).toHaveLength(1);
    expect(pubsub.published[0]!.channel).toBe(FORGE_REALTIME_EVENT_CHANNEL);
    expect(deserializeForgeEvent(pubsub.published[0]!.message)).toEqual(event);
  });

  it('honours a custom channel', async () => {
    const pubsub = new FakeRedisPubSub();
    const publisher = new RedisEventPublisher({ pubsub, channel: 'forge:realtime:events:test' });

    await publisher.publish(event);

    expect(pubsub.published[0]!.channel).toBe('forge:realtime:events:test');
  });

  it('propagates a transport failure to the caller (dispatcher/safePublish handles it)', async () => {
    const pubsub = new FakeRedisPubSub();
    pubsub.failNextPublish = new Error('redis down');
    const publisher = new RedisEventPublisher({ pubsub });

    await expect(publisher.publish(event)).rejects.toThrow('redis down');
  });

  it('rejects an invalid event before touching the transport', async () => {
    const pubsub = new FakeRedisPubSub();
    const publisher = new RedisEventPublisher({ pubsub });
    const invalid = { ...event, payload: {} } as unknown as typeof event;

    await expect(publisher.publish(invalid)).rejects.toThrow(RealtimeEventDecodeError);
    expect(pubsub.published).toHaveLength(0);
  });

  it('rejects when a publish exceeds the configured timeout', async () => {
    const pubsub = new FakeRedisPubSub();
    pubsub.hangNextPublish = true;
    const publisher = new RedisEventPublisher({ pubsub, publishTimeoutMs: 20 });

    await expect(publisher.publish(event)).rejects.toThrow(/exceeded 20ms/);
  });
});
