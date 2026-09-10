import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { DEFAULT_REDIS_URL } from './config.js';
import { createRedisPubSub, type RedisPubSub } from './pubsub.js';

/**
 * Live Redis Pub/Sub integration coverage. Requires a reachable Redis
 * (`docker compose up -d`). Channels are namespaced per test for isolation.
 */
describe('RedisPubSub (live Redis)', () => {
  let pubsub: RedisPubSub;
  let channel: string;

  beforeEach(async () => {
    pubsub = createRedisPubSub({ url: DEFAULT_REDIS_URL, maxRetriesPerRequest: 2 });
    await pubsub.connect();
    channel = `forge:test:pubsub:${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  });

  afterEach(async () => {
    await pubsub.close();
  });

  const waitFor = <T>(fn: () => T | undefined, timeoutMs = 2000): Promise<T> =>
    new Promise((resolve, reject) => {
      const started = Date.now();
      const tick = (): void => {
        const value = fn();
        if (value !== undefined) {
          resolve(value);
          return;
        }
        if (Date.now() - started > timeoutMs) {
          reject(new Error('waitFor timed out'));
          return;
        }
        setTimeout(tick, 10);
      };
      tick();
    });

  it('delivers a published message to a subscribed handler', async () => {
    const received: string[] = [];
    await pubsub.subscribe(channel, (message) => received.push(message));

    await pubsub.publish(channel, 'hello');

    await waitFor(() => (received.length > 0 ? received : undefined));
    expect(received).toEqual(['hello']);
  });

  it('fans one message out to every handler on the channel', async () => {
    const a: string[] = [];
    const b: string[] = [];
    await pubsub.subscribe(channel, (m) => a.push(m));
    await pubsub.subscribe(channel, (m) => b.push(m));

    await pubsub.publish(channel, 'x');

    await waitFor(() => (a.length && b.length ? true : undefined));
    expect(a).toEqual(['x']);
    expect(b).toEqual(['x']);
  });

  it('stops delivering after unsubscribe', async () => {
    const received: string[] = [];
    await pubsub.subscribe(channel, (m) => received.push(m));
    await pubsub.unsubscribe(channel);

    await pubsub.publish(channel, 'ignored');
    await new Promise((r) => setTimeout(r, 150));
    expect(received).toEqual([]);
  });

  it('isolates a throwing handler from siblings', async () => {
    const ok: string[] = [];
    await pubsub.subscribe(channel, () => {
      throw new Error('handler boom');
    });
    await pubsub.subscribe(channel, (m) => ok.push(m));

    await pubsub.publish(channel, 'survives');
    await waitFor(() => (ok.length ? true : undefined));
    expect(ok).toEqual(['survives']);
  });

  it('close() is idempotent and rejects further use', async () => {
    await pubsub.close();
    await expect(pubsub.close()).resolves.toBeUndefined();
    await expect(pubsub.publish(channel, 'nope')).rejects.toThrow(/closed/);
  });

  it('does not receive its own messages on an unrelated channel', async () => {
    const received: string[] = [];
    await pubsub.subscribe(channel, (m) => received.push(m));

    await pubsub.publish(`${channel}:other`, 'elsewhere');
    await new Promise((r) => setTimeout(r, 150));
    expect(received).toEqual([]);
  });
});
