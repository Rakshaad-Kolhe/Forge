import { describe, it, expect, vi } from 'vitest';
import { createLogger } from '@forge/logging';
import { createForgeEvent } from './factory.js';
import type { ForgeEvent } from './events.js';
import { InProcessEventBus } from './in-process-bus.js';
import { EventBusClosedError } from './errors.js';

function heartbeat(id?: string): ForgeEvent {
  return createForgeEvent(
    'WorkerHeartbeat',
    { correlation: { worker_id: 'w-1' }, payload: { worker_id: 'w-1', status: 'READY' } },
    id ? { eventId: id } : {},
  );
}

describe('InProcessEventBus', () => {
  it('publishing with no subscribers resolves quietly', async () => {
    const bus = new InProcessEventBus();
    await expect(bus.publish(heartbeat())).resolves.toBeUndefined();
    await bus.close();
  });

  it('delivers each event to every subscriber', async () => {
    const bus = new InProcessEventBus();
    const a: ForgeEvent[] = [];
    const b: ForgeEvent[] = [];
    bus.subscribe((e) => void a.push(e));
    bus.subscribe(async (e) => {
      await Promise.resolve();
      b.push(e);
    });

    const event = heartbeat();
    await bus.publish(event);

    expect(a).toEqual([event]);
    expect(b).toEqual([event]);
    expect(bus.subscriberCount).toBe(2);
    await bus.close();
  });

  it('isolates a failing subscriber: others still receive, error is logged, publish resolves', async () => {
    const logs: string[] = [];
    const logger = createLogger({
      service: 'events',
      environment: 'test',
      writeFn: (m) => logs.push(m),
    });
    const bus = new InProcessEventBus({ logger });

    const received: string[] = [];
    bus.subscribe(() => void received.push('A'));
    bus.subscribe(() => {
      throw new Error('subscriber B blew up');
    });
    bus.subscribe(async () => {
      await Promise.resolve();
      throw new Error('subscriber C rejected');
    });
    bus.subscribe(() => void received.push('D'));

    await expect(bus.publish(heartbeat())).resolves.toBeUndefined();

    expect(received).toEqual(['A', 'D']);
    expect(logs.filter((l) => l.includes('Event subscriber handler failed'))).toHaveLength(2);
    expect(logs.some((l) => l.includes('subscriber B blew up'))).toBe(true);
    expect(logs.some((l) => l.includes('subscriber C rejected'))).toBe(true);
    await bus.close();
  });

  it('stops delivering after unsubscribe (idempotent)', async () => {
    const bus = new InProcessEventBus();
    const seen: ForgeEvent[] = [];
    const unsubscribe = bus.subscribe((e) => void seen.push(e));

    await bus.publish(heartbeat('00000000-0000-4000-8000-000000000001'));
    unsubscribe();
    unsubscribe(); // no throw, no double-effect
    await bus.publish(heartbeat('00000000-0000-4000-8000-000000000002'));

    expect(seen).toHaveLength(1);
    expect(bus.subscriberCount).toBe(0);
    await bus.close();
  });

  it('rejects publish and subscribe after close, and drops subscribers', async () => {
    const bus = new InProcessEventBus();
    const seen: ForgeEvent[] = [];
    bus.subscribe((e) => void seen.push(e));

    await bus.close();
    expect(bus.closed).toBe(true);
    expect(bus.subscriberCount).toBe(0);

    await expect(bus.publish(heartbeat())).rejects.toBeInstanceOf(EventBusClosedError);
    expect(() => bus.subscribe(() => undefined)).toThrow(EventBusClosedError);
    expect(seen).toHaveLength(0);

    await expect(bus.close()).resolves.toBeUndefined(); // idempotent
  });

  it('does not deduplicate: re-publishing the same event_id delivers twice', async () => {
    const bus = new InProcessEventBus();
    const seen: string[] = [];
    bus.subscribe((e) => void seen.push(e.event_id));

    const event = heartbeat('00000000-0000-4000-8000-0000000000ff');
    await bus.publish(event);
    await bus.publish(event);

    expect(seen).toEqual([
      '00000000-0000-4000-8000-0000000000ff',
      '00000000-0000-4000-8000-0000000000ff',
    ]);
    await bus.close();
  });

  it('a handler that subscribes/unsubscribes mid-dispatch does not perturb the current fan-out', async () => {
    const bus = new InProcessEventBus();
    const order: string[] = [];
    bus.subscribe(() => {
      order.push('first');
      bus.subscribe(() => void order.push('late')); // must not receive THIS event
    });
    bus.subscribe(() => void order.push('second'));

    await bus.publish(heartbeat());
    expect(order).toEqual(['first', 'second']);

    await bus.publish(heartbeat());
    expect(order).toEqual(['first', 'second', 'first', 'second', 'late']);
    await bus.close();
  });

  it('never invokes vi mock subscribers after close', async () => {
    const bus = new InProcessEventBus();
    const handler = vi.fn();
    bus.subscribe(handler);
    await bus.close();
    await expect(bus.publish(heartbeat())).rejects.toThrow();
    expect(handler).not.toHaveBeenCalled();
  });
});
