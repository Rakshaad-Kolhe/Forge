import { describe, it, expect } from 'vitest';
import { createLogger } from '@forge/logging';
import { createForgeEvent } from './factory.js';
import type { ForgeEvent } from './events.js';
import { InProcessEventBus } from './in-process-bus.js';

function makeEvent(producer: number, n: number): ForgeEvent {
  return createForgeEvent('WorkerHeartbeat', {
    correlation: { worker_id: `w-${producer}` },
    payload: { worker_id: `w-${producer}`, status: `tick-${n}` },
  });
}

describe('InProcessEventBus — concurrent publication', () => {
  for (const producerCount of [2, 5, 10]) {
    it(`handles ${producerCount} concurrent producers without loss, mutation, or id collision`, async () => {
      const logs: string[] = [];
      const logger = createLogger({
        service: 'events',
        environment: 'test',
        writeFn: (m) => logs.push(m),
      });
      const bus = new InProcessEventBus({ logger });

      const perProducer = 50;
      const received: ForgeEvent[] = [];
      bus.subscribe((e) => {
        // Frozen envelope: any attempt to mutate would throw and surface here.
        expect(Object.isFrozen(e)).toBe(true);
        received.push(e);
      });

      await Promise.all(
        Array.from({ length: producerCount }, (_v, producer) =>
          Promise.all(
            Array.from({ length: perProducer }, (_w, n) => bus.publish(makeEvent(producer, n))),
          ),
        ),
      );

      const expectedTotal = producerCount * perProducer;
      expect(received).toHaveLength(expectedTotal);
      expect(new Set(received.map((e) => e.event_id)).size).toBe(expectedTotal);
      expect(logs.filter((l) => l.includes('handler failed'))).toHaveLength(0);

      // Every producer's ticks all arrived.
      for (let producer = 0; producer < producerCount; producer++) {
        const forProducer = received.filter((e) => e.worker_id === `w-${producer}`);
        expect(forProducer).toHaveLength(perProducer);
      }

      await bus.close();
    });
  }

  it('close() interleaved with in-flight publishes is race-safe', async () => {
    const bus = new InProcessEventBus();
    let delivered = 0;
    bus.subscribe(async () => {
      await Promise.resolve();
      delivered += 1;
    });

    // In-flight publishes issued before close() are accepted and fan out fully.
    const inFlight = Array.from({ length: 40 }, (_v, n) => bus.publish(makeEvent(0, n)));
    await bus.close();
    await Promise.all(inFlight);
    expect(delivered).toBe(40);

    // Publishes issued after close() reject deterministically, never hang.
    const afterClose = await Promise.allSettled(
      Array.from({ length: 10 }, (_v, n) => bus.publish(makeEvent(1, n))),
    );
    expect(afterClose.every((r) => r.status === 'rejected')).toBe(true);
    expect(
      afterClose.every(
        (r) => r.status === 'rejected' && (r.reason as Error).name === 'EventBusClosedError',
      ),
    ).toBe(true);
    expect(bus.closed).toBe(true);
  });
});
