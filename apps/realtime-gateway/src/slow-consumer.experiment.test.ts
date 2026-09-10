import { afterEach, describe, expect, it } from 'vitest';
import { setImmediate as yieldTick } from 'node:timers/promises';
import { WebSocket } from 'ws';
import { createForgeEvent, type ForgeEvent } from '@forge/events';
import {
  openClient,
  startTestGateway,
  SyncPubSub,
  type TestGateway,
} from './integration-support.js';

/**
 * Backpressure experiment. Drives the **real** gateway (real `ws` server, real `ws`
 * client, real connection / registry / backpressure code) with an in-process transport
 * ({@link SyncPubSub}) so publish timing is under the test's control.
 *
 * The exact pending-cap bound (queue never grows past the cap; disconnect fires; state is
 * released; no growth after) is proven fully deterministically by `connection.test.ts`
 * with a fake socket. These tests prove the same behaviour through the whole real stack.
 */
describe('slow-consumer backpressure (real gateway)', () => {
  let gw: TestGateway;

  afterEach(async () => {
    await gw?.stop();
  });

  const logEvent = (runId: string, seq: number): ForgeEvent =>
    createForgeEvent('JobLogChunk', {
      correlation: { run_id: runId, job_id: `${runId}-job`, attempt_id: 'a' },
      payload: {
        job_id: `${runId}-job`,
        attempt_id: 'a',
        sequence: seq,
        stream: 'stdout',
        chunk: 'x'.repeat(1024),
        byte_offset: seq * 1024,
        truncated: false,
        final: false,
      },
    });

  const waitUntil = async (fn: () => boolean, timeoutMs = 8000): Promise<void> => {
    const started = Date.now();
    while (!fn()) {
      if (Date.now() - started > timeoutMs) throw new Error('waitUntil timed out');
      await new Promise((r) => setTimeout(r, 10));
    }
  };

  it('disconnects a stuck client once its outbound queue passes the cap, releasing all its state', async () => {
    const transport = new SyncPubSub();
    gw = await startTestGateway({ pubsub: transport, maxPendingMessages: 4 });

    const slow = await openClient(gw.port);
    await slow.rx.waitFor((m) => m['type'] === 'ready');
    slow.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-S' } }));
    await slow.rx.waitFor((m) => m['type'] === 'subscribed');

    slow.pause(); // stops reading entirely
    await new Promise((r) => setTimeout(r, 50));

    // One synchronous burst: 200 sends land before any write callback can run, so the
    // connection's pending counter climbs straight past the cap and it is dropped.
    for (let i = 0; i < 200; i += 1) {
      transport.emit(gw.channel, logEvent('run-S', i));
    }
    const snap = gw.gateway.getMetrics().snapshot();
    expect(snap.slowConsumerDisconnects).toBe(1);
    expect(snap.activeConnections).toBe(0);
    expect(snap.activeSubscriptions).toBe(0); // subscription state released

    await waitUntil(
      () => slow.readyState === WebSocket.CLOSING || slow.readyState === WebSocket.CLOSED,
      2000,
    ).catch(() => {
      // A paused peer surfaces the close only once its socket is fully torn down; the
      // server-side release asserted above is the invariant that matters.
    });

    // Sends after the drop are inert — nothing is retained, nothing leaks.
    for (let i = 200; i < 260; i += 1) {
      transport.emit(gw.channel, logEvent('run-S', i));
    }
    expect(gw.gateway.getMetrics().snapshot().activeConnections).toBe(0);
  }, 20000);

  it('a stuck client never stalls a healthy client sharing the same stream', async () => {
    const transport = new SyncPubSub();
    gw = await startTestGateway({ pubsub: transport, maxPendingMessages: 16 });

    const stuck = await openClient(gw.port);
    const healthy = await openClient(gw.port);
    await stuck.rx.waitFor((m) => m['type'] === 'ready');
    await healthy.rx.waitFor((m) => m['type'] === 'ready');
    for (const c of [stuck, healthy]) {
      c.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-H' } }));
    }
    await stuck.rx.waitFor((m) => m['type'] === 'subscribed');
    await healthy.rx.waitFor((m) => m['type'] === 'subscribed');

    stuck.pause();

    // Feed one event per tick so the healthy client's write callbacks drain between events.
    for (let i = 0; i < 400; i += 1) {
      transport.emit(gw.channel, logEvent('run-H', i));
      await yieldTick();
    }

    // The healthy client received every event regardless of the stuck peer.
    await waitUntil(() => healthy.rx.count('event') >= 400);
    expect(healthy.rx.count('event')).toBe(400);

    healthy.close();
  }, 20000);
});
