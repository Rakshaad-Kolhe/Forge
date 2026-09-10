import { once } from 'node:events';
import { afterEach, describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import { createForgeEvent } from '@forge/events';
import {
  makeChannelPublisher,
  openClient,
  startTestGateway,
  type TestGateway,
} from './integration-support.js';

/**
 * Lifecycle experiments (live Redis): graceful shutdown, gateway restart, and a
 * Redis-drop mid-session. The gateway holds no authoritative state — a restart or a Redis
 * blip only means clients reconnect; PostgreSQL + the outbox are untouched here by
 * construction (the gateway has no database access).
 */
describe('RealtimeGateway lifecycle (live Redis)', () => {
  const gateways: TestGateway[] = [];

  afterEach(async () => {
    await Promise.all(gateways.splice(0).map((g) => g.stop().catch(() => undefined)));
  });

  it('graceful shutdown: sends closing, ends sockets, resolves bounded, idempotent', async () => {
    const gw = await startTestGateway();
    gateways.push(gw);
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    const closed = once(ws, 'close');
    const started = Date.now();
    await gw.gateway.stop();
    await gw.gateway.stop(); // idempotent
    expect(Date.now() - started).toBeLessThan(6000);

    await closed;
    expect(ws.readyState).toBe(WebSocket.CLOSED);
    expect(rx.messages.some((m) => m['type'] === 'closing')).toBe(true);
  });

  it('gateway restart: a client reconnects to a fresh instance and events flow', async () => {
    const channel = `forge:test:restart:${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const first = await startTestGateway({ channel });
    const pub = await makeChannelPublisher(channel);

    const c1 = await openClient(first.port);
    await c1.rx.waitFor((m) => m['type'] === 'ready');
    const c1Closed = once(c1, 'close');
    await first.stop();
    await c1Closed;

    const second = await startTestGateway({ channel });
    gateways.push(second);

    const c2 = await openClient(second.port);
    const r2 = c2.rx;
    await r2.waitFor((m) => m['type'] === 'ready');
    c2.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-RS' } }));
    await r2.waitFor((m) => m['type'] === 'subscribed');

    const event = createForgeEvent('JobSucceeded', {
      correlation: { run_id: 'run-RS', job_id: 'j', attempt_id: 'a', worker_id: 'w' },
      payload: {
        job_id: 'j',
        attempt_id: 'a',
        worker_id: 'w',
        attempt_number: 1,
        duration_ms: 1,
        exit_code: 0,
      },
    });
    await pub.publisher.publish(event);
    expect((await r2.waitFor((m) => m['type'] === 'event'))['event']).toEqual(event);

    c2.close();
    await pub.close();
  });

  it('Redis drop mid-session: gateway stays up, connection survives, shutdown still completes', async () => {
    const gw = await startTestGateway();
    gateways.push(gw);
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');
    ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-D' } }));
    await rx.waitFor((m) => m['type'] === 'subscribed');

    // Simulate the transport dropping out from under the gateway.
    await gw.pubsub.close();

    // The gateway process does not crash; the client stays connected and still gets pongs.
    await new Promise((r) => setTimeout(r, 150));
    expect(ws.readyState).toBe(WebSocket.OPEN);
    ws.send(JSON.stringify({ type: 'ping', v: 1 }));
    await rx.waitFor((m) => m['type'] === 'pong');

    // Shutdown remains bounded even with a dead transport.
    const started = Date.now();
    await gw.gateway.stop();
    expect(Date.now() - started).toBeLessThan(6000);
  });
});
