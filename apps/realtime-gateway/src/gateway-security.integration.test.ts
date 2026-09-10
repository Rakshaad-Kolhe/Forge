import { once } from 'node:events';
import { afterEach, describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import {
  openClient,
  startTestGateway,
  TEST_TOKEN,
  type TestGateway,
} from './integration-support.js';
import { AllowAuthenticatedAuthorizer } from './authorization.js';
import type { SubscriptionAuthorizer } from './authorization.js';
import type { Principal } from './auth.js';
import type { SubscriptionTarget } from './protocol.js';

const expectHandshakeRejected = async (ws: WebSocket): Promise<void> => {
  const [err] = (await once(ws, 'error')) as [Error];
  expect(err).toBeInstanceOf(Error);
};

describe('RealtimeGateway security (live Redis)', () => {
  let gw: TestGateway;

  afterEach(async () => {
    await gw?.stop();
  });

  it('rejects a handshake with no credential', async () => {
    gw = await startTestGateway();
    const ws = new WebSocket(`ws://127.0.0.1:${gw.port}`);
    await expectHandshakeRejected(ws);
  });

  it('rejects a handshake with a wrong credential', async () => {
    gw = await startTestGateway();
    const ws = new WebSocket(`ws://127.0.0.1:${gw.port}`, {
      headers: { authorization: 'Bearer not-the-token' },
    });
    await expectHandshakeRejected(ws);
  });

  it('rejects an untrusted Origin and accepts a trusted one', async () => {
    gw = await startTestGateway({ originAllowlist: 'https://trusted.example' });

    const bad = new WebSocket(`ws://127.0.0.1:${gw.port}`, {
      headers: { authorization: `Bearer ${TEST_TOKEN}`, origin: 'https://evil.example' },
    });
    await expectHandshakeRejected(bad);

    const good = await openClient(gw.port, { origin: 'https://trusted.example' });
    expect(good.readyState).toBe(WebSocket.OPEN);
    good.close();
  });

  it('closes the connection when an inbound frame exceeds the byte cap', async () => {
    gw = await startTestGateway({ maxMessageBytes: 256 });
    const ws = await openClient(gw.port);
    const closed = once(ws, 'close');
    ws.send(JSON.stringify({ type: 'ping', v: 1, pad: 'x'.repeat(2000) }));
    const [code] = (await closed) as [number];
    expect(code).toBeGreaterThanOrEqual(1002);
  });

  it('answers a malformed frame with an error and keeps the connection open', async () => {
    gw = await startTestGateway();
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    ws.send('{ not json');
    const err = await rx.waitFor((m) => m['type'] === 'error');
    expect(err).toMatchObject({ code: 'INVALID_MESSAGE' });
    expect(ws.readyState).toBe(WebSocket.OPEN);

    ws.send(JSON.stringify({ type: 'ping', v: 1 }));
    await rx.waitFor((m) => m['type'] === 'pong');
    ws.close();
  });

  it('rejects a version mismatch with UNSUPPORTED_VERSION', async () => {
    gw = await startTestGateway();
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    ws.send(JSON.stringify({ type: 'subscribe', v: 2, target: { kind: 'run', id: 'r' } }));
    const err = await rx.waitFor((m) => m['type'] === 'error');
    expect(err).toMatchObject({ code: 'UNSUPPORTED_VERSION' });
    ws.close();
  });

  it('denies a subscription the authorizer rejects (cross-resource block)', async () => {
    const onlyRunA: SubscriptionAuthorizer = {
      authorize: (_p: Principal, t: SubscriptionTarget) => t.id === 'run-A',
    };
    gw = await startTestGateway({ authorizer: onlyRunA });
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-B' } }));
    const err = await rx.waitFor((m) => m['type'] === 'error');
    expect(err).toMatchObject({ code: 'FORBIDDEN' });

    ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-A' } }));
    await rx.waitFor((m) => m['type'] === 'subscribed');
    ws.close();
  });

  it('enforces the per-connection subscription limit', async () => {
    gw = await startTestGateway({ maxSubscriptionsPerConnection: 2 });
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    for (const id of ['r1', 'r2']) {
      ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id } }));
      await rx.waitFor(
        (m) => m['type'] === 'subscribed' && (m['target'] as SubscriptionTarget).id === id,
      );
    }
    ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'r3' } }));
    const err = await rx.waitFor((m) => m['type'] === 'error');
    expect(err).toMatchObject({ code: 'SUBSCRIPTION_LIMIT' });
    ws.close();
  });

  it('does not let a client name a Redis channel via extra fields', async () => {
    gw = await startTestGateway();
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    ws.send(
      JSON.stringify({
        type: 'subscribe',
        v: 1,
        target: { kind: 'run', id: 'run-A' },
        channel: 'forge:realtime:events',
      }),
    );
    const ack = await rx.waitFor((m) => m['type'] === 'subscribed');
    expect(ack['target']).toEqual({ kind: 'run', id: 'run-A' });
    expect(ack).not.toHaveProperty('channel');
    ws.close();
  });

  it('placeholder authorizer allows any authenticated principal (documented gap)', async () => {
    gw = await startTestGateway({ authorizer: new AllowAuthenticatedAuthorizer() });
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');
    ws.send(
      JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'pipeline', id: 'anything' } }),
    );
    await rx.waitFor((m) => m['type'] === 'subscribed');
    ws.close();
  });
});
