import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createForgeEvent, type ForgeEvent } from '@forge/events';
import {
  makeChannelPublisher,
  openClient,
  startTestGateway,
  type TestGateway,
} from './integration-support.js';

/**
 * End-to-end gateway behaviour against a live Redis (`docker compose up -d`) and a real
 * `ws` client: handshake, subscribe/filter, event delivery, unsubscribe, ping/pong.
 */
describe('RealtimeGateway (live Redis + ws client)', () => {
  let gw: TestGateway;
  let pub: Awaited<ReturnType<typeof makeChannelPublisher>>;

  beforeEach(async () => {
    gw = await startTestGateway();
    pub = await makeChannelPublisher(gw.channel);
  });

  afterEach(async () => {
    await pub.close();
    await gw.stop();
  });

  const jobEvent = (runId: string, jobId: string): ForgeEvent =>
    createForgeEvent('JobSucceeded', {
      correlation: { run_id: runId, job_id: jobId, attempt_id: 'a1', worker_id: 'w1' },
      payload: {
        job_id: jobId,
        attempt_id: 'a1',
        worker_id: 'w1',
        attempt_number: 1,
        duration_ms: 5,
        exit_code: 0,
      },
    });

  it('sends ready on connect and delivers a matching event after subscribe', async () => {
    const ws = await openClient(gw.port);
    const rx = ws.rx;

    const ready = await rx.waitFor((m) => m['type'] === 'ready');
    expect(ready).toMatchObject({ type: 'ready', v: 1 });
    expect(typeof ready['connection_id']).toBe('string');

    ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-A' } }));
    await rx.waitFor((m) => m['type'] === 'subscribed');

    const event = jobEvent('run-A', 'job-1');
    await pub.publisher.publish(event);

    const received = await rx.waitFor((m) => m['type'] === 'event');
    expect(received['event']).toEqual(event);

    ws.close();
  });

  it('does not deliver events for a run the client is not subscribed to', async () => {
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-A' } }));
    await rx.waitFor((m) => m['type'] === 'subscribed');

    await pub.publisher.publish(jobEvent('run-OTHER', 'job-x'));
    await pub.publisher.publish(jobEvent('run-A', 'job-ok'));

    const evt = await rx.waitFor((m) => m['type'] === 'event');
    expect((evt['event'] as ForgeEvent).job_id).toBe('job-ok');
    expect(rx.count('event')).toBe(1);

    ws.close();
  });

  it('stops delivery after unsubscribe', async () => {
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-A' } }));
    await rx.waitFor((m) => m['type'] === 'subscribed');
    ws.send(JSON.stringify({ type: 'unsubscribe', v: 1, target: { kind: 'run', id: 'run-A' } }));
    await rx.waitFor((m) => m['type'] === 'unsubscribed');

    await pub.publisher.publish(jobEvent('run-A', 'job-late'));
    await new Promise((r) => setTimeout(r, 200));
    expect(rx.count('event')).toBe(0);

    ws.close();
  });

  it('answers a protocol ping with pong', async () => {
    const ws = await openClient(gw.port);
    const rx = ws.rx;
    await rx.waitFor((m) => m['type'] === 'ready');

    ws.send(JSON.stringify({ type: 'ping', v: 1 }));
    await rx.waitFor((m) => m['type'] === 'pong');

    ws.close();
  });

  it('delivers to a job-scoped subscriber and a run-scoped subscriber from one publish', async () => {
    const a = await openClient(gw.port);
    const b = await openClient(gw.port);
    const ra = a.rx;
    const rb = b.rx;
    await ra.waitFor((m) => m['type'] === 'ready');
    await rb.waitFor((m) => m['type'] === 'ready');

    a.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-Z' } }));
    b.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'job', id: 'job-Z1' } }));
    await ra.waitFor((m) => m['type'] === 'subscribed');
    await rb.waitFor((m) => m['type'] === 'subscribed');

    await pub.publisher.publish(jobEvent('run-Z', 'job-Z1'));

    expect((await ra.waitFor((m) => m['type'] === 'event'))['event']).toBeDefined();
    expect((await rb.waitFor((m) => m['type'] === 'event'))['event']).toBeDefined();

    a.close();
    b.close();
  });
});
