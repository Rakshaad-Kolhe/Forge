import { afterEach, describe, expect, it } from 'vitest';
import { createForgeEvent } from '@forge/events';
import {
  makeChannelPublisher,
  openClient,
  startTestGateway,
  type TestGateway,
} from './integration-support.js';

/**
 * Two independent gateway instances on the same Redis transport. Each keeps only its own
 * connection state; a single publish reaches clients on both. No sticky sessions.
 */
describe('multi-gateway fan-out (live Redis)', () => {
  const gateways: TestGateway[] = [];

  afterEach(async () => {
    await Promise.all(gateways.splice(0).map((g) => g.stop()));
  });

  it('delivers one published event to clients on gateway A and gateway B', async () => {
    const sharedChannel = `forge:test:multigw:${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const gwA = await startTestGateway({ channel: sharedChannel });
    const gwB = await startTestGateway({ channel: sharedChannel });
    gateways.push(gwA, gwB);

    const pub = await makeChannelPublisher(sharedChannel);

    const cA = await openClient(gwA.port);
    const cB = await openClient(gwB.port);
    const rA = cA.rx;
    const rB = cB.rx;
    await rA.waitFor((m) => m['type'] === 'ready');
    await rB.waitFor((m) => m['type'] === 'ready');

    for (const c of [cA, cB]) {
      c.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-M' } }));
    }
    await rA.waitFor((m) => m['type'] === 'subscribed');
    await rB.waitFor((m) => m['type'] === 'subscribed');

    const event = createForgeEvent('JobFailed', {
      correlation: { run_id: 'run-M', job_id: 'job-M', attempt_id: 'a', worker_id: 'w' },
      payload: {
        job_id: 'job-M',
        attempt_id: 'a',
        worker_id: 'w',
        attempt_number: 1,
        failure_kind: 'FAILED',
        reason: 'boom',
        exit_code: 1,
        retry_scheduled: false,
      },
    });
    await pub.publisher.publish(event);

    expect((await rA.waitFor((m) => m['type'] === 'event'))['event']).toEqual(event);
    expect((await rB.waitFor((m) => m['type'] === 'event'))['event']).toEqual(event);

    cA.close();
    cB.close();
    await pub.close();
  });

  it('a client on gateway B is unaffected when gateway A stops', async () => {
    const sharedChannel = `forge:test:multigw:${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const gwA = await startTestGateway({ channel: sharedChannel });
    const gwB = await startTestGateway({ channel: sharedChannel });
    gateways.push(gwB); // gwA is stopped explicitly below

    const pub = await makeChannelPublisher(sharedChannel);
    const cB = await openClient(gwB.port);
    const rB = cB.rx;
    await rB.waitFor((m) => m['type'] === 'ready');
    cB.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-N' } }));
    await rB.waitFor((m) => m['type'] === 'subscribed');

    await gwA.stop();

    const event = createForgeEvent('JobStarted', {
      correlation: { run_id: 'run-N', job_id: 'job-N', attempt_id: 'a', worker_id: 'w' },
      payload: { job_id: 'job-N', attempt_id: 'a', worker_id: 'w', attempt_number: 1 },
    });
    await pub.publisher.publish(event);

    expect((await rB.waitFor((m) => m['type'] === 'event'))['event']).toEqual(event);

    cB.close();
    await pub.close();
  });
});
