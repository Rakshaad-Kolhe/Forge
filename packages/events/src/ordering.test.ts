import { describe, it, expect } from 'vitest';
import { createForgeEvent } from './factory.js';
import type { ForgeEventType } from './envelope.js';
import type { ForgeEvent } from './events.js';
import { InProcessEventBus } from './in-process-bus.js';

/**
 * Documented guarantee (see docs/architecture/events.md § Ordering):
 * a single producer that sequentially awaits publish() calls has every subscriber observe
 * the events in that exact order. No cross-producer / global ordering is claimed.
 */
describe('event ordering — per-producer logical order', () => {
  it('observes JobQueued -> JobClaimed -> JobStarted -> JobSucceeded in order', async () => {
    const bus = new InProcessEventBus();
    const observed: ForgeEventType[] = [];
    bus.subscribe(async (e) => {
      await Promise.resolve();
      observed.push(e.event_type);
    });

    const now = new Date('2026-09-08T12:00:00.000Z');
    const sequence: ForgeEvent[] = [
      createForgeEvent(
        'JobQueued',
        {
          correlation: { run_id: 'run-1', job_id: 'job-1' },
          payload: { job_id: 'job-1', run_id: 'run-1', priority: 0, attempt_number: 1 },
        },
        { now },
      ),
      createForgeEvent(
        'JobClaimed',
        {
          correlation: { job_id: 'job-1', worker_id: 'w-1' },
          payload: {
            job_id: 'job-1',
            worker_id: 'w-1',
            lease_id: 'lease-1',
            lease_expires_at: '2026-09-08T12:00:30.000Z',
          },
        },
        { now },
      ),
      createForgeEvent(
        'JobStarted',
        {
          correlation: { job_id: 'job-1', attempt_id: 'job-1-attempt-1', worker_id: 'w-1' },
          payload: {
            job_id: 'job-1',
            attempt_id: 'job-1-attempt-1',
            worker_id: 'w-1',
            attempt_number: 1,
          },
        },
        { now },
      ),
      createForgeEvent(
        'JobSucceeded',
        {
          correlation: { job_id: 'job-1', attempt_id: 'job-1-attempt-1', worker_id: 'w-1' },
          payload: {
            job_id: 'job-1',
            attempt_id: 'job-1-attempt-1',
            worker_id: 'w-1',
            attempt_number: 1,
            duration_ms: 10,
            exit_code: 0,
          },
        },
        { now },
      ),
    ];

    for (const event of sequence) {
      await bus.publish(event);
    }

    expect(observed).toEqual(['JobQueued', 'JobClaimed', 'JobStarted', 'JobSucceeded']);
    await bus.close();
  });

  it('delivers to multiple subscribers each in registration order for a single publish', async () => {
    const bus = new InProcessEventBus();
    const calls: string[] = [];
    bus.subscribe(() => void calls.push('sub-1'));
    bus.subscribe(() => void calls.push('sub-2'));
    bus.subscribe(() => void calls.push('sub-3'));

    await bus.publish(
      createForgeEvent('WorkerHeartbeat', { payload: { worker_id: 'w-1', status: 'READY' } }),
    );

    expect(calls).toEqual(['sub-1', 'sub-2', 'sub-3']);
    await bus.close();
  });
});
