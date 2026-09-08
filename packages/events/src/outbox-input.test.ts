import { describe, expect, it } from 'vitest';
import { createForgeEvent } from './factory.js';
import { toOutboxEnqueueInput } from './outbox-input.js';

describe('toOutboxEnqueueInput', () => {
  it('projects envelope fields and lifts correlation ids', () => {
    const event = createForgeEvent(
      'JobStarted',
      {
        correlation: { run_id: 'run1', job_id: 'job1', attempt_id: 'att1', worker_id: 'w1' },
        payload: { job_id: 'job1', attempt_id: 'att1', worker_id: 'w1', attempt_number: 1 },
      },
      {
        eventId: '11111111-1111-4111-8111-111111111111',
        now: new Date('2026-09-08T00:00:00.000Z'),
      },
    );

    const input = toOutboxEnqueueInput(event);

    expect(input.id).toMatch(/^outbox_[0-9a-f-]{36}$/);
    expect(input.eventId).toBe('11111111-1111-4111-8111-111111111111');
    expect(input.eventType).toBe('JobStarted');
    expect(input.version).toBe(1);
    expect(input.occurredAt).toBe('2026-09-08T00:00:00.000Z');
    expect(input.correlation).toEqual({
      runId: 'run1',
      jobId: 'job1',
      attemptId: 'att1',
      workerId: 'w1',
    });
    expect(input.payload).toBe(event); // the frozen envelope itself
  });

  it('omits absent correlation keys', () => {
    const event = createForgeEvent('WorkerHeartbeat', {
      correlation: { worker_id: 'w1' },
      payload: { worker_id: 'w1', status: 'READY' },
    });
    const input = toOutboxEnqueueInput(event);
    expect(input.correlation).toEqual({ workerId: 'w1' });
  });

  it('generates a distinct outbox row id each call', () => {
    const event = createForgeEvent('WorkerHeartbeat', {
      correlation: { worker_id: 'w1' },
      payload: { worker_id: 'w1', status: 'READY' },
    });
    expect(toOutboxEnqueueInput(event).id).not.toBe(toOutboxEnqueueInput(event).id);
  });
});
