import { describe, expect, it } from 'vitest';
import { createForgeEvent } from '@forge/events';
import { keysForEvent, subscriptionKey } from './subscription.js';

describe('subscriptionKey', () => {
  it('namespaces by kind', () => {
    expect(subscriptionKey({ kind: 'run', id: 'R' })).toBe('run:R');
    expect(subscriptionKey({ kind: 'job', id: 'J' })).toBe('job:J');
    expect(subscriptionKey({ kind: 'pipeline', id: 'P' })).toBe('pipeline:P');
  });
});

describe('keysForEvent', () => {
  it('derives every correlation key an event carries', () => {
    const event = createForgeEvent('JobStarted', {
      correlation: { pipeline_id: 'P', run_id: 'R', job_id: 'J', attempt_id: 'A', worker_id: 'W' },
      payload: { job_id: 'J', attempt_id: 'A', worker_id: 'W', attempt_number: 1 },
    });
    expect(keysForEvent(event).sort()).toEqual(['job:J', 'pipeline:P', 'run:R']);
  });

  it('omits keys for absent correlation fields', () => {
    const event = createForgeEvent('WorkerHeartbeat', {
      correlation: { worker_id: 'W' },
      payload: { worker_id: 'W', status: 'READY' },
    });
    expect(keysForEvent(event)).toEqual([]);
  });
});
