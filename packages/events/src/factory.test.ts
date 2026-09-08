import { describe, it, expect } from 'vitest';
import { EVENT_SCHEMA_VERSION } from './envelope.js';
import { createForgeEvent, isForgeEventShape } from './factory.js';
import { EVENT_ID_PATTERN } from './event-id.js';
import { parseForgeEvent } from './schema.js';

describe('createForgeEvent', () => {
  it('stamps event_id, occurred_at and version onto every event', () => {
    const event = createForgeEvent('JobStarted', {
      correlation: { job_id: 'job-1', attempt_id: 'job-1-attempt-1', worker_id: 'w-1' },
      payload: {
        job_id: 'job-1',
        attempt_id: 'job-1-attempt-1',
        worker_id: 'w-1',
        attempt_number: 1,
      },
    });

    expect(event.event_type).toBe('JobStarted');
    expect(event.event_id).toMatch(EVENT_ID_PATTERN);
    expect(event.version).toBe(EVENT_SCHEMA_VERSION);
    expect(() => new Date(event.occurred_at).toISOString()).not.toThrow();
    expect(new Date(event.occurred_at).toISOString()).toBe(event.occurred_at);
  });

  it('passes correlation ids and payload through unchanged', () => {
    const event = createForgeEvent('JobClaimed', {
      correlation: { job_id: 'job-9', worker_id: 'w-9' },
      payload: {
        job_id: 'job-9',
        worker_id: 'w-9',
        lease_id: 'lease-9',
        lease_expires_at: '2026-09-08T12:00:30.000Z',
      },
    });

    expect(event.job_id).toBe('job-9');
    expect(event.worker_id).toBe('w-9');
    expect(event.pipeline_id).toBeUndefined();
    expect(event.payload.lease_id).toBe('lease-9');
  });

  it('omits absent correlation ids rather than setting them undefined', () => {
    const event = createForgeEvent('WorkerHeartbeat', {
      correlation: { worker_id: 'w-1' },
      payload: { worker_id: 'w-1', status: 'READY' },
    });
    expect(Object.prototype.hasOwnProperty.call(event, 'job_id')).toBe(false);
    expect(Object.prototype.hasOwnProperty.call(event, 'run_id')).toBe(false);
  });

  it('is deterministic when now and eventId are injected', () => {
    const now = new Date('2026-09-08T12:00:00.000Z');
    const event = createForgeEvent(
      'WorkerHeartbeat',
      { payload: { worker_id: 'w-1', status: 'READY' } },
      { now, eventId: '00000000-0000-4000-8000-000000000000' },
    );
    expect(event.occurred_at).toBe('2026-09-08T12:00:00.000Z');
    expect(event.event_id).toBe('00000000-0000-4000-8000-000000000000');
  });

  it('freezes the envelope (immutable after creation)', () => {
    const event = createForgeEvent('WorkerHeartbeat', {
      payload: { worker_id: 'w-1', status: 'READY' },
    });
    expect(Object.isFrozen(event)).toBe(true);
    expect(() => {
      (event as { event_id: string }).event_id = 'tampered';
    }).toThrow();
  });

  it('produces envelopes that pass runtime validation', () => {
    const event = createForgeEvent('JobSucceeded', {
      correlation: { job_id: 'job-1', attempt_id: 'job-1-attempt-1', worker_id: 'w-1' },
      payload: {
        job_id: 'job-1',
        attempt_id: 'job-1-attempt-1',
        worker_id: 'w-1',
        attempt_number: 1,
        duration_ms: 1234,
        exit_code: 0,
      },
    });
    expect(() => parseForgeEvent(event)).not.toThrow();
    expect(isForgeEventShape(event)).toBe(true);
  });
});
