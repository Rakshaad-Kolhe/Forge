import { describe, expect, it } from 'vitest';
import { createForgeEvent } from '@forge/events';
import { RealtimeEventDecodeError } from './errors.js';
import { deserializeForgeEvent, serializeForgeEvent } from './serialization.js';

const sampleEvent = createForgeEvent(
  'JobSucceeded',
  {
    correlation: { run_id: 'run-1', job_id: 'job-1', attempt_id: 'att-1', worker_id: 'w-1' },
    payload: {
      job_id: 'job-1',
      attempt_id: 'att-1',
      worker_id: 'w-1',
      attempt_number: 1,
      duration_ms: 1234,
      exit_code: 0,
    },
  },
  { eventId: '11111111-1111-4111-8111-111111111111', now: new Date('2026-01-02T03:04:05.000Z') },
);

describe('realtime serialization', () => {
  it('round-trips a ForgeEvent without mutating envelope fields', () => {
    const wire = serializeForgeEvent(sampleEvent);
    const decoded = deserializeForgeEvent(wire);
    expect(decoded).toEqual(sampleEvent);
    expect(decoded.event_id).toBe(sampleEvent.event_id);
    expect(decoded.event_type).toBe('JobSucceeded');
    expect(decoded.version).toBe(sampleEvent.version);
    expect(decoded.occurred_at).toBe('2026-01-02T03:04:05.000Z');
    expect(decoded.payload).toEqual(sampleEvent.payload);
  });

  it('rejects malformed JSON', () => {
    expect(() => deserializeForgeEvent('{not json')).toThrow(RealtimeEventDecodeError);
  });

  it('rejects a payload that fails schema validation', () => {
    const bad = JSON.stringify({ ...sampleEvent, version: 999 });
    expect(() => deserializeForgeEvent(bad)).toThrow(RealtimeEventDecodeError);
  });

  it('rejects an unknown event_type', () => {
    const bad = JSON.stringify({ ...sampleEvent, event_type: 'NotAThing' });
    expect(() => deserializeForgeEvent(bad)).toThrow(RealtimeEventDecodeError);
  });

  it('strips unknown additive fields on the wire (forward compatibility)', () => {
    const withExtra = JSON.stringify({ ...sampleEvent, futuristic_field: 'ignored' });
    const decoded = deserializeForgeEvent(withExtra) as unknown as Record<string, unknown>;
    expect(decoded['futuristic_field']).toBeUndefined();
  });

  it('refuses to serialize an invalid event', () => {
    const invalid = {
      ...sampleEvent,
      payload: { job_id: 'job-1' },
    } as unknown as typeof sampleEvent;
    expect(() => serializeForgeEvent(invalid)).toThrow(RealtimeEventDecodeError);
  });
});
