import { describe, it, expect } from 'vitest';
import { FORGE_EVENT_TYPES, type ForgeEventType } from './envelope.js';
import { createForgeEvent } from './factory.js';
import type { ForgeEvent, ForgeEventPayloadMap } from './events.js';
import { forgeEventSchema, parseForgeEvent, safeParseForgeEvent } from './schema.js';

const FIXED_NOW = new Date('2026-09-08T12:00:00.000Z');
const ISO = '2026-09-08T12:00:30.000Z';

/** One valid payload per event type, populated from realistic field values. */
const payloads: { [K in ForgeEventType]: ForgeEventPayloadMap[K] } = {
  PipelineCreated: { pipeline_id: 'pl-1', name: 'ci', step_count: 3 },
  PipelineQueued: { pipeline_id: 'pl-1', run_id: 'run-1' },
  PipelineCompleted: { run_id: 'run-1', status: 'SUCCEEDED', job_count: 3 },
  JobQueued: {
    job_id: 'job-1',
    run_id: 'run-1',
    priority: 0,
    attempt_number: 2,
    next_attempt_at: ISO,
  },
  JobClaimed: { job_id: 'job-1', worker_id: 'w-1', lease_id: 'lease-1', lease_expires_at: ISO },
  JobStarted: {
    job_id: 'job-1',
    attempt_id: 'job-1-attempt-1',
    worker_id: 'w-1',
    attempt_number: 1,
  },
  JobLogChunk: {
    job_id: 'job-1',
    attempt_id: 'job-1-attempt-1',
    sequence: 0,
    stream: 'stdout',
    chunk: 'hello',
    byte_offset: 0,
    truncated: false,
    final: true,
  },
  JobSucceeded: {
    job_id: 'job-1',
    attempt_id: 'job-1-attempt-1',
    worker_id: 'w-1',
    attempt_number: 1,
    duration_ms: 1200,
    exit_code: 0,
  },
  JobFailed: {
    job_id: 'job-1',
    attempt_id: 'job-1-attempt-1',
    worker_id: 'w-1',
    attempt_number: 1,
    failure_kind: 'FAILED',
    reason: 'Process exited with code 1',
    exit_code: 1,
    retry_scheduled: false,
  },
  JobCancelled: {
    job_id: 'job-1',
    attempt_id: 'job-1-attempt-1',
    worker_id: 'w-1',
    attempt_number: 1,
  },
  WorkerRegistered: {
    worker_id: 'w-1',
    hostname: 'host-1',
    capabilities: ['docker'],
    cpu_cores: 8,
    memory_bytes: 16_000_000_000,
  },
  WorkerHeartbeat: { worker_id: 'w-1', status: 'READY' },
  WorkerLost: {
    worker_id: 'w-1',
    job_id: 'job-1',
    lease_id: 'lease-1',
    recovery_action: 'REQUEUED',
  },
};

function sampleEvent<K extends ForgeEventType>(type: K): ForgeEvent {
  return createForgeEvent(type, { payload: payloads[type] }, { now: FIXED_NOW }) as ForgeEvent;
}

describe('forgeEventSchema — contract', () => {
  it('the runtime type list matches the discriminated-union options', () => {
    expect(FORGE_EVENT_TYPES.length).toBe(forgeEventSchema.options.length);
    const schemaTypes = forgeEventSchema.options
      .map((opt) => (opt.shape.event_type as { value: string }).value)
      .sort();
    expect(schemaTypes).toEqual([...FORGE_EVENT_TYPES].sort());
  });

  it.each(FORGE_EVENT_TYPES)('validates a well-formed %s event', (type) => {
    const event = sampleEvent(type);
    const parsed = parseForgeEvent(event);
    expect(parsed.event_type).toBe(type);
    expect(parsed.event_id).toBe(event.event_id);
  });

  it('requires event_id', () => {
    const { event_id: _omit, ...rest } = sampleEvent('WorkerHeartbeat');
    expect(safeParseForgeEvent(rest).success).toBe(false);
  });

  it('rejects a non-UUID event_id', () => {
    const event = { ...sampleEvent('WorkerHeartbeat'), event_id: 'not-a-uuid' };
    expect(safeParseForgeEvent(event).success).toBe(false);
  });

  it('requires occurred_at to be an ISO-8601 timestamp', () => {
    const event = { ...sampleEvent('WorkerHeartbeat'), occurred_at: 'yesterday' };
    expect(safeParseForgeEvent(event).success).toBe(false);
  });

  it('requires version to equal the current schema version', () => {
    const event = { ...sampleEvent('WorkerHeartbeat'), version: 2 };
    const result = safeParseForgeEvent(event);
    expect(result.success).toBe(false);
  });

  it('rejects an unknown event_type', () => {
    const event = { ...sampleEvent('WorkerHeartbeat'), event_type: 'JobExploded' };
    expect(safeParseForgeEvent(event).success).toBe(false);
  });

  it('rejects a payload that does not match its event_type', () => {
    const event = { ...sampleEvent('JobClaimed'), payload: { worker_id: 'w-1' } };
    expect(safeParseForgeEvent(event).success).toBe(false);
  });

  it('rejects a missing required correlation-style payload field', () => {
    const event = sampleEvent('JobStarted');
    const broken = { ...event, payload: { ...event.payload, job_id: undefined } };
    expect(safeParseForgeEvent(broken).success).toBe(false);
  });

  it('strips unknown fields rather than failing (forward compatibility)', () => {
    const event = sampleEvent('WorkerHeartbeat');
    const withExtra = {
      ...event,
      trace_id: 'abc',
      payload: { ...event.payload, region: 'us-east-1' },
    };
    const parsed = parseForgeEvent(withExtra);
    expect((parsed as unknown as Record<string, unknown>)['trace_id']).toBeUndefined();
    expect((parsed.payload as unknown as Record<string, unknown>)['region']).toBeUndefined();
    expect((parsed.payload as { worker_id?: string }).worker_id).toBe('w-1');
  });

  it('accepts optional correlation ids and rejects empty-string ids', () => {
    const ok = { ...sampleEvent('WorkerHeartbeat'), worker_id: 'w-1' };
    expect(safeParseForgeEvent(ok).success).toBe(true);

    const bad = { ...sampleEvent('WorkerHeartbeat'), job_id: '' };
    expect(safeParseForgeEvent(bad).success).toBe(false);
  });
});
