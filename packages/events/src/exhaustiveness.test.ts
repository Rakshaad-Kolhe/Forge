import { describe, it, expect } from 'vitest';
import { FORGE_EVENT_TYPES, type ForgeEventType } from './envelope.js';
import { createForgeEvent } from './factory.js';
import type { ForgeEvent, ForgeEventPayloadMap } from './events.js';
import { assertNever, summarizeForgeEvent } from './summarize.js';

const payloads: { [K in ForgeEventType]: ForgeEventPayloadMap[K] } = {
  PipelineCreated: { pipeline_id: 'pl-1', name: 'ci', step_count: 2 },
  PipelineQueued: { pipeline_id: 'pl-1', run_id: 'run-1' },
  PipelineCompleted: { run_id: 'run-1', status: 'FAILED', job_count: 2 },
  JobQueued: { job_id: 'job-1', run_id: 'run-1', priority: 0, attempt_number: 2 },
  JobClaimed: {
    job_id: 'job-1',
    worker_id: 'w-1',
    lease_id: 'lease-1',
    lease_expires_at: '2026-09-08T12:00:30.000Z',
  },
  JobStarted: { job_id: 'job-1', attempt_id: 'a-1', worker_id: 'w-1', attempt_number: 1 },
  JobLogChunk: {
    job_id: 'job-1',
    attempt_id: 'a-1',
    sequence: 0,
    stream: 'stderr',
    chunk: 'x',
    byte_offset: 0,
    truncated: true,
    final: true,
  },
  JobSucceeded: {
    job_id: 'job-1',
    attempt_id: 'a-1',
    worker_id: 'w-1',
    attempt_number: 1,
    duration_ms: 5,
    exit_code: 0,
  },
  JobFailed: {
    job_id: 'job-1',
    attempt_id: 'a-1',
    worker_id: 'w-1',
    attempt_number: 1,
    failure_kind: 'TIMED_OUT',
    reason: 'timeout',
    exit_code: null,
    retry_scheduled: true,
    next_attempt_at: '2026-09-08T12:05:00.000Z',
  },
  JobCancelled: { job_id: 'job-1', attempt_id: 'a-1', worker_id: 'w-1', attempt_number: 1 },
  WorkerRegistered: {
    worker_id: 'w-1',
    capabilities: ['docker'],
    cpu_cores: 4,
    memory_bytes: 1,
  },
  WorkerHeartbeat: { worker_id: 'w-1', status: 'DRAINING' },
  WorkerLost: {
    worker_id: 'w-1',
    job_id: 'job-1',
    lease_id: 'lease-1',
    recovery_action: 'DEAD_LETTERED',
    dead_letter_reason: 'RETRY_EXHAUSTED',
  },
};

describe('exhaustiveness', () => {
  it('summarizeForgeEvent handles every event type with a non-empty string', () => {
    for (const type of FORGE_EVENT_TYPES) {
      const event = createForgeEvent(type, { payload: payloads[type] }) as ForgeEvent;
      const summary = summarizeForgeEvent(event);
      expect(typeof summary).toBe('string');
      expect(summary.length).toBeGreaterThan(0);
    }
  });

  it('assertNever throws if ever reached at runtime', () => {
    expect(() => assertNever('unreachable' as never)).toThrow(/Unhandled ForgeEvent variant/);
  });

  it('rejects an unknown discriminant via the default branch', () => {
    const bogus = { ...createForgeEvent('WorkerHeartbeat', { payload: payloads.WorkerHeartbeat }) };
    (bogus as { event_type: string }).event_type = 'NotARealEvent';
    expect(() => summarizeForgeEvent(bogus as unknown as ForgeEvent)).toThrow(
      /Unhandled ForgeEvent variant/,
    );
  });
});

/*
 * Compile-time guard (documentation): adding a member to `ForgeEvent` without a `case` in
 * `summarizeForgeEvent` fails `tsc` at the `default: return assertNever(event)` line, because
 * `event` would no longer be narrowed to `never`. This block proves the union is closed.
 */
// @ts-expect-error - 'DefinitelyNotAnEvent' is not assignable to ForgeEventType
const _closedUnionCheck: ForgeEventType = 'DefinitelyNotAnEvent';
void _closedUnionCheck;
