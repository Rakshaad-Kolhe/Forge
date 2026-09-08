import { describe, it, expect } from 'vitest';
import { deriveLogChunkEvents, DEFAULT_LOG_CHUNK_BYTES } from './log-chunk.js';
import { parseForgeEvent } from './schema.js';

const CORR = { job_id: 'job-1', attempt_id: 'job-1-attempt-1' };

describe('deriveLogChunkEvents', () => {
  it('returns [] when there is no output', () => {
    expect(deriveLogChunkEvents({ stdout: '', stderr: '', truncated: false }, CORR)).toEqual([]);
  });

  it('emits one chunk per stream for small output, stdout before stderr', () => {
    const events = deriveLogChunkEvents(
      { stdout: 'hello world', stderr: 'oops', truncated: false },
      CORR,
    );
    expect(events.map((e) => e.payload.stream)).toEqual(['stdout', 'stderr']);
    expect(events.map((e) => e.payload.sequence)).toEqual([0, 1]);
    expect(events[0]?.payload.chunk).toBe('hello world');
    expect(events[1]?.payload.chunk).toBe('oops');
    expect(events[0]?.payload.byte_offset).toBe(0);
    expect(events[1]?.payload.byte_offset).toBe(0);
  });

  it('splits a large stream into monotonic byte-bounded chunks that reassemble exactly', () => {
    const stdout = 'a'.repeat(200_000);
    const events = deriveLogChunkEvents({ stdout, stderr: 'tail', truncated: true }, CORR, {
      chunkBytes: DEFAULT_LOG_CHUNK_BYTES,
    });

    const stdoutChunks = events.filter((e) => e.payload.stream === 'stdout');
    const stderrChunks = events.filter((e) => e.payload.stream === 'stderr');
    expect(stdoutChunks).toHaveLength(4); // 65536 * 3 + 3392
    expect(stderrChunks).toHaveLength(1);

    // sequence is 0..n monotonic across the whole derivation
    expect(events.map((e) => e.payload.sequence)).toEqual([0, 1, 2, 3, 4]);
    // stdout chunks precede stderr chunks
    expect(events.map((e) => e.payload.stream)).toEqual([
      'stdout',
      'stdout',
      'stdout',
      'stdout',
      'stderr',
    ]);
    // per-stream byte offsets advance by chunk size
    expect(stdoutChunks.map((e) => e.payload.byte_offset)).toEqual([0, 65536, 131072, 196608]);
    // reassembly is lossless
    expect(stdoutChunks.map((e) => e.payload.chunk).join('')).toBe(stdout);
    expect(stderrChunks.map((e) => e.payload.chunk).join('')).toBe('tail');
  });

  it('marks only the last event final and propagates the truncation flag to all chunks', () => {
    const events = deriveLogChunkEvents(
      { stdout: 'x'.repeat(100_000), stderr: 'y'.repeat(100_000), truncated: true },
      CORR,
      { chunkBytes: 65536 },
    );
    expect(events.every((e) => e.payload.truncated)).toBe(true);
    expect(events.filter((e) => e.payload.final)).toHaveLength(1);
    expect(events[events.length - 1]?.payload.final).toBe(true);
    expect(events.slice(0, -1).every((e) => e.payload.final === false)).toBe(true);
  });

  it('carries job/attempt correlation and validates against the schema', () => {
    const events = deriveLogChunkEvents({ stdout: 'line', stderr: '', truncated: false }, CORR);
    for (const event of events) {
      expect(event.job_id).toBe('job-1');
      expect(event.attempt_id).toBe('job-1-attempt-1');
      expect(event.payload.job_id).toBe('job-1');
      expect(() => parseForgeEvent(event)).not.toThrow();
    }
  });

  it('accepts a deterministic id factory for reproducible tests', () => {
    let n = 0;
    const events = deriveLogChunkEvents({ stdout: 'abcdef', stderr: '', truncated: false }, CORR, {
      chunkBytes: 2,
      eventIdFactory: () => `00000000-0000-4000-8000-00000000000${n++}`,
    });
    expect(events.map((e) => e.event_id)).toEqual([
      '00000000-0000-4000-8000-000000000000',
      '00000000-0000-4000-8000-000000000001',
      '00000000-0000-4000-8000-000000000002',
    ]);
  });
});
