import { describe, it, expect } from 'vitest';
import { JobAttempt } from './job-attempt.js';
import { createJobAttemptId, createJobId } from './types.js';
import { InvalidStateTransitionError } from './errors.js';

describe('JobAttempt Domain Model', () => {
  it('creates an attempt with attempt number 1', () => {
    const attempt = new JobAttempt({
      id: createJobAttemptId('att-1'),
      jobId: createJobId('job-1'),
      attemptNumber: 1,
    });

    expect(attempt.attemptNumber).toBe(1);
    expect(attempt.status).toBe('PENDING');
    expect(attempt.isTerminal()).toBe(false);
  });

  it('rejects attempt number less than 1', () => {
    expect(() => {
      new JobAttempt({
        id: createJobAttemptId('att-0'),
        jobId: createJobId('job-1'),
        attemptNumber: 0,
      });
    }).toThrow('JobAttempt attemptNumber must be at least 1');
  });

  it('tracks execution lifecycle and exit codes', () => {
    const attempt = new JobAttempt({
      id: createJobAttemptId('att-1'),
      jobId: createJobId('job-1'),
      attemptNumber: 1,
    });

    attempt.start('2026-09-06T10:00:00.000Z');
    expect(attempt.status).toBe('RUNNING');
    expect(attempt.startedAt).toBe('2026-09-06T10:00:00.000Z');

    attempt.succeed(0, '2026-09-06T10:01:00.000Z');
    expect(attempt.status).toBe('SUCCEEDED');
    expect(attempt.exitCode).toBe(0);
    expect(attempt.finishedAt).toBe('2026-09-06T10:01:00.000Z');
    expect(attempt.isTerminal()).toBe(true);
  });

  it('tracks failure reason on error', () => {
    const attempt = new JobAttempt({
      id: createJobAttemptId('att-2'),
      jobId: createJobId('job-1'),
      attemptNumber: 2,
    });

    attempt.start();
    attempt.fail(137, 'Container out of memory (OOMKilled)');

    expect(attempt.status).toBe('FAILED');
    expect(attempt.exitCode).toBe(137);
    expect(attempt.failureReason).toBe('Container out of memory (OOMKilled)');
  });

  it('tracks timeout status', () => {
    const attempt = new JobAttempt({
      id: createJobAttemptId('att-3'),
      jobId: createJobId('job-1'),
      attemptNumber: 3,
    });

    attempt.start();
    attempt.timeout();

    expect(attempt.status).toBe('TIMED_OUT');
    expect(attempt.failureReason).toBe('Execution timed out');
  });

  it('rejects illegal transitions once terminal', () => {
    const attempt = new JobAttempt({
      id: createJobAttemptId('att-4'),
      jobId: createJobId('job-1'),
      attemptNumber: 1,
    });

    attempt.start();
    attempt.succeed();

    expect(() => attempt.fail(1)).toThrow(InvalidStateTransitionError);
  });

  it('serializes cleanly to JSON', () => {
    const attempt = new JobAttempt({
      id: createJobAttemptId('att-5'),
      jobId: createJobId('job-1'),
      attemptNumber: 1,
      startedAt: '2026-09-06T10:00:00.000Z',
      finishedAt: '2026-09-06T10:02:00.000Z',
      exitCode: 0,
      initialStatus: 'SUCCEEDED',
    });

    expect(attempt.toJSON()).toEqual({
      id: 'att-5',
      jobId: 'job-1',
      attemptNumber: 1,
      status: 'SUCCEEDED',
      startedAt: '2026-09-06T10:00:00.000Z',
      finishedAt: '2026-09-06T10:02:00.000Z',
      exitCode: 0,
    });
  });
});
