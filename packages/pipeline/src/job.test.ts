import { describe, it, expect } from 'vitest';
import { Job } from './job.js';
import { createJobId, createPipelineRunId } from './types.js';
import { InvalidStateTransitionError } from './errors.js';

describe('Job Domain Model', () => {
  it('creates a job with valid initial properties', () => {
    const job = new Job({
      id: createJobId('job-test'),
      pipelineRunId: createPipelineRunId('run-1'),
      stepName: 'test',
      command: 'npm test',
      dependsOn: ['install'],
    });

    expect(job.id).toBe('job-test');
    expect(job.pipelineRunId).toBe('run-1');
    expect(job.stepName).toBe('test');
    expect(job.command).toBe('npm test');
    expect(job.dependsOn).toEqual(['install']);
    expect(job.status).toBe('PENDING');
    expect(job.attempts).toHaveLength(0);
    expect(job.currentAttempt).toBeUndefined();
  });

  it('spawns sequential attempts without mutating historical attempts', () => {
    const job = new Job({
      id: createJobId('job-1'),
      pipelineRunId: createPipelineRunId('run-1'),
      stepName: 'compile',
      command: 'tsc',
    });

    // Attempt 1 fails
    const attempt1 = job.createAttempt();
    expect(attempt1.attemptNumber).toBe(1);
    expect(attempt1.id).toBe('job-1-attempt-1');
    expect(job.attempts).toHaveLength(1);
    expect(job.currentAttempt).toBe(attempt1);

    attempt1.start();
    attempt1.fail(1, 'Compile error');

    // Attempt 2 succeeds
    const attempt2 = job.createAttempt();
    expect(attempt2.attemptNumber).toBe(2);
    expect(attempt2.id).toBe('job-1-attempt-2');
    expect(job.attempts).toHaveLength(2);
    expect(job.currentAttempt).toBe(attempt2);

    // Verify historical attempt 1 is untouched
    expect(job.attempts[0]!.status).toBe('FAILED');
    expect(job.attempts[0]!.exitCode).toBe(1);
    expect(job.attempts[1]!.status).toBe('PENDING');

    attempt2.start();
    attempt2.succeed(0);
    expect(job.attempts[1]!.status).toBe('SUCCEEDED');
  });

  it('manages Job status lifecycle correctly', () => {
    const job = new Job({
      id: createJobId('job-lifecycle'),
      pipelineRunId: createPipelineRunId('run-1'),
      stepName: 'lint',
      command: 'eslint .',
    });

    job.markQueued();
    expect(job.status).toBe('QUEUED');

    job.start();
    expect(job.status).toBe('RUNNING');

    job.succeed();
    expect(job.status).toBe('SUCCEEDED');
    expect(job.isTerminal()).toBe(true);
  });

  it('rejects terminal state regressions', () => {
    const job = new Job({
      id: createJobId('job-terminal'),
      pipelineRunId: createPipelineRunId('run-1'),
      stepName: 'test',
      command: 'vitest',
    });

    job.markQueued();
    job.start();
    job.fail();

    expect(() => job.start()).toThrow(InvalidStateTransitionError);
    expect(() => job.succeed()).toThrow(InvalidStateTransitionError);
  });

  it('serializes cleanly to JSON', () => {
    const job = new Job({
      id: createJobId('job-serialize'),
      pipelineRunId: createPipelineRunId('run-1'),
      stepName: 'build',
      command: 'npm run build',
      dependsOn: ['test'],
    });

    job.createAttempt();

    expect(job.toJSON()).toEqual({
      id: 'job-serialize',
      pipelineRunId: 'run-1',
      stepName: 'build',
      command: 'npm run build',
      dependsOn: ['test'],
      priority: 0,
      status: 'PENDING',

      attempts: [
        {
          id: 'job-serialize-attempt-1',
          jobId: 'job-serialize',
          attemptNumber: 1,
          status: 'PENDING',
        },
      ],
    });
  });
});
