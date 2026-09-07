import { describe, expect, it } from 'vitest';
import { InvalidJobPriorityError, PipelineValidationError } from './errors.js';
import { Job } from './job.js';
import { PipelineRun } from './pipeline-run.js';
import { PipelineStep } from './pipeline-step.js';
import { Pipeline } from './pipeline.js';
import {
  checkJobPriorityValidity,
  DEFAULT_JOB_PRIORITY,
  MAX_JOB_PRIORITY,
  MIN_JOB_PRIORITY,
  validateJobPriority,
} from './priority.js';
import { createJobId, createPipelineId, createPipelineRunId } from './types.js';

describe('Job Priority Domain Logic & Validation', () => {
  describe('checkJobPriorityValidity', () => {
    it('accepts undefined and null as valid (defaultable)', () => {
      expect(checkJobPriorityValidity(undefined).valid).toBe(true);
      expect(checkJobPriorityValidity(null).valid).toBe(true);
    });

    it('accepts valid integers within [-1000, 1000]', () => {
      expect(checkJobPriorityValidity(0).valid).toBe(true);
      expect(checkJobPriorityValidity(10).valid).toBe(true);
      expect(checkJobPriorityValidity(100).valid).toBe(true);
      expect(checkJobPriorityValidity(MAX_JOB_PRIORITY).valid).toBe(true);
      expect(checkJobPriorityValidity(-10).valid).toBe(true);
      expect(checkJobPriorityValidity(MIN_JOB_PRIORITY).valid).toBe(true);
    });

    it('rejects non-numeric types', () => {
      expect(checkJobPriorityValidity('high').valid).toBe(false);
      expect(checkJobPriorityValidity({}).valid).toBe(false);
      expect(checkJobPriorityValidity([]).valid).toBe(false);
      expect(checkJobPriorityValidity(true).valid).toBe(false);
    });

    it('rejects NaN and Infinity', () => {
      expect(checkJobPriorityValidity(Number.NaN).valid).toBe(false);
      expect(checkJobPriorityValidity(Number.POSITIVE_INFINITY).valid).toBe(false);
      expect(checkJobPriorityValidity(Number.NEGATIVE_INFINITY).valid).toBe(false);
    });

    it('rejects non-integer floating point numbers', () => {
      expect(checkJobPriorityValidity(1.5).valid).toBe(false);
      expect(checkJobPriorityValidity(-0.1).valid).toBe(false);
      expect(checkJobPriorityValidity(99.99).valid).toBe(false);
    });

    it('rejects values outside bounded range [-1000, 1000]', () => {
      expect(checkJobPriorityValidity(1001).valid).toBe(false);
      expect(checkJobPriorityValidity(-1001).valid).toBe(false);
      expect(checkJobPriorityValidity(50000).valid).toBe(false);
    });
  });

  describe('validateJobPriority', () => {
    it('returns DEFAULT_JOB_PRIORITY (0) when omitted', () => {
      expect(validateJobPriority(undefined)).toBe(DEFAULT_JOB_PRIORITY);
      expect(validateJobPriority(null)).toBe(DEFAULT_JOB_PRIORITY);
    });

    it('returns exact integer when valid', () => {
      expect(validateJobPriority(0)).toBe(0);
      expect(validateJobPriority(50)).toBe(50);
      expect(validateJobPriority(-100)).toBe(-100);
      expect(validateJobPriority(1000)).toBe(1000);
    });

    it('throws InvalidJobPriorityError on invalid input', () => {
      expect(() => validateJobPriority('10')).toThrow(InvalidJobPriorityError);
      expect(() => validateJobPriority(2.5)).toThrow(InvalidJobPriorityError);
      expect(() => validateJobPriority(2000)).toThrow(InvalidJobPriorityError);
      expect(() => validateJobPriority(-2000)).toThrow(InvalidJobPriorityError);
    });
  });

  describe('PipelineStep Priority Integration', () => {
    it('defaults step priority to 0 when unspecified', () => {
      const step = new PipelineStep({ name: 'build', command: 'make build' });
      expect(step.priority).toBe(0);
      expect(step.toJSON().priority).toBe(0);
    });

    it('stores declared step priority', () => {
      const step = new PipelineStep({
        name: 'urgent-deploy',
        command: 'make deploy',
        priority: 100,
      });
      expect(step.priority).toBe(100);
      expect(step.toJSON().priority).toBe(100);
    });

    it('rejects invalid priority during PipelineStep construction', () => {
      expect(
        () =>
          new PipelineStep({
            name: 'bad-step',
            command: 'echo bad',
            priority: 5000,
          }),
      ).toThrow(PipelineValidationError);
    });
  });

  describe('Job Priority Integration', () => {
    it('defaults job priority to 0 when unspecified', () => {
      const job = new Job({
        id: createJobId('job-1'),
        pipelineRunId: createPipelineRunId('run-1'),
        stepName: 'test',
        command: 'npm test',
      });
      expect(job.priority).toBe(0);
      expect(job.toJSON().priority).toBe(0);
    });

    it('stores declared job priority and serializes to JSON', () => {
      const job = new Job({
        id: createJobId('job-2'),
        pipelineRunId: createPipelineRunId('run-1'),
        stepName: 'test',
        command: 'npm test',
        priority: 75,
      });
      expect(job.priority).toBe(75);
      expect(job.toJSON().priority).toBe(75);
    });

    it('rejects invalid priority during Job construction', () => {
      expect(
        () =>
          new Job({
            id: createJobId('job-bad'),
            pipelineRunId: createPipelineRunId('run-1'),
            stepName: 'test',
            command: 'npm test',
            priority: Number.NaN,
          }),
      ).toThrow(InvalidJobPriorityError);
    });
  });

  describe('PipelineRun Propagation', () => {
    it('propagates step priority faithfully to generated jobs in PipelineRun.create()', () => {
      const pipeline = new Pipeline({
        id: createPipelineId('pipe-priority-test'),
        name: 'Priority Pipeline',
        steps: [
          { name: 'background-task', command: 'echo low', priority: -50 },
          { name: 'normal-task', command: 'echo normal' }, // default 0
          { name: 'critical-task', command: 'echo high', priority: 100 },
        ],
      });

      const run = PipelineRun.create(createPipelineRunId('run-priority-1'), pipeline);
      const jobs = run.getJobs();

      expect(jobs).toHaveLength(3);
      expect(jobs.find((j) => j.stepName === 'background-task')?.priority).toBe(-50);
      expect(jobs.find((j) => j.stepName === 'normal-task')?.priority).toBe(0);
      expect(jobs.find((j) => j.stepName === 'critical-task')?.priority).toBe(100);
    });
  });
});
