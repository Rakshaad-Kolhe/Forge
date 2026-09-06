import { describe, it, expect } from 'vitest';
import { Pipeline } from './pipeline.js';
import { PipelineRun } from './pipeline-run.js';
import { createJobId, createPipelineRunId } from './types.js';
import { DuplicateJobError, InvalidStateTransitionError } from './errors.js';
import { Job } from './job.js';

describe('PipelineRun Domain Model', () => {
  const samplePipeline = new Pipeline({
    id: 'pipe-ci',
    name: 'CI Pipeline',
    steps: [
      { name: 'install', command: 'npm ci' },
      { name: 'lint', command: 'npm run lint', dependsOn: ['install'] },
      { name: 'test', command: 'npm test', dependsOn: ['install'] },
      { name: 'build', command: 'npm run build', dependsOn: ['lint', 'test'] },
    ],
  });

  it('creates a PipelineRun with deterministic 1:1 job mapping from steps', () => {
    const runId = createPipelineRunId('run-100');
    const run = PipelineRun.create(runId, samplePipeline);

    expect(run.id).toBe('run-100');
    expect(run.pipelineId).toBe('pipe-ci');
    expect(run.pipelineName).toBe('CI Pipeline');
    expect(run.status).toBe('PENDING');

    const jobs = run.getJobs();
    expect(jobs).toHaveLength(4);

    const installJob = run.getJob('install');
    expect(installJob).toBeDefined();
    expect(installJob?.command).toBe('npm ci');
    expect(installJob?.dependsOn).toEqual([]);

    const buildJob = run.getJob('build');
    expect(buildJob).toBeDefined();
    expect(buildJob?.dependsOn).toEqual(['lint', 'test']);
  });

  it('rejects adding duplicate job for the same step', () => {
    const runId = createPipelineRunId('run-dup');
    const run = PipelineRun.create(runId, samplePipeline);

    const extraJob = new Job({
      id: createJobId('job-extra'),
      pipelineRunId: runId,
      stepName: 'install',
      command: 'npm install',
    });

    expect(() => run.addJob(extraJob)).toThrow(DuplicateJobError);
  });

  it('evaluates overall completion: SUCCEEDED when all jobs succeed', () => {
    const run = PipelineRun.create(createPipelineRunId('run-success'), samplePipeline);

    run.markQueued();
    run.start();
    expect(run.status).toBe('RUNNING');

    for (const job of run.getJobs()) {
      job.markQueued();
      job.start();
      job.succeed();
    }

    const completion = run.evaluateCompletion();
    expect(completion).toBe('SUCCEEDED');
    expect(run.status).toBe('SUCCEEDED');
    expect(run.isTerminal()).toBe(true);
  });

  it('evaluates overall completion: FAILED if any job fails', () => {
    const run = PipelineRun.create(createPipelineRunId('run-failure'), samplePipeline);

    run.markQueued();
    run.start();

    const installJob = run.getJob('install')!;
    installJob.markQueued();
    installJob.start();
    installJob.fail();

    const completion = run.evaluateCompletion();
    expect(completion).toBe('FAILED');
    expect(run.status).toBe('FAILED');
    expect(run.isTerminal()).toBe(true);
  });

  it('evaluates overall completion: FAILED if any job times out', () => {
    const run = PipelineRun.create(createPipelineRunId('run-timeout'), samplePipeline);

    run.markQueued();
    run.start();

    const testJob = run.getJob('test')!;
    testJob.markQueued();
    testJob.start();
    testJob.timeout();

    expect(run.evaluateCompletion()).toBe('FAILED');
  });

  it('evaluates overall completion: CANCELLED if any job is cancelled and none failed', () => {
    const run = PipelineRun.create(createPipelineRunId('run-cancel'), samplePipeline);

    run.markQueued();
    run.start();

    const installJob = run.getJob('install')!;
    installJob.markQueued();
    installJob.start();
    installJob.succeed();

    const lintJob = run.getJob('lint')!;
    lintJob.cancel();

    expect(run.evaluateCompletion()).toBe('CANCELLED');
    expect(run.status).toBe('CANCELLED');
  });

  it('remains in active status while jobs are still running', () => {
    const run = PipelineRun.create(createPipelineRunId('run-active'), samplePipeline);

    run.markQueued();
    run.start();

    const installJob = run.getJob('install')!;
    installJob.markQueued();
    installJob.start();
    installJob.succeed();

    expect(run.evaluateCompletion()).toBe('RUNNING');
  });

  it('rejects illegal state transitions from terminal states', () => {
    const run = PipelineRun.create(createPipelineRunId('run-term'), samplePipeline);

    run.markQueued();
    run.start();
    run.transitionTo('SUCCEEDED');

    expect(() => run.start()).toThrow(InvalidStateTransitionError);
    expect(() => run.markQueued()).toThrow(InvalidStateTransitionError);
  });

  it('serializes cleanly to JSON', () => {
    const run = PipelineRun.create(createPipelineRunId('run-json'), samplePipeline);

    const json = run.toJSON();
    expect(json.id).toBe('run-json');
    expect(json.pipelineId).toBe('pipe-ci');
    expect(json.jobs).toHaveLength(4);
    expect(json.status).toBe('PENDING');
  });
});
