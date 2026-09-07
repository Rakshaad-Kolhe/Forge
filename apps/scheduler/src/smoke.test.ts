import { createJobId, createPipelineRunId, Job, type WorkerCandidate } from '@forge/pipeline';
import { describe, expect, it } from 'vitest';
import { evaluatePlacement, evaluatePrioritizedWork } from './scheduler.js';

describe('Section 34 Manual Verification Smoke Test Matrix', () => {
  const workerA: WorkerCandidate = {
    workerId: 'Worker-A',
    capabilities: { executors: ['docker'] },
    resources: { cpuCores: 4, memoryBytes: 8 * 1024 * 1024 * 1024, gpuCount: 0 },
    ...({ status: 'READY', liveness: 'ALIVE' } as unknown as WorkerCandidate),
  };

  const workerB: WorkerCandidate = {
    workerId: 'Worker-B',
    capabilities: { executors: ['shell'] },
    resources: { cpuCores: 8, memoryBytes: 16 * 1024 * 1024 * 1024, gpuCount: 0 },
    ...({ status: 'READY', liveness: 'ALIVE' } as unknown as WorkerCandidate),
  };

  const workerC: WorkerCandidate = {
    workerId: 'Worker-C',
    capabilities: { executors: ['docker'] },
    resources: { cpuCores: 2, memoryBytes: 4 * 1024 * 1024 * 1024, gpuCount: 1 },
    ...({ status: 'READY', liveness: 'ALIVE' } as unknown as WorkerCandidate),
  };

  const job1 = {
    id: 'job-1',
    requirements: {
      executor: 'docker',
      cpuCores: 2,
      memoryBytes: 4 * 1024 * 1024 * 1024,
      gpuCount: 0,
    },
  };

  const job2 = {
    id: 'job-2',
    requirements: {
      executor: 'docker',
      cpuCores: 8,
      memoryBytes: 16 * 1024 * 1024 * 1024,
      gpuCount: 0,
    },
  };

  it('Step 1: selects Worker-A for Job 1 with candidate set [Worker-A, Worker-B, Worker-C]', () => {
    const decision = evaluatePlacement(job1, [workerA, workerB, workerC]);

    console.log('Smoke Test Step 1 Decision:', JSON.stringify(decision, null, 2));

    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.jobId).toBe('job-1');
      expect(decision.workerId).toBe('Worker-A');
      expect(decision.candidateWorkerCount).toBe(3);
      expect(decision.eligibleWorkerCount).toBe(2);
    }
  });

  it('Step 2: selects Worker-A for Job 1 with shuffled candidate set [Worker-C, Worker-B, Worker-A]', () => {
    const decision = evaluatePlacement(job1, [workerC, workerB, workerA]);

    console.log('Smoke Test Step 2 (Shuffled) Decision:', JSON.stringify(decision, null, 2));

    expect(decision.status).toBe('SCHEDULED');
    if (decision.status === 'SCHEDULED') {
      expect(decision.jobId).toBe('job-1');
      expect(decision.workerId).toBe('Worker-A');
      expect(decision.candidateWorkerCount).toBe(3);
      expect(decision.eligibleWorkerCount).toBe(2);
    }
  });

  it('Step 3: returns UNSCHEDULABLE for Job 2 with candidate set [Worker-B, Worker-C]', () => {
    const decision = evaluatePlacement(job2, [workerB, workerC]);

    console.log(
      'Smoke Test Step 3 (Job 2 High Resource) Decision:',
      JSON.stringify(decision, null, 2),
    );

    expect(decision.status).toBe('UNSCHEDULABLE');
    if (decision.status === 'UNSCHEDULABLE') {
      expect(decision.jobId).toBe('job-2');
      expect(decision.reason).toBe('NO_ELIGIBLE_WORKER');
      expect(decision.candidateWorkerCount).toBe(2);
      expect(decision.eligibleWorkerCount).toBe(0);
      expect(decision.failureReasons).toContain('EXECUTOR_UNSUPPORTED');
      expect(decision.failureReasons).toContain('INSUFFICIENT_CPU');
      expect(decision.failureReasons).toContain('INSUFFICIENT_MEMORY');
    }
  });
});

describe('Section 35 PR 11 Priority Scheduling Verification Matrix', () => {
  const workerDocker: WorkerCandidate = {
    workerId: 'Worker-Docker',
    capabilities: { executors: ['docker'] },
    resources: { cpuCores: 4, memoryBytes: 8 * 1024 * 1024 * 1024, gpuCount: 0 },
    ...({ status: 'READY', liveness: 'ALIVE' } as unknown as WorkerCandidate),
  };

  const createSmokeJob = (id: string, priority: number, cpuCores: number): Job =>
    new Job({
      id: createJobId(id),
      pipelineRunId: createPipelineRunId('run-smoke'),
      stepName: id,
      command: 'echo 1',
      priority,
      requirements: { executor: 'docker', cpuCores },
    });

  it('Step 1: Priority ordering — 3 jobs with priorities [100, 0, -50] evaluated in [100, 0, -50] order', () => {
    const jobHigh = createSmokeJob('job-high', 100, 2);
    const jobDefault = createSmokeJob('job-default', 0, 2);
    const jobLow = createSmokeJob('job-low', -50, 2);

    const result = evaluatePrioritizedWork([jobLow, jobHigh, jobDefault], [workerDocker]);

    expect(result.orderedDecisions.map((d) => d.jobId)).toEqual([
      'job-high',
      'job-default',
      'job-low',
    ]);
    expect(result.orderedDecisions.map((d) => d.priority)).toEqual([100, 0, -50]);
  });

  it('Step 2: Tie-breaking — 2 jobs with same priority (10) evaluated with lower jobId first ("job-a" before "job-b")', () => {
    const jobB = createSmokeJob('job-b', 10, 2);
    const jobA = createSmokeJob('job-a', 10, 2);

    const result = evaluatePrioritizedWork([jobB, jobA], [workerDocker]);

    expect(result.orderedDecisions.map((d) => d.jobId)).toEqual(['job-a', 'job-b']);
  });

  it('Step 3: Non-blocking unschedulable — Job A (priority 100, 16 CPU) unschedulable, Job B (priority 50, 2 CPU) scheduled', () => {
    const jobA = createSmokeJob('job-a-huge', 100, 16);
    const jobB = createSmokeJob('job-b-fit', 50, 2);

    const result = evaluatePrioritizedWork([jobA, jobB], [workerDocker]);

    expect(result.orderedDecisions).toHaveLength(2);
    expect(result.orderedDecisions[0]?.jobId).toBe('job-a-huge');
    expect(result.orderedDecisions[0]?.status).toBe('UNSCHEDULABLE');
    expect(result.orderedDecisions[0]?.priority).toBe(100);

    expect(result.orderedDecisions[1]?.jobId).toBe('job-b-fit');
    expect(result.orderedDecisions[1]?.status).toBe('SCHEDULED');
    expect(result.orderedDecisions[1]?.priority).toBe(50);
  });

  it('Step 4: Input permutation invariance — permutations yield identical evaluation order', () => {
    const j1 = createSmokeJob('j-1', 100, 1);
    const j2 = createSmokeJob('j-2', 50, 1);
    const j3 = createSmokeJob('j-3', 0, 1);

    const res1 = evaluatePrioritizedWork([j1, j2, j3], [workerDocker]);
    const res2 = evaluatePrioritizedWork([j3, j1, j2], [workerDocker]);
    const res3 = evaluatePrioritizedWork([j2, j3, j1], [workerDocker]);

    expect(res1.orderedDecisions.map((d) => d.jobId)).toEqual(['j-1', 'j-2', 'j-3']);
    expect(res2.orderedDecisions.map((d) => d.jobId)).toEqual(['j-1', 'j-2', 'j-3']);
    expect(res3.orderedDecisions.map((d) => d.jobId)).toEqual(['j-1', 'j-2', 'j-3']);
  });
});
