import type { WorkerCandidate } from '@forge/pipeline';
import { describe, expect, it } from 'vitest';
import { evaluatePlacement } from './scheduler.js';

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
