import { describe, expect, it } from 'vitest';
import {
  createJobId,
  createPipelineRunId,
  filterEligibleWorkers,
  Job,
  matchesWorker,
  validateJobRequirements,
  JobRequirementsValidationError,
  type JobRequirements,
  type WorkerCandidate,
} from './index.js';

describe('Worker Capability & Resource Matcher', () => {
  const baseWorker: WorkerCandidate = {
    workerId: 'worker-1',
    capabilities: {
      executors: ['shell', 'docker'],
    },
    resources: {
      cpuCores: 4,
      memoryBytes: 8 * 1024 * 1024 * 1024, // 8 GB
      gpuCount: 1,
    },
  };

  describe('JobRequirements Validation', () => {
    it('normalizes valid requirements', () => {
      const req = validateJobRequirements({
        executor: ' docker ',
        cpuCores: 2,
        memoryBytes: 4096,
        gpuCount: 0,
      });

      expect(req).toEqual({
        executor: 'docker',
        cpuCores: 2,
        memoryBytes: 4096,
        gpuCount: 0,
      });
      expect(Object.isFrozen(req)).toBe(true);
    });

    it('allows undefined / empty requirements', () => {
      expect(validateJobRequirements()).toEqual({});
      expect(validateJobRequirements(null)).toEqual({});
      expect(validateJobRequirements({})).toEqual({});
    });

    it('rejects invalid executor name', () => {
      expect(() => validateJobRequirements({ executor: '' })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ executor: '   ' })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ executor: 123 as unknown as string })).toThrow(
        JobRequirementsValidationError,
      );
    });

    it('rejects invalid CPU numbers', () => {
      expect(() => validateJobRequirements({ cpuCores: 0 })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ cpuCores: -2 })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ cpuCores: NaN })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ cpuCores: Infinity })).toThrow(
        JobRequirementsValidationError,
      );
    });

    it('rejects invalid memory numbers', () => {
      expect(() => validateJobRequirements({ memoryBytes: 0 })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ memoryBytes: -1024 })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ memoryBytes: NaN })).toThrow(
        JobRequirementsValidationError,
      );
    });

    it('rejects invalid GPU counts', () => {
      expect(() => validateJobRequirements({ gpuCount: -1 })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ gpuCount: NaN })).toThrow(
        JobRequirementsValidationError,
      );
      expect(() => validateJobRequirements({ gpuCount: Infinity })).toThrow(
        JobRequirementsValidationError,
      );
    });

    it('allows zero GPU count', () => {
      const req = validateJobRequirements({ gpuCount: 0 });
      expect(req.gpuCount).toBe(0);
    });
  });

  describe('Executor Matching', () => {
    it('matches when worker advertises the required executor', () => {
      const result = matchesWorker({ executor: 'docker' }, baseWorker);
      expect(result.matched).toBe(true);
    });

    it('fails when worker does not advertise the required executor', () => {
      const result = matchesWorker({ executor: 'kubernetes' }, baseWorker);
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toEqual(['EXECUTOR_UNSUPPORTED']);
      }
    });

    it('matches when worker has multiple executors and one matches', () => {
      const result = matchesWorker({ executor: 'shell' }, baseWorker);
      expect(result.matched).toBe(true);
    });

    it('matches when job executor requirement is unspecified', () => {
      const result = matchesWorker({}, baseWorker);
      expect(result.matched).toBe(true);
    });
  });

  describe('CPU Matching', () => {
    it('matches exact CPU requirement', () => {
      const result = matchesWorker({ cpuCores: 4 }, baseWorker);
      expect(result.matched).toBe(true);
    });

    it('matches when worker has excess CPU capacity', () => {
      const result = matchesWorker({ cpuCores: 2 }, baseWorker);
      expect(result.matched).toBe(true);
    });

    it('fails when worker has insufficient CPU capacity', () => {
      const result = matchesWorker({ cpuCores: 8 }, baseWorker);
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toEqual(['INSUFFICIENT_CPU']);
      }
    });
  });

  describe('Memory Matching', () => {
    const eightGb = 8 * 1024 * 1024 * 1024;
    const fourGb = 4 * 1024 * 1024 * 1024;
    const sixteenGb = 16 * 1024 * 1024 * 1024;

    it('matches exact memory requirement', () => {
      const result = matchesWorker({ memoryBytes: eightGb }, baseWorker);
      expect(result.matched).toBe(true);
    });

    it('matches when worker has excess memory capacity', () => {
      const result = matchesWorker({ memoryBytes: fourGb }, baseWorker);
      expect(result.matched).toBe(true);
    });

    it('fails when worker has insufficient memory capacity', () => {
      const result = matchesWorker({ memoryBytes: sixteenGb }, baseWorker);
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toEqual(['INSUFFICIENT_MEMORY']);
      }
    });
  });

  describe('GPU Matching', () => {
    it('matches exact GPU count', () => {
      const result = matchesWorker({ gpuCount: 1 }, baseWorker);
      expect(result.matched).toBe(true);
    });

    it('matches when worker has more GPUs than required', () => {
      const gpuWorker: WorkerCandidate = {
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 8192, gpuCount: 4 },
      };
      const result = matchesWorker({ gpuCount: 2 }, gpuWorker);
      expect(result.matched).toBe(true);
    });

    it('fails when worker has fewer GPUs than required', () => {
      const result = matchesWorker({ gpuCount: 2 }, baseWorker);
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toEqual(['INSUFFICIENT_GPU']);
      }
    });

    it('matches zero GPU requirement even if worker has no GPUs', () => {
      const noGpuWorker: WorkerCandidate = {
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 8192, gpuCount: 0 },
      };
      const result = matchesWorker({ gpuCount: 0 }, noGpuWorker);
      expect(result.matched).toBe(true);
    });

    it('matches zero GPU requirement when worker gpuCount is undefined', () => {
      const noGpuWorker: WorkerCandidate = {
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 8192 },
      };
      const result = matchesWorker({ gpuCount: 0 }, noGpuWorker);
      expect(result.matched).toBe(true);
    });
  });

  describe('Combined Multi-Requirement Matching', () => {
    it('matches when all requirements are satisfied', () => {
      const result = matchesWorker(
        {
          executor: 'docker',
          cpuCores: 2,
          memoryBytes: 4 * 1024 * 1024 * 1024,
          gpuCount: 1,
        },
        baseWorker,
      );
      expect(result.matched).toBe(true);
    });

    it('reports single failure reason when one check fails', () => {
      const result = matchesWorker(
        {
          executor: 'docker',
          cpuCores: 16, // Fails
          memoryBytes: 4 * 1024 * 1024 * 1024,
          gpuCount: 1,
        },
        baseWorker,
      );
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toEqual(['INSUFFICIENT_CPU']);
      }
    });

    it('reports multiple failure reasons when multiple checks fail', () => {
      const result = matchesWorker(
        {
          executor: 'kubernetes', // Fails
          cpuCores: 16, // Fails
          memoryBytes: 32 * 1024 * 1024 * 1024, // Fails
          gpuCount: 4, // Fails
        },
        baseWorker,
      );
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toEqual([
          'EXECUTOR_UNSUPPORTED',
          'INSUFFICIENT_CPU',
          'INSUFFICIENT_MEMORY',
          'INSUFFICIENT_GPU',
        ]);
      }
    });
  });

  describe('Invalid Requirements Safety & Isolation', () => {
    it('returns INVALID_REQUIREMENTS and rejects worker when requirement is negative', () => {
      const result = matchesWorker({ cpuCores: -4 } as JobRequirements, baseWorker);
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toContain('INVALID_REQUIREMENTS');
      }
    });

    it('returns INVALID_REQUIREMENTS for non-finite values', () => {
      const result = matchesWorker({ memoryBytes: NaN } as JobRequirements, baseWorker);
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toContain('INVALID_REQUIREMENTS');
      }
    });

    it('returns INVALID_REQUIREMENTS for empty executor string', () => {
      const result = matchesWorker({ executor: '   ' } as JobRequirements, baseWorker);
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toContain('INVALID_REQUIREMENTS');
      }
    });
  });

  describe('Job Domain Aggregate Integration', () => {
    it('extracts requirements directly from a Job instance', () => {
      const job = new Job({
        id: createJobId('job-1'),
        pipelineRunId: createPipelineRunId('run-1'),
        stepName: 'build',
        command: 'npm run build',
        requirements: {
          executor: 'docker',
          cpuCores: 2,
        },
      });

      expect(job.requirements).toEqual({
        executor: 'docker',
        cpuCores: 2,
      });

      const result = matchesWorker(job, baseWorker);
      expect(result.matched).toBe(true);

      const jobDemanding = new Job({
        id: createJobId('job-2'),
        pipelineRunId: createPipelineRunId('run-1'),
        stepName: 'heavy-test',
        command: 'npm test',
        requirements: {
          executor: 'docker',
          cpuCores: 16,
        },
      });

      const resultDemanding = matchesWorker(jobDemanding, baseWorker);
      expect(resultDemanding.matched).toBe(false);
      if (!resultDemanding.matched) {
        expect(resultDemanding.reasons).toEqual(['INSUFFICIENT_CPU']);
      }
    });
  });

  describe('Candidate Polymorphism', () => {
    it('matches against nested worker info shape (WorkerInfo / WorkerMetadata)', () => {
      const workerInfo: WorkerCandidate = {
        worker: {
          capabilities: { executors: ['docker'] },
          resources: { cpuCores: 8, memoryBytes: 16000, gpuCount: 0 },
        },
      };

      const result = matchesWorker({ executor: 'docker', cpuCores: 4 }, workerInfo);
      expect(result.matched).toBe(true);
    });

    it('matches against flat database record shape (WorkerRecord)', () => {
      const workerRecord: WorkerCandidate = {
        id: 'worker-db-1',
        executors: ['shell', 'docker'],
        resources: { cpuCores: 4, memoryBytes: 8192 },
      };

      const result = matchesWorker({ executor: 'shell' }, workerRecord);
      expect(result.matched).toBe(true);
    });

    it('handles null/undefined worker gracefully', () => {
      const result = matchesWorker({ executor: 'docker' }, null);
      expect(result.matched).toBe(false);
      if (!result.matched) {
        expect(result.reasons).toContain('EXECUTOR_UNSUPPORTED');
      }
    });
  });

  describe('Multi-Worker Filtering', () => {
    const workerA: WorkerCandidate = {
      workerId: 'worker-A',
      capabilities: { executors: ['shell', 'docker'] },
      resources: { cpuCores: 4, memoryBytes: 8192, gpuCount: 0 },
    };

    const workerB: WorkerCandidate = {
      workerId: 'worker-B',
      capabilities: { executors: ['shell'] },
      resources: { cpuCores: 8, memoryBytes: 16384, gpuCount: 0 },
    };

    const workerC: WorkerCandidate = {
      workerId: 'worker-C',
      capabilities: { executors: ['docker'] },
      resources: { cpuCores: 2, memoryBytes: 4096, gpuCount: 1 },
    };

    const allWorkers = [workerA, workerB, workerC];

    it('filters correctly for Job 1 (docker, 4 CPU, 8 GB, 0 GPU) -> returns [Worker A]', () => {
      const eligible = filterEligibleWorkers(
        { executor: 'docker', cpuCores: 4, memoryBytes: 8192, gpuCount: 0 },
        allWorkers,
      );

      expect(eligible.length).toBe(1);
      expect(eligible[0]?.workerId).toBe('worker-A');
    });

    it('filters correctly for Job 2 (docker, 2 CPU, 4 GB, 1 GPU) -> returns [Worker C]', () => {
      const eligible = filterEligibleWorkers(
        { executor: 'docker', cpuCores: 2, memoryBytes: 4096, gpuCount: 1 },
        allWorkers,
      );

      expect(eligible.length).toBe(1);
      expect(eligible[0]?.workerId).toBe('worker-C');
    });

    it('preserves exact input ordering', () => {
      const reverseWorkers = [workerC, workerA];
      const eligible = filterEligibleWorkers({ executor: 'docker' }, reverseWorkers);

      expect(eligible.length).toBe(2);
      expect(eligible[0]?.workerId).toBe('worker-C');
      expect(eligible[1]?.workerId).toBe('worker-A');
    });

    it('returns empty array when no workers match', () => {
      const eligible = filterEligibleWorkers({ executor: 'kubernetes' }, allWorkers);
      expect(eligible).toEqual([]);
    });

    it('returns empty array when input workers list is empty', () => {
      const eligible = filterEligibleWorkers({ executor: 'docker' }, []);
      expect(eligible).toEqual([]);
    });

    it('returns all workers when requirements are empty/unspecified', () => {
      const eligible = filterEligibleWorkers({}, allWorkers);
      expect(eligible.length).toBe(3);
    });

    it('returns empty array when requirements are invalid', () => {
      const eligible = filterEligibleWorkers({ cpuCores: -1 } as JobRequirements, allWorkers);
      expect(eligible).toEqual([]);
    });
  });

  describe('Determinism', () => {
    it('produces identical results across repeated calls', () => {
      const req: JobRequirements = { executor: 'docker', cpuCores: 4, memoryBytes: 8192 };
      const res1 = matchesWorker(req, baseWorker);
      const res2 = matchesWorker(req, baseWorker);
      const res3 = matchesWorker(req, baseWorker);

      expect(res1).toEqual(res2);
      expect(res2).toEqual(res3);
    });
  });
});
