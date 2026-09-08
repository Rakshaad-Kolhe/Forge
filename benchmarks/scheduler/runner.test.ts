import { describe, expect, it } from 'vitest';
import { createSeededRandom } from './utils/prng.js';
import { calculateStats, runBenchmark } from './utils/timer.js';
import {
  generateBenchmarkJobs,
  generateBenchmarkWorkers,
  createJobCandidate,
  createWorkerCandidateFixture,
} from './utils/fixtures.js';
import { formatTable } from './utils/reporter.js';

describe('Benchmark Harness & Utilities Unit Tests', () => {
  describe('Mulberry32 PRNG', () => {
    it('generates reproducible pseudo-random numbers from the same seed', () => {
      const rng1 = createSeededRandom(12345);
      const seq1 = [rng1.nextFloat(), rng1.nextFloat(), rng1.nextInt(1, 100)];

      const rng2 = createSeededRandom(12345);
      const seq2 = [rng2.nextFloat(), rng2.nextFloat(), rng2.nextInt(1, 100)];

      expect(seq1).toEqual(seq2);
    });

    it('generates distinct sequences from different seeds', () => {
      const rng1 = createSeededRandom(11111);
      const rng2 = createSeededRandom(22222);

      expect(rng1.nextFloat()).not.toEqual(rng2.nextFloat());
    });

    it('generates integers strictly within [min, max] inclusive', () => {
      const rng = createSeededRandom(999);
      for (let i = 0; i < 100; i++) {
        const val = rng.nextInt(10, 20);
        expect(val).toBeGreaterThanOrEqual(10);
        expect(val).toBeLessThanOrEqual(20);
        expect(Number.isInteger(val)).toBe(true);
      }
    });
  });

  describe('calculateStats', () => {
    it('calculates correct statistical metrics for sample set', () => {
      const samples = [10, 20, 30, 40, 50];
      const stats = calculateStats(samples, 150);

      expect(stats.count).toBe(5);
      expect(stats.min).toBe(10);
      expect(stats.max).toBe(50);
      expect(stats.mean).toBe(30);
      expect(stats.median).toBe(30);
      expect(stats.opsPerSec).toBeCloseTo(33.33, 1);
    });

    it('handles empty sample set gracefully', () => {
      const stats = calculateStats([]);
      expect(stats.count).toBe(0);
      expect(stats.min).toBe(0);
      expect(stats.mean).toBe(0);
    });

    it('handles single sample correctly', () => {
      const stats = calculateStats([42]);
      expect(stats.count).toBe(1);
      expect(stats.min).toBe(42);
      expect(stats.max).toBe(42);
      expect(stats.mean).toBe(42);
      expect(stats.median).toBe(42);
      expect(stats.p95).toBe(42);
      expect(stats.p99).toBe(42);
    });
  });

  describe('Fixture Generators', () => {
    it('generates deterministic job candidates with valid properties', () => {
      const jobs = generateBenchmarkJobs(15, { seed: 555 });
      expect(jobs.length).toBe(15);

      const uniqueIds = new Set(jobs.map((j) => j.id));
      expect(uniqueIds.size).toBe(15);

      for (const job of jobs) {
        expect(typeof job.priority).toBe('number');
        expect(Number.isFinite(job.priority)).toBe(true);
        expect(job.requirements?.executor).toBeDefined();
        expect(job.createdAt).toBeInstanceOf(Date);
      }
    });

    it('generates deterministic worker candidates with valid properties', () => {
      const workers = generateBenchmarkWorkers(10, { seed: 777 });
      expect(workers.length).toBe(10);

      const uniqueIds = new Set(workers.map((w) => w.workerId));
      expect(uniqueIds.size).toBe(10);

      for (const worker of workers) {
        expect(worker.capabilities.executors.length).toBeGreaterThanOrEqual(1);
        expect(worker.resources.cpuCores).toBeGreaterThanOrEqual(1);
        expect(worker.resources.memoryBytes).toBeGreaterThanOrEqual(1024);
      }
    });

    it('creates targeted single job and worker fixtures', () => {
      const job = createJobCandidate('test-target', 99);
      expect(job.id).toBe('test-target');
      expect(job.priority).toBe(99);

      const worker = createWorkerCandidateFixture('test-worker');
      expect(worker.workerId).toBe('test-worker');
      expect(worker.status).toBe('READY');
    });
  });

  describe('runBenchmark Harness', () => {
    it('runs controlled execution with warmup and records metrics', async () => {
      let counter = 0;
      const result = await runBenchmark({
        name: 'test:smoke',
        warmupIterations: 2,
        measuredIterations: 5,
        fn: () => {
          counter++;
          return counter;
        },
      });

      expect(result.name).toBe('test:smoke');
      expect(result.warmupIterations).toBe(2);
      expect(result.measuredIterations).toBe(5);
      expect(counter).toBe(7); // 2 warmup + 5 measured
      expect(result.failures.success).toBe(5);
      expect(result.failures.failures).toBe(0);
      expect(result.stats.count).toBe(5);
      expect(result.stats.mean).toBeGreaterThanOrEqual(0);
    });
  });

  describe('formatTable', () => {
    it('formats aligned markdown table from run results', async () => {
      const result = await runBenchmark({
        name: 'test:format',
        warmupIterations: 1,
        measuredIterations: 3,
        fn: () => 1 + 1,
      });

      const table = formatTable([result]);
      expect(table).toContain('| Benchmark Name');
      expect(table).toContain('| Mean (ms)');
      expect(table).toContain('test:format');
    });
  });
});
