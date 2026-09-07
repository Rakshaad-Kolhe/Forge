import { DEFAULT_JOB_PRIORITY } from '@forge/contracts';
import { describe, expect, it } from 'vitest';
import {
  compareJobPriority,
  HighestPriorityFirstPolicy,
  highestPriorityFirstPolicy,
  orderJobsByPriority,
} from './job-policy.js';

describe('compareJobPriority', () => {
  it('exports DEFAULT_JOB_PRIORITY as 0', () => {
    expect(DEFAULT_JOB_PRIORITY).toBe(0);
  });

  it('orders jobs by priority descending', () => {
    const jobHigh = { id: 'job-1', priority: 100 };
    const jobMid = { id: 'job-2', priority: 10 };
    const jobLow = { id: 'job-3', priority: -50 };

    expect(compareJobPriority(jobHigh, jobMid)).toBeLessThan(0);
    expect(compareJobPriority(jobMid, jobHigh)).toBeGreaterThan(0);
    expect(compareJobPriority(jobMid, jobLow)).toBeLessThan(0);
    expect(compareJobPriority(jobLow, jobMid)).toBeGreaterThan(0);
  });

  it('defaults undefined priority to DEFAULT_JOB_PRIORITY (0)', () => {
    const jobExplicitZero = { id: 'job-1', priority: 0 };
    const jobUndefined = { id: 'job-2' };
    const jobPositive = { id: 'job-3', priority: 5 };
    const jobNegative = { id: 'job-4', priority: -5 };

    // undefined vs positive
    expect(compareJobPriority(jobUndefined, jobPositive)).toBeGreaterThan(0);
    expect(compareJobPriority(jobPositive, jobUndefined)).toBeLessThan(0);

    // undefined vs negative
    expect(compareJobPriority(jobUndefined, jobNegative)).toBeLessThan(0);
    expect(compareJobPriority(jobNegative, jobUndefined)).toBeGreaterThan(0);

    // undefined vs 0 (equal priority, tied, breaks tie by ID: 'job-1' < 'job-2')
    expect(compareJobPriority(jobExplicitZero, jobUndefined)).toBeLessThan(0);
  });

  it('breaks ties deterministically by ascending job ID (code-point ordering)', () => {
    const jobA = { id: 'job-a', priority: 10 };
    const jobB = { id: 'job-b', priority: 10 };
    const jobZ = { id: 'job-z', priority: 10 };

    expect(compareJobPriority(jobA, jobB)).toBe(-1);
    expect(compareJobPriority(jobB, jobA)).toBe(1);
    expect(compareJobPriority(jobA, jobZ)).toBe(-1);
    expect(compareJobPriority(jobA, jobA)).toBe(0);
  });

  it('supports jobId field as fallback to id', () => {
    const itemA = { jobId: 'job-01', priority: 50 };
    const itemB = { jobId: 'job-02', priority: 50 };

    expect(compareJobPriority(itemA, itemB)).toBe(-1);
    expect(compareJobPriority(itemB, itemA)).toBe(1);
  });
});

describe('orderJobsByPriority', () => {
  it('returns empty array for empty input without mutation', () => {
    const empty: { id: string; priority: number }[] = [];
    const result = orderJobsByPriority(empty);
    expect(result).toEqual([]);
    expect(result).not.toBe(empty);
  });

  it('preserves a single-item array', () => {
    const single = [{ id: 'job-single', priority: 42 }];
    expect(orderJobsByPriority(single)).toEqual(single);
  });

  it('sorts mixed priority jobs strictly descending with deterministic tie-breaking', () => {
    const jobs = [
      { id: 'job-low', priority: -100 },
      { id: 'job-med-b', priority: 0 },
      { id: 'job-high', priority: 500 },
      { id: 'job-med-a', priority: 0 },
      { id: 'job-critical', priority: 1000 },
    ];

    const sorted = orderJobsByPriority(jobs);
    expect(sorted.map((j) => j.id)).toEqual([
      'job-critical', // 1000
      'job-high', // 500
      'job-med-a', // 0, tie-breaker 'a' < 'b'
      'job-med-b', // 0
      'job-low', // -100
    ]);

    // Ensure original array was not mutated
    expect(jobs[0]?.id).toBe('job-low');
  });
});

describe('HighestPriorityFirstPolicy', () => {
  const policy = new HighestPriorityFirstPolicy();

  it('has canonical policy name', () => {
    expect(policy.name).toBe('HighestPriorityFirst');
    expect(highestPriorityFirstPolicy.name).toBe('HighestPriorityFirst');
  });

  it('is completely order-invariant across all input permutations (deterministic reproducibility)', () => {
    const j1 = { id: 'job-alpha', priority: 100 };
    const j2 = { id: 'job-beta', priority: 50 };
    const j3 = { id: 'job-gamma', priority: 50 }; // tied with beta, 'beta' < 'gamma'
    const j4 = { id: 'job-delta', priority: -10 };

    const expectedOrder = ['job-alpha', 'job-beta', 'job-gamma', 'job-delta'];

    const permutations = [
      [j1, j2, j3, j4],
      [j4, j3, j2, j1],
      [j2, j4, j1, j3],
      [j3, j1, j4, j2],
      [j1, j4, j3, j2],
      [j4, j2, j1, j3],
    ];

    for (const permutation of permutations) {
      const result = policy.orderJobs(permutation);
      expect(result.map((j) => j.id)).toEqual(expectedOrder);
    }
  });
});
