import { describe, expect, it } from 'vitest';
import {
  DEFAULT_FAIRNESS_AGE_BONUS_STEP,
  DEFAULT_FAIRNESS_AGING_INTERVAL_MS,
  DEFAULT_FAIRNESS_MAX_AGE_BONUS,
  DEFAULT_JOB_PRIORITY,
  type QueueAgingConfig,
} from '@forge/contracts';
import {
  calculateAgeBonus,
  calculateEffectivePriority,
  compareFairAgingPriority,
  FairAgingPriorityPolicy,
  fairAgingPriorityPolicy,
  getJobEligibleWaitingSince,
  orderJobsWithFairAging,
} from './fairness-policy.js';

describe('FairAgingPolicy Unit Tests (PR 16)', () => {
  const customConfig: QueueAgingConfig = {
    agingIntervalMs: 60000, // 1 minute
    ageBonusStep: 10,
    maxAgeBonus: 50,
  };

  describe('calculateAgeBonus', () => {
    it('returns 0 for waiting times less than one aging interval', () => {
      expect(calculateAgeBonus(0, customConfig)).toBe(0);
      expect(calculateAgeBonus(30000, customConfig)).toBe(0);
      expect(calculateAgeBonus(59999, customConfig)).toBe(0);
    });

    it('calculates discrete step increments per interval', () => {
      expect(calculateAgeBonus(60000, customConfig)).toBe(10);
      expect(calculateAgeBonus(119999, customConfig)).toBe(10);
      expect(calculateAgeBonus(120000, customConfig)).toBe(20);
      expect(calculateAgeBonus(180000, customConfig)).toBe(30);
      expect(calculateAgeBonus(240000, customConfig)).toBe(40);
      expect(calculateAgeBonus(300000, customConfig)).toBe(50);
    });

    it('enforces maximum age bonus ceiling', () => {
      // 6 intervals = 60 bonus without cap; capped at 50
      expect(calculateAgeBonus(360000, customConfig)).toBe(50);
      expect(calculateAgeBonus(3600000, customConfig)).toBe(50);
      expect(calculateAgeBonus(Infinity, customConfig)).toBe(0); // Non-finite safety
    });

    it('safely handles non-positive or invalid configurations', () => {
      expect(calculateAgeBonus(-1000, customConfig)).toBe(0);
      expect(calculateAgeBonus(100000, { ...customConfig, agingIntervalMs: 0 })).toBe(0);
      expect(calculateAgeBonus(100000, { ...customConfig, agingIntervalMs: -60000 })).toBe(0);
      expect(calculateAgeBonus(100000, { ...customConfig, ageBonusStep: 0 })).toBe(0);
      expect(calculateAgeBonus(100000, { ...customConfig, maxAgeBonus: 0 })).toBe(0);
      expect(calculateAgeBonus(NaN, customConfig)).toBe(0);
    });
  });

  describe('getJobEligibleWaitingSince', () => {
    it('prioritizes nextAttemptAt when present for retried jobs', () => {
      const createdAt = new Date('2026-09-08T10:00:00.000Z');
      const queuedAt = new Date('2026-09-08T10:01:00.000Z');
      const nextAttemptAt = new Date('2026-09-08T10:15:00.000Z');

      const job = { id: 'job-1', createdAt, queuedAt, nextAttemptAt };
      expect(getJobEligibleWaitingSince(job)).toEqual(nextAttemptAt);
    });

    it('falls back to queuedAt when nextAttemptAt is absent', () => {
      const createdAt = new Date('2026-09-08T10:00:00.000Z');
      const queuedAt = new Date('2026-09-08T10:01:00.000Z');

      const job = { id: 'job-1', createdAt, queuedAt };
      expect(getJobEligibleWaitingSince(job)).toEqual(queuedAt);
    });

    it('falls back to createdAt when queuedAt and nextAttemptAt are absent', () => {
      const createdAt = new Date('2026-09-08T10:00:00.000Z');
      const job = { id: 'job-1', createdAt };
      expect(getJobEligibleWaitingSince(job)).toEqual(createdAt);
    });

    it('defaults to epoch 0 when no timestamp is provided', () => {
      expect(getJobEligibleWaitingSince({})).toEqual(new Date(0));
    });
  });

  describe('calculateEffectivePriority', () => {
    const baseTime = new Date('2026-09-08T10:00:00.000Z');

    it('preserves base priority when job is newly created (0 wait)', () => {
      const job = {
        id: 'job-new',
        priority: 100,
        createdAt: baseTime,
      };

      const info = calculateEffectivePriority(job, baseTime, customConfig);
      expect(info.basePriority).toBe(100);
      expect(info.ageBonus).toBe(0);
      expect(info.effectivePriority).toBe(100);
      expect(info.waitingMs).toBe(0);
      expect(job.priority).toBe(100); // Durable priority MUST NEVER be mutated
    });

    it('calculates age bonus without mutating base priority', () => {
      const job = {
        id: 'job-aging',
        priority: 50,
        createdAt: baseTime,
      };

      // Virtual evaluation 2 intervals later (120,000ms = 2 mins)
      const evalTime = new Date('2026-09-08T10:02:00.000Z');
      const info = calculateEffectivePriority(job, evalTime, customConfig);

      expect(info.basePriority).toBe(50);
      expect(info.ageBonus).toBe(20);
      expect(info.effectivePriority).toBe(70);
      expect(info.waitingMs).toBe(120000);
      expect(job.priority).toBe(50); // Unmutated
    });

    it('applies default job priority if job priority is missing or invalid', () => {
      const job = {
        id: 'job-no-prio',
        createdAt: baseTime,
      };

      const info = calculateEffectivePriority(job, baseTime);
      expect(info.basePriority).toBe(DEFAULT_JOB_PRIORITY);
      expect(info.effectivePriority).toBe(DEFAULT_JOB_PRIORITY);
    });

    it('handles active retry backoff (nextAttemptAt in future) with 0 waiting age', () => {
      const nextAttemptAt = new Date('2026-09-08T10:05:00.000Z');
      const job = {
        id: 'job-backoff',
        priority: 10,
        createdAt: new Date('2026-09-08T09:00:00.000Z'), // 1 hour ago
        nextAttemptAt,
      };

      // Current virtual time is before backoff expires
      const now = new Date('2026-09-08T10:02:00.000Z');
      const info = calculateEffectivePriority(job, now, customConfig);

      expect(info.waitingMs).toBe(0);
      expect(info.ageBonus).toBe(0);
      expect(info.effectivePriority).toBe(10);
    });

    it('resets retry waiting age to start from nextAttemptAt once due', () => {
      const nextAttemptAt = new Date('2026-09-08T10:00:00.000Z');
      const job = {
        id: 'job-retried',
        priority: 10,
        createdAt: new Date('2026-09-08T08:00:00.000Z'), // 2 hours ago
        nextAttemptAt,
      };

      // Evaluated 2 minutes after retry became due
      const now = new Date('2026-09-08T10:02:00.000Z');
      const info = calculateEffectivePriority(job, now, customConfig);

      // Waiting duration MUST be 2 minutes, NOT 2 hours!
      expect(info.waitingMs).toBe(120000);
      expect(info.ageBonus).toBe(20);
      expect(info.effectivePriority).toBe(30);
    });
  });

  describe('compareFairAgingPriority & orderJobsWithFairAging', () => {
    const baseTime = new Date('2026-09-08T10:00:00.000Z');

    it('compares jobs directly via compareFairAgingPriority', () => {
      const jobLow = { id: 'job-low', priority: 10, createdAt: baseTime };
      const jobHigh = { id: 'job-high', priority: 50, createdAt: baseTime };

      expect(compareFairAgingPriority(jobLow, jobHigh, baseTime, customConfig)).toBeGreaterThan(0);
      expect(compareFairAgingPriority(jobHigh, jobLow, baseTime, customConfig)).toBeLessThan(0);
      expect(compareFairAgingPriority(jobHigh, jobHigh, baseTime, customConfig)).toBe(0);
    });

    it('orders jobs by higher effective priority descending', () => {
      const jobLow = {
        id: 'job-low',
        priority: 10,
        createdAt: new Date('2026-09-08T10:00:00.000Z'),
      };
      const jobHigh = {
        id: 'job-high',
        priority: 50,
        createdAt: new Date('2026-09-08T10:00:00.000Z'),
      };

      const ordered = orderJobsWithFairAging([jobLow, jobHigh], baseTime, customConfig);
      expect(ordered.map((j) => j.id)).toEqual(['job-high', 'job-low']);
    });

    it('breaks ties deterministically by ascending alphanumeric jobId', () => {
      const jobB = {
        id: 'job-zebra',
        priority: 50,
        createdAt: baseTime,
      };
      const jobA = {
        id: 'job-alpha',
        priority: 50,
        createdAt: baseTime,
      };

      const ordered1 = orderJobsWithFairAging([jobB, jobA], baseTime, customConfig);
      const ordered2 = orderJobsWithFairAging([jobA, jobB], baseTime, customConfig);

      expect(ordered1.map((j) => j.id)).toEqual(['job-alpha', 'job-zebra']);
      expect(ordered2.map((j) => j.id)).toEqual(['job-alpha', 'job-zebra']);
    });

    it('is strictly permutation invariant', () => {
      const jobs = [
        { id: 'job-1', priority: 10, createdAt: new Date('2026-09-08T09:50:00.000Z') },
        { id: 'job-2', priority: 25, createdAt: new Date('2026-09-08T09:58:00.000Z') },
        { id: 'job-3', priority: 5, createdAt: new Date('2026-09-08T09:30:00.000Z') },
        { id: 'job-4', priority: 20, createdAt: new Date('2026-09-08T10:00:00.000Z') },
      ];

      const perm1 = [jobs[0]!, jobs[1]!, jobs[2]!, jobs[3]!];
      const perm2 = [jobs[3]!, jobs[1]!, jobs[0]!, jobs[2]!];
      const perm3 = [jobs[2]!, jobs[0]!, jobs[3]!, jobs[1]!];

      const res1 = orderJobsWithFairAging(perm1, baseTime, customConfig).map((j) => j.id);
      const res2 = orderJobsWithFairAging(perm2, baseTime, customConfig).map((j) => j.id);
      const res3 = orderJobsWithFairAging(perm3, baseTime, customConfig).map((j) => j.id);

      expect(res1).toEqual(res2);
      expect(res2).toEqual(res3);
    });

    it('allows a low-priority job to overtake a newer higher-priority job when sufficiently aged', () => {
      // Job Low: base priority 10, waiting for 4 minutes (4 * 10 = +40 bonus -> effective priority 50)
      const jobOldLow = {
        id: 'job-old-low',
        priority: 10,
        createdAt: new Date('2026-09-08T09:56:00.000Z'), // 4 mins ago
      };

      // Job New High: base priority 40, just arrived (0 wait -> effective priority 40)
      const jobNewHigh = {
        id: 'job-new-high',
        priority: 40,
        createdAt: new Date('2026-09-08T10:00:00.000Z'), // 0 mins ago
      };

      // Under HighestPriorityFirst baseline: jobNewHigh (40) would be first
      // Under FairAgingPriority: jobOldLow has effective 50 > jobNewHigh 40 -> jobOldLow wins!
      const ordered = orderJobsWithFairAging([jobNewHigh, jobOldLow], baseTime, customConfig);
      expect(ordered.map((j) => j.id)).toEqual(['job-old-low', 'job-new-high']);
    });

    it('respects saturation ceiling: low priority cannot overtake high priority when gap exceeds maxAgeBonus', () => {
      // Max age bonus is 50.
      // Low priority job has priority 0. Maximum possible effective priority = 50.
      const jobOldLow = {
        id: 'job-old-zero',
        priority: 0,
        createdAt: new Date('2026-09-08T08:00:00.000Z'), // 2 hours ago (saturated at +50)
      };

      // Critical priority job has priority 100.
      const jobNewCritical = {
        id: 'job-critical',
        priority: 100,
        createdAt: baseTime, // 0 wait
      };

      // Critical job (100) must still schedule before saturated low job (50)
      const ordered = orderJobsWithFairAging([jobOldLow, jobNewCritical], baseTime, customConfig);
      expect(ordered.map((j) => j.id)).toEqual(['job-critical', 'job-old-zero']);
    });
  });

  describe('FairAgingPriorityPolicy Class', () => {
    it('implements JobOrderingPolicy contract with default configuration', () => {
      const policy = new FairAgingPriorityPolicy();
      expect(policy.name).toBe('FairAgingPriority');
      expect(policy.getConfig()).toEqual({
        agingIntervalMs: DEFAULT_FAIRNESS_AGING_INTERVAL_MS,
        ageBonusStep: DEFAULT_FAIRNESS_AGE_BONUS_STEP,
        maxAgeBonus: DEFAULT_FAIRNESS_MAX_AGE_BONUS,
      });
    });

    it('allows custom configuration via constructor', () => {
      const policy = new FairAgingPriorityPolicy(customConfig);
      expect(policy.getConfig()).toEqual(customConfig);

      const jobA = { id: 'a', priority: 10, createdAt: new Date('2026-09-08T10:00:00.000Z') };
      const jobB = { id: 'b', priority: 20, createdAt: new Date('2026-09-08T10:00:00.000Z') };

      const ordered = policy.orderJobs([jobA, jobB]);
      expect(ordered[0]!.id).toBe('b');
    });

    it('exports default singleton instance', () => {
      expect(fairAgingPriorityPolicy).toBeInstanceOf(FairAgingPriorityPolicy);
      expect(fairAgingPriorityPolicy.name).toBe('FairAgingPriority');
    });
  });
});
