import {
  DEFAULT_FAIRNESS_AGE_BONUS_STEP,
  DEFAULT_FAIRNESS_AGING_INTERVAL_MS,
  DEFAULT_FAIRNESS_MAX_AGE_BONUS,
  DEFAULT_JOB_PRIORITY,
  type EffectivePriorityInfo,
  type QueueAgingConfig,
} from '@forge/contracts';
import type { JobOrderingPolicy } from './types.js';

/**
 * Extracts a normalized job identifier from a job-like object.
 */
function getJobId<T extends { readonly id?: string; readonly jobId?: string }>(item: T): string {
  if (typeof item.id === 'string' && item.id.trim()) {
    return item.id.trim();
  }
  if (typeof item.jobId === 'string' && item.jobId.trim()) {
    return item.jobId.trim();
  }
  return '';
}

/**
 * Resolves the instant when a job became eligible to wait for scheduler placement.
 *
 * Invariants:
 * 1. For newly queued jobs: waiting starts at `queuedAt` (if set) or `createdAt`.
 * 2. For retried jobs: waiting starts at `nextAttemptAt` (the instant retry backoff expired
 *    and the job became schedulable again). This prevents retried jobs from unfairly accumulating
 *    huge age bonuses during their backoff sleep.
 */
export function getJobEligibleWaitingSince<
  T extends {
    readonly createdAt?: Date | string;
    readonly queuedAt?: Date | string;
    readonly nextAttemptAt?: Date | string;
  },
>(job: T): Date {
  if (job.nextAttemptAt !== undefined && job.nextAttemptAt !== null) {
    return new Date(job.nextAttemptAt);
  }
  if (job.queuedAt !== undefined && job.queuedAt !== null) {
    return new Date(job.queuedAt);
  }
  if (job.createdAt !== undefined && job.createdAt !== null) {
    return new Date(job.createdAt);
  }
  return new Date(0);
}

/**
 * Calculates the bounded age bonus for a given waiting duration according to the queue-aging formula:
 *
 *   age_bonus = min(max_age_bonus, floor(waiting_ms / aging_interval_ms) * age_bonus_step)
 *
 * Guaranteed properties:
 * - Deterministic, pure function
 * - Monotonically non-decreasing with respect to waiting_ms
 * - Bounded within [0, max_age_bonus]
 * - Safe against non-finite, negative, or invalid configuration inputs
 */
export function calculateAgeBonus(waitingMs: number, config: QueueAgingConfig): number {
  if (
    !Number.isFinite(waitingMs) ||
    waitingMs <= 0 ||
    !Number.isFinite(config.agingIntervalMs) ||
    config.agingIntervalMs <= 0 ||
    !Number.isFinite(config.ageBonusStep) ||
    config.ageBonusStep <= 0 ||
    !Number.isFinite(config.maxAgeBonus) ||
    config.maxAgeBonus <= 0
  ) {
    return 0;
  }

  const intervals = Math.floor(waitingMs / config.agingIntervalMs);
  const rawBonus = intervals * config.ageBonusStep;

  if (!Number.isFinite(rawBonus) || rawBonus <= 0) {
    return 0;
  }

  return Math.min(config.maxAgeBonus, rawBonus);
}

/**
 * Computes dynamic effective priority and age bonus for a job without mutating its durable base priority.
 *
 * @param job - The candidate job object
 * @param now - Virtual or wall-clock evaluation instant (defaults to Date.now())
 * @param config - Queue aging configuration overrides
 */
export function calculateEffectivePriority<
  T extends {
    readonly priority?: number;
    readonly createdAt?: Date | string;
    readonly queuedAt?: Date | string;
    readonly nextAttemptAt?: Date | string;
  },
>(job: T, now: Date = new Date(), config?: Partial<QueueAgingConfig>): EffectivePriorityInfo {
  const resolvedConfig: QueueAgingConfig = {
    agingIntervalMs: config?.agingIntervalMs ?? DEFAULT_FAIRNESS_AGING_INTERVAL_MS,
    ageBonusStep: config?.ageBonusStep ?? DEFAULT_FAIRNESS_AGE_BONUS_STEP,
    maxAgeBonus: config?.maxAgeBonus ?? DEFAULT_FAIRNESS_MAX_AGE_BONUS,
  };

  const basePriority =
    typeof job.priority === 'number' && Number.isFinite(job.priority)
      ? job.priority
      : DEFAULT_JOB_PRIORITY;

  const waitingSince = getJobEligibleWaitingSince(job);

  // If the job has a future nextAttemptAt, it is still under backoff and has 0 waiting age
  if (waitingSince.getTime() > now.getTime()) {
    return {
      basePriority,
      ageBonus: 0,
      effectivePriority: basePriority,
      waitingSince,
      waitingMs: 0,
    };
  }

  const waitingMs = Math.max(0, now.getTime() - waitingSince.getTime());
  const ageBonus = calculateAgeBonus(waitingMs, resolvedConfig);

  return {
    basePriority,
    ageBonus,
    effectivePriority: basePriority + ageBonus,
    waitingSince,
    waitingMs,
  };
}

/**
 * Deterministic comparator for job ordering based on descending effective priority,
 * breaking ties deterministically by ascending job ID code-point ordering.
 *
 * Invariants:
 * 1. Higher effective priority jobs sort before lower effective priority jobs.
 * 2. Equal effective priority jobs break ties strictly by alphanumeric job ID ascending.
 * 3. Permutation-invariant: The sequence of input items does not affect the output ordering.
 * 4. Time-deterministic: Output is strictly reproducible for any fixed `now` timestamp.
 */
export function compareFairAgingPriority<
  T extends {
    readonly priority?: number;
    readonly id?: string;
    readonly jobId?: string;
    readonly createdAt?: Date | string;
    readonly queuedAt?: Date | string;
    readonly nextAttemptAt?: Date | string;
  },
>(a: T, b: T, now: Date = new Date(), config?: Partial<QueueAgingConfig>): number {
  const effA = calculateEffectivePriority(a, now, config).effectivePriority;
  const effB = calculateEffectivePriority(b, now, config).effectivePriority;

  if (effA !== effB) {
    return effB - effA;
  }

  const idA = getJobId(a);
  const idB = getJobId(b);

  if (idA < idB) return -1;
  if (idA > idB) return 1;
  return 0;
}

/**
 * Pure helper function that orders candidate jobs with fair queue aging.
 * Does not mutate the original array or the jobs within it.
 */
export function orderJobsWithFairAging<
  T extends {
    readonly priority?: number;
    readonly id?: string;
    readonly jobId?: string;
    readonly createdAt?: Date | string;
    readonly queuedAt?: Date | string;
    readonly nextAttemptAt?: Date | string;
  },
>(jobs: readonly T[], now: Date = new Date(), config?: Partial<QueueAgingConfig>): T[] {
  if (!Array.isArray(jobs) || jobs.length === 0) {
    return [];
  }
  return [...jobs].sort((a, b) => compareFairAgingPriority(a, b, now, config));
}

/**
 * Starvation-prevention queue aging job ordering policy (PR 16).
 *
 * Implements:
 *   effective_priority = base_priority + min(max_bonus, floor(waiting_ms / interval_ms) * step)
 *
 * Preserves base priority while allowing older jobs to progressively overtake
 * newer higher-priority jobs up to a bounded saturation limit.
 */
export class FairAgingPriorityPolicy implements JobOrderingPolicy {
  public readonly name = 'FairAgingPriority';
  private readonly config: QueueAgingConfig;

  constructor(config?: Partial<QueueAgingConfig>) {
    this.config = {
      agingIntervalMs: config?.agingIntervalMs ?? DEFAULT_FAIRNESS_AGING_INTERVAL_MS,
      ageBonusStep: config?.ageBonusStep ?? DEFAULT_FAIRNESS_AGE_BONUS_STEP,
      maxAgeBonus: config?.maxAgeBonus ?? DEFAULT_FAIRNESS_MAX_AGE_BONUS,
    };
  }

  public getConfig(): QueueAgingConfig {
    return { ...this.config };
  }

  public orderJobs<
    T extends {
      readonly priority?: number;
      readonly id?: string;
      readonly jobId?: string;
      readonly createdAt?: Date | string;
      readonly queuedAt?: Date | string;
      readonly nextAttemptAt?: Date | string;
    },
  >(jobs: readonly T[], now?: Date): T[] {
    return orderJobsWithFairAging(jobs, now, this.config);
  }
}

/**
 * Default singleton instance of FairAgingPriorityPolicy using standard configuration.
 */
export const fairAgingPriorityPolicy = new FairAgingPriorityPolicy();
