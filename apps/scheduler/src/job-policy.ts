import { DEFAULT_JOB_PRIORITY } from '@forge/contracts';
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
 * Deterministic comparator for job ordering based on descending priority,
 * with ties broken deterministically by ascending job ID code-point ordering.
 *
 * Sorting semantics:
 * 1. Higher priority jobs sort before lower priority jobs (descending: b.priority - a.priority).
 * 2. Equal priority jobs break ties by job ID ascending (idA < idB ? -1 : (idA > idB ? 1 : 0)).
 * 3. Permutation-invariant: Order of items in the input does not alter the final sequence.
 */
export function compareJobPriority<
  T extends { readonly priority?: number; readonly id?: string; readonly jobId?: string },
>(a: T, b: T): number {
  const prioA = typeof a.priority === 'number' ? a.priority : DEFAULT_JOB_PRIORITY;
  const prioB = typeof b.priority === 'number' ? b.priority : DEFAULT_JOB_PRIORITY;

  if (prioA !== prioB) {
    return prioB - prioA;
  }

  const idA = getJobId(a);
  const idB = getJobId(b);

  if (idA < idB) return -1;
  if (idA > idB) return 1;
  return 0;
}

/**
 * Pure helper function that orders a list of job-like items by priority descending,
 * with ties broken deterministically by ascending job ID.
 *
 * Does not mutate the original array.
 */
export function orderJobsByPriority<
  T extends { readonly priority?: number; readonly id?: string; readonly jobId?: string },
>(jobs: readonly T[]): T[] {
  if (!Array.isArray(jobs) || jobs.length === 0) {
    return [];
  }
  return [...jobs].sort(compareJobPriority);
}

/**
 * Baseline priority job ordering policy for Forge scheduler (PR 11).
 * Orders jobs strictly by highest priority first, breaking ties deterministically by ascending job ID.
 */
export class HighestPriorityFirstPolicy implements JobOrderingPolicy {
  public readonly name = 'HighestPriorityFirst';

  public orderJobs<
    T extends {
      readonly priority?: number;
      readonly id?: string;
      readonly jobId?: string;
      readonly createdAt?: Date | string;
      readonly queuedAt?: Date | string;
      readonly nextAttemptAt?: Date | string;
    },
  >(jobs: readonly T[], _now?: Date): T[] {
    return orderJobsByPriority(jobs);
  }
}

/**
 * Singleton instance of HighestPriorityFirstPolicy.
 */
export const highestPriorityFirstPolicy = new HighestPriorityFirstPolicy();

export {
  calculateAgeBonus,
  calculateEffectivePriority,
  compareFairAgingPriority,
  fairAgingPriorityPolicy,
  FairAgingPriorityPolicy,
  getJobEligibleWaitingSince,
  orderJobsWithFairAging,
} from './fairness-policy.js';
