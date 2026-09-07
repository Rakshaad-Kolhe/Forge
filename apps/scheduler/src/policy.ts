import type { WorkerCandidate } from '@forge/pipeline';
import type { WorkerSelectionPolicy } from './types.js';

/**
 * Extracts a normalized, stable worker identifier from a WorkerCandidate or WorkerInfo.
 */
export function getWorkerCandidateId(worker: WorkerCandidate | null | undefined): string {
  if (!worker) {
    return '';
  }

  if (typeof worker.workerId === 'string' && worker.workerId.trim()) {
    return worker.workerId.trim();
  }

  if (typeof worker.id === 'string' && worker.id.trim()) {
    return worker.id.trim();
  }

  // Handle nested worker metadata (such as in WorkerInfo from @forge/worker-registry)
  const nested = (worker as { worker?: { workerId?: string; id?: string } }).worker;
  if (nested) {
    if (typeof nested.workerId === 'string' && nested.workerId.trim()) {
      return nested.workerId.trim();
    }
    if (typeof nested.id === 'string' && nested.id.trim()) {
      return nested.id.trim();
    }
  }

  return '';
}

/**
 * Deterministic, locale-independent comparator for candidate workers based on ascending worker ID.
 * Uses exact character code-point comparison to guarantee identical sorting across platforms.
 */
export function compareWorkerCandidates(a: WorkerCandidate, b: WorkerCandidate): number {
  const idA = getWorkerCandidateId(a);
  const idB = getWorkerCandidateId(b);

  if (idA < idB) return -1;
  if (idA > idB) return 1;
  return 0;
}

/**
 * Baseline deterministic worker selection policy for PR 10.
 *
 * Algorithm:
 * 1. Takes the pre-filtered set of eligible workers satisfying capability and resource constraints.
 * 2. Establishes canonical order by sorting workers by workerId ascending (code-point order).
 * 3. Chooses the first eligible worker in that canonical order.
 *
 * Guaranteed properties:
 * - Order-invariant: Input array order does not affect the outcome.
 * - Deterministic: Identical candidate sets always produce the exact same selected worker.
 * - Zero randomness or unmodelled heuristics (no load, CPU, memory, or registration-time bias).
 */
export class DeterministicFirstEligiblePolicy implements WorkerSelectionPolicy {
  public readonly name = 'DeterministicFirstEligible';

  public selectWorker(eligibleWorkers: readonly WorkerCandidate[]): WorkerCandidate | null {
    if (!Array.isArray(eligibleWorkers) || eligibleWorkers.length === 0) {
      return null;
    }

    const sorted = [...eligibleWorkers].sort(compareWorkerCandidates);
    return sorted[0] ?? null;
  }
}

/**
 * Default singleton instance of the DeterministicFirstEligiblePolicy.
 */
export const deterministicFirstEligiblePolicy = new DeterministicFirstEligiblePolicy();
