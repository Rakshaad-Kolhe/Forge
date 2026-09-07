import type { WorkerCandidate } from '@forge/pipeline';
import { describe, expect, it } from 'vitest';
import {
  compareWorkerCandidates,
  DeterministicFirstEligiblePolicy,
  deterministicFirstEligiblePolicy,
  getWorkerCandidateId,
} from './policy.js';

describe('Policy — getWorkerCandidateId', () => {
  it('extracts ID from candidate with workerId property', () => {
    const candidate: WorkerCandidate = { workerId: 'worker-alpha' };
    expect(getWorkerCandidateId(candidate)).toBe('worker-alpha');
  });

  it('extracts ID from candidate with id property', () => {
    const candidate: WorkerCandidate = { id: 'worker-beta' };
    expect(getWorkerCandidateId(candidate)).toBe('worker-beta');
  });

  it('extracts ID from nested WorkerInfo shape', () => {
    const candidate: WorkerCandidate = {
      worker: {
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 8192 },
      },
      ...({ worker: { workerId: 'worker-gamma' } } as unknown as WorkerCandidate),
    };
    expect(getWorkerCandidateId(candidate)).toBe('worker-gamma');
  });

  it('returns empty string for null, undefined, or empty candidates', () => {
    expect(getWorkerCandidateId(null)).toBe('');
    expect(getWorkerCandidateId(undefined)).toBe('');
    expect(getWorkerCandidateId({})).toBe('');
  });
});

describe('Policy — compareWorkerCandidates', () => {
  it('orders candidates deterministically by ascending workerId', () => {
    const w1: WorkerCandidate = { workerId: 'worker-01' };
    const w2: WorkerCandidate = { workerId: 'worker-02' };
    const w10: WorkerCandidate = { workerId: 'worker-10' };

    expect(compareWorkerCandidates(w1, w2)).toBe(-1);
    expect(compareWorkerCandidates(w2, w1)).toBe(1);
    expect(compareWorkerCandidates(w1, w1)).toBe(0);
    expect(compareWorkerCandidates(w2, w10)).toBe(-1);
  });
});

describe('DeterministicFirstEligiblePolicy', () => {
  const policy = new DeterministicFirstEligiblePolicy();

  it('has canonical policy name', () => {
    expect(policy.name).toBe('DeterministicFirstEligible');
    expect(deterministicFirstEligiblePolicy.name).toBe('DeterministicFirstEligible');
  });

  it('returns null when eligible candidate list is empty', () => {
    expect(policy.selectWorker([])).toBeNull();
  });

  it('selects the sole worker when only one eligible candidate exists', () => {
    const candidate: WorkerCandidate = { workerId: 'worker-single' };
    const selected = policy.selectWorker([candidate]);
    expect(selected).toBe(candidate);
  });

  it('selects the first worker in ascending deterministic order', () => {
    const w1: WorkerCandidate = { workerId: 'worker-b' };
    const w2: WorkerCandidate = { workerId: 'worker-a' };
    const w3: WorkerCandidate = { workerId: 'worker-c' };

    const selected = policy.selectWorker([w1, w2, w3]);
    expect(getWorkerCandidateId(selected)).toBe('worker-a');
  });

  it('is completely order-invariant across all input permutations (deterministic reproducibility)', () => {
    const wA: WorkerCandidate = { workerId: 'worker-a' };
    const wB: WorkerCandidate = { workerId: 'worker-b' };
    const wC: WorkerCandidate = { workerId: 'worker-c' };

    const permutations = [
      [wA, wB, wC],
      [wA, wC, wB],
      [wB, wA, wC],
      [wB, wC, wA],
      [wC, wA, wB],
      [wC, wB, wA],
    ];

    for (const permutation of permutations) {
      const selected = policy.selectWorker(permutation);
      expect(getWorkerCandidateId(selected)).toBe('worker-a');
    }
  });
});
