import { describe, expect, it } from 'vitest';
import { createJobId, createPipelineRunId, Job } from '@forge/pipeline';
import {
  evaluatePlacement,
  evaluatePrioritizedWork,
  FairAgingPriorityPolicy,
  HighestPriorityFirstPolicy,
} from './index.js';

describe('Controlled Starvation Experiment (PR 16)', () => {
  const sampleWorker = {
    workerId: 'worker-node-1',
    status: 'READY' as const,
    liveness: 'ALIVE' as const,
    capabilities: { executors: ['docker'] },
    resources: { cpuCores: 4, memoryBytes: 8192 },
  };

  interface ExperimentRunResult {
    policyName: string;
    bypassedRounds: number;
    overtakeRound: number | null;
    overtakeTimeMs: number | null;
    scheduledAtTime: Date | null;
    finalEffectivePriority: number;
  }

  function runStarvationSimulation(options: {
    useFairness: boolean;
    agingIntervalMs: number;
    ageBonusStep: number;
    maxAgeBonus: number;
    lowPriority: number;
    highPriority: number;
    maxRounds: number;
  }): ExperimentRunResult {
    const {
      useFairness,
      agingIntervalMs,
      ageBonusStep,
      maxAgeBonus,
      lowPriority,
      highPriority,
      maxRounds,
    } = options;

    const baseEpoch = new Date('2026-09-08T12:00:00.000Z');
    const policy = useFairness
      ? new FairAgingPriorityPolicy({ agingIntervalMs, ageBonusStep, maxAgeBonus })
      : new HighestPriorityFirstPolicy();

    // The low priority job is submitted at t0 = baseEpoch
    const lowPriorityJob = new Job({
      id: createJobId('job-low-starvation-target'),
      pipelineRunId: createPipelineRunId('run-low'),
      stepName: 'build',
      command: 'echo low',
      priority: lowPriority,
      createdAt: baseEpoch,
    });
    lowPriorityJob.markQueued(baseEpoch);

    let bypassedRounds = 0;
    let overtakeRound: number | null = null;
    let overtakeTimeMs: number | null = null;
    let scheduledAtTime: Date | null = null;

    // Simulate rounds where at each aging interval a fresh high-priority job arrives
    for (let round = 0; round < maxRounds; round++) {
      const currentTime = new Date(baseEpoch.getTime() + round * agingIntervalMs);

      // Fresh high-priority job arrives at currentTime
      const highPriorityJob = new Job({
        id: createJobId(`job-high-stream-${round.toString().padStart(3, '0')}`),
        pipelineRunId: createPipelineRunId(`run-high-${round}`),
        stepName: 'build',
        command: 'echo high',
        priority: highPriority,
        createdAt: currentTime,
      });
      highPriorityJob.markQueued(currentTime);

      // Candidate pool for this round: the waiting low-priority job and the new high-priority job
      const batch = [lowPriorityJob, highPriorityJob];

      // Scheduler orders batch and evaluates placement for single worker
      const ordered = policy.orderJobs(batch, currentTime);
      const topJob = ordered[0]!;

      if (topJob.id === lowPriorityJob.id) {
        // Low priority job has overtaken high-priority stream!
        overtakeRound = round;
        overtakeTimeMs = round * agingIntervalMs;
        scheduledAtTime = currentTime;

        // Verify placement
        const decision = evaluatePlacement(
          topJob,
          [sampleWorker],
          undefined,
          undefined,
          currentTime,
        );
        expect(decision.status).toBe('SCHEDULED');
        expect(decision.jobId).toBe(lowPriorityJob.id);
        break;
      } else {
        bypassedRounds++;
      }
    }

    const finalEffectivePriority = useFairness
      ? (policy as FairAgingPriorityPolicy).orderJobs(
          [lowPriorityJob],
          scheduledAtTime ?? new Date(baseEpoch.getTime() + maxRounds * agingIntervalMs),
        )[0]!.priority
      : lowPriority;

    return {
      policyName: policy.name,
      bypassedRounds,
      overtakeRound,
      overtakeTimeMs,
      scheduledAtTime,
      finalEffectivePriority,
    };
  }

  it('demonstrates indefinite starvation under strict priority (HighestPriorityFirst)', () => {
    // Under strict priority, a low-priority job (prio 10) is starved indefinitely
    // across all 10 simulation rounds by arrival of high-priority jobs (prio 50)
    const result = runStarvationSimulation({
      useFairness: false,
      agingIntervalMs: 60000,
      ageBonusStep: 10,
      maxAgeBonus: 100,
      lowPriority: 10,
      highPriority: 50,
      maxRounds: 10,
    });

    expect(result.policyName).toBe('HighestPriorityFirst');
    expect(result.overtakeRound).toBeNull(); // Never overtakes
    expect(result.bypassedRounds).toBe(10); // Starved on all rounds
    expect(result.scheduledAtTime).toBeNull();
  });

  it('demonstrates bounded starvation recovery under queue aging (FairAgingPriority)', () => {
    // Base low priority = 10
    // Stream high priority = 50
    // agingInterval = 60,000ms (1 min), step = 10, maxAgeBonus = 100
    //
    // Round 0 (t = 0m):   low eff = 10, high eff = 50 -> high wins (bypassed 1)
    // Round 1 (t = 1m):   low eff = 20, high eff = 50 -> high wins (bypassed 2)
    // Round 2 (t = 2m):   low eff = 30, high eff = 50 -> high wins (bypassed 3)
    // Round 3 (t = 3m):   low eff = 40, high eff = 50 -> high wins (bypassed 4)
    // Round 4 (t = 4m):   low eff = 50, high eff = 50 -> tie-break by jobId:
    //                     'job-high-stream-004' < 'job-low-starvation-target' -> high wins (bypassed 5)
    // Round 5 (t = 5m):   low eff = 60, high eff = 50 -> 60 > 50 -> LOW PRIORITY WINS!
    //
    // Bounded bypass count = 5 rounds, overtake time = 300,000ms (5 minutes).
    const result = runStarvationSimulation({
      useFairness: true,
      agingIntervalMs: 60000,
      ageBonusStep: 10,
      maxAgeBonus: 100,
      lowPriority: 10,
      highPriority: 50,
      maxRounds: 10,
    });

    expect(result.policyName).toBe('FairAgingPriority');
    expect(result.overtakeRound).toBe(5);
    expect(result.overtakeTimeMs).toBe(300000); // 5 minutes
    expect(result.bypassedRounds).toBe(5);
    expect(result.scheduledAtTime).toEqual(new Date('2026-09-08T12:05:00.000Z'));
  });

  it('demonstrates saturation limit: low priority cannot overtake high priority if gap exceeds maxAgeBonus', () => {
    // Low priority = 10
    // Urgent priority = 200
    // maxAgeBonus = 100 -> low priority can reach at most 110 effective priority.
    // Urgent priority (200) must ALWAYS take precedence, confirming that queue aging
    // is safely bounded and cannot overpower critical/urgent priority bands.
    const result = runStarvationSimulation({
      useFairness: true,
      agingIntervalMs: 60000,
      ageBonusStep: 10,
      maxAgeBonus: 100,
      lowPriority: 10,
      highPriority: 200,
      maxRounds: 15,
    });

    expect(result.overtakeRound).toBeNull();
    expect(result.bypassedRounds).toBe(15);
  });

  it('evaluates prioritized batch with fair aging through ForgeScheduler', () => {
    const t0 = new Date('2026-09-08T12:00:00.000Z');
    const tAged = new Date('2026-09-08T12:06:00.000Z'); // 6 intervals (60 bonus)

    const lowJob = new Job({
      id: createJobId('job-batch-low'),
      pipelineRunId: createPipelineRunId('run-1'),
      stepName: 'test',
      command: 'echo test',
      priority: 10,
      createdAt: t0,
    });
    lowJob.markQueued(t0);

    const highJob = new Job({
      id: createJobId('job-batch-high'),
      pipelineRunId: createPipelineRunId('run-2'),
      stepName: 'test',
      command: 'echo test',
      priority: 50,
      createdAt: tAged, // just arrived
    });
    highJob.markQueued(tAged);

    const policy = new FairAgingPriorityPolicy({
      agingIntervalMs: 60000,
      ageBonusStep: 10,
      maxAgeBonus: 100,
    });

    const result = evaluatePrioritizedWork(
      [highJob, lowJob],
      [sampleWorker],
      policy,
      undefined,
      undefined,
      tAged,
    );

    // lowJob effective priority is 10 + 60 = 70 > highJob 50
    expect(result.orderedDecisions[0]!.jobId).toBe('job-batch-low');
    expect(result.orderedDecisions[1]!.jobId).toBe('job-batch-high');
    expect(result.scheduledDecisions).toHaveLength(2);
  });
});
