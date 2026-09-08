import type {
  JobRequirements,
  ScheduleDecision,
  ScheduledDecision,
  ScheduleDecisionStatus,
  UnschedulableDecision,
  UnschedulableReason,
} from '@forge/contracts';
import type { LeaseRecoveryService, WorkerLeaseRepository } from '@forge/database';
import type { Logger } from '@forge/logging';
import type { Job, WorkerCandidate } from '@forge/pipeline';
import type { WorkerLiveness, WorkerStatus } from '@forge/worker-registry';

export type {
  JobRequirements,
  ScheduleDecision,
  ScheduledDecision,
  ScheduleDecisionStatus,
  UnschedulableDecision,
  UnschedulableReason,
  WorkerLeaseRepository,
  LeaseRecoveryService,
};

/**
 * Pluggable job ordering policy interface.
 * Given a collection of jobs, orders them according to priority and scheduling policy rules.
 */
export interface JobOrderingPolicy {
  /**
   * Unique descriptive name of the policy.
   */
  readonly name: string;

  /**
   * Orders candidate jobs according to policy rules.
   */
  orderJobs<
    T extends { readonly priority?: number; readonly id?: string; readonly jobId?: string },
  >(
    jobs: readonly T[],
  ): T[];
}

/**
 * Aggregated result of evaluating placement for a prioritized batch of jobs.
 */
export interface PrioritizedScheduleResult {
  /**
   * All decisions ordered matching the evaluation sequence (highest priority first).
   */
  readonly orderedDecisions: readonly ScheduleDecision[];

  /**
   * Decisions that successfully placed a job on an eligible worker.
   */
  readonly scheduledDecisions: readonly ScheduledDecision[];

  /**
   * Decisions where the job could not be placed, with reasons.
   */
  readonly unschedulableDecisions: readonly UnschedulableDecision[];
}

/**
 * Pluggable worker selection policy interface.
 * Given a set of eligible workers that satisfy job requirements,
 * selects exactly one worker according to the policy rules.
 */
export interface WorkerSelectionPolicy {
  /**
   * Unique descriptive name of the policy.
   */
  readonly name: string;

  /**
   * Evaluates the eligible worker set and returns the selected candidate,
   * or null if the candidate set is empty.
   */
  selectWorker(eligibleWorkers: readonly WorkerCandidate[]): WorkerCandidate | null;
}

/**
 * Abstraction for retrieving candidate workers from an underlying store or registry.
 */
export interface WorkerSource {
  listWorkers(filter?: {
    status?: WorkerStatus;
    liveness?: WorkerLiveness;
  }): Promise<readonly WorkerCandidate[]>;
}

/**
 * Abstraction for retrieving job definitions by ID.
 */
export interface JobSource {
  getJob(jobId: string): Promise<Job | null>;
  findSchedulableJobs?(options?: { now?: Date; limit?: number }): Promise<Job[]>;
}

/**
 * Pure predicate/filtering function type for evaluating worker capability and resource eligibility.
 */
export type EligibilityMatcher = <T extends WorkerCandidate>(
  jobOrRequirements:
    { readonly requirements?: JobRequirements } | JobRequirements | undefined | null,
  workers: readonly T[],
) => T[];

/**
 * High-level scheduler service interface.
 */
export interface Scheduler {
  /**
   * Evaluates candidate workers for the specified job and produces an explainable scheduling decision.
   */
  schedule(jobOrId: Job | string): Promise<ScheduleDecision>;
}

/**
 * Dependency injection options for configuring the ForgeScheduler.
 */
export interface SchedulerOptions {
  readonly workerSource?: WorkerSource;
  readonly jobSource?: JobSource;
  readonly selectionPolicy?: WorkerSelectionPolicy;
  readonly jobPolicy?: JobOrderingPolicy;
  readonly matcher?: EligibilityMatcher;
  readonly leaseRepository?: WorkerLeaseRepository;
  readonly leaseDurationMs?: number;
  readonly recoveryService?: LeaseRecoveryService;
  readonly logger?: Logger;
}
