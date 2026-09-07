import type {
  JobRequirements,
  ScheduleDecision,
  ScheduledDecision,
  ScheduleDecisionStatus,
  UnschedulableDecision,
  UnschedulableReason,
} from '@forge/contracts';
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
};

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
  readonly matcher?: EligibilityMatcher;
  readonly logger?: Logger;
}
