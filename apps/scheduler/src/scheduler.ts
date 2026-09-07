import type {
  JobRequirements,
  ScheduleDecision,
  ScheduledDecision,
  UnschedulableDecision,
} from '@forge/contracts';
import type { Logger } from '@forge/logging';
import {
  checkJobRequirementsValidity,
  createJobId,
  filterEligibleWorkers,
  matchesWorker,
  type Job,
  type WorkerCandidate,
} from '@forge/pipeline';
import type { JobQueue, QueueDelivery } from '@forge/queue';
import { JobNotFoundError, JobSourceError, WorkerSourceError } from './errors.js';
import { deterministicFirstEligiblePolicy, getWorkerCandidateId } from './policy.js';
import type {
  EligibilityMatcher,
  JobSource,
  Scheduler,
  SchedulerOptions,
  WorkerSelectionPolicy,
  WorkerSource,
} from './types.js';

/**
 * Checks whether a candidate worker satisfies the operational eligibility requirement:
 * status = READY and liveness = ALIVE.
 */
export function isOperationallyEligible(candidate: WorkerCandidate): boolean {
  const info = candidate as {
    status?: string;
    liveness?: string;
    worker?: { status?: string };
  };

  const status = info.worker?.status ?? info.status;
  const liveness = info.liveness;

  if (status !== undefined && status !== 'READY') {
    return false;
  }

  if (liveness !== undefined && liveness !== 'ALIVE') {
    return false;
  }

  return true;
}

/**
 * Pure, deterministic evaluation of worker placement for a job.
 * Has zero side-effects and performs no I/O.
 *
 * Sequence:
 * 1. Validates job requirements safety.
 * 2. Restricts candidate workers to operationally eligible ones (READY + ALIVE).
 * 3. Evaluates capability and resource matching via PR 09 matcher.
 * 4. Deterministically selects one worker using the selection policy.
 * 5. Produces an explainable ScheduleDecision.
 */
export function evaluatePlacement(
  jobOrRequirements:
    | { readonly id?: string; readonly requirements?: JobRequirements }
    | JobRequirements
    | undefined
    | null,
  candidates: readonly WorkerCandidate[],
  policy: WorkerSelectionPolicy = deterministicFirstEligiblePolicy,
  matcher: EligibilityMatcher = filterEligibleWorkers,
): ScheduleDecision {
  const candidateArray = Array.isArray(candidates) ? candidates : [];
  const candidateWorkerCount = candidateArray.length;

  const jobId =
    jobOrRequirements && 'id' in jobOrRequirements && typeof jobOrRequirements.id === 'string'
      ? jobOrRequirements.id
      : 'unspecified';

  // 1. Resolve and validate job requirements
  const req: JobRequirements | undefined =
    jobOrRequirements && 'requirements' in jobOrRequirements
      ? jobOrRequirements.requirements
      : (jobOrRequirements as JobRequirements | undefined);

  const validity = checkJobRequirementsValidity(req);
  if (!validity.valid) {
    const unschedulable: UnschedulableDecision = {
      status: 'UNSCHEDULABLE',
      jobId,
      candidateWorkerCount,
      eligibleWorkerCount: 0,
      reason: 'INVALID_JOB_REQUIREMENTS',
      failureReasons: Object.freeze(['INVALID_REQUIREMENTS']),
    };
    return unschedulable;
  }

  // 2. Restrict to operationally eligible workers (READY + ALIVE)
  const operationalCandidates = candidateArray.filter(isOperationallyEligible);

  // 3. Filter using capability/resource matcher (PR 09)
  const eligibleWorkers = matcher(jobOrRequirements, operationalCandidates);
  const eligibleWorkerCount = eligibleWorkers.length;

  if (eligibleWorkerCount === 0) {
    // Collect failure reasons across candidates for diagnostic explainability
    const failureSet = new Set<string>();
    for (const cand of operationalCandidates) {
      const matchResult = matchesWorker(jobOrRequirements, cand);
      if (!matchResult.matched && matchResult.reasons) {
        for (const reason of matchResult.reasons) {
          failureSet.add(reason);
        }
      }
    }

    const unschedulable: UnschedulableDecision = {
      status: 'UNSCHEDULABLE',
      jobId,
      candidateWorkerCount,
      eligibleWorkerCount: 0,
      reason: 'NO_ELIGIBLE_WORKER',
      ...(failureSet.size > 0 ? { failureReasons: Object.freeze(Array.from(failureSet)) } : {}),
    };
    return unschedulable;
  }

  // 4. Deterministically select a worker using selection policy
  const selectedWorker = policy.selectWorker(eligibleWorkers);
  if (!selectedWorker) {
    const unschedulable: UnschedulableDecision = {
      status: 'UNSCHEDULABLE',
      jobId,
      candidateWorkerCount,
      eligibleWorkerCount,
      reason: 'NO_ELIGIBLE_WORKER',
    };
    return unschedulable;
  }

  const selectedWorkerId = getWorkerCandidateId(selectedWorker);

  // 5. Return explainable scheduling decision
  const scheduled: ScheduledDecision = {
    status: 'SCHEDULED',
    jobId,
    workerId: selectedWorkerId,
    candidateWorkerCount,
    eligibleWorkerCount,
    reason: `Selected worker "${selectedWorkerId}" via policy "${policy.name}"`,
  };

  return scheduled;
}

/**
 * Helper to adapt a JobRepository into a JobSource for the ForgeScheduler.
 */
export function createJobSourceFromRepository(repository: {
  findById(id: ReturnType<typeof createJobId>): Promise<Job | null>;
}): JobSource {
  return {
    getJob: (jobId: string) => repository.findById(createJobId(jobId)),
  };
}

/**
 * First-class Forge Scheduler service layer.
 * Implements deterministic worker selection decoupled from job claiming, leases, and execution.
 */
export class ForgeScheduler implements Scheduler {
  private readonly workerSource?: WorkerSource;
  private readonly jobSource?: JobSource;
  private readonly selectionPolicy: WorkerSelectionPolicy;
  private readonly matcher: EligibilityMatcher;
  private readonly logger?: Logger;

  constructor(options?: SchedulerOptions) {
    this.workerSource = options?.workerSource;
    this.jobSource = options?.jobSource;
    this.selectionPolicy = options?.selectionPolicy ?? deterministicFirstEligiblePolicy;
    this.matcher = options?.matcher ?? filterEligibleWorkers;
    this.logger = options?.logger;
  }

  /**
   * Schedules a job either directly by domain object or by ID via configured JobSource.
   */
  public async schedule(jobOrId: Job | string): Promise<ScheduleDecision> {
    let job: Job;

    if (typeof jobOrId === 'string') {
      if (!this.jobSource) {
        throw new JobSourceError('JobSource is required to schedule by jobId');
      }

      let retrieved: Job | null;
      try {
        retrieved = await this.jobSource.getJob(jobOrId);
      } catch (err) {
        throw new JobSourceError(
          `Failed to retrieve job "${jobOrId}": ${(err as Error).message}`,
          err as Error,
        );
      }

      if (!retrieved) {
        throw new JobNotFoundError(jobOrId);
      }

      job = retrieved;
    } else {
      job = jobOrId;
    }

    let candidates: readonly WorkerCandidate[];
    if (this.workerSource) {
      try {
        candidates = await this.workerSource.listWorkers({
          status: 'READY',
          liveness: 'ALIVE',
        });
      } catch (err) {
        throw new WorkerSourceError(
          `Failed to list candidate workers: ${(err as Error).message}`,
          err as Error,
        );
      }
    } else {
      candidates = [];
    }

    const decision = evaluatePlacement(job, candidates, this.selectionPolicy, this.matcher);

    this.logger?.info('Scheduler placement evaluated', {
      jobId: decision.jobId,
      status: decision.status,
      candidateWorkerCount: decision.candidateWorkerCount,
      eligibleWorkerCount: decision.eligibleWorkerCount,
      policy: this.selectionPolicy.name,
      ...(decision.status === 'SCHEDULED'
        ? { selectedWorkerId: decision.workerId }
        : { reason: decision.reason }),
    });

    return decision;
  }

  /**
   * Dequeues the next ready job message from the FIFO queue, reconstructs the job,
   * and evaluates worker placement without permanently acknowledging the message.
   *
   * This preserves unacknowledged queue message recoverability under visibility timeout.
   */
  public async scheduleNext(
    queue: JobQueue,
    options?: { visibilityTimeoutSeconds?: number },
  ): Promise<{ delivery: QueueDelivery; decision: ScheduleDecision } | null> {
    const delivery = await queue.dequeue(options);
    if (!delivery) {
      return null;
    }

    const decision = await this.schedule(delivery.message.jobId);

    // CRITICAL: We intentionally do NOT call queue.acknowledge(delivery.message.messageId).
    // A scheduling decision is NOT a job claim, worker lease, or execution start.
    // The message remains safely recoverable under visibility timeout until future lease claim.

    return { delivery, decision };
  }
}
