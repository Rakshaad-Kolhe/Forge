import {
  DEFAULT_JOB_PRIORITY,
  type JobRequirements,
  type ScheduleDecision,
  type ScheduledDecision,
  type UnschedulableDecision,
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
import { highestPriorityFirstPolicy } from './job-policy.js';
import { deterministicFirstEligiblePolicy, getWorkerCandidateId } from './policy.js';
import type {
  EligibilityMatcher,
  JobOrderingPolicy,
  JobSource,
  PrioritizedScheduleResult,
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
 * 5. Produces an explainable ScheduleDecision including job priority.
 */
export function evaluatePlacement(
  jobOrRequirements:
    | { readonly id?: string; readonly requirements?: JobRequirements; readonly priority?: number }
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

  const priority: number =
    jobOrRequirements &&
    typeof jobOrRequirements === 'object' &&
    'priority' in jobOrRequirements &&
    typeof (jobOrRequirements as { priority?: unknown }).priority === 'number'
      ? (jobOrRequirements as { priority: number }).priority
      : DEFAULT_JOB_PRIORITY;

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
      priority,
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
      priority,
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
      priority,
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
    priority,
  };

  return scheduled;
}

/**
 * Pure, deterministic evaluation of placement for a batch of jobs in priority order.
 *
 * Sequence:
 * 1. Orders jobs using the specified JobOrderingPolicy (defaults to HighestPriorityFirstPolicy).
 * 2. Evaluates placement for each job in order against candidate workers.
 * 3. Non-blocking unschedulable semantics: If a higher-priority job cannot be scheduled,
 *    it is recorded as UNSCHEDULABLE and evaluation proceeds to the next job.
 * 4. Aggregates and returns PrioritizedScheduleResult.
 */
export function evaluatePrioritizedWork(
  jobs: readonly Job[],
  candidates: readonly WorkerCandidate[],
  jobPolicy: JobOrderingPolicy = highestPriorityFirstPolicy,
  workerPolicy: WorkerSelectionPolicy = deterministicFirstEligiblePolicy,
  matcher: EligibilityMatcher = filterEligibleWorkers,
): PrioritizedScheduleResult {
  const orderedJobs = jobPolicy.orderJobs(jobs);

  const orderedDecisions: ScheduleDecision[] = [];
  const scheduledDecisions: ScheduledDecision[] = [];
  const unschedulableDecisions: UnschedulableDecision[] = [];

  for (const job of orderedJobs) {
    const decision = evaluatePlacement(job, candidates, workerPolicy, matcher);
    orderedDecisions.push(decision);

    if (decision.status === 'SCHEDULED') {
      scheduledDecisions.push(decision);
    } else {
      unschedulableDecisions.push(decision);
    }
  }

  return {
    orderedDecisions: Object.freeze(orderedDecisions),
    scheduledDecisions: Object.freeze(scheduledDecisions),
    unschedulableDecisions: Object.freeze(unschedulableDecisions),
  };
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
  private readonly jobPolicy: JobOrderingPolicy;
  private readonly matcher: EligibilityMatcher;
  private readonly logger?: Logger;

  constructor(options?: SchedulerOptions) {
    this.workerSource = options?.workerSource;
    this.jobSource = options?.jobSource;
    this.selectionPolicy = options?.selectionPolicy ?? deterministicFirstEligiblePolicy;
    this.jobPolicy = options?.jobPolicy ?? highestPriorityFirstPolicy;
    this.matcher = options?.matcher ?? filterEligibleWorkers;
    this.logger = options?.logger;
  }

  /**
   * Schedules a single job either directly by domain object or by ID via configured JobSource.
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
   * Evaluates placement for a collection of jobs in priority order.
   * Jobs can be passed as domain Job instances or string job IDs.
   */
  public async schedulePrioritized(
    jobsOrIds: readonly (Job | string)[],
  ): Promise<PrioritizedScheduleResult> {
    const jobs: Job[] = [];

    for (const item of jobsOrIds) {
      if (typeof item === 'string') {
        if (!this.jobSource) {
          throw new JobSourceError('JobSource is required to resolve job by ID');
        }
        let retrieved: Job | null;
        try {
          retrieved = await this.jobSource.getJob(item);
        } catch (err) {
          throw new JobSourceError(
            `Failed to retrieve job "${item}": ${(err as Error).message}`,
            err as Error,
          );
        }
        if (!retrieved) {
          throw new JobNotFoundError(item);
        }
        jobs.push(retrieved);
      } else {
        jobs.push(item);
      }
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

    const result = evaluatePrioritizedWork(
      jobs,
      candidates,
      this.jobPolicy,
      this.selectionPolicy,
      this.matcher,
    );

    this.logger?.info('Prioritized placement batch evaluated', {
      totalJobs: jobs.length,
      scheduledCount: result.scheduledDecisions.length,
      unschedulableCount: result.unschedulableDecisions.length,
      jobPolicy: this.jobPolicy.name,
      workerPolicy: this.selectionPolicy.name,
    });

    return result;
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

  /**
   * Dequeues a batch of ready job messages from the FIFO queue, reconstructs jobs,
   * orders them by priority, and evaluates worker placement in priority order.
   *
   * Crucially preserves unacknowledged queue message recoverability under visibility timeout:
   * does NOT acknowledge messages upon scheduling.
   */
  public async scheduleNextBatch(
    queue: JobQueue,
    batchSize: number,
    options?: { visibilityTimeoutSeconds?: number },
  ): Promise<{ deliveries: readonly QueueDelivery[]; result: PrioritizedScheduleResult }> {
    const deliveries: QueueDelivery[] = [];

    for (let i = 0; i < batchSize; i++) {
      const delivery = await queue.dequeue(options);
      if (!delivery) {
        break;
      }
      deliveries.push(delivery);
    }

    if (deliveries.length === 0) {
      return {
        deliveries: Object.freeze([]),
        result: {
          orderedDecisions: Object.freeze([]),
          scheduledDecisions: Object.freeze([]),
          unschedulableDecisions: Object.freeze([]),
        },
      };
    }

    const jobs: Job[] = [];
    for (const delivery of deliveries) {
      const jobId = delivery.message.jobId;
      if (!this.jobSource) {
        throw new JobSourceError('JobSource is required to schedule dequeued jobs');
      }

      let retrieved: Job | null;
      try {
        retrieved = await this.jobSource.getJob(jobId);
      } catch (err) {
        throw new JobSourceError(
          `Failed to retrieve job "${jobId}": ${(err as Error).message}`,
          err as Error,
        );
      }

      if (!retrieved) {
        throw new JobNotFoundError(jobId);
      }

      jobs.push(retrieved);
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

    const result = evaluatePrioritizedWork(
      jobs,
      candidates,
      this.jobPolicy,
      this.selectionPolicy,
      this.matcher,
    );

    return {
      deliveries: Object.freeze(deliveries),
      result,
    };
  }
}
