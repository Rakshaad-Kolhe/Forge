import {
  DEFAULT_JOB_PRIORITY,
  type JobRequirements,
  type LeaseRecoveryOptions,
  type RecoverExpiredLeasesResult,
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
import type { LeaseRecoveryService, WorkerLeaseRepository } from '@forge/database';
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
    | {
        readonly id?: string;
        readonly requirements?: JobRequirements;
        readonly priority?: number;
        readonly nextAttemptAt?: Date | null;
      }
    | JobRequirements
    | undefined
    | null,
  candidates: readonly WorkerCandidate[],
  policyOrNow?: WorkerSelectionPolicy | Date,
  matcher: EligibilityMatcher = filterEligibleWorkers,
  now?: Date,
): ScheduleDecision {
  let policy: WorkerSelectionPolicy = deterministicFirstEligiblePolicy;
  let effectiveNow = now ?? new Date();

  if (policyOrNow instanceof Date) {
    effectiveNow = policyOrNow;
  } else if (policyOrNow) {
    policy = policyOrNow;
  }

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

  // 1. Check if retry backoff is active
  if (
    jobOrRequirements &&
    typeof jobOrRequirements === 'object' &&
    'nextAttemptAt' in jobOrRequirements &&
    jobOrRequirements.nextAttemptAt instanceof Date
  ) {
    if (jobOrRequirements.nextAttemptAt > effectiveNow) {
      const unschedulable: UnschedulableDecision = {
        status: 'UNSCHEDULABLE',
        jobId,
        candidateWorkerCount,
        eligibleWorkerCount: 0,
        reason: 'RETRY_BACKOFF_ACTIVE',
        failureReasons: Object.freeze([
          `Retry backoff active until ${jobOrRequirements.nextAttemptAt.toISOString()}`,
        ]),
        priority,
      };
      return unschedulable;
    }
  }

  // 2. Resolve and validate job requirements
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
  findSchedulableJobs?(options?: { now?: Date; limit?: number }): Promise<Job[]>;
}): JobSource {
  return {
    getJob: (jobId: string) => repository.findById(createJobId(jobId)),
    findSchedulableJobs: repository.findSchedulableJobs
      ? (options?: { now?: Date; limit?: number }) => repository.findSchedulableJobs!(options)
      : undefined,
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
  private readonly leaseRepository?: WorkerLeaseRepository;
  private readonly leaseDurationMs: number;
  private readonly recoveryService?: LeaseRecoveryService;
  private readonly logger?: Logger;
  private recoveryTimer?: NodeJS.Timeout;
  private isSweeping = false;

  constructor(options?: SchedulerOptions) {
    this.workerSource = options?.workerSource;
    this.jobSource = options?.jobSource;
    this.selectionPolicy = options?.selectionPolicy ?? deterministicFirstEligiblePolicy;
    this.jobPolicy = options?.jobPolicy ?? highestPriorityFirstPolicy;
    this.matcher = options?.matcher ?? filterEligibleWorkers;
    this.leaseRepository = options?.leaseRepository;
    this.leaseDurationMs = options?.leaseDurationMs ?? 30000;
    this.recoveryService = options?.recoveryService;
    this.logger = options?.logger;
  }

  /**
   * Schedules a single job either directly by domain object or by ID via configured JobSource.
   * If an eligible worker is selected and a leaseRepository is configured, atomically claims
   * a time-bounded distributed worker lease.
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

    let finalDecision: ScheduleDecision = decision;
    if (decision.status === 'SCHEDULED' && this.leaseRepository) {
      finalDecision = await this.claimLeaseForDecision(decision);
    }

    this.logger?.info('Scheduler placement evaluated', {
      jobId: finalDecision.jobId,
      status: finalDecision.status,
      candidateWorkerCount: finalDecision.candidateWorkerCount,
      eligibleWorkerCount: finalDecision.eligibleWorkerCount,
      policy: this.selectionPolicy.name,
      ...(finalDecision.status === 'SCHEDULED'
        ? {
            selectedWorkerId: finalDecision.workerId,
            ...(finalDecision.lease ? { leaseId: finalDecision.lease.id } : {}),
          }
        : { reason: finalDecision.reason }),
    });

    return finalDecision;
  }

  /**
   * Evaluates placement for a collection of jobs in priority order.
   * Jobs can be passed as domain Job instances or string job IDs.
   * If a leaseRepository is configured, atomically claims worker leases for placed jobs.
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

    const baseResult = evaluatePrioritizedWork(
      jobs,
      candidates,
      this.jobPolicy,
      this.selectionPolicy,
      this.matcher,
    );

    let result = baseResult;
    if (this.leaseRepository) {
      const finalOrdered: ScheduleDecision[] = [];
      const finalScheduled: ScheduledDecision[] = [];
      const finalUnschedulable: UnschedulableDecision[] = [...baseResult.unschedulableDecisions];

      for (const dec of baseResult.orderedDecisions) {
        if (dec.status === 'SCHEDULED') {
          const finalDec = await this.claimLeaseForDecision(dec);
          finalOrdered.push(finalDec);
          if (finalDec.status === 'SCHEDULED') {
            finalScheduled.push(finalDec);
          } else {
            finalUnschedulable.push(finalDec);
          }
        } else {
          finalOrdered.push(dec);
        }
      }

      result = {
        orderedDecisions: Object.freeze(finalOrdered),
        scheduledDecisions: Object.freeze(finalScheduled),
        unschedulableDecisions: Object.freeze(finalUnschedulable),
      };
    }

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
   * Discovers and evaluates placement for all currently due schedulable jobs from the JobSource.
   */
  public async scheduleDueJobs(options?: {
    now?: Date;
    limit?: number;
  }): Promise<PrioritizedScheduleResult & { processedCount: number; scheduledCount: number }> {
    if (!this.jobSource) {
      throw new JobSourceError('JobSource is required to schedule due jobs');
    }
    if (!this.jobSource.findSchedulableJobs) {
      throw new JobSourceError('JobSource does not support findSchedulableJobs');
    }

    let dueJobs: Job[];
    try {
      dueJobs = await this.jobSource.findSchedulableJobs(options);
    } catch (err) {
      throw new JobSourceError(
        `Failed to find schedulable jobs: ${(err as Error).message}`,
        err as Error,
      );
    }

    if (dueJobs.length === 0) {
      return {
        orderedDecisions: Object.freeze([]),
        scheduledDecisions: Object.freeze([]),
        unschedulableDecisions: Object.freeze([]),
        processedCount: 0,
        scheduledCount: 0,
      };
    }

    const result = await this.schedulePrioritized(dueJobs);
    return {
      ...result,
      processedCount: result.orderedDecisions.length,
      scheduledCount: result.scheduledDecisions.length,
    };
  }

  /**
   * Helper to claim a worker lease for a placement decision.
   */
  private async claimLeaseForDecision(decision: ScheduledDecision): Promise<ScheduleDecision> {
    if (!this.leaseRepository) {
      return decision;
    }

    const claimResult = await this.leaseRepository.claim({
      jobId: decision.jobId,
      workerId: decision.workerId,
      durationMs: this.leaseDurationMs,
    });

    if (claimResult.status === 'ACQUIRED') {
      const decisionWithLease: ScheduledDecision = {
        ...decision,
        lease: claimResult.lease,
      };
      this.logger?.info('Scheduler placed job and acquired worker lease', {
        jobId: decision.jobId,
        workerId: decision.workerId,
        leaseId: claimResult.lease.id,
        expiresAt: claimResult.lease.expiresAt,
      });
      return decisionWithLease;
    }

    if (claimResult.status === 'CONFLICT') {
      const unschedulable: UnschedulableDecision = {
        status: 'UNSCHEDULABLE',
        jobId: decision.jobId,
        candidateWorkerCount: decision.candidateWorkerCount,
        eligibleWorkerCount: decision.eligibleWorkerCount,
        priority: decision.priority,
        reason: 'LEASE_CONFLICT',
        failureReasons: Object.freeze([
          `Active lease already held by worker "${claimResult.currentOwnerId}" until ${claimResult.expiresAt.toISOString()}`,
        ]),
      };
      this.logger?.warn('Scheduler placement failed due to active lease conflict', {
        jobId: decision.jobId,
        currentOwnerId: claimResult.currentOwnerId,
        expiresAt: claimResult.expiresAt,
      });
      return unschedulable;
    }

    // NOT_CLAIMABLE
    const unschedulable: UnschedulableDecision = {
      status: 'UNSCHEDULABLE',
      jobId: decision.jobId,
      candidateWorkerCount: decision.candidateWorkerCount,
      eligibleWorkerCount: decision.eligibleWorkerCount,
      priority: decision.priority,
      reason: 'LEASE_CONFLICT',
      failureReasons: Object.freeze([claimResult.details ?? 'Job not claimable']),
    };
    this.logger?.warn('Scheduler placement failed: job not claimable', {
      jobId: decision.jobId,
      reason: claimResult.reason,
      details: claimResult.details,
    });
    return unschedulable;
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

  /**
   * Sweeps and recovers expired active leases via the configured LeaseRecoveryService.
   */
  public async recoverExpiredLeases(
    options?: LeaseRecoveryOptions,
  ): Promise<RecoverExpiredLeasesResult> {
    if (!this.recoveryService) {
      throw new Error('LeaseRecoveryService is required to recover expired leases');
    }
    return this.recoveryService.recoverExpiredLeases(options);
  }

  /**
   * Starts a non-overlapping periodic background recovery sweep loop.
   */
  public startRecoveryLoop(intervalMs = 5000): void {
    if (this.recoveryTimer) {
      return;
    }
    if (!this.recoveryService) {
      throw new Error('LeaseRecoveryService is required to start recovery loop');
    }

    this.recoveryTimer = setInterval(async () => {
      if (this.isSweeping) {
        return;
      }
      this.isSweeping = true;
      try {
        const result = await this.recoverExpiredLeases();
        if (result.recoveredCount > 0) {
          this.logger?.info('Background lease recovery sweep completed', {
            recoveredCount: result.recoveredCount,
          });
        }
      } catch (err: unknown) {
        this.logger?.error('Error during background lease recovery sweep', {
          error: err instanceof Error ? err.message : String(err),
        });
      } finally {
        this.isSweeping = false;
      }
    }, intervalMs);

    this.recoveryTimer.unref();
  }

  /**
   * Stops the background recovery sweep loop cleanly, waiting for any in-flight sweep to finish.
   */
  public async stopRecoveryLoop(): Promise<void> {
    if (this.recoveryTimer) {
      clearInterval(this.recoveryTimer);
      this.recoveryTimer = undefined;
    }
    while (this.isSweeping) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
}
