import os from 'node:os';
import crypto from 'node:crypto';
import { loadConfig } from '@forge/config';
import type {
  ClaimJobResult,
  ExecutionResult,
  Executor,
  ReleaseLeaseResult,
  RenewLeaseResult,
  RetryDecision,
  WorkerLease,
} from '@forge/contracts';
import {
  withTransaction,
  type DatabasePool,
  type JobRepository,
  type WorkerLeaseRepository,
} from '@forge/database';
import {
  createForgeEvent,
  deriveLogChunkEvents,
  safePublish,
  type EventPublisher,
  type ForgeEvent,
  type JobFailureKind,
} from '@forge/events';
import { DockerExecutor } from '@forge/executor';
import { createLogger, type Logger } from '@forge/logging';
import { evaluateRetry, type Job, type JobAttempt } from '@forge/pipeline';
import {
  type WorkerRegistry,
  type WorkerId,
  type WorkerStatus,
  type WorkerResources,
  type WorkerCapabilities,
  createWorkerId,
} from '@forge/worker-registry';

export interface StartWorkerOptions {
  logger?: Logger;
  workerId?: string;
  registry?: WorkerRegistry;
  leaseRepository?: WorkerLeaseRepository;
  jobRepository?: JobRepository;
  pool?: DatabasePool;
  executor?: Executor;
  defaultLeaseDurationMs?: number;
  defaultLeaseRenewalIntervalMs?: number;
  hostname?: string;
  capabilities?: WorkerCapabilities;
  resources?: WorkerResources;
  heartbeatIntervalMs?: number;
  drainTimeoutMs?: number;
  /**
   * Optional typed lifecycle event publisher. When set, the worker emits `WorkerRegistered`,
   * `WorkerHeartbeat`, `JobStarted`, `JobLogChunk` (derived from the completed execution
   * result), the terminal `JobSucceeded` / `JobFailed` / `JobCancelled`, and `JobQueued`
   * when an attempt is re-queued for retry. Publication is best-effort and happens only
   * after the authoritative PostgreSQL state has been persisted; a failure is logged and
   * never disrupts execution. When omitted, the worker behaves exactly as before.
   */
  eventPublisher?: EventPublisher;
}

export interface StopWorkerOptions {
  readonly drain?: boolean;
  readonly timeoutMs?: number;
}

export interface ExecuteJobOptions {
  readonly job: Job;
  readonly leaseId: string;
  readonly image?: string;
  readonly environment?: Readonly<Record<string, string>>;
  readonly timeoutMs?: number;
  readonly executor?: Executor;
}

export interface ExecuteJobResult {
  readonly result: ExecutionResult;
  readonly attempt: JobAttempt;
  readonly job: Job;
  readonly retryDecision?: RetryDecision;
}

export interface WorkerShell {
  workerId: WorkerId;
  getStatus: () => WorkerStatus;
  claimJob: (jobId: string, durationMs?: number) => Promise<ClaimJobResult>;
  renewLease: (leaseId: string, jobId: string, durationMs?: number) => Promise<RenewLeaseResult>;
  releaseLease: (leaseId: string, jobId: string) => Promise<ReleaseLeaseResult>;
  executeJob: (options: ExecuteJobOptions) => Promise<ExecuteJobResult>;
  getActiveLeases: () => readonly WorkerLease[];
  drain: (options?: { timeoutMs?: number }) => Promise<void>;
  stop: (options?: StopWorkerOptions) => Promise<void>;
}

/**
 * Starts the Forge Worker service shell.
 * Coordinates worker identity, registration, periodic heartbeat, and containerized job execution.
 */
export function startWorker(options?: StartWorkerOptions): WorkerShell {
  const config = loadConfig();
  const logger =
    options?.logger ??
    createLogger({
      service: 'worker',
      environment: config.nodeEnv,
      minLevel: config.logLevel,
    });

  const workerId = createWorkerId(options?.workerId ?? crypto.randomUUID());

  /**
   * Best-effort typed lifecycle event emission. No-op when no publisher is configured;
   * never throws; never blocks the execution path (see `docs/architecture/events.md`).
   */
  const publish = (event: ForgeEvent): Promise<void> =>
    safePublish(options?.eventPublisher, event, logger);

  let currentStatus: WorkerStatus = 'READY';
  let heartbeatTimer: NodeJS.Timeout | undefined;
  let shutdownPromise: Promise<void> | null = null;
  const activeLeases = new Map<string, WorkerLease>();
  const activeExecutions = new Set<AbortController>();
  const activeTasks = new Set<Promise<unknown>>();

  const defaultExecutor =
    options?.executor ??
    new DockerExecutor({
      defaultImage: config.defaultDockerImage,
      defaultTimeoutMs: config.defaultExecutionTimeoutMs,
      maxTimeoutMs: config.maxExecutionTimeoutMs,
      maxOutputBytes: config.maxOutputBytes,
      dockerHost: config.dockerHost,
      logger,
    });

  logger.info('Forge Worker service shell started', {
    status: 'running',
    service: 'worker',
    workerId,
    environment: config.nodeEnv,
  });

  if (options?.registry) {
    const capabilities: WorkerCapabilities = options.capabilities ?? {
      executors: ['docker'],
    };
    const resources: WorkerResources = options.resources ?? {
      cpuCores: os.cpus().length,
      memoryBytes: os.totalmem(),
    };
    const hostname = options.hostname ?? os.hostname();

    // Register worker durably and send initial heartbeat
    options.registry
      .register({
        workerId,
        status: 'READY',
        hostname,
        capabilities,
        resources,
      })
      .then(() => {
        logger.info('Worker registered with registry', {
          workerId,
          hostname,
          capabilities,
          resources,
        });
        void publish(
          createForgeEvent('WorkerRegistered', {
            correlation: { worker_id: workerId },
            payload: {
              worker_id: workerId,
              hostname,
              capabilities: [...capabilities.executors],
              cpu_cores: resources.cpuCores,
              memory_bytes: resources.memoryBytes,
            },
          }),
        );
      })
      .catch((err: unknown) => {
        logger.error('Failed to register worker with registry', {
          workerId,
          error: err instanceof Error ? err.message : String(err),
        });
      });

    const intervalMs = options.heartbeatIntervalMs ?? config.workerHeartbeatIntervalMs;
    heartbeatTimer = setInterval(async () => {
      try {
        await options.registry!.heartbeat(workerId, currentStatus);
        await publish(
          createForgeEvent('WorkerHeartbeat', {
            correlation: { worker_id: workerId },
            payload: { worker_id: workerId, status: currentStatus },
          }),
        );
      } catch (err: unknown) {
        logger.error('Worker heartbeat renewal failed', {
          workerId,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }, intervalMs);

    heartbeatTimer.unref();
  }

  const claimJob = async (jobId: string, durationMs?: number): Promise<ClaimJobResult> => {
    if (currentStatus === 'DRAINING' || currentStatus === 'OFFLINE') {
      return {
        status: 'NOT_CLAIMABLE',
        reason: 'JOB_NOT_CLAIMABLE',
        details: `Worker "${workerId}" is in ${currentStatus} state and not accepting new jobs`,
      };
    }
    if (!options?.leaseRepository) {
      throw new Error('Worker cannot claim job without configured leaseRepository');
    }
    const leaseDuration =
      durationMs ?? options.defaultLeaseDurationMs ?? config.workerJobLeaseDurationMs;
    const result = await options.leaseRepository.claim({
      jobId,
      workerId,
      durationMs: leaseDuration,
    });
    if (result.status === 'ACQUIRED') {
      activeLeases.set(result.lease.id, result.lease);
      logger.info('Worker claimed job lease', {
        workerId,
        jobId,
        leaseId: result.lease.id,
        expiresAt: result.lease.expiresAt,
      });
    }
    return result;
  };

  const renewLease = async (
    leaseId: string,
    jobId: string,
    durationMs?: number,
  ): Promise<RenewLeaseResult> => {
    if (!options?.leaseRepository) {
      throw new Error('Worker cannot renew lease without configured leaseRepository');
    }
    const result = await options.leaseRepository.renew({
      leaseId,
      jobId,
      workerId,
      durationMs,
    });
    if (result.status === 'RENEWED') {
      activeLeases.set(result.lease.id, result.lease);
      logger.info('Worker renewed job lease', {
        workerId,
        jobId,
        leaseId,
        expiresAt: result.lease.expiresAt,
      });
    } else {
      activeLeases.delete(leaseId);
      logger.warn('Worker failed to renew job lease', {
        workerId,
        jobId,
        leaseId,
        reason: result.reason,
      });
    }
    return result;
  };

  const releaseLease = async (leaseId: string, jobId: string): Promise<ReleaseLeaseResult> => {
    if (!options?.leaseRepository) {
      throw new Error('Worker cannot release lease without configured leaseRepository');
    }
    const result = await options.leaseRepository.release({
      leaseId,
      jobId,
      workerId,
    });
    activeLeases.delete(leaseId);
    if (result.status === 'RELEASED') {
      logger.info('Worker released job lease', {
        workerId,
        jobId,
        leaseId,
      });
    }
    return result;
  };

  const executeJob = async (jobOpts: ExecuteJobOptions): Promise<ExecuteJobResult> => {
    if (currentStatus === 'DRAINING' || currentStatus === 'OFFLINE') {
      throw new Error(
        `Worker "${workerId}" is in ${currentStatus} state and not accepting new executions`,
      );
    }

    const run = async (): Promise<ExecuteJobResult> => {
      const { job, leaseId } = jobOpts;

      // 1. Pre-execution lease ownership validation
      if (options?.leaseRepository) {
        const activeLease = await options.leaseRepository.findActiveByJobId(job.id);
        if (!activeLease || activeLease.id !== leaseId || activeLease.workerId !== workerId) {
          throw new Error(
            `Worker "${workerId}" does not hold active lease "${leaseId}" for job "${job.id}"`,
          );
        }
      }

      // 2. Lifecycle start: spawn JobAttempt and transition Job/Attempt to RUNNING
      const attempt = job.createAttempt();
      attempt.start(new Date().toISOString());
      job.start();

      // Persist RUNNING state to authoritative store
      if (options?.pool) {
        await withTransaction(options.pool, async (tx) => {
          await tx.jobs.save(job);
          await tx.jobAttempts.save(attempt);
        });
      } else if (options?.jobRepository) {
        await options.jobRepository.save(job);
      }

      // Lifecycle event: execution has started and RUNNING state is committed.
      if (options?.eventPublisher) {
        await publish(
          createForgeEvent('JobStarted', {
            correlation: {
              run_id: job.pipelineRunId,
              job_id: job.id,
              attempt_id: attempt.id,
              worker_id: workerId,
            },
            payload: {
              job_id: job.id,
              attempt_id: attempt.id,
              worker_id: workerId,
              attempt_number: attempt.attemptNumber,
            },
          }),
        );
      }

      // 3. Periodic lease renewal background timer
      const abortController = new AbortController();
      activeExecutions.add(abortController);
      let renewalTimer: NodeJS.Timeout | undefined;
      let ownershipLost = false;

      if (options?.leaseRepository) {
        const renewalInterval =
          options.defaultLeaseRenewalIntervalMs ?? config.workerJobLeaseRenewalIntervalMs;
        const leaseDuration = options.defaultLeaseDurationMs ?? config.workerJobLeaseDurationMs;

        renewalTimer = setInterval(async () => {
          try {
            const renewRes = await renewLease(leaseId, job.id, leaseDuration);
            if (renewRes.status === 'REJECTED') {
              if (
                renewRes.reason === 'LEASE_EXPIRED' ||
                renewRes.reason === 'LEASE_OWNER_MISMATCH'
              ) {
                ownershipLost = true;
                logger.warn('Lease ownership lost during execution, aborting running container', {
                  workerId,
                  jobId: job.id,
                  leaseId,
                  reason: renewRes.reason,
                });
                abortController.abort();
              }
            }
          } catch (err: unknown) {
            logger.error('Error during periodic lease renewal tick', {
              workerId,
              jobId: job.id,
              leaseId,
              error: err instanceof Error ? err.message : String(err),
            });
          }
        }, renewalInterval);
        renewalTimer.unref();
      }

      // 4. Execute container workload via Executor
      const executorToUse = jobOpts.executor ?? defaultExecutor;
      let execResult: ExecutionResult;
      let executorThrew = false;

      try {
        execResult = await executorToUse.execute({
          jobId: job.id,
          attemptId: attempt.id,
          workerId,
          command: job.command,
          image: jobOpts.image,
          environment: jobOpts.environment,
          cpuCores: job.requirements?.cpuCores,
          memoryBytes: job.requirements?.memoryBytes,
          timeoutMs: jobOpts.timeoutMs,
          abortSignal: abortController.signal,
        });
      } catch (err) {
        executorThrew = true;
        execResult = {
          status: 'FAILED',
          exitCode: null,
          startedAt: new Date(),
          finishedAt: new Date(),
          durationMs: 0,
          stdout: '',
          stderr: err instanceof Error ? err.message : String(err),
          truncated: false,
          failureReason: err instanceof Error ? err.message : String(err),
        };
      } finally {
        if (renewalTimer) {
          clearInterval(renewalTimer);
        }
        activeExecutions.delete(abortController);
      }

      // 5. Apply state machine transitions and evaluate retry policies
      let retryDecision: RetryDecision | undefined;

      if (ownershipLost) {
        attempt.fail(1, 'Lease ownership lost during execution', new Date().toISOString());
        job.fail();
        job.clearNextAttemptAt?.();
      } else if (execResult.status === 'SUCCEEDED') {
        attempt.succeed(execResult.exitCode ?? 0, execResult.finishedAt.toISOString());
        job.succeed();
        job.clearNextAttemptAt?.();
      } else if (execResult.status === 'CANCELLED') {
        attempt.cancel(execResult.finishedAt.toISOString());
        job.cancel();
        job.clearNextAttemptAt?.();
      } else {
        // execResult.status is FAILED or TIMED_OUT
        if (execResult.status === 'TIMED_OUT') {
          attempt.timeout(execResult.finishedAt.toISOString());
        } else {
          attempt.fail(
            execResult.exitCode ?? 1,
            execResult.failureReason,
            execResult.finishedAt.toISOString(),
          );
        }

        // Pure retry decision evaluation
        retryDecision = evaluateRetry(attempt, job.retryPolicy);

        if (retryDecision.action === 'RETRY') {
          job.transitionTo('QUEUED');
          const nextAttemptAt = new Date(Date.now() + retryDecision.delayMs);
          job.setNextAttemptAt?.(nextAttemptAt);

          logger.info('Job execution failed but retry scheduled', {
            workerId,
            jobId: job.id,
            attemptNumber: attempt.attemptNumber,
            nextAttemptNumber: retryDecision.nextAttemptNumber,
            delayMs: retryDecision.delayMs,
            nextAttemptAt: nextAttemptAt.toISOString(),
            reason: retryDecision.reason,
          });
        } else {
          if (execResult.status === 'TIMED_OUT') {
            job.timeout();
          } else {
            job.fail();
          }
          job.clearNextAttemptAt?.();

          logger.info(
            'Job execution failed permanently; retry policy exhausted or not applicable',
            {
              workerId,
              jobId: job.id,
              attemptNumber: attempt.attemptNumber,
              reason: retryDecision.reason,
            },
          );
        }
      }

      // 6. Transactional persistence
      if (options?.pool) {
        await withTransaction(options.pool, async (tx) => {
          await tx.jobs.save(job);
          await tx.jobAttempts.save(attempt);
        });
      } else if (options?.jobRepository) {
        await options.jobRepository.save(job);
      }

      // 6b. Lifecycle events (best-effort, after the committed state):
      //     the derived bounded log stream, then the terminal outcome, then — on retry —
      //     the re-queue notification. Emitted before lease release so the events reflect
      //     committed job state regardless of the release result.
      if (options?.eventPublisher) {
        for (const chunkEvent of deriveLogChunkEvents(
          { stdout: execResult.stdout, stderr: execResult.stderr, truncated: execResult.truncated },
          { job_id: job.id, attempt_id: attempt.id },
        )) {
          await publish(chunkEvent);
        }

        const terminalCorrelation = {
          run_id: job.pipelineRunId,
          job_id: job.id,
          attempt_id: attempt.id,
          worker_id: workerId,
        };

        if (ownershipLost) {
          await publish(
            createForgeEvent('JobFailed', {
              correlation: terminalCorrelation,
              payload: {
                job_id: job.id,
                attempt_id: attempt.id,
                worker_id: workerId,
                attempt_number: attempt.attemptNumber,
                failure_kind: 'LEASE_LOST',
                reason: 'Lease ownership lost during execution',
                exit_code: execResult.exitCode,
                retry_scheduled: false,
              },
            }),
          );
        } else if (execResult.status === 'SUCCEEDED') {
          await publish(
            createForgeEvent('JobSucceeded', {
              correlation: terminalCorrelation,
              payload: {
                job_id: job.id,
                attempt_id: attempt.id,
                worker_id: workerId,
                attempt_number: attempt.attemptNumber,
                duration_ms: execResult.durationMs,
                exit_code: execResult.exitCode,
              },
            }),
          );
        } else if (execResult.status === 'CANCELLED') {
          await publish(
            createForgeEvent('JobCancelled', {
              correlation: terminalCorrelation,
              payload: {
                job_id: job.id,
                attempt_id: attempt.id,
                worker_id: workerId,
                attempt_number: attempt.attemptNumber,
              },
            }),
          );
        } else {
          const retryScheduled = retryDecision?.action === 'RETRY';
          const failureKind: JobFailureKind =
            execResult.status === 'TIMED_OUT'
              ? 'TIMED_OUT'
              : executorThrew
                ? 'EXECUTOR_ERROR'
                : 'FAILED';
          const nextAttemptAt = job.nextAttemptAt?.toISOString();

          await publish(
            createForgeEvent('JobFailed', {
              correlation: terminalCorrelation,
              payload: {
                job_id: job.id,
                attempt_id: attempt.id,
                worker_id: workerId,
                attempt_number: attempt.attemptNumber,
                failure_kind: failureKind,
                reason:
                  execResult.failureReason ??
                  (execResult.status === 'TIMED_OUT' ? 'Execution timed out' : 'Execution failed'),
                exit_code: execResult.exitCode,
                retry_scheduled: retryScheduled,
                ...(retryScheduled && nextAttemptAt ? { next_attempt_at: nextAttemptAt } : {}),
              },
            }),
          );

          if (retryDecision?.action === 'RETRY') {
            await publish(
              createForgeEvent('JobQueued', {
                correlation: { run_id: job.pipelineRunId, job_id: job.id },
                payload: {
                  job_id: job.id,
                  run_id: job.pipelineRunId,
                  priority: job.priority,
                  attempt_number: retryDecision.nextAttemptNumber,
                  ...(nextAttemptAt ? { next_attempt_at: nextAttemptAt } : {}),
                },
              }),
            );
          }
        }
      }

      // 7. Authoritative lease release (only if ownership was not lost)
      if (!ownershipLost && options?.leaseRepository) {
        try {
          await releaseLease(leaseId, job.id);
        } catch (err) {
          logger.warn('Failed to release lease after execution', {
            workerId,
            jobId: job.id,
            leaseId,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }

      return {
        result: execResult,
        attempt,
        job,
        retryDecision,
      };
    };

    const taskPromise = run();
    activeTasks.add(taskPromise);
    try {
      return await taskPromise;
    } finally {
      activeTasks.delete(taskPromise);
    }
  };

  const getActiveLeases = (): readonly WorkerLease[] => {
    return Object.freeze(Array.from(activeLeases.values()));
  };

  const drain = async (drainOpts?: { timeoutMs?: number }): Promise<void> => {
    if (currentStatus === 'OFFLINE') {
      return;
    }
    if (currentStatus === 'DRAINING') {
      if (shutdownPromise) {
        await shutdownPromise;
      }
      return;
    }

    currentStatus = 'DRAINING';
    logger.info('Worker entered DRAINING state', { workerId });

    if (options?.registry) {
      try {
        await options.registry.heartbeat(workerId, 'DRAINING');
      } catch (err: unknown) {
        logger.warn('Failed to update worker status to DRAINING in registry', {
          workerId,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    const timeoutMs = drainOpts?.timeoutMs ?? options?.drainTimeoutMs ?? 30000;
    if (activeTasks.size > 0) {
      logger.info('Worker waiting for in-flight executions to finish during drain', {
        workerId,
        inFlightCount: activeTasks.size,
        timeoutMs,
      });

      const timeoutPromise = new Promise((resolve) => setTimeout(resolve, timeoutMs));
      await Promise.race([Promise.allSettled(Array.from(activeTasks)), timeoutPromise]);
    }

    for (const controller of activeExecutions) {
      try {
        controller.abort();
      } catch {
        // ignore
      }
    }
  };

  const stop = async (stopOpts?: StopWorkerOptions): Promise<void> => {
    if (shutdownPromise) {
      return shutdownPromise;
    }

    shutdownPromise = (async () => {
      if (stopOpts?.drain !== false) {
        await drain({ timeoutMs: stopOpts?.timeoutMs });
      } else {
        for (const controller of activeExecutions) {
          try {
            controller.abort();
          } catch {
            // ignore
          }
        }
      }
      activeExecutions.clear();

      if (options?.leaseRepository && activeLeases.size > 0) {
        for (const [leaseId, lease] of Array.from(activeLeases.entries())) {
          try {
            await options.leaseRepository.release({
              leaseId,
              jobId: lease.jobId,
              workerId,
            });
            logger.info('Worker released lease during graceful stop', {
              workerId,
              leaseId,
              jobId: lease.jobId,
            });
          } catch (err: unknown) {
            logger.error('Failed to release lease during graceful stop', {
              workerId,
              leaseId,
              error: err instanceof Error ? err.message : String(err),
            });
          }
        }
        activeLeases.clear();
      }

      currentStatus = 'OFFLINE';

      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = undefined;
      }

      if (options?.registry) {
        try {
          await options.registry.deregister(workerId);
        } catch (err: unknown) {
          logger.error('Worker deregistration failed', {
            workerId,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }

      logger.info('Forge Worker service shell stopped', { workerId });
    })();

    return shutdownPromise;
  };

  return {
    workerId,
    getStatus: () => currentStatus,
    claimJob,
    renewLease,
    releaseLease,
    executeJob,
    getActiveLeases,
    drain,
    stop,
  };
}

const normalizedArgv1 = process.argv[1]?.replace(/\\/g, '/') ?? '';
const isDirectRun =
  Boolean(normalizedArgv1) &&
  (normalizedArgv1.endsWith('worker/dist/index.js') ||
    normalizedArgv1.endsWith('worker/src/index.ts'));

if (isDirectRun) {
  const shell = startWorker();

  const shutdown = async (signal: string) => {
    console.info(`[forge-worker] Received ${signal}, initiating graceful drain and shutdown`);
    await shell.stop({ drain: true });
    process.exit(0);
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}
