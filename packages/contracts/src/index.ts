/**
 * Recognized Forge service names across the monorepo.
 */
export type ServiceName = 'api' | 'scheduler' | 'worker' | 'web' | 'cli';

/**
 * Health check status values.
 */
export type HealthStatus = 'ok' | 'degraded' | 'error';

/**
 * Deterministic API health check response payload.
 */
export interface HealthResponse {
  status: HealthStatus;
  service: string;
  timestamp: string;
  version: string;
  uptime: number;
}

/**
 * Structured log levels supported across Forge services.
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * Common shape for structured log entries.
 */
export interface LogEntry {
  timestamp: string;
  level: LogLevel;
  service: string;
  message: string;
  environment?: string;
  request_id?: string;
  context?: Record<string, unknown>;
}

/**
 * Node execution environment.
 */
export type NodeEnvironment = 'development' | 'production' | 'test';

/**
 * Core typed application configuration contract for PR 01 foundation.
 */
export interface AppConfig {
  nodeEnv: NodeEnvironment;
  logLevel: LogLevel;
  apiPort: number;
  databaseUrl: string;
  redisUrl: string;
  workerHeartbeatIntervalMs: number;
  workerHeartbeatTtlSeconds: number;
  workerJobLeaseDurationMs: number;
  workerJobLeaseRenewalIntervalMs: number;
  defaultDockerImage: string;
  defaultExecutionTimeoutMs: number;
  maxExecutionTimeoutMs: number;
  maxOutputBytes: number;
  dockerHost?: string;
  defaultMaxAttempts: number;
  maxJobAttempts: number;
  defaultRetryBaseDelayMs: number;
  maxRetryBackoffMs: number;
}

/**
 * Worker execution capabilities advertised to the cluster.
 */
export interface WorkerCapabilities {
  readonly executors: readonly string[];
}

/**
 * Hardware capacity and execution resource limits of the worker node.
 */
export interface WorkerResources {
  readonly cpuCores: number;
  readonly memoryBytes: number;
  readonly gpuCount?: number;
}

/**
 * Declared execution requirements for running a job.
 */
export interface JobRequirements {
  readonly executor?: string;
  readonly cpuCores?: number;
  readonly memoryBytes?: number;
  readonly gpuCount?: number;
}

/**
 * Default integer scheduling priority assigned to jobs when unspecified.
 */
export const DEFAULT_JOB_PRIORITY = 0;

/**
 * Minimum permitted integer job scheduling priority.
 */
export const MIN_JOB_PRIORITY = -1000;

/**
 * Maximum permitted integer job scheduling priority.
 */
export const MAX_JOB_PRIORITY = 1000;

/**
 * Lifecycle status of a distributed worker lease.
 */
export type JobLeaseStatus = 'ACTIVE' | 'RELEASED' | 'EXPIRED';

/**
 * Immutable representation of a distributed worker job lease.
 */
export interface WorkerLease {
  readonly id: string;
  readonly jobId: string;
  readonly workerId: string;
  readonly status: JobLeaseStatus;
  readonly durationMs: number;
  readonly acquiredAt: Date;
  readonly renewedAt: Date;
  readonly expiresAt: Date;
  readonly createdAt: Date;
}

/**
 * Parameters for claiming a job lease.
 */
export interface ClaimJobOptions {
  readonly jobId: string;
  readonly workerId: string;
  readonly durationMs: number;
}

/**
 * Result of an atomic job lease claim attempt.
 */
export type ClaimJobResult =
  | {
      readonly status: 'ACQUIRED';
      readonly lease: WorkerLease;
      readonly isIdempotent?: boolean;
    }
  | {
      readonly status: 'CONFLICT';
      readonly reason: 'LEASE_ALREADY_HELD';
      readonly currentOwnerId: string;
      readonly expiresAt: Date;
    }
  | {
      readonly status: 'NOT_CLAIMABLE';
      readonly reason: 'JOB_NOT_FOUND' | 'JOB_NOT_CLAIMABLE';
      readonly details?: string;
    };

/**
 * Parameters for renewing an active job lease.
 */
export interface RenewLeaseOptions {
  readonly leaseId: string;
  readonly jobId: string;
  readonly workerId: string;
  readonly durationMs?: number;
}

/**
 * Result of an atomic job lease renewal attempt.
 */
export type RenewLeaseResult =
  | {
      readonly status: 'RENEWED';
      readonly lease: WorkerLease;
    }
  | {
      readonly status: 'REJECTED';
      readonly reason:
        'LEASE_EXPIRED' | 'LEASE_NOT_FOUND' | 'LEASE_OWNER_MISMATCH' | 'LEASE_TOKEN_MISMATCH';
      readonly details?: string;
    };

/**
 * Parameters for releasing an active job lease.
 */
export interface ReleaseLeaseOptions {
  readonly leaseId: string;
  readonly jobId: string;
  readonly workerId: string;
}

/**
 * Result of an atomic job lease release attempt.
 */
export type ReleaseLeaseResult =
  | {
      readonly status: 'RELEASED';
      readonly leaseId: string;
      readonly jobId: string;
    }
  | {
      readonly status: 'REJECTED';
      readonly reason:
        | 'LEASE_NOT_FOUND'
        | 'LEASE_OWNER_MISMATCH'
        | 'LEASE_TOKEN_MISMATCH'
        | 'LEASE_ALREADY_INACTIVE';
      readonly details?: string;
    };

/**
 * Status of a scheduler placement decision.
 */
export type ScheduleDecisionStatus = 'SCHEDULED' | 'UNSCHEDULABLE';

/**
 * Standard reasons explaining why a job cannot be scheduled on any worker.
 */
export type UnschedulableReason =
  'NO_ELIGIBLE_WORKER' | 'INVALID_JOB_REQUIREMENTS' | 'LEASE_CONFLICT' | 'RETRY_BACKOFF_ACTIVE';

/**
 * Explainable result when a job is successfully matched and assigned to a worker.
 */
export interface ScheduledDecision {
  readonly status: 'SCHEDULED';
  readonly jobId: string;
  readonly workerId: string;
  readonly candidateWorkerCount: number;
  readonly eligibleWorkerCount: number;
  readonly priority?: number;
  readonly reason?: string;
  readonly lease?: WorkerLease;
}

/**
 * Explainable result when a job cannot be placed on any candidate worker.
 */
export interface UnschedulableDecision {
  readonly status: 'UNSCHEDULABLE';
  readonly jobId: string;
  readonly candidateWorkerCount: number;
  readonly eligibleWorkerCount: number;
  readonly priority?: number;
  readonly reason: UnschedulableReason;
  readonly failureReasons?: readonly string[];
}

/**
 * Complete typed scheduling placement decision contract.
 */
export type ScheduleDecision = ScheduledDecision | UnschedulableDecision;

/**
 * Lifecycle status of an ephemeral job execution attempt.
 */
export type ExecutionStatus = 'SUCCEEDED' | 'FAILED' | 'TIMED_OUT' | 'CANCELLED';

/**
 * Result produced by an Executor upon completing a single job execution attempt.
 */
export interface ExecutionResult {
  readonly status: ExecutionStatus;
  readonly exitCode: number | null;
  readonly startedAt: Date;
  readonly finishedAt: Date;
  readonly durationMs: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly truncated: boolean;
  readonly failureReason?: string;
}

/**
 * Context and parameters provided to an Executor to execute a single job attempt.
 */
export interface ExecutionContext {
  readonly jobId: string;
  readonly attemptId: string;
  readonly workerId: string;
  readonly command: string;
  readonly image?: string;
  readonly environment?: Readonly<Record<string, string>>;
  readonly workingDirectory?: string;
  readonly cpuCores?: number;
  readonly memoryBytes?: number;
  readonly timeoutMs?: number;
  readonly abortSignal?: AbortSignal;
}

/**
 * Pluggable executor interface abstracting task execution away from specific container runtimes.
 */
export interface Executor {
  readonly name: string;
  execute(context: ExecutionContext): Promise<ExecutionResult>;
  isAvailable(): Promise<boolean>;
}

/**
 * Outcomes that can trigger a retry.
 */
export type RetryCondition = 'FAILED' | 'TIMED_OUT';

/**
 * Exponential backoff configuration.
 */
export interface BackoffPolicy {
  readonly baseDelayMs: number;
  readonly maxDelayMs: number;
  readonly factor?: number;
}

/**
 * Declarative retry configuration for a job or step.
 */
export interface RetryPolicy {
  /**
   * Maximum total execution attempts including the initial attempt (>= 1).
   */
  readonly maxAttempts: number;
  /**
   * Backoff policy for delay between attempts.
   */
  readonly backoff?: BackoffPolicy;
  /**
   * Execution outcomes that trigger a retry attempt. Defaults to ['FAILED', 'TIMED_OUT'].
   */
  readonly retryOn?: readonly RetryCondition[];
}

/**
 * Pure evaluation decision produced when assessing retryability for a completed attempt.
 */
export type RetryDecision =
  | {
      readonly action: 'RETRY';
      readonly attemptNumber: number;
      readonly nextAttemptNumber: number;
      readonly delayMs: number;
      readonly reason: string;
    }
  | {
      readonly action: 'FINAL_FAILURE';
      readonly attemptNumber: number;
      readonly reason: 'MAX_ATTEMPTS_EXHAUSTED' | 'OUTCOME_NOT_RETRYABLE';
      readonly details: string;
    }
  | {
      readonly action: 'NOT_RETRYABLE';
      readonly attemptNumber: number;
      readonly reason: 'NO_POLICY' | 'SUCCEEDED' | 'CANCELLED';
      readonly details: string;
    };

export const DEFAULT_MAX_ATTEMPTS = 1;
export const MAX_JOB_ATTEMPTS_LIMIT = 10;
export const DEFAULT_RETRY_BASE_DELAY_MS = 1000;
export const DEFAULT_MAX_BACKOFF_MS = 60000;
export const MAX_RETRY_BACKOFF_LIMIT_MS = 3600000;

/**
 * Standard reasons explaining why a job entered the Dead-Letter Queue (DLQ).
 */
export type DeadLetterReason =
  'RETRY_EXHAUSTED' | 'WORKER_LOSS_RETRY_EXHAUSTED' | 'NON_RETRYABLE_FAILURE';

/**
 * Durable record representing a dead-lettered job that is no longer eligible for normal scheduling.
 */
export interface DeadLetterJob {
  readonly id: string;
  readonly jobId: string;
  readonly pipelineRunId: string;
  readonly reason: DeadLetterReason;
  readonly failedAttemptCount: number;
  readonly lastAttemptId?: string;
  readonly lastWorkerId?: string;
  readonly errorDetails?: string;
  readonly metadata?: Readonly<Record<string, unknown>>;
  readonly createdAt: Date;
}

/**
 * Actions taken by the recovery service for an expired lease.
 */
export type RecoveryAction = 'REQUEUED' | 'DEAD_LETTERED' | 'SKIPPED_TERMINAL' | 'NO_OP';

/**
 * Detailed outcome for an individual expired lease evaluated during recovery.
 */
export interface RecoveredLeaseRecord {
  readonly leaseId: string;
  readonly jobId: string;
  readonly workerId: string;
  readonly action: RecoveryAction;
  readonly nextAttemptAt?: Date;
  readonly deadLetterReason?: DeadLetterReason;
  readonly details?: string;
}

/**
 * Overall summary produced by a lease recovery sweep.
 */
export interface RecoverExpiredLeasesResult {
  readonly recoveredCount: number;
  readonly details: readonly RecoveredLeaseRecord[];
}

/**
 * Bounded options for controlling lease recovery execution.
 */
export interface LeaseRecoveryOptions {
  readonly batchSize?: number;
  readonly now?: Date;
}
