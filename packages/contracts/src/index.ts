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
 * Status of a scheduler placement decision.
 */
export type ScheduleDecisionStatus = 'SCHEDULED' | 'UNSCHEDULABLE';

/**
 * Standard reasons explaining why a job cannot be scheduled on any worker.
 */
export type UnschedulableReason = 'NO_ELIGIBLE_WORKER' | 'INVALID_JOB_REQUIREMENTS';

/**
 * Explainable result when a job is successfully matched and assigned to a worker.
 */
export interface ScheduledDecision {
  readonly status: 'SCHEDULED';
  readonly jobId: string;
  readonly workerId: string;
  readonly candidateWorkerCount: number;
  readonly eligibleWorkerCount: number;
  readonly reason?: string;
}

/**
 * Explainable result when a job cannot be placed on any candidate worker.
 */
export interface UnschedulableDecision {
  readonly status: 'UNSCHEDULABLE';
  readonly jobId: string;
  readonly candidateWorkerCount: number;
  readonly eligibleWorkerCount: number;
  readonly reason: UnschedulableReason;
  readonly failureReasons?: readonly string[];
}

/**
 * Complete typed scheduling placement decision contract.
 */
export type ScheduleDecision = ScheduledDecision | UnschedulableDecision;
