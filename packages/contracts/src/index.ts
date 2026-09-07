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
