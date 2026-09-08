/**
 * Type-safe branded domain identifiers.
 */
declare const __brand: unique symbol;
export type Brand<T, B> = T & { readonly [__brand]: B };

export type PipelineId = Brand<string, 'PipelineId'>;
export type PipelineRunId = Brand<string, 'PipelineRunId'>;
export type JobId = Brand<string, 'JobId'>;
export type JobAttemptId = Brand<string, 'JobAttemptId'>;

export function createPipelineId(id: string): PipelineId {
  return id as PipelineId;
}

export function createPipelineRunId(id: string): PipelineRunId {
  return id as PipelineRunId;
}

export function createJobId(id: string): JobId {
  return id as JobId;
}

export function createJobAttemptId(id: string): JobAttemptId {
  return id as JobAttemptId;
}

/**
 * Pipeline Run Lifecycle Statuses.
 */
export type PipelineRunStatus =
  'PENDING' | 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED' | 'TIMED_OUT';

/**
 * Job Lifecycle Statuses.
 */
export type JobStatus =
  'PENDING' | 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED' | 'TIMED_OUT';

/**
 * Job Attempt Lifecycle Statuses.
 */
export type JobAttemptStatus =
  'PENDING' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED' | 'TIMED_OUT';

import type { JobRequirements, RetryPolicy } from '@forge/contracts';

/**
 * Plain object definition schemas for authoring pipelines.
 */
export interface StepDefinition {
  name: string;
  command: string;
  dependsOn?: string[];
  requirements?: JobRequirements;
  priority?: number;
  retry?: RetryPolicy;
}

export interface PipelineDefinition {
  id?: string;
  name: string;
  steps: StepDefinition[];
}

/**
 * Plain object serialized interfaces for inspection, testing, and debugging.
 */
export interface PipelineStepSerialized {
  name: string;
  command: string;
  dependsOn: string[];
  requirements?: JobRequirements;
  priority: number;
  retry?: RetryPolicy;
}

export interface PipelineSerialized {
  id: string;
  name: string;
  steps: PipelineStepSerialized[];
}

export interface JobAttemptSerialized {
  id: string;
  jobId: string;
  attemptNumber: number;
  status: JobAttemptStatus;
  startedAt?: string;
  finishedAt?: string;
  exitCode?: number;
  failureReason?: string;
}

export interface JobSerialized {
  id: string;
  pipelineRunId: string;
  stepName: string;
  command: string;
  dependsOn: string[];
  requirements?: JobRequirements;
  priority: number;
  retryPolicy?: RetryPolicy;
  nextAttemptAt?: string;
  createdAt?: string;
  queuedAt?: string;
  status: JobStatus;
  attempts: JobAttemptSerialized[];
}

export interface PipelineRunSerialized {
  id: string;
  pipelineId: string;
  pipelineName: string;
  status: PipelineRunStatus;
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
  jobs: JobSerialized[];
}
