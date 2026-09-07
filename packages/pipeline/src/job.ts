import type { RetryPolicy } from '@forge/contracts';
import { JobAttempt } from './job-attempt.js';
import { validateJobPriority } from './priority.js';
import { validateJobRequirements, type JobRequirements } from './requirements.js';
import { validateRetryPolicy } from './retry.js';
import { createJobStateMachine, type StateMachine } from './state-machine.js';
import {
  createJobAttemptId,
  type JobId,
  type JobSerialized,
  type JobStatus,
  type PipelineRunId,
} from './types.js';

export interface JobOptions {
  id: JobId;
  pipelineRunId: PipelineRunId;
  stepName: string;
  command: string;
  dependsOn?: readonly string[];
  requirements?: JobRequirements;
  priority?: number;
  retryPolicy?: RetryPolicy;
  nextAttemptAt?: Date | string;
  initialStatus?: JobStatus;
  attempts?: readonly JobAttempt[];
}

/**
 * Domain model representing execution of a specific PipelineStep within a PipelineRun.
 */
export class Job {
  public readonly id: JobId;
  public readonly pipelineRunId: PipelineRunId;
  public readonly stepName: string;
  public readonly command: string;
  public readonly dependsOn: readonly string[];
  public readonly requirements: JobRequirements;
  public readonly priority: number;
  private readonly configuredRetryPolicy?: RetryPolicy;
  private nextScheduledAttemptAt?: Date;
  private readonly stateMachine: StateMachine<JobStatus>;
  private readonly attemptsList: JobAttempt[] = [];

  constructor(options: JobOptions) {
    this.id = options.id;
    this.pipelineRunId = options.pipelineRunId;
    this.stepName = options.stepName;
    this.command = options.command;
    this.dependsOn = Object.freeze([...(options.dependsOn ?? [])]);
    this.requirements = validateJobRequirements(options.requirements);
    this.priority = validateJobPriority(options.priority);
    this.configuredRetryPolicy = validateRetryPolicy(options.retryPolicy);
    this.nextScheduledAttemptAt = options.nextAttemptAt
      ? new Date(options.nextAttemptAt)
      : undefined;
    this.stateMachine = createJobStateMachine(options.id, options.initialStatus ?? 'PENDING');
    if (options.attempts) {
      this.attemptsList.push(...options.attempts);
    }
  }

  public get retryPolicy(): RetryPolicy | undefined {
    return this.configuredRetryPolicy;
  }

  public get nextAttemptAt(): Date | undefined {
    return this.nextScheduledAttemptAt;
  }

  public setNextAttemptAt(timestamp: Date | string): void {
    this.nextScheduledAttemptAt = new Date(timestamp);
  }

  public clearNextAttemptAt(): void {
    this.nextScheduledAttemptAt = undefined;
  }

  public get status(): JobStatus {
    return this.stateMachine.getStatus();
  }

  public get attempts(): readonly JobAttempt[] {
    return Object.freeze([...this.attemptsList]);
  }

  public get currentAttempt(): JobAttempt | undefined {
    return this.attemptsList[this.attemptsList.length - 1];
  }

  public isTerminal(): boolean {
    return this.stateMachine.isTerminal();
  }

  /**
   * Spawns a new execution attempt with an incremented attempt number.
   * Preserves historical attempts immutably.
   */
  public createAttempt(): JobAttempt {
    const nextAttemptNumber = this.attemptsList.length + 1;
    const attemptId = createJobAttemptId(`${this.id}-attempt-${nextAttemptNumber}`);

    const attempt = new JobAttempt({
      id: attemptId,
      jobId: this.id,
      attemptNumber: nextAttemptNumber,
    });

    this.attemptsList.push(attempt);
    return attempt;
  }

  public markQueued(): void {
    this.stateMachine.transitionTo('QUEUED');
  }

  public start(): void {
    this.stateMachine.transitionTo('RUNNING');
  }

  public succeed(): void {
    this.stateMachine.transitionTo('SUCCEEDED');
  }

  public fail(): void {
    this.stateMachine.transitionTo('FAILED');
  }

  public cancel(): void {
    this.stateMachine.transitionTo('CANCELLED');
  }

  public timeout(): void {
    this.stateMachine.transitionTo('TIMED_OUT');
  }

  public transitionTo(nextStatus: JobStatus): void {
    this.stateMachine.transitionTo(nextStatus);
  }

  public toJSON(): JobSerialized {
    return {
      id: this.id,
      pipelineRunId: this.pipelineRunId,
      stepName: this.stepName,
      command: this.command,
      dependsOn: [...this.dependsOn],
      ...(Object.keys(this.requirements).length > 0 ? { requirements: this.requirements } : {}),
      priority: this.priority,
      ...(this.configuredRetryPolicy ? { retryPolicy: this.configuredRetryPolicy } : {}),
      ...(this.nextScheduledAttemptAt
        ? { nextAttemptAt: this.nextScheduledAttemptAt.toISOString() }
        : {}),
      status: this.status,
      attempts: this.attemptsList.map((att) => att.toJSON()),
    };
  }
}
