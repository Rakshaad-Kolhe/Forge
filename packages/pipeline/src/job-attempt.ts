import { createJobAttemptStateMachine, type StateMachine } from './state-machine.js';
import type { JobAttemptId, JobAttemptSerialized, JobAttemptStatus, JobId } from './types.js';

export interface JobAttemptOptions {
  id: JobAttemptId;
  jobId: JobId;
  attemptNumber: number;
  initialStatus?: JobAttemptStatus;
  startedAt?: string;
  finishedAt?: string;
  exitCode?: number;
  failureReason?: string;
}

/**
 * Domain model representing a single physical execution attempt of a Job.
 */
export class JobAttempt {
  public readonly id: JobAttemptId;
  public readonly jobId: JobId;
  public readonly attemptNumber: number;
  private readonly stateMachine: StateMachine<JobAttemptStatus>;
  private startedAtTime?: string;
  private finishedAtTime?: string;
  private processExitCode?: number;
  private failureMessage?: string;

  constructor(options: JobAttemptOptions) {
    if (options.attemptNumber < 1) {
      throw new Error(
        `JobAttempt attemptNumber must be at least 1, received: ${options.attemptNumber}`,
      );
    }

    this.id = options.id;
    this.jobId = options.jobId;
    this.attemptNumber = options.attemptNumber;
    this.stateMachine = createJobAttemptStateMachine(
      options.id,
      options.initialStatus ?? 'PENDING',
    );
    this.startedAtTime = options.startedAt;
    this.finishedAtTime = options.finishedAt;
    this.processExitCode = options.exitCode;
    this.failureMessage = options.failureReason;
  }

  public get status(): JobAttemptStatus {
    return this.stateMachine.getStatus();
  }

  public get startedAt(): string | undefined {
    return this.startedAtTime;
  }

  public get finishedAt(): string | undefined {
    return this.finishedAtTime;
  }

  public get exitCode(): number | undefined {
    return this.processExitCode;
  }

  public get failureReason(): string | undefined {
    return this.failureMessage;
  }

  public isTerminal(): boolean {
    return this.stateMachine.isTerminal();
  }

  /**
   * Starts the execution attempt.
   */
  public start(startedAt: string = new Date().toISOString()): void {
    this.stateMachine.transitionTo('RUNNING');
    this.startedAtTime = startedAt;
  }

  /**
   * Marks the attempt as succeeded.
   */
  public succeed(exitCode = 0, finishedAt: string = new Date().toISOString()): void {
    this.stateMachine.transitionTo('SUCCEEDED');
    this.processExitCode = exitCode;
    this.finishedAtTime = finishedAt;
  }

  /**
   * Marks the attempt as failed.
   */
  public fail(exitCode = 1, reason?: string, finishedAt: string = new Date().toISOString()): void {
    this.stateMachine.transitionTo('FAILED');
    this.processExitCode = exitCode;
    this.failureMessage = reason;
    this.finishedAtTime = finishedAt;
  }

  /**
   * Marks the attempt as cancelled.
   */
  public cancel(finishedAt: string = new Date().toISOString()): void {
    this.stateMachine.transitionTo('CANCELLED');
    this.finishedAtTime = finishedAt;
  }

  /**
   * Marks the attempt as timed out.
   */
  public timeout(finishedAt: string = new Date().toISOString()): void {
    this.stateMachine.transitionTo('TIMED_OUT');
    this.finishedAtTime = finishedAt;
    this.failureMessage = 'Execution timed out';
  }

  public toJSON(): JobAttemptSerialized {
    return {
      id: this.id,
      jobId: this.jobId,
      attemptNumber: this.attemptNumber,
      status: this.status,
      ...(this.startedAtTime ? { startedAt: this.startedAtTime } : {}),
      ...(this.finishedAtTime ? { finishedAt: this.finishedAtTime } : {}),
      ...(this.processExitCode !== undefined ? { exitCode: this.processExitCode } : {}),
      ...(this.failureMessage ? { failureReason: this.failureMessage } : {}),
    };
  }
}
