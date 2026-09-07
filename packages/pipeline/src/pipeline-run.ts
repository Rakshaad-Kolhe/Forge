import { DuplicateJobError } from './errors.js';
import { Job } from './job.js';
import { Pipeline } from './pipeline.js';
import { createPipelineRunStateMachine, type StateMachine } from './state-machine.js';
import {
  createJobId,
  type PipelineId,
  type PipelineRunId,
  type PipelineRunSerialized,
  type PipelineRunStatus,
} from './types.js';

export interface PipelineRunOptions {
  id: PipelineRunId;
  pipelineId: PipelineId;
  pipelineName: string;
  initialStatus?: PipelineRunStatus;
  createdAt?: string;
  startedAt?: string;
  finishedAt?: string;
}

/**
 * Domain model representing an execution instance of a Pipeline definition.
 */
export class PipelineRun {
  public readonly id: PipelineRunId;
  public readonly pipelineId: PipelineId;
  public readonly pipelineName: string;
  public readonly createdAt: string;
  private startedAtTime?: string;
  private finishedAtTime?: string;
  private readonly stateMachine: StateMachine<PipelineRunStatus>;
  private readonly jobsMap = new Map<string, Job>();
  private readonly jobsList: Job[] = [];

  constructor(options: PipelineRunOptions) {
    this.id = options.id;
    this.pipelineId = options.pipelineId;
    this.pipelineName = options.pipelineName;
    this.createdAt = options.createdAt ?? new Date().toISOString();
    this.startedAtTime = options.startedAt;
    this.finishedAtTime = options.finishedAt;
    this.stateMachine = createPipelineRunStateMachine(
      options.id,
      options.initialStatus ?? 'PENDING',
    );
  }

  /**
   * Factory creating a PipelineRun and deterministically generating
   * corresponding Job instances for every step defined in the Pipeline.
   */
  public static create(id: PipelineRunId, pipeline: Pipeline): PipelineRun {
    const run = new PipelineRun({
      id,
      pipelineId: pipeline.id,
      pipelineName: pipeline.name,
    });

    for (const step of pipeline.getSteps()) {
      const jobId = createJobId(`${id}-${step.name}`);
      const job = new Job({
        id: jobId,
        pipelineRunId: id,
        stepName: step.name,
        command: step.command,
        dependsOn: step.dependsOn,
        requirements: step.requirements,
      });

      run.addJob(job);
    }

    return run;
  }

  public get status(): PipelineRunStatus {
    return this.stateMachine.getStatus();
  }

  public get startedAt(): string | undefined {
    return this.startedAtTime;
  }

  public get finishedAt(): string | undefined {
    return this.finishedAtTime;
  }

  public isTerminal(): boolean {
    return this.stateMachine.isTerminal();
  }

  /**
   * Adds a Job to this pipeline run.
   * Enforces 1:1 mapping from step to job per run.
   *
   * @throws {DuplicateJobError} If a job for this step already exists
   */
  public addJob(job: Job): void {
    if (this.jobsMap.has(job.stepName)) {
      throw new DuplicateJobError(job.stepName, this.id);
    }
    this.jobsMap.set(job.stepName, job);
    this.jobsList.push(job);
  }

  /**
   * Retrieves a Job by its corresponding pipeline step name.
   */
  public getJob(stepName: string): Job | undefined {
    return this.jobsMap.get(stepName);
  }

  /**
   * Returns all jobs belonging to this run in their deterministic order.
   */
  public getJobs(): readonly Job[] {
    return Object.freeze([...this.jobsList]);
  }

  public markQueued(): void {
    this.stateMachine.transitionTo('QUEUED');
  }

  public start(startedAt: string = new Date().toISOString()): void {
    this.stateMachine.transitionTo('RUNNING');
    this.startedAtTime = startedAt;
  }

  public cancel(finishedAt: string = new Date().toISOString()): void {
    this.stateMachine.transitionTo('CANCELLED');
    this.finishedAtTime = finishedAt;
  }

  public timeout(finishedAt: string = new Date().toISOString()): void {
    this.stateMachine.transitionTo('TIMED_OUT');
    this.finishedAtTime = finishedAt;
  }

  public transitionTo(nextStatus: PipelineRunStatus): void {
    this.stateMachine.transitionTo(nextStatus);
    if (this.stateMachine.isTerminal() && !this.finishedAtTime) {
      this.finishedAtTime = new Date().toISOString();
    }
  }

  /**
   * Evaluates and updates the pipeline run completion status based on constituent job states.
   *
   * Rules:
   * - If already terminal, no-op.
   * - If any job is FAILED or TIMED_OUT, transitions run to FAILED.
   * - If any job is CANCELLED (and none failed), transitions run to CANCELLED.
   * - If all jobs are SUCCEEDED, transitions run to SUCCEEDED.
   * - Otherwise remains in current active status.
   */
  public evaluateCompletion(): PipelineRunStatus {
    if (this.isTerminal()) {
      return this.status;
    }

    if (this.jobsList.length === 0) {
      return this.status;
    }

    const statuses = this.jobsList.map((j) => j.status);

    // Any failure causes overall pipeline failure
    if (statuses.some((s) => s === 'FAILED' || s === 'TIMED_OUT')) {
      this.transitionTo('FAILED');
      return this.status;
    }

    // Any cancellation without failure causes pipeline cancellation
    if (statuses.some((s) => s === 'CANCELLED')) {
      this.transitionTo('CANCELLED');
      return this.status;
    }

    // All jobs succeeded
    if (statuses.every((s) => s === 'SUCCEEDED')) {
      this.transitionTo('SUCCEEDED');
      return this.status;
    }

    return this.status;
  }

  public toJSON(): PipelineRunSerialized {
    return {
      id: this.id,
      pipelineId: this.pipelineId,
      pipelineName: this.pipelineName,
      status: this.status,
      createdAt: this.createdAt,
      ...(this.startedAtTime ? { startedAt: this.startedAtTime } : {}),
      ...(this.finishedAtTime ? { finishedAt: this.finishedAtTime } : {}),
      jobs: this.jobsList.map((job) => job.toJSON()),
    };
  }
}
