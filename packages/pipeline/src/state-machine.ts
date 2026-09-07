import { InvalidStateTransitionError } from './errors.js';
import type { JobAttemptStatus, JobStatus, PipelineRunStatus } from './types.js';

/**
 * Reusable deterministic finite state machine.
 */
export class StateMachine<TStatus extends string> {
  private status: TStatus;
  private readonly entityType: string;
  private readonly entityId: string;
  private readonly transitions: Map<TStatus, Set<TStatus>>;
  private readonly terminalStates: Set<TStatus>;

  constructor(
    entityType: string,
    entityId: string,
    initialStatus: TStatus,
    transitions: Record<TStatus, readonly TStatus[]>,
    terminalStates: readonly TStatus[],
  ) {
    this.entityType = entityType;
    this.entityId = entityId;
    this.status = initialStatus;
    this.terminalStates = new Set(terminalStates);

    this.transitions = new Map<TStatus, Set<TStatus>>();
    for (const [state, nextStates] of Object.entries(transitions) as [
      TStatus,
      readonly TStatus[],
    ][]) {
      this.transitions.set(state, new Set(nextStates));
    }
  }

  /**
   * Returns current status.
   */
  public getStatus(): TStatus {
    return this.status;
  }

  /**
   * Checks if the machine has reached a terminal state.
   */
  public isTerminal(): boolean {
    return this.terminalStates.has(this.status);
  }

  /**
   * Checks whether transitioning to the requested status is permitted.
   */
  public canTransitionTo(nextStatus: TStatus): boolean {
    const allowed = this.transitions.get(this.status);
    return Boolean(allowed && allowed.has(nextStatus));
  }

  /**
   * Performs an explicit state transition.
   *
   * @throws {InvalidStateTransitionError} If the transition is illegal or if already terminal
   */
  public transitionTo(nextStatus: TStatus): void {
    if (this.status === nextStatus) {
      // Re-applying current status is a no-op if already in that state
      return;
    }

    if (this.isTerminal()) {
      throw new InvalidStateTransitionError(
        this.entityType,
        this.entityId,
        this.status,
        nextStatus,
        'Terminal states cannot transition to another state',
      );
    }

    if (!this.canTransitionTo(nextStatus)) {
      throw new InvalidStateTransitionError(
        this.entityType,
        this.entityId,
        this.status,
        nextStatus,
        `Allowed transitions from "${this.status}": ${Array.from(this.transitions.get(this.status) ?? []).join(', ') || 'none'}`,
      );
    }

    this.status = nextStatus;
  }
}

/**
 * Pipeline Run State Machine Configuration.
 */
const PIPELINE_RUN_TRANSITIONS: Record<PipelineRunStatus, readonly PipelineRunStatus[]> = {
  PENDING: ['QUEUED', 'CANCELLED'],
  QUEUED: ['RUNNING', 'CANCELLED'],
  RUNNING: ['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'],
  SUCCEEDED: [],
  FAILED: [],
  CANCELLED: [],
  TIMED_OUT: [],
};

const PIPELINE_RUN_TERMINALS: readonly PipelineRunStatus[] = [
  'SUCCEEDED',
  'FAILED',
  'CANCELLED',
  'TIMED_OUT',
];

export function createPipelineRunStateMachine(
  id: string,
  initial: PipelineRunStatus = 'PENDING',
): StateMachine<PipelineRunStatus> {
  return new StateMachine<PipelineRunStatus>(
    'PipelineRun',
    id,
    initial,
    PIPELINE_RUN_TRANSITIONS,
    PIPELINE_RUN_TERMINALS,
  );
}

/**
 * Job State Machine Configuration.
 */
const JOB_TRANSITIONS: Record<JobStatus, readonly JobStatus[]> = {
  PENDING: ['QUEUED', 'CANCELLED'],
  QUEUED: ['RUNNING', 'CANCELLED'],
  RUNNING: ['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'QUEUED'],
  SUCCEEDED: [],
  FAILED: [],
  CANCELLED: [],
  TIMED_OUT: [],
};

const JOB_TERMINALS: readonly JobStatus[] = ['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'];

export function createJobStateMachine(
  id: string,
  initial: JobStatus = 'PENDING',
): StateMachine<JobStatus> {
  return new StateMachine<JobStatus>('Job', id, initial, JOB_TRANSITIONS, JOB_TERMINALS);
}

/**
 * Job Attempt State Machine Configuration.
 */
const JOB_ATTEMPT_TRANSITIONS: Record<JobAttemptStatus, readonly JobAttemptStatus[]> = {
  PENDING: ['RUNNING', 'CANCELLED'],
  RUNNING: ['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'],
  SUCCEEDED: [],
  FAILED: [],
  CANCELLED: [],
  TIMED_OUT: [],
};

const JOB_ATTEMPT_TERMINALS: readonly JobAttemptStatus[] = [
  'SUCCEEDED',
  'FAILED',
  'CANCELLED',
  'TIMED_OUT',
];

export function createJobAttemptStateMachine(
  id: string,
  initial: JobAttemptStatus = 'PENDING',
): StateMachine<JobAttemptStatus> {
  return new StateMachine<JobAttemptStatus>(
    'JobAttempt',
    id,
    initial,
    JOB_ATTEMPT_TRANSITIONS,
    JOB_ATTEMPT_TERMINALS,
  );
}
