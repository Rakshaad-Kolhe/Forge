/**
 * Base error class for all scheduler-related exceptions.
 */
export class SchedulerError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = this.constructor.name;
    if (cause && !this.stack && cause.stack) {
      this.stack = cause.stack;
    }
  }
}

/**
 * Thrown when worker retrieval from the worker source or registry fails.
 */
export class WorkerSourceError extends SchedulerError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
  }
}

/**
 * Thrown when job retrieval from the job source or repository fails.
 */
export class JobSourceError extends SchedulerError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
  }
}

/**
 * Thrown when a job referenced by ID cannot be found in the job source.
 */
export class JobNotFoundError extends SchedulerError {
  constructor(public readonly jobId: string) {
    super(`Job with ID "${jobId}" was not found in job source`);
  }
}
