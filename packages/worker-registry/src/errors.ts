/**
 * Base error class for all worker-related errors in Forge.
 */
export class WorkerError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'WorkerError';
    if (cause && cause.stack) {
      this.stack = `${this.stack}\nCaused by: ${cause.stack}`;
    }
  }
}

/**
 * Thrown when worker input parameters or metadata fail domain validation rules.
 */
export class WorkerValidationError extends WorkerError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'WorkerValidationError';
  }
}

/**
 * Thrown when an operation targets a worker that is not registered.
 */
export class WorkerNotFoundError extends WorkerError {
  constructor(workerId: string) {
    super(`Worker "${workerId}" is not registered in the cluster`);
    this.name = 'WorkerNotFoundError';
  }
}

/**
 * Thrown when worker registration or metadata persistence fails.
 */
export class WorkerRegistrationError extends WorkerError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'WorkerRegistrationError';
  }
}

/**
 * Thrown when recording or inspecting a worker heartbeat in Redis fails.
 */
export class WorkerHeartbeatError extends WorkerError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'WorkerHeartbeatError';
  }
}
