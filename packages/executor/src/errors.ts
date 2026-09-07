/**
 * Base class for all executor-related errors.
 */
export class ExecutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ExecutionError';
  }
}

/**
 * Thrown when the Docker CLI or Docker daemon is unreachable or unresponsive.
 */
export class DockerUnavailableError extends ExecutionError {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'DockerUnavailableError';
  }
}

/**
 * Thrown when container provisioning or startup fails prior to running user commands.
 */
export class ContainerStartupError extends ExecutionError {
  constructor(
    message: string,
    public readonly image?: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'ContainerStartupError';
  }
}

/**
 * Thrown when an execution exceeds its configured wall-clock timeout.
 */
export class ExecutionTimeoutError extends ExecutionError {
  constructor(
    public readonly timeoutMs: number,
    message = `Execution exceeded wall-clock timeout of ${timeoutMs}ms`,
  ) {
    super(message);
    this.name = 'ExecutionTimeoutError';
  }
}

/**
 * Thrown when an execution is explicitly aborted via AbortSignal.
 */
export class ExecutionCancelledError extends ExecutionError {
  constructor(message = 'Execution was cancelled by abort signal') {
    super(message);
    this.name = 'ExecutionCancelledError';
  }
}

/**
 * Recorded when an error occurs during post-execution cleanup without altering the primary result.
 */
export class CleanupError extends ExecutionError {
  constructor(
    message: string,
    public readonly target: 'container' | 'workspace',
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'CleanupError';
  }
}
