/**
 * Base error class for all queue-specific errors in Forge.
 */
export class QueueError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'QueueError';
    if (cause && cause.stack) {
      this.stack = `${this.stack}\nCaused by: ${cause.stack}`;
    }
  }
}

/**
 * Thrown when queue message payload or input parameters fail validation.
 */
export class QueueValidationError extends QueueError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'QueueValidationError';
  }
}

/**
 * Thrown when attempting a queue operation while the underlying Redis coordination
 * service is disconnected or unreachable.
 */
export class QueueUnavailableError extends QueueError {
  constructor(
    message: string = 'Queue coordination service is currently unavailable',
    cause?: Error,
  ) {
    super(message, cause);
    this.name = 'QueueUnavailableError';
  }
}

/**
 * Thrown when a low-level queue command or atomic script execution fails.
 */
export class QueueOperationError extends QueueError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'QueueOperationError';
  }
}

/**
 * Thrown when attempting an operation on a message that does not exist or has already been acknowledged.
 */
export class QueueMessageNotFoundError extends QueueError {
  constructor(messageId: string) {
    super(`Message "${messageId}" was not found or has already been acknowledged`);
    this.name = 'QueueMessageNotFoundError';
  }
}
