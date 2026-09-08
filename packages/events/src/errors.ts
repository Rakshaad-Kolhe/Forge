/**
 * Base error class for all event-layer errors in Forge.
 */
export class EventError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'EventError';
    if (cause && cause.stack) {
      this.stack = `${this.stack}\nCaused by: ${cause.stack}`;
    }
  }
}

/**
 * Thrown when {@link '../in-process-bus.ts'.InProcessEventBus.publish} or `subscribe` is
 * called after the bus has been closed. Producers using
 * {@link './publisher.ts'.safePublish} never see this — it is caught and logged as a
 * best-effort publication failure.
 */
export class EventBusClosedError extends EventError {
  constructor(operation: 'publish' | 'subscribe') {
    super(`Cannot ${operation} on a closed InProcessEventBus`);
    this.name = 'EventBusClosedError';
  }
}
