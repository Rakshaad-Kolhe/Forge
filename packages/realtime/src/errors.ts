/** Base error for the realtime transport layer. */
export class RealtimeError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'RealtimeError';
    if (cause && cause.stack) {
      this.stack = `${this.stack}\nCaused by: ${cause.stack}`;
    }
  }
}

/**
 * Thrown when a payload received off the transport is not a valid {@link '@forge/events'.ForgeEvent}
 * (malformed JSON, failed schema validation, or an unknown schema version). The subscriber
 * catches this, logs `realtime.event_rejected`, and keeps its subscription loop alive.
 */
export class RealtimeEventDecodeError extends RealtimeError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'RealtimeEventDecodeError';
  }
}
