export class OutboxDispatcherError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'OutboxDispatcherError';
  }
}

export class OutboxPublishTimeoutError extends OutboxDispatcherError {
  constructor(public readonly timeoutMs: number) {
    super(`Outbox publish did not settle within ${timeoutMs}ms`);
    this.name = 'OutboxPublishTimeoutError';
  }
}
