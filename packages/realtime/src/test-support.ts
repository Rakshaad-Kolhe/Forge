/**
 * In-memory {@link RedisPubSub} double for unit tests. Synchronous delivery, no I/O.
 * Not exported from the package entrypoint — test-only.
 */
import type {
  RedisPubSub,
  RedisPubSubConnectionState,
  RedisPubSubMessageHandler,
} from '@forge/redis';

export class FakeRedisPubSub implements RedisPubSub {
  public status = 'ready';
  public readonly published: Array<{ channel: string; message: string }> = [];
  public subscribeCalls = 0;
  public unsubscribeCalls = 0;
  /** Set to make the next `publish` throw this error, then clear itself. */
  public failNextPublish?: Error;
  /** Set to make the next `subscribe` throw this error, then clear itself. */
  public failNextSubscribe?: Error;
  /** Set to make the next `publish` never resolve (simulates an unresponsive Redis). */
  public hangNextPublish = false;

  private readonly handlers = new Map<string, Set<RedisPubSubMessageHandler>>();
  private readonly connectionListeners = new Set<(state: RedisPubSubConnectionState) => void>();
  private closed = false;

  public async connect(): Promise<void> {}

  public async publish(channel: string, message: string): Promise<number> {
    if (this.closed) {
      throw new Error('Cannot publish on a closed RedisPubSub');
    }
    if (this.failNextPublish) {
      const err = this.failNextPublish;
      this.failNextPublish = undefined;
      throw err;
    }
    if (this.hangNextPublish) {
      this.hangNextPublish = false;
      return new Promise<number>(() => {});
    }
    this.published.push({ channel, message });
    const channelHandlers = this.handlers.get(channel);
    if (!channelHandlers) {
      return 0;
    }
    for (const handler of Array.from(channelHandlers)) {
      handler(message, channel);
    }
    return channelHandlers.size;
  }

  public async subscribe(channel: string, handler: RedisPubSubMessageHandler): Promise<void> {
    if (this.failNextSubscribe) {
      const err = this.failNextSubscribe;
      this.failNextSubscribe = undefined;
      throw err;
    }
    this.subscribeCalls += 1;
    let channelHandlers = this.handlers.get(channel);
    if (!channelHandlers) {
      channelHandlers = new Set();
      this.handlers.set(channel, channelHandlers);
    }
    channelHandlers.add(handler);
  }

  public async unsubscribe(channel: string): Promise<void> {
    this.unsubscribeCalls += 1;
    this.handlers.delete(channel);
  }

  public async close(): Promise<void> {
    this.closed = true;
    this.handlers.clear();
  }

  public onConnectionChange(listener: (state: RedisPubSubConnectionState) => void): () => void {
    this.connectionListeners.add(listener);
    return () => this.connectionListeners.delete(listener);
  }

  /** Test helper: simulate a subscriber-connection lifecycle transition. */
  public emitConnectionState(state: RedisPubSubConnectionState): void {
    for (const listener of this.connectionListeners) {
      listener(state);
    }
  }

  /** Test helper: number of registered handlers for a channel. */
  public handlerCountFor(channel: string): number {
    return this.handlers.get(channel)?.size ?? 0;
  }
}
