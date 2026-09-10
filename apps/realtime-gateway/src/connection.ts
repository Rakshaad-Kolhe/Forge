/**
 * One live WebSocket client: identity, its subscription set, a bounded outbound queue,
 * and liveness state. Deliberately decoupled from `ws` via {@link GatewaySocket} so it is
 * unit-testable with a fake socket.
 *
 * Backpressure: every `send` counts against `maxPendingMessages` (incremented before the
 * write, decremented in its completion callback). A client whose queue reaches the bound
 * is disconnected with `SLOW_CONSUMER` — its memory footprint is capped and it never
 * stalls other clients (each connection owns its own counter and socket).
 */
import type { Logger } from '@forge/logging';
import type { ForgeEvent } from '@forge/events';
import { REALTIME_PROTOCOL_VERSION } from '@forge/contracts';
import {
  CLOSE_CODES,
  type ProtocolErrorCode,
  type ServerMessage,
  type SubscriptionTarget,
} from './protocol.js';
import { subscriptionKey } from './subscription.js';

/** The subset of the `ws` socket the gateway relies on. A real `ws.WebSocket` satisfies it. */
export interface GatewaySocket {
  send(data: string, cb?: (err?: Error) => void): void;
  close(code?: number, reason?: string): void;
  terminate(): void;
  ping(): void;
  on(event: 'message', listener: (data: unknown, isBinary: boolean) => void): void;
  on(event: 'close', listener: () => void): void;
  on(event: 'pong', listener: () => void): void;
  on(event: 'error', listener: (err: Error) => void): void;
}

export interface GatewayConnectionOptions {
  readonly id: string;
  readonly socket: GatewaySocket;
  readonly maxPendingMessages: number;
  readonly maxSubscriptions: number;
  readonly maxMessageBytes: number;
  readonly heartbeatIntervalMs: number;
  readonly logger?: Logger;
  /** Called exactly once when the socket has closed (for whatever reason). */
  readonly onClose: (connection: GatewayConnection) => void;
}

export class GatewayConnection {
  public readonly id: string;
  public readonly createdAt = Date.now();
  private readonly socket: GatewaySocket;
  private readonly opts: GatewayConnectionOptions;
  private readonly logger?: Logger;
  private readonly targets = new Map<string, SubscriptionTarget>();
  private pending = 0;
  private isAlive = true;
  private closed = false;
  /** True when this connection was dropped for outbound backpressure. */
  public slowConsumerDisconnect = false;

  constructor(options: GatewayConnectionOptions) {
    this.id = options.id;
    this.socket = options.socket;
    this.opts = options;
    this.logger = options.logger;

    this.socket.on('close', () => this.handleClosed());
    this.socket.on('error', (err) => {
      this.logger?.warn('websocket.socket_error', { connection_id: this.id, error: err.message });
    });
    this.socket.on('pong', () => {
      this.isAlive = true;
    });
  }

  public onMessage(listener: (raw: string) => void): void {
    this.socket.on('message', (data, isBinary) => {
      if (isBinary) {
        this.sendError('INVALID_MESSAGE', 'binary frames are not supported');
        return;
      }
      listener(String(data));
    });
  }

  public get subscriptionKeys(): IterableIterator<string> {
    return this.targets.keys();
  }

  public get subscriptionCount(): number {
    return this.targets.size;
  }

  public hasSubscription(key: string): boolean {
    return this.targets.has(key);
  }

  /** @returns `false` when the per-connection subscription limit is already reached. */
  public addSubscription(target: SubscriptionTarget): boolean {
    const key = subscriptionKey(target);
    if (this.targets.has(key)) {
      return true;
    }
    if (this.targets.size >= this.opts.maxSubscriptions) {
      return false;
    }
    this.targets.set(key, target);
    return true;
  }

  public removeSubscription(target: SubscriptionTarget): boolean {
    return this.targets.delete(subscriptionKey(target));
  }

  public sendReady(): void {
    this.send({
      type: 'ready',
      v: REALTIME_PROTOCOL_VERSION,
      connection_id: this.id,
      heartbeat_interval_ms: this.opts.heartbeatIntervalMs,
      limits: {
        max_subscriptions: this.opts.maxSubscriptions,
        max_pending_messages: this.opts.maxPendingMessages,
        max_message_bytes: this.opts.maxMessageBytes,
      },
    });
  }

  public sendEvent(event: ForgeEvent): void {
    this.send({ type: 'event', v: REALTIME_PROTOCOL_VERSION, event });
  }

  public sendAck(type: 'subscribed' | 'unsubscribed', target: SubscriptionTarget): void {
    this.send({ type, v: REALTIME_PROTOCOL_VERSION, target });
  }

  public sendPong(): void {
    this.send({ type: 'pong', v: REALTIME_PROTOCOL_VERSION });
  }

  public sendError(code: ProtocolErrorCode, message: string, target?: SubscriptionTarget): void {
    this.send(
      target
        ? { type: 'error', v: REALTIME_PROTOCOL_VERSION, code, message, target }
        : { type: 'error', v: REALTIME_PROTOCOL_VERSION, code, message },
    );
  }

  public send(message: ServerMessage): void {
    if (this.closed) {
      return;
    }
    if (this.pending >= this.opts.maxPendingMessages) {
      this.disconnectSlowConsumer();
      return;
    }
    let serialized: string;
    try {
      serialized = JSON.stringify(message);
    } catch (err) {
      this.logger?.error('websocket.serialize_failed', {
        connection_id: this.id,
        error: err instanceof Error ? err.message : String(err),
      });
      return;
    }
    this.pending += 1;
    this.socket.send(serialized, (err) => {
      this.pending = Math.max(0, this.pending - 1);
      if (err) {
        this.logger?.debug('websocket.send_failed', {
          connection_id: this.id,
          error: err.message,
        });
      }
    });
  }

  /** Heartbeat tick: terminate a connection that missed the previous pong, else ping. */
  public heartbeatTick(): void {
    if (this.closed) {
      return;
    }
    if (!this.isAlive) {
      this.logger?.info('websocket.heartbeat_timeout', { connection_id: this.id });
      this.terminate();
      return;
    }
    this.isAlive = false;
    try {
      this.socket.ping();
    } catch {
      this.terminate();
    }
  }

  public disconnectSlowConsumer(): void {
    if (this.closed) {
      return;
    }
    this.logger?.warn('websocket.slow_consumer', {
      connection_id: this.id,
      pending: this.pending,
      max_pending: this.opts.maxPendingMessages,
    });
    this.slowConsumerDisconnect = true;
    this.close(CLOSE_CODES.SLOW_CONSUMER, 'outbound buffer exceeded');
    // Force the socket shut even if the close frame itself cannot drain.
    this.terminate();
  }

  public close(code: number, reason: string): void {
    if (this.closed) {
      return;
    }
    try {
      this.socket.close(code, reason);
    } catch {
      this.terminate();
    }
  }

  public terminate(): void {
    // Always attempt the hard close even if a graceful close frame was already sent —
    // a stuck socket must not linger. `handleClosed` is idempotent.
    try {
      this.socket.terminate();
    } catch {
      /* already gone */
    }
    this.handleClosed();
  }

  public get pendingMessages(): number {
    return this.pending;
  }

  private handleClosed(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    // Notify BEFORE clearing so the registry can still read this connection's keys to
    // purge its inverted index.
    this.opts.onClose(this);
    this.targets.clear();
  }
}
