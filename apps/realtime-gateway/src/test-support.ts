/**
 * Test doubles for gateway unit tests. Not exported from the package entrypoint.
 */
import type { GatewaySocket } from './connection.js';

type Listener = (...args: unknown[]) => void;

/** Synchronous in-memory {@link GatewaySocket}. */
export class FakeSocket implements GatewaySocket {
  public readonly sent: string[] = [];
  public closedWith?: { code: number; reason: string };
  public terminated = false;
  public pings = 0;
  /** When true, `send` withholds its completion callback until {@link flush}. */
  public deferSend = false;

  private readonly listeners = new Map<string, Listener>();
  private readonly pendingCallbacks: Array<(err?: Error) => void> = [];

  public send(data: string, cb?: (err?: Error) => void): void {
    this.sent.push(data);
    if (this.deferSend) {
      if (cb) {
        this.pendingCallbacks.push(cb);
      }
      return;
    }
    cb?.();
  }

  /** Resolve every withheld send callback (simulates the client finally draining). */
  public flush(): void {
    const cbs = this.pendingCallbacks.splice(0);
    for (const cb of cbs) {
      cb();
    }
  }

  public close(code?: number, reason?: string): void {
    this.closedWith = { code: code ?? 1000, reason: reason ?? '' };
    this.emit('close');
  }

  public terminate(): void {
    this.terminated = true;
    this.emit('close');
  }

  public ping(): void {
    this.pings += 1;
  }

  public on(event: string, listener: (...args: never[]) => void): void {
    this.listeners.set(event, listener as Listener);
  }

  private emit(event: string, ...args: unknown[]): void {
    this.listeners.get(event)?.(...args);
  }

  // --- test helpers -------------------------------------------------------

  public receiveMessage(raw: string, isBinary = false): void {
    this.emit('message', raw, isBinary);
  }

  public receivePong(): void {
    this.emit('pong');
  }

  public parsedMessages(): Array<Record<string, unknown>> {
    return this.sent.map((s) => JSON.parse(s) as Record<string, unknown>);
  }
}
