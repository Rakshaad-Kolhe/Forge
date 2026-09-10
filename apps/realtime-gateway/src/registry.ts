/**
 * Owns every live connection for *this* gateway instance and an inverted
 * subscription-key → connections index for O(matched-keys) fan-out (no full scan of all
 * connections per event). State is per-instance only — nothing is shared in memory across
 * gateways, so horizontal scale-out needs no sticky sessions.
 */
import type { ForgeEvent } from '@forge/events';
import type { GatewayConnection } from './connection.js';
import type { SubscriptionTarget } from './protocol.js';
import { keysForEvent, subscriptionKey } from './subscription.js';

export interface DispatchResult {
  readonly matchedConnections: number;
  readonly delivered: number;
}

export class ConnectionRegistry {
  private readonly connections = new Map<string, GatewayConnection>();
  private readonly byKey = new Map<string, Set<string>>();
  private totalSubscriptions = 0;

  constructor(private readonly maxConnections: number) {}

  public get size(): number {
    return this.connections.size;
  }

  public get subscriptionCount(): number {
    return this.totalSubscriptions;
  }

  public get atCapacity(): boolean {
    return this.connections.size >= this.maxConnections;
  }

  /** @returns `false` when the instance connection cap is reached. */
  public add(connection: GatewayConnection): boolean {
    if (this.connections.size >= this.maxConnections) {
      return false;
    }
    this.connections.set(connection.id, connection);
    return true;
  }

  /** Full teardown: drop the connection and every index entry that referenced it. */
  public remove(connection: GatewayConnection): void {
    if (!this.connections.delete(connection.id)) {
      return;
    }
    for (const key of Array.from(connection.subscriptionKeys)) {
      const set = this.byKey.get(key);
      if (set && set.delete(connection.id)) {
        this.totalSubscriptions -= 1;
        if (set.size === 0) {
          this.byKey.delete(key);
        }
      }
    }
  }

  /** @returns `true` on success, `false` if the connection's per-connection limit is hit. */
  public subscribe(connection: GatewayConnection, target: SubscriptionTarget): boolean {
    const key = subscriptionKey(target);
    const wasSubscribed = connection.hasSubscription(key);
    if (!connection.addSubscription(target)) {
      return false;
    }
    if (!wasSubscribed) {
      let set = this.byKey.get(key);
      if (!set) {
        set = new Set();
        this.byKey.set(key, set);
      }
      set.add(connection.id);
      this.totalSubscriptions += 1;
    }
    return true;
  }

  public unsubscribe(connection: GatewayConnection, target: SubscriptionTarget): void {
    const key = subscriptionKey(target);
    if (!connection.removeSubscription(target)) {
      return;
    }
    const set = this.byKey.get(key);
    if (set && set.delete(connection.id)) {
      this.totalSubscriptions -= 1;
      if (set.size === 0) {
        this.byKey.delete(key);
      }
    }
  }

  /** Fan `event` out to every connection subscribed to one of the event's keys. */
  public dispatch(event: ForgeEvent): DispatchResult {
    const keys = keysForEvent(event);
    if (keys.length === 0) {
      return { matchedConnections: 0, delivered: 0 };
    }
    const recipients = new Set<string>();
    for (const key of keys) {
      const set = this.byKey.get(key);
      if (set) {
        for (const id of set) {
          recipients.add(id);
        }
      }
    }
    let delivered = 0;
    for (const id of recipients) {
      const connection = this.connections.get(id);
      if (connection) {
        connection.sendEvent(event);
        delivered += 1;
      }
    }
    return { matchedConnections: recipients.size, delivered };
  }

  public closeAll(code: number, reason: string): void {
    for (const connection of Array.from(this.connections.values())) {
      connection.close(code, reason);
    }
  }

  public terminateAll(): void {
    for (const connection of Array.from(this.connections.values())) {
      connection.terminate();
    }
  }

  public forEach(fn: (connection: GatewayConnection) => void): void {
    for (const connection of Array.from(this.connections.values())) {
      fn(connection);
    }
  }
}
