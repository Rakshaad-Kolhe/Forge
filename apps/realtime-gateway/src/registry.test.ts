import { describe, expect, it, vi } from 'vitest';
import { createForgeEvent, type ForgeEvent } from '@forge/events';
import { GatewayConnection } from './connection.js';
import { ConnectionRegistry } from './registry.js';
import { FakeSocket } from './test-support.js';

const mkConn = (id: string, registry: ConnectionRegistry) => {
  const socket = new FakeSocket();
  const conn = new GatewayConnection({
    id,
    socket,
    maxPendingMessages: 100,
    maxSubscriptions: 5,
    maxMessageBytes: 4096,
    heartbeatIntervalMs: 1000,
    onClose: (c) => registry.remove(c),
  });
  return { conn, socket };
};

const runEvent = (runId: string): ForgeEvent =>
  createForgeEvent('JobSucceeded', {
    correlation: { run_id: runId, job_id: `${runId}-job`, attempt_id: 'a', worker_id: 'w' },
    payload: {
      job_id: `${runId}-job`,
      attempt_id: 'a',
      worker_id: 'w',
      attempt_number: 1,
      duration_ms: 1,
      exit_code: 0,
    },
  });

describe('ConnectionRegistry', () => {
  it('enforces the instance connection cap', () => {
    const registry = new ConnectionRegistry(2);
    expect(registry.add(mkConn('a', registry).conn)).toBe(true);
    expect(registry.add(mkConn('b', registry).conn)).toBe(true);
    expect(registry.add(mkConn('c', registry).conn)).toBe(false);
    expect(registry.atCapacity).toBe(true);
  });

  it('fans an event only to connections subscribed to a matching key', () => {
    const registry = new ConnectionRegistry(10);
    const { conn: a, socket: sa } = mkConn('a', registry);
    const { conn: b, socket: sb } = mkConn('b', registry);
    const { conn: c, socket: sc } = mkConn('c', registry);
    registry.add(a);
    registry.add(b);
    registry.add(c);
    registry.subscribe(a, { kind: 'run', id: 'R1' });
    registry.subscribe(b, { kind: 'run', id: 'R1' });
    registry.subscribe(c, { kind: 'run', id: 'R2' });

    const result = registry.dispatch(runEvent('R1'));

    expect(result).toEqual({ matchedConnections: 2, delivered: 2 });
    expect(sa.parsedMessages().at(-1)).toMatchObject({ type: 'event' });
    expect(sb.parsedMessages().at(-1)).toMatchObject({ type: 'event' });
    expect(sc.sent).toEqual([]);
  });

  it('reports zero matches for an event with no subscribers (filtered)', () => {
    const registry = new ConnectionRegistry(10);
    const { conn } = mkConn('a', registry);
    registry.add(conn);
    registry.subscribe(conn, { kind: 'job', id: 'other' });
    expect(registry.dispatch(runEvent('R9'))).toEqual({ matchedConnections: 0, delivered: 0 });
  });

  it('rejects a subscription past the per-connection limit', () => {
    const registry = new ConnectionRegistry(10);
    const socket = new FakeSocket();
    const conn = new GatewayConnection({
      id: 'a',
      socket,
      maxPendingMessages: 100,
      maxSubscriptions: 1,
      maxMessageBytes: 4096,
      heartbeatIntervalMs: 1000,
      onClose: vi.fn(),
    });
    registry.add(conn);
    expect(registry.subscribe(conn, { kind: 'run', id: 'R1' })).toBe(true);
    expect(registry.subscribe(conn, { kind: 'run', id: 'R2' })).toBe(false);
    expect(registry.subscriptionCount).toBe(1);
  });

  it('fully removes a connection and its index entries on close', () => {
    const registry = new ConnectionRegistry(10);
    const { conn, socket } = mkConn('a', registry);
    registry.add(conn);
    registry.subscribe(conn, { kind: 'run', id: 'R1' });
    expect(registry.subscriptionCount).toBe(1);

    socket.close(1000, 'bye');

    expect(registry.size).toBe(0);
    expect(registry.subscriptionCount).toBe(0);
    // A later dispatch finds nothing and does not throw.
    expect(registry.dispatch(runEvent('R1'))).toEqual({ matchedConnections: 0, delivered: 0 });
  });

  it('unsubscribe removes the key and decrements the count', () => {
    const registry = new ConnectionRegistry(10);
    const { conn } = mkConn('a', registry);
    registry.add(conn);
    registry.subscribe(conn, { kind: 'run', id: 'R1' });
    registry.unsubscribe(conn, { kind: 'run', id: 'R1' });
    expect(registry.subscriptionCount).toBe(0);
    expect(registry.dispatch(runEvent('R1'))).toEqual({ matchedConnections: 0, delivered: 0 });
  });

  it('survives repeated connect/subscribe/disconnect cycles with no leaked state', () => {
    const registry = new ConnectionRegistry(100);
    for (let i = 0; i < 200; i += 1) {
      const { conn, socket } = mkConn(`c${i}`, registry);
      registry.add(conn);
      registry.subscribe(conn, { kind: 'run', id: `R${i % 5}` });
      socket.close(1000, 'cycle');
    }
    expect(registry.size).toBe(0);
    expect(registry.subscriptionCount).toBe(0);
  });
});
