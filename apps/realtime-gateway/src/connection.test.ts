import { describe, expect, it, vi } from 'vitest';
import { createForgeEvent } from '@forge/events';
import { GatewayConnection } from './connection.js';
import { CLOSE_CODES } from './protocol.js';
import { FakeSocket } from './test-support.js';

const baseOpts = (socket: FakeSocket, onClose = vi.fn()) => ({
  id: 'c1',
  socket,
  maxPendingMessages: 3,
  maxSubscriptions: 2,
  maxMessageBytes: 1024,
  heartbeatIntervalMs: 1000,
  onClose,
});

describe('GatewayConnection', () => {
  it('frames a ready message with the negotiated limits', () => {
    const socket = new FakeSocket();
    const conn = new GatewayConnection(baseOpts(socket));
    conn.sendReady();
    expect(socket.parsedMessages()[0]).toEqual({
      type: 'ready',
      v: 1,
      connection_id: 'c1',
      heartbeat_interval_ms: 1000,
      limits: { max_subscriptions: 2, max_pending_messages: 3, max_message_bytes: 1024 },
    });
  });

  it('enforces the per-connection subscription limit', () => {
    const conn = new GatewayConnection(baseOpts(new FakeSocket()));
    expect(conn.addSubscription({ kind: 'run', id: 'a' })).toBe(true);
    expect(conn.addSubscription({ kind: 'run', id: 'b' })).toBe(true);
    expect(conn.addSubscription({ kind: 'run', id: 'c' })).toBe(false);
    expect(conn.subscriptionCount).toBe(2);
  });

  it('disconnects a slow consumer once the outbound queue fills, without unbounded buffering', () => {
    const socket = new FakeSocket();
    socket.deferSend = true; // client never drains
    const onClose = vi.fn();
    const conn = new GatewayConnection(baseOpts(socket, onClose));
    const event = createForgeEvent('JobStarted', {
      correlation: { run_id: 'r', job_id: 'j' },
      payload: { job_id: 'j', attempt_id: 'a', worker_id: 'w', attempt_number: 1 },
    });

    conn.sendEvent(event); // pending 1
    conn.sendEvent(event); // pending 2
    conn.sendEvent(event); // pending 3 (== max)
    expect(conn.pendingMessages).toBe(3);

    conn.sendEvent(event); // would be 4 -> slow-consumer disconnect
    expect(conn.slowConsumerDisconnect).toBe(true);
    expect(socket.closedWith?.code).toBe(CLOSE_CODES.SLOW_CONSUMER);
    expect(socket.terminated).toBe(true);
    expect(onClose).toHaveBeenCalledTimes(1);
    // No further growth: sends after close are no-ops.
    conn.sendEvent(event);
    expect(socket.sent.length).toBe(3);
  });

  it('terminates a connection that misses a heartbeat pong', () => {
    const socket = new FakeSocket();
    const onClose = vi.fn();
    const conn = new GatewayConnection(baseOpts(socket, onClose));

    conn.heartbeatTick(); // alive -> ping, expect pong
    expect(socket.pings).toBe(1);
    expect(socket.terminated).toBe(false);

    conn.heartbeatTick(); // no pong arrived -> terminate
    expect(socket.terminated).toBe(true);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('stays alive across ticks when pongs arrive', () => {
    const socket = new FakeSocket();
    const conn = new GatewayConnection(baseOpts(socket));
    conn.heartbeatTick();
    socket.receivePong();
    conn.heartbeatTick();
    socket.receivePong();
    expect(socket.terminated).toBe(false);
    expect(socket.pings).toBe(2);
  });

  it('rejects a binary frame but keeps the connection open', () => {
    const socket = new FakeSocket();
    const conn = new GatewayConnection(baseOpts(socket));
    const seen: string[] = [];
    conn.onMessage((raw) => seen.push(raw));

    socket.receiveMessage('anything', true);
    expect(seen).toEqual([]);
    expect(socket.parsedMessages()[0]).toMatchObject({ type: 'error', code: 'INVALID_MESSAGE' });
    expect(socket.terminated).toBe(false);
  });

  it('clears subscriptions and notifies once on close', () => {
    const socket = new FakeSocket();
    const onClose = vi.fn();
    const conn = new GatewayConnection(baseOpts(socket, onClose));
    conn.addSubscription({ kind: 'run', id: 'a' });

    socket.close(1000, 'client gone');
    socket.close(1000, 'again');

    expect(conn.subscriptionCount).toBe(0);
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
