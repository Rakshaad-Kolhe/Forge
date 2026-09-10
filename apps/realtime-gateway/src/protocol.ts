/**
 * Versioned WebSocket application protocol for the Forge realtime gateway.
 *
 * The client speaks a small, explicit message set; the server never leaks Redis or
 * transport internals. `v` is the protocol version ({@link REALTIME_PROTOCOL_VERSION});
 * a mismatch is rejected, never silently reinterpreted.
 */
import { z } from 'zod';
import { REALTIME_PROTOCOL_VERSION } from '@forge/contracts';
import type { ForgeEvent } from '@forge/events';

/** Structured error codes surfaced to the client (never internal detail). */
export const PROTOCOL_ERROR_CODES = [
  'INVALID_MESSAGE',
  'UNSUPPORTED_VERSION',
  'UNAUTHORIZED',
  'FORBIDDEN',
  'INVALID_SUBSCRIPTION',
  'SUBSCRIPTION_LIMIT',
  'RATE_LIMITED',
  'MESSAGE_TOO_LARGE',
  'SERVER_SHUTTING_DOWN',
  'SLOW_CONSUMER',
] as const;
export type ProtocolErrorCode = (typeof PROTOCOL_ERROR_CODES)[number];

/**
 * Application-level WebSocket close codes (RFC 6455 private range 4000–4999) plus the
 * standard `1001` (going away) used on graceful shutdown.
 */
export const CLOSE_CODES = {
  GOING_AWAY: 1001,
  UNAUTHORIZED: 4001,
  FORBIDDEN: 4003,
  MESSAGE_TOO_LARGE: 4009,
  SLOW_CONSUMER: 4008,
  PROTOCOL_VIOLATION: 4000,
} as const;

/** Subscribable Forge resources. Clients can never name a raw Redis channel. */
export const SUBSCRIPTION_KINDS = ['pipeline', 'run', 'job'] as const;
export type SubscriptionKind = (typeof SUBSCRIPTION_KINDS)[number];

export interface SubscriptionTarget {
  readonly kind: SubscriptionKind;
  readonly id: string;
}

const targetSchema = z.object({
  kind: z.enum(SUBSCRIPTION_KINDS),
  id: z.string().min(1).max(256),
});

const versionSchema = z.literal(REALTIME_PROTOCOL_VERSION);

const subscribeSchema = z.object({
  type: z.literal('subscribe'),
  v: versionSchema,
  target: targetSchema,
});

const unsubscribeSchema = z.object({
  type: z.literal('unsubscribe'),
  v: versionSchema,
  target: targetSchema,
});

const pingSchema = z.object({
  type: z.literal('ping'),
  v: versionSchema,
});

export const clientMessageSchema = z.discriminatedUnion('type', [
  subscribeSchema,
  unsubscribeSchema,
  pingSchema,
]);

export type ClientMessage = z.infer<typeof clientMessageSchema>;

export type ServerMessage =
  | {
      type: 'ready';
      v: number;
      connection_id: string;
      heartbeat_interval_ms: number;
      limits: {
        max_subscriptions: number;
        max_pending_messages: number;
        max_message_bytes: number;
      };
    }
  | { type: 'subscribed'; v: number; target: SubscriptionTarget }
  | { type: 'unsubscribed'; v: number; target: SubscriptionTarget }
  | { type: 'event'; v: number; event: ForgeEvent }
  | { type: 'pong'; v: number }
  | { type: 'closing'; v: number; reason: 'SERVER_SHUTTING_DOWN' }
  | {
      type: 'error';
      v: number;
      code: ProtocolErrorCode;
      message: string;
      target?: SubscriptionTarget;
    };

export interface ParsedClientMessage {
  readonly ok: true;
  readonly message: ClientMessage;
}
export interface ParseFailure {
  readonly ok: false;
  readonly code: 'INVALID_MESSAGE' | 'UNSUPPORTED_VERSION';
  readonly reason: string;
}

/**
 * Parses a raw inbound frame. Never throws — a malformed frame yields a typed failure the
 * caller answers with an `error` message; the connection stays open.
 */
export function parseClientMessage(raw: string): ParsedClientMessage | ParseFailure {
  let json: unknown;
  try {
    json = JSON.parse(raw);
  } catch {
    return { ok: false, code: 'INVALID_MESSAGE', reason: 'payload is not valid JSON' };
  }
  const result = clientMessageSchema.safeParse(json);
  if (result.success) {
    return { ok: true, message: result.data };
  }
  const mentionsVersion = result.error.issues.some((i) => i.path.includes('v'));
  if (mentionsVersion) {
    return {
      ok: false,
      code: 'UNSUPPORTED_VERSION',
      reason: `unsupported protocol version; this gateway speaks v${REALTIME_PROTOCOL_VERSION}`,
    };
  }
  return {
    ok: false,
    code: 'INVALID_MESSAGE',
    reason: result.error.issues
      .map((i) => `${i.path.join('.') || '<root>'}: ${i.message}`)
      .join('; '),
  };
}
