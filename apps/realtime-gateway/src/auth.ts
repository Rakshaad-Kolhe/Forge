/**
 * WebSocket handshake authentication — the smallest explicit bridge over Forge's current
 * (absent) auth system.
 *
 * ⚠️ LIMITATION: Forge has no user/session model yet (see `docs/architecture/*`). This
 * verifies a single shared secret and yields one opaque principal. It is NOT per-user
 * identity and NOT authorization — it only proves the caller holds the gateway secret.
 * Replace with the real session mechanism when the API grows one; the
 * {@link '@forge/realtime-gateway'.SubscriptionAuthorizer} seam is where per-resource
 * checks then live.
 *
 * The secret is read from, in order:
 *   1. `Authorization: Bearer <token>`                     (services / CLI)
 *   2. `Sec-WebSocket-Protocol: forge.v1.token.<token>`    (browsers — cannot set headers
 *                                                            on `new WebSocket`, can set
 *                                                            subprotocols)
 * It is never read from the URL query string.
 */
import { timingSafeEqual } from 'node:crypto';
import type { IncomingMessage } from 'node:http';

export interface Principal {
  /** Opaque identity. Currently a single shared value — not a user id. */
  readonly id: string;
}

export type AuthResult =
  | { readonly ok: true; readonly principal: Principal; readonly acceptSubprotocol?: string }
  | { readonly ok: false; readonly reason: string };

const SUBPROTOCOL = 'forge.v1';
const TOKEN_SUBPROTOCOL_PREFIX = 'forge.v1.token.';

function constantTimeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) {
    return false;
  }
  return timingSafeEqual(ab, bb);
}

function extractOfferedToken(req: IncomingMessage): {
  token?: string;
  offeredSubprotocol: boolean;
} {
  const authz = req.headers['authorization'];
  if (typeof authz === 'string' && authz.startsWith('Bearer ')) {
    return { token: authz.slice('Bearer '.length).trim(), offeredSubprotocol: false };
  }

  const raw = req.headers['sec-websocket-protocol'];
  if (typeof raw === 'string') {
    const offered = raw.split(',').map((p) => p.trim());
    const tokenEntry = offered.find((p) => p.startsWith(TOKEN_SUBPROTOCOL_PREFIX));
    if (tokenEntry) {
      return {
        token: tokenEntry.slice(TOKEN_SUBPROTOCOL_PREFIX.length),
        offeredSubprotocol: offered.includes(SUBPROTOCOL),
      };
    }
    return { offeredSubprotocol: offered.includes(SUBPROTOCOL) };
  }

  return { offeredSubprotocol: false };
}

/**
 * @param expectedToken the configured gateway secret (guaranteed non-empty by config).
 */
export function authenticateHandshake(req: IncomingMessage, expectedToken: string): AuthResult {
  const { token, offeredSubprotocol } = extractOfferedToken(req);
  if (!token) {
    return { ok: false, reason: 'missing gateway credential' };
  }
  if (!constantTimeEqual(token, expectedToken)) {
    return { ok: false, reason: 'invalid gateway credential' };
  }
  const result: AuthResult = { ok: true, principal: { id: 'shared-secret' } };
  return offeredSubprotocol ? { ...result, acceptSubprotocol: SUBPROTOCOL } : result;
}
