import type { IncomingMessage } from 'node:http';
import { describe, expect, it } from 'vitest';
import { authenticateHandshake } from './auth.js';

const req = (headers: Record<string, string>): IncomingMessage =>
  ({ headers }) as unknown as IncomingMessage;

const SECRET = 'super-secret-token';

describe('authenticateHandshake', () => {
  it('accepts a valid Authorization: Bearer token', () => {
    const r = authenticateHandshake(req({ authorization: `Bearer ${SECRET}` }), SECRET);
    expect(r).toMatchObject({ ok: true, principal: { id: 'shared-secret' } });
  });

  it('accepts a valid token carried as a subprotocol and echoes forge.v1', () => {
    const r = authenticateHandshake(
      req({ 'sec-websocket-protocol': `forge.v1, forge.v1.token.${SECRET}` }),
      SECRET,
    );
    expect(r).toMatchObject({ ok: true, acceptSubprotocol: 'forge.v1' });
  });

  it('does not echo a subprotocol when only the token entry is offered', () => {
    const r = authenticateHandshake(
      req({ 'sec-websocket-protocol': `forge.v1.token.${SECRET}` }),
      SECRET,
    );
    expect(r).toEqual({ ok: true, principal: { id: 'shared-secret' } });
  });

  it('rejects a missing credential', () => {
    expect(authenticateHandshake(req({}), SECRET)).toMatchObject({ ok: false });
  });

  it('rejects a wrong token', () => {
    expect(authenticateHandshake(req({ authorization: 'Bearer nope' }), SECRET)).toMatchObject({
      ok: false,
    });
  });

  it('rejects a token of a different length without leaking via timing', () => {
    expect(
      authenticateHandshake(req({ authorization: `Bearer ${SECRET}extra` }), SECRET),
    ).toMatchObject({ ok: false });
  });
});
