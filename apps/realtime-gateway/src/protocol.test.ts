import { describe, expect, it } from 'vitest';
import { parseClientMessage } from './protocol.js';

describe('parseClientMessage', () => {
  it('accepts a well-formed subscribe', () => {
    const r = parseClientMessage(
      JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-1' } }),
    );
    expect(r).toEqual({
      ok: true,
      message: { type: 'subscribe', v: 1, target: { kind: 'run', id: 'run-1' } },
    });
  });

  it('accepts unsubscribe and ping', () => {
    expect(
      parseClientMessage(
        JSON.stringify({ type: 'unsubscribe', v: 1, target: { kind: 'job', id: 'j1' } }),
      ).ok,
    ).toBe(true);
    expect(parseClientMessage(JSON.stringify({ type: 'ping', v: 1 })).ok).toBe(true);
  });

  it('rejects non-JSON with INVALID_MESSAGE and keeps the socket implicitly alive', () => {
    const r = parseClientMessage('{not json');
    expect(r).toMatchObject({ ok: false, code: 'INVALID_MESSAGE' });
  });

  it('rejects an unknown message type', () => {
    const r = parseClientMessage(JSON.stringify({ type: 'evil', v: 1 }));
    expect(r).toMatchObject({ ok: false, code: 'INVALID_MESSAGE' });
  });

  it('rejects a subscribe with no target', () => {
    const r = parseClientMessage(JSON.stringify({ type: 'subscribe', v: 1 }));
    expect(r).toMatchObject({ ok: false, code: 'INVALID_MESSAGE' });
  });

  it('rejects an unknown subscription kind', () => {
    const r = parseClientMessage(
      JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'secret', id: 'x' } }),
    );
    expect(r).toMatchObject({ ok: false, code: 'INVALID_MESSAGE' });
  });

  it('reports a version mismatch distinctly', () => {
    const r = parseClientMessage(
      JSON.stringify({ type: 'subscribe', v: 99, target: { kind: 'run', id: 'r' } }),
    );
    expect(r).toMatchObject({ ok: false, code: 'UNSUPPORTED_VERSION' });
  });

  it('ignores unknown extra fields (no Redis channel control surface)', () => {
    const r = parseClientMessage(
      JSON.stringify({
        type: 'subscribe',
        v: 1,
        target: { kind: 'run', id: 'run-1' },
        channel: 'forge:internal:secret',
      }),
    );
    expect(r.ok).toBe(true);
    if (r.ok) {
      expect(r.message).not.toHaveProperty('channel');
    }
  });
});
