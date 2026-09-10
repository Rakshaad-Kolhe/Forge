import { describe, expect, it } from 'vitest';
import { isOriginAllowed } from './origin.js';

describe('isOriginAllowed', () => {
  const allow = ['https://app.forge.example', 'https://admin.forge.example'];

  it('allows a request with no Origin header (non-browser client)', () => {
    expect(isOriginAllowed(undefined, allow)).toBe(true);
    expect(isOriginAllowed('', allow)).toBe(true);
  });

  it('accepts an exact allowlisted origin', () => {
    expect(isOriginAllowed('https://app.forge.example', allow)).toBe(true);
  });

  it('rejects an origin not on the allowlist', () => {
    expect(isOriginAllowed('https://evil.example', allow)).toBe(false);
  });

  it('rejects every browser origin when the allowlist is empty (never "*")', () => {
    expect(isOriginAllowed('https://app.forge.example', [])).toBe(false);
  });

  it('rejects the opaque "null" origin', () => {
    expect(isOriginAllowed('null', allow)).toBe(false);
  });
});
