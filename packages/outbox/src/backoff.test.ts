import { describe, expect, it } from 'vitest';
import { outboxBackoffMs } from './backoff.js';

describe('outboxBackoffMs', () => {
  it('is base on the first failure (attempt count 0)', () => {
    expect(outboxBackoffMs(0, 500, 60000)).toBe(500);
  });

  it('doubles per attempt', () => {
    expect(outboxBackoffMs(1, 500, 60000)).toBe(1000);
    expect(outboxBackoffMs(2, 500, 60000)).toBe(2000);
    expect(outboxBackoffMs(5, 500, 60000)).toBe(16000);
  });

  it('is capped at maxMs', () => {
    expect(outboxBackoffMs(20, 500, 60000)).toBe(60000);
  });

  it('treats a negative attempt count as base', () => {
    expect(outboxBackoffMs(-1, 500, 60000)).toBe(500);
  });
});
