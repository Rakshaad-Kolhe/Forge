import { describe, it, expect } from 'vitest';
import { EVENT_ID_PATTERN, generateEventId, isEventId } from './event-id.js';
import { createForgeEvent } from './factory.js';

describe('event-id', () => {
  it('generates canonical v4 UUIDs', () => {
    const id = generateEventId();
    expect(id).toMatch(EVENT_ID_PATTERN);
    expect(isEventId(id)).toBe(true);
  });

  it('generates a unique id on every call', () => {
    const count = 10_000;
    const ids = new Set<string>();
    for (let i = 0; i < count; i++) {
      ids.add(generateEventId());
    }
    expect(ids.size).toBe(count);
  });

  it('is never derived from the event type — two same-typed events differ', () => {
    const a = createForgeEvent('WorkerHeartbeat', {
      payload: { worker_id: 'w-1', status: 'READY' },
    });
    const b = createForgeEvent('WorkerHeartbeat', {
      payload: { worker_id: 'w-1', status: 'READY' },
    });
    expect(a.event_id).not.toBe(b.event_id);
  });

  it('rejects non-UUID values', () => {
    expect(isEventId('WorkerHeartbeat')).toBe(false);
    expect(isEventId('')).toBe(false);
    expect(isEventId(undefined)).toBe(false);
    expect(isEventId('123e4567-e89b-12d3-a456-426614174000')).toBe(false); // v1, not v4
  });
});
