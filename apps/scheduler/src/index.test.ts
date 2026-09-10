import { afterEach, describe, it, expect } from 'vitest';
import { startScheduler } from './index.js';
import { createLogger } from '@forge/logging';

describe('Scheduler Service Shell', () => {
  const originalRealtimeFlag = process.env['REALTIME_PUBLISH_ENABLED'];

  afterEach(() => {
    if (originalRealtimeFlag === undefined) {
      delete process.env['REALTIME_PUBLISH_ENABLED'];
    } else {
      process.env['REALTIME_PUBLISH_ENABLED'] = originalRealtimeFlag;
    }
  });

  it('starts successfully and logs startup event', () => {
    const logs: string[] = [];
    const testLogger = createLogger({
      service: 'scheduler',
      environment: 'development',
      writeFn: (msg) => logs.push(msg),
    });

    const scheduler = startScheduler({ logger: testLogger });
    expect(logs.some((l) => l.includes('Forge Scheduler service shell started'))).toBe(true);

    scheduler.stop();
    expect(logs.some((l) => l.includes('Forge Scheduler service shell stopped'))).toBe(true);
  });

  it('does not wire a realtime publisher without a database pool, even when enabled', () => {
    process.env['REALTIME_PUBLISH_ENABLED'] = 'true';
    const logs: string[] = [];
    const testLogger = createLogger({
      service: 'scheduler',
      environment: 'development',
      writeFn: (msg) => logs.push(msg),
    });

    const scheduler = startScheduler({ logger: testLogger });
    expect(logs.some((l) => l.includes('Realtime event publisher wired'))).toBe(false);
    expect(logs.some((l) => l.includes('Outbox dispatcher started'))).toBe(false);

    scheduler.stop();
  });
});
