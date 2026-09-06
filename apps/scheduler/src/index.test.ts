import { describe, it, expect } from 'vitest';
import { startScheduler } from './index.js';
import { createLogger } from '@forge/logging';

describe('Scheduler Service Shell', () => {
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
});
