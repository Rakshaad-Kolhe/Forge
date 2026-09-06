import { describe, it, expect } from 'vitest';
import { startWorker } from './index.js';
import { createLogger } from '@forge/logging';

describe('Worker Service Shell', () => {
  it('starts successfully and logs startup event', () => {
    const logs: string[] = [];
    const testLogger = createLogger({
      service: 'worker',
      environment: 'development',
      writeFn: (msg) => logs.push(msg),
    });

    const worker = startWorker({ logger: testLogger });
    expect(logs.some((l) => l.includes('Forge Worker service shell started'))).toBe(true);

    worker.stop();
    expect(logs.some((l) => l.includes('Forge Worker service shell stopped'))).toBe(true);
  });
});
