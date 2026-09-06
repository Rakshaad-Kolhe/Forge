import { describe, it, expect, vi } from 'vitest';
import { createLogger, StructuredLogger } from './index.js';

describe('StructuredLogger', () => {
  it('formats human-readable logs in development mode', () => {
    const outputs: string[] = [];
    const logger = createLogger({
      service: 'api',
      environment: 'development',
      minLevel: 'info',
      writeFn: (msg) => outputs.push(msg),
    });

    logger.info('Server started', { port: 3000 });

    expect(outputs).toHaveLength(1);
    expect(outputs[0]).toMatch(/INFO \[api\]: Server started {"port":3000}/);
  });

  it('formats machine-readable JSON logs in production mode', () => {
    const outputs: string[] = [];
    const logger = createLogger({
      service: 'worker',
      environment: 'production',
      minLevel: 'info',
      writeFn: (msg) => outputs.push(msg),
    });

    logger.info('Worker initialized', { request_id: 'req-123', worker_id: 'w-1' });

    expect(outputs).toHaveLength(1);
    const parsed = JSON.parse(outputs[0]!);
    expect(parsed.service).toBe('worker');
    expect(parsed.level).toBe('info');
    expect(parsed.environment).toBe('production');
    expect(parsed.message).toBe('Worker initialized');
    expect(parsed.request_id).toBe('req-123');
    expect(parsed.context.worker_id).toBe('w-1');
  });

  it('filters out messages below the configured minimum level', () => {
    const writeFn = vi.fn();
    const logger = new StructuredLogger({
      service: 'scheduler',
      minLevel: 'warn',
      writeFn,
    });

    logger.debug('Debug trace');
    logger.info('Info notice');
    expect(writeFn).not.toHaveBeenCalled();

    logger.warn('Warning notice');
    expect(writeFn).toHaveBeenCalledTimes(1);

    logger.error('Error notice');
    expect(writeFn).toHaveBeenCalledTimes(2);
  });

  it('inherits and merges context across child loggers', () => {
    const outputs: string[] = [];
    const parent = createLogger({
      service: 'api',
      environment: 'production',
      writeFn: (msg) => outputs.push(msg),
    });

    const child = parent.child({ request_id: 'trace-abc', trace: true });
    child.info('Handling route', { path: '/health' });

    expect(outputs).toHaveLength(1);
    const parsed = JSON.parse(outputs[0]!);
    expect(parsed.request_id).toBe('trace-abc');
    expect(parsed.context.trace).toBe(true);
    expect(parsed.context.path).toBe('/health');
  });
});
