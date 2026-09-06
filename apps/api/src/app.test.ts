import { describe, it, expect } from 'vitest';
import request from 'supertest';
import { createApp } from './app.js';
import { createLogger } from '@forge/logging';

describe('API Shell - /health endpoint', () => {
  it('returns 200 OK with deterministic health status payload', async () => {
    const logger = createLogger({ service: 'api', minLevel: 'error' });
    const app = createApp({ logger, version: '0.1.0' });

    const res = await request(app).get('/health');

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      status: 'ok',
      service: 'api',
      version: '0.1.0',
    });
    expect(typeof res.body.timestamp).toBe('string');
    expect(typeof res.body.uptime).toBe('number');
  });

  it('returns 404 for unmapped routes', async () => {
    const logger = createLogger({ service: 'api', minLevel: 'error' });
    const app = createApp({ logger });

    const res = await request(app).get('/unknown');

    expect(res.status).toBe(404);
    expect(res.body).toHaveProperty('error', 'Not Found');
  });
});
