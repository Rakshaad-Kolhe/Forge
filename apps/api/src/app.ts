import express, { type Express, type Request, type Response } from 'express';
import type { HealthResponse } from '@forge/contracts';
import { type Logger } from '@forge/logging';

export interface CreateAppOptions {
  logger: Logger;
  version?: string;
}

/**
 * Factory creating the Express API application shell.
 * Exposes only the foundational /health endpoint for PR 01.
 */
export function createApp(options: CreateAppOptions): Express {
  const app = express();
  const { logger, version = '0.1.0' } = options;

  app.use(express.json());

  // Foundational health endpoint
  app.get('/health', (_req: Request, res: Response) => {
    const payload: HealthResponse = {
      status: 'ok',
      service: 'api',
      timestamp: new Date().toISOString(),
      version,
      uptime: Math.floor(process.uptime()),
    };

    res.status(200).json(payload);
  });

  // Catch-all 404 handler for undefined routes
  app.use((req: Request, res: Response) => {
    logger.warn('Route not found', { path: req.path, method: req.method });
    res.status(404).json({
      error: 'Not Found',
      message: `Cannot ${req.method} ${req.path}`,
    });
  });

  return app;
}
