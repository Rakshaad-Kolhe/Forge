import { loadConfig } from '@forge/config';
import { createLogger } from '@forge/logging';
import { createApp } from './app.js';

const config = loadConfig();
const logger = createLogger({
  service: 'api',
  environment: config.nodeEnv,
  minLevel: config.logLevel,
});

const app = createApp({ logger });

const server = app.listen(config.apiPort, () => {
  logger.info('Forge API server started', {
    port: config.apiPort,
    environment: config.nodeEnv,
    logLevel: config.logLevel,
  });
});

function shutdown(signal: string): void {
  logger.info(`Received ${signal}, shutting down API gracefully`);
  server.close(() => {
    logger.info('API server closed');
    process.exit(0);
  });
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
