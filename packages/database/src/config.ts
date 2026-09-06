import type { DatabaseConfig } from './types.js';

export const DEFAULT_DATABASE_URL = 'postgresql://forge:forge@127.0.0.1:5432/forge';

/**
 * Resolves database configuration from an optional connection string or environment variables.
 */
export function resolveDatabaseConfig(connectionString?: string): DatabaseConfig {
  const url = connectionString ?? process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL;

  return {
    connectionString: url,
    maxConnections: process.env.DATABASE_MAX_CONNECTIONS
      ? parseInt(process.env.DATABASE_MAX_CONNECTIONS, 10)
      : 10,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 5000,
  };
}
