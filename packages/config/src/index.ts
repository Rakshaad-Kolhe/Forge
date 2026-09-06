import { z } from 'zod';
import type { AppConfig, LogLevel, NodeEnvironment } from '@forge/contracts';

/**
 * Custom error class thrown when configuration validation fails.
 */
export class ConfigValidationError extends Error {
  public readonly issues: z.ZodIssue[];

  constructor(message: string, issues: z.ZodIssue[]) {
    super(message);
    this.name = 'ConfigValidationError';
    this.issues = issues;
  }
}

const configSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
  API_PORT: z
    .string()
    .regex(/^\d+$/, 'API_PORT must be a valid integer')
    .default('3000')
    .transform(Number)
    .pipe(
      z
        .number()
        .int()
        .min(1, 'API_PORT must be at least 1')
        .max(65535, 'API_PORT cannot exceed 65535'),
    ),
  DATABASE_URL: z.string().default('postgresql://forge:forge@127.0.0.1:5432/forge'),
  REDIS_URL: z.string().default('redis://127.0.0.1:6379'),
});

export type RawConfigInput = Record<string, string | undefined>;

/**
 * Parses and validates environment variables into a strongly-typed AppConfig.
 *
 * @param env - Source environment dictionary (defaults to process.env)
 * @throws {ConfigValidationError} When required configuration is invalid
 */
export function loadConfig(env: RawConfigInput = process.env): AppConfig {
  const result = configSchema.safeParse(env);

  if (!result.success) {
    const formattedErrors = result.error.issues
      .map((issue) => `  - ${issue.path.join('.') || 'root'}: ${issue.message}`)
      .join('\n');

    throw new ConfigValidationError(
      `Configuration validation failed:\n${formattedErrors}`,
      result.error.issues,
    );
  }

  const { NODE_ENV, LOG_LEVEL, API_PORT, DATABASE_URL, REDIS_URL } = result.data;

  return {
    nodeEnv: NODE_ENV as NodeEnvironment,
    logLevel: LOG_LEVEL as LogLevel,
    apiPort: API_PORT,
    databaseUrl: DATABASE_URL,
    redisUrl: REDIS_URL,
  };
}
