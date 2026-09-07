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

const configSchema = z
  .object({
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
    WORKER_HEARTBEAT_INTERVAL_MS: z
      .string()
      .regex(/^\d+$/, 'WORKER_HEARTBEAT_INTERVAL_MS must be a valid integer')
      .default('5000')
      .transform(Number)
      .pipe(z.number().int().min(100, 'WORKER_HEARTBEAT_INTERVAL_MS must be at least 100ms')),
    WORKER_HEARTBEAT_TTL_SECONDS: z
      .string()
      .regex(/^\d+$/, 'WORKER_HEARTBEAT_TTL_SECONDS must be a valid integer')
      .default('15')
      .transform(Number)
      .pipe(z.number().int().min(1, 'WORKER_HEARTBEAT_TTL_SECONDS must be at least 1 second')),
    WORKER_JOB_LEASE_DURATION_MS: z
      .string()
      .regex(/^\d+$/, 'WORKER_JOB_LEASE_DURATION_MS must be a valid integer')
      .default('30000')
      .transform(Number)
      .pipe(z.number().int().min(1000, 'WORKER_JOB_LEASE_DURATION_MS must be at least 1000ms')),
    WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS: z
      .string()
      .regex(/^\d+$/, 'WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS must be a valid integer')
      .default('10000')
      .transform(Number)
      .pipe(
        z.number().int().min(500, 'WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS must be at least 500ms'),
      ),
  })
  .refine((data) => data.WORKER_HEARTBEAT_TTL_SECONDS * 1000 > data.WORKER_HEARTBEAT_INTERVAL_MS, {
    message:
      'WORKER_HEARTBEAT_TTL_SECONDS (in ms) must be greater than WORKER_HEARTBEAT_INTERVAL_MS',
    path: ['WORKER_HEARTBEAT_TTL_SECONDS'],
  })
  .refine((data) => data.WORKER_JOB_LEASE_DURATION_MS > data.WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS, {
    message:
      'WORKER_JOB_LEASE_DURATION_MS must be greater than WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS',
    path: ['WORKER_JOB_LEASE_DURATION_MS'],
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

  const {
    NODE_ENV,
    LOG_LEVEL,
    API_PORT,
    DATABASE_URL,
    REDIS_URL,
    WORKER_HEARTBEAT_INTERVAL_MS,
    WORKER_HEARTBEAT_TTL_SECONDS,
    WORKER_JOB_LEASE_DURATION_MS,
    WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS,
  } = result.data;

  return {
    nodeEnv: NODE_ENV as NodeEnvironment,
    logLevel: LOG_LEVEL as LogLevel,
    apiPort: API_PORT,
    databaseUrl: DATABASE_URL,
    redisUrl: REDIS_URL,
    workerHeartbeatIntervalMs: WORKER_HEARTBEAT_INTERVAL_MS,
    workerHeartbeatTtlSeconds: WORKER_HEARTBEAT_TTL_SECONDS,
    workerJobLeaseDurationMs: WORKER_JOB_LEASE_DURATION_MS,
    workerJobLeaseRenewalIntervalMs: WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS,
  };
}
