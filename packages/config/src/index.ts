import { z } from 'zod';
import type { AppConfig, LogLevel, NodeEnvironment } from '@forge/contracts';
import {
  DEFAULT_FAIRNESS_AGING_INTERVAL_MS,
  MIN_FAIRNESS_AGING_INTERVAL_MS,
  DEFAULT_FAIRNESS_AGE_BONUS_STEP,
  MIN_FAIRNESS_AGE_BONUS_STEP,
  DEFAULT_FAIRNESS_MAX_AGE_BONUS,
  MAX_FAIRNESS_AGE_BONUS_LIMIT,
  DEFAULT_OUTBOX_DISPATCH_POLL_INTERVAL_MS,
  DEFAULT_OUTBOX_DISPATCH_BATCH_SIZE,
  DEFAULT_OUTBOX_CLAIM_TIMEOUT_MS,
  DEFAULT_OUTBOX_PUBLISH_TIMEOUT_MS,
  DEFAULT_OUTBOX_MAX_DELIVERY_ATTEMPTS,
  MIN_OUTBOX_MAX_DELIVERY_ATTEMPTS,
  MAX_OUTBOX_MAX_DELIVERY_ATTEMPTS,
  DEFAULT_OUTBOX_DELIVERY_BASE_BACKOFF_MS,
  DEFAULT_OUTBOX_DELIVERY_MAX_BACKOFF_MS,
  DEFAULT_OUTBOX_MAX_PAYLOAD_BYTES,
  DEFAULT_OUTBOX_RETENTION_MAX_AGE_MS,
  DEFAULT_OUTBOX_RETENTION_BATCH_SIZE,
  DEFAULT_OUTBOX_RETENTION_EVERY_N_TICKS,
} from '@forge/contracts';

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
    DEFAULT_DOCKER_IMAGE: z.string().default('alpine:3.19'),
    DEFAULT_EXECUTION_TIMEOUT_MS: z
      .string()
      .regex(/^\d+$/, 'DEFAULT_EXECUTION_TIMEOUT_MS must be a valid integer')
      .default('60000')
      .transform(Number)
      .pipe(z.number().int().min(1000, 'DEFAULT_EXECUTION_TIMEOUT_MS must be at least 1000ms')),
    MAX_EXECUTION_TIMEOUT_MS: z
      .string()
      .regex(/^\d+$/, 'MAX_EXECUTION_TIMEOUT_MS must be a valid integer')
      .default('1800000')
      .transform(Number)
      .pipe(z.number().int().min(1000, 'MAX_EXECUTION_TIMEOUT_MS must be at least 1000ms')),
    MAX_OUTPUT_BYTES: z
      .string()
      .regex(/^\d+$/, 'MAX_OUTPUT_BYTES must be a valid integer')
      .default('1048576')
      .transform(Number)
      .pipe(z.number().int().min(1024, 'MAX_OUTPUT_BYTES must be at least 1024 bytes')),
    DOCKER_HOST: z.string().optional(),
    DEFAULT_MAX_ATTEMPTS: z
      .string()
      .regex(/^\d+$/, 'DEFAULT_MAX_ATTEMPTS must be a valid integer')
      .default('1')
      .transform(Number)
      .pipe(z.number().int().min(1, 'DEFAULT_MAX_ATTEMPTS must be at least 1')),
    MAX_JOB_ATTEMPTS: z
      .string()
      .regex(/^\d+$/, 'MAX_JOB_ATTEMPTS must be a valid integer')
      .default('10')
      .transform(Number)
      .pipe(
        z
          .number()
          .int()
          .min(1, 'MAX_JOB_ATTEMPTS must be at least 1')
          .max(100, 'MAX_JOB_ATTEMPTS cannot exceed 100'),
      ),
    DEFAULT_RETRY_BASE_DELAY_MS: z
      .string()
      .regex(/^\d+$/, 'DEFAULT_RETRY_BASE_DELAY_MS must be a valid integer')
      .default('1000')
      .transform(Number)
      .pipe(z.number().int().min(100, 'DEFAULT_RETRY_BASE_DELAY_MS must be at least 100ms')),
    MAX_RETRY_BACKOFF_MS: z
      .string()
      .regex(/^\d+$/, 'MAX_RETRY_BACKOFF_MS must be a valid integer')
      .default('3600000')
      .transform(Number)
      .pipe(z.number().int().min(1000, 'MAX_RETRY_BACKOFF_MS must be at least 1000ms')),
    FAIRNESS_AGING_INTERVAL_MS: z
      .string()
      .regex(/^\d+$/, 'FAIRNESS_AGING_INTERVAL_MS must be a valid integer')
      .default(String(DEFAULT_FAIRNESS_AGING_INTERVAL_MS))
      .transform(Number)
      .pipe(
        z
          .number()
          .int()
          .min(
            MIN_FAIRNESS_AGING_INTERVAL_MS,
            `FAIRNESS_AGING_INTERVAL_MS must be at least ${MIN_FAIRNESS_AGING_INTERVAL_MS}ms`,
          ),
      ),
    FAIRNESS_AGE_BONUS_STEP: z
      .string()
      .regex(/^\d+$/, 'FAIRNESS_AGE_BONUS_STEP must be a valid integer')
      .default(String(DEFAULT_FAIRNESS_AGE_BONUS_STEP))
      .transform(Number)
      .pipe(
        z
          .number()
          .int()
          .min(
            MIN_FAIRNESS_AGE_BONUS_STEP,
            `FAIRNESS_AGE_BONUS_STEP must be at least ${MIN_FAIRNESS_AGE_BONUS_STEP}`,
          ),
      ),
    FAIRNESS_MAX_AGE_BONUS: z
      .string()
      .regex(/^\d+$/, 'FAIRNESS_MAX_AGE_BONUS must be a valid integer')
      .default(String(DEFAULT_FAIRNESS_MAX_AGE_BONUS))
      .transform(Number)
      .pipe(
        z
          .number()
          .int()
          .min(0, 'FAIRNESS_MAX_AGE_BONUS must be non-negative')
          .max(
            MAX_FAIRNESS_AGE_BONUS_LIMIT,
            `FAIRNESS_MAX_AGE_BONUS cannot exceed ${MAX_FAIRNESS_AGE_BONUS_LIMIT}`,
          ),
      ),
    OUTBOX_DISPATCH_POLL_INTERVAL_MS: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_DISPATCH_POLL_INTERVAL_MS must be a valid integer')
      .default(String(DEFAULT_OUTBOX_DISPATCH_POLL_INTERVAL_MS))
      .transform(Number)
      .pipe(z.number().int().min(100, 'OUTBOX_DISPATCH_POLL_INTERVAL_MS must be at least 100ms')),
    OUTBOX_DISPATCH_BATCH_SIZE: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_DISPATCH_BATCH_SIZE must be a valid integer')
      .default(String(DEFAULT_OUTBOX_DISPATCH_BATCH_SIZE))
      .transform(Number)
      .pipe(
        z
          .number()
          .int()
          .min(1, 'OUTBOX_DISPATCH_BATCH_SIZE must be at least 1')
          .max(1000, 'OUTBOX_DISPATCH_BATCH_SIZE cannot exceed 1000'),
      ),
    OUTBOX_CLAIM_TIMEOUT_MS: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_CLAIM_TIMEOUT_MS must be a valid integer')
      .default(String(DEFAULT_OUTBOX_CLAIM_TIMEOUT_MS))
      .transform(Number)
      .pipe(z.number().int().min(1000, 'OUTBOX_CLAIM_TIMEOUT_MS must be at least 1000ms')),
    OUTBOX_PUBLISH_TIMEOUT_MS: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_PUBLISH_TIMEOUT_MS must be a valid integer')
      .default(String(DEFAULT_OUTBOX_PUBLISH_TIMEOUT_MS))
      .transform(Number)
      .pipe(z.number().int().min(500, 'OUTBOX_PUBLISH_TIMEOUT_MS must be at least 500ms')),
    OUTBOX_MAX_DELIVERY_ATTEMPTS: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_MAX_DELIVERY_ATTEMPTS must be a valid integer')
      .default(String(DEFAULT_OUTBOX_MAX_DELIVERY_ATTEMPTS))
      .transform(Number)
      .pipe(
        z
          .number()
          .int()
          .min(
            MIN_OUTBOX_MAX_DELIVERY_ATTEMPTS,
            `OUTBOX_MAX_DELIVERY_ATTEMPTS must be at least ${MIN_OUTBOX_MAX_DELIVERY_ATTEMPTS}`,
          )
          .max(
            MAX_OUTBOX_MAX_DELIVERY_ATTEMPTS,
            `OUTBOX_MAX_DELIVERY_ATTEMPTS cannot exceed ${MAX_OUTBOX_MAX_DELIVERY_ATTEMPTS}`,
          ),
      ),
    OUTBOX_DELIVERY_BASE_BACKOFF_MS: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_DELIVERY_BASE_BACKOFF_MS must be a valid integer')
      .default(String(DEFAULT_OUTBOX_DELIVERY_BASE_BACKOFF_MS))
      .transform(Number)
      .pipe(z.number().int().min(50, 'OUTBOX_DELIVERY_BASE_BACKOFF_MS must be at least 50ms')),
    OUTBOX_DELIVERY_MAX_BACKOFF_MS: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_DELIVERY_MAX_BACKOFF_MS must be a valid integer')
      .default(String(DEFAULT_OUTBOX_DELIVERY_MAX_BACKOFF_MS))
      .transform(Number)
      .pipe(z.number().int().min(1000, 'OUTBOX_DELIVERY_MAX_BACKOFF_MS must be at least 1000ms')),
    OUTBOX_MAX_PAYLOAD_BYTES: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_MAX_PAYLOAD_BYTES must be a valid integer')
      .default(String(DEFAULT_OUTBOX_MAX_PAYLOAD_BYTES))
      .transform(Number)
      .pipe(z.number().int().min(1024, 'OUTBOX_MAX_PAYLOAD_BYTES must be at least 1024 bytes')),
    OUTBOX_RETENTION_MAX_AGE_MS: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_RETENTION_MAX_AGE_MS must be a valid integer')
      .default(String(DEFAULT_OUTBOX_RETENTION_MAX_AGE_MS))
      .transform(Number)
      .pipe(z.number().int().min(0, 'OUTBOX_RETENTION_MAX_AGE_MS must be non-negative')),
    OUTBOX_RETENTION_BATCH_SIZE: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_RETENTION_BATCH_SIZE must be a valid integer')
      .default(String(DEFAULT_OUTBOX_RETENTION_BATCH_SIZE))
      .transform(Number)
      .pipe(z.number().int().min(1, 'OUTBOX_RETENTION_BATCH_SIZE must be at least 1')),
    OUTBOX_RETENTION_EVERY_N_TICKS: z
      .string()
      .regex(/^\d+$/, 'OUTBOX_RETENTION_EVERY_N_TICKS must be a valid integer')
      .default(String(DEFAULT_OUTBOX_RETENTION_EVERY_N_TICKS))
      .transform(Number)
      .pipe(z.number().int().min(1, 'OUTBOX_RETENTION_EVERY_N_TICKS must be at least 1')),
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
  })
  .refine((data) => data.MAX_EXECUTION_TIMEOUT_MS >= data.DEFAULT_EXECUTION_TIMEOUT_MS, {
    message:
      'MAX_EXECUTION_TIMEOUT_MS must be greater than or equal to DEFAULT_EXECUTION_TIMEOUT_MS',
    path: ['MAX_EXECUTION_TIMEOUT_MS'],
  })
  .refine((data) => data.MAX_JOB_ATTEMPTS >= data.DEFAULT_MAX_ATTEMPTS, {
    message: 'MAX_JOB_ATTEMPTS must be greater than or equal to DEFAULT_MAX_ATTEMPTS',
    path: ['MAX_JOB_ATTEMPTS'],
  })
  .refine((data) => data.MAX_RETRY_BACKOFF_MS >= data.DEFAULT_RETRY_BASE_DELAY_MS, {
    message: 'MAX_RETRY_BACKOFF_MS must be greater than or equal to DEFAULT_RETRY_BASE_DELAY_MS',
    path: ['MAX_RETRY_BACKOFF_MS'],
  })
  .refine((data) => data.FAIRNESS_MAX_AGE_BONUS >= data.FAIRNESS_AGE_BONUS_STEP, {
    message: 'FAIRNESS_MAX_AGE_BONUS must be greater than or equal to FAIRNESS_AGE_BONUS_STEP',
    path: ['FAIRNESS_MAX_AGE_BONUS'],
  })
  .refine(
    (d) =>
      d.OUTBOX_CLAIM_TIMEOUT_MS >= d.OUTBOX_PUBLISH_TIMEOUT_MS + d.OUTBOX_DISPATCH_POLL_INTERVAL_MS,
    {
      message:
        'OUTBOX_CLAIM_TIMEOUT_MS must be >= OUTBOX_PUBLISH_TIMEOUT_MS + OUTBOX_DISPATCH_POLL_INTERVAL_MS',
      path: ['OUTBOX_CLAIM_TIMEOUT_MS'],
    },
  )
  .refine((d) => d.OUTBOX_DELIVERY_MAX_BACKOFF_MS >= d.OUTBOX_DELIVERY_BASE_BACKOFF_MS, {
    message: 'OUTBOX_DELIVERY_MAX_BACKOFF_MS must be >= OUTBOX_DELIVERY_BASE_BACKOFF_MS',
    path: ['OUTBOX_DELIVERY_MAX_BACKOFF_MS'],
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
    DEFAULT_DOCKER_IMAGE,
    DEFAULT_EXECUTION_TIMEOUT_MS,
    MAX_EXECUTION_TIMEOUT_MS,
    MAX_OUTPUT_BYTES,
    DOCKER_HOST,
    DEFAULT_MAX_ATTEMPTS,
    MAX_JOB_ATTEMPTS,
    DEFAULT_RETRY_BASE_DELAY_MS,
    MAX_RETRY_BACKOFF_MS,
    FAIRNESS_AGING_INTERVAL_MS,
    FAIRNESS_AGE_BONUS_STEP,
    FAIRNESS_MAX_AGE_BONUS,
    OUTBOX_DISPATCH_POLL_INTERVAL_MS,
    OUTBOX_DISPATCH_BATCH_SIZE,
    OUTBOX_CLAIM_TIMEOUT_MS,
    OUTBOX_PUBLISH_TIMEOUT_MS,
    OUTBOX_MAX_DELIVERY_ATTEMPTS,
    OUTBOX_DELIVERY_BASE_BACKOFF_MS,
    OUTBOX_DELIVERY_MAX_BACKOFF_MS,
    OUTBOX_MAX_PAYLOAD_BYTES,
    OUTBOX_RETENTION_MAX_AGE_MS,
    OUTBOX_RETENTION_BATCH_SIZE,
    OUTBOX_RETENTION_EVERY_N_TICKS,
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
    defaultDockerImage: DEFAULT_DOCKER_IMAGE,
    defaultExecutionTimeoutMs: DEFAULT_EXECUTION_TIMEOUT_MS,
    maxExecutionTimeoutMs: MAX_EXECUTION_TIMEOUT_MS,
    maxOutputBytes: MAX_OUTPUT_BYTES,
    dockerHost: DOCKER_HOST,
    defaultMaxAttempts: DEFAULT_MAX_ATTEMPTS,
    maxJobAttempts: MAX_JOB_ATTEMPTS,
    defaultRetryBaseDelayMs: DEFAULT_RETRY_BASE_DELAY_MS,
    maxRetryBackoffMs: MAX_RETRY_BACKOFF_MS,
    fairnessAgingIntervalMs: FAIRNESS_AGING_INTERVAL_MS,
    fairnessAgeBonusStep: FAIRNESS_AGE_BONUS_STEP,
    fairnessMaxAgeBonus: FAIRNESS_MAX_AGE_BONUS,
    outboxDispatchPollIntervalMs: OUTBOX_DISPATCH_POLL_INTERVAL_MS,
    outboxDispatchBatchSize: OUTBOX_DISPATCH_BATCH_SIZE,
    outboxClaimTimeoutMs: OUTBOX_CLAIM_TIMEOUT_MS,
    outboxPublishTimeoutMs: OUTBOX_PUBLISH_TIMEOUT_MS,
    outboxMaxDeliveryAttempts: OUTBOX_MAX_DELIVERY_ATTEMPTS,
    outboxDeliveryBaseBackoffMs: OUTBOX_DELIVERY_BASE_BACKOFF_MS,
    outboxDeliveryMaxBackoffMs: OUTBOX_DELIVERY_MAX_BACKOFF_MS,
    outboxMaxPayloadBytes: OUTBOX_MAX_PAYLOAD_BYTES,
    outboxRetentionMaxAgeMs: OUTBOX_RETENTION_MAX_AGE_MS,
    outboxRetentionBatchSize: OUTBOX_RETENTION_BATCH_SIZE,
    outboxRetentionEveryNTicks: OUTBOX_RETENTION_EVERY_N_TICKS,
  };
}
