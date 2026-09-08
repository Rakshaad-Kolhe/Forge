import { describe, it, expect } from 'vitest';
import { loadConfig, ConfigValidationError } from './index.js';

describe('loadConfig', () => {
  it('loads valid default configuration when environment is empty', () => {
    const config = loadConfig({});
    expect(config).toEqual({
      nodeEnv: 'development',
      logLevel: 'info',
      apiPort: 3000,
      databaseUrl: 'postgresql://forge:forge@127.0.0.1:5432/forge',
      redisUrl: 'redis://127.0.0.1:6379',
      workerHeartbeatIntervalMs: 5000,
      workerHeartbeatTtlSeconds: 15,
      workerJobLeaseDurationMs: 30000,
      workerJobLeaseRenewalIntervalMs: 10000,
      defaultDockerImage: 'alpine:3.19',
      defaultExecutionTimeoutMs: 60000,
      maxExecutionTimeoutMs: 1800000,
      maxOutputBytes: 1048576,
      dockerHost: undefined,
      defaultMaxAttempts: 1,
      maxJobAttempts: 10,
      defaultRetryBaseDelayMs: 1000,
      maxRetryBackoffMs: 3600000,
      fairnessAgingIntervalMs: 60000,
      fairnessAgeBonusStep: 10,
      fairnessMaxAgeBonus: 500,
      outboxDispatchPollIntervalMs: 1000,
      outboxDispatchBatchSize: 100,
      outboxClaimTimeoutMs: 60000,
      outboxPublishTimeoutMs: 10000,
      outboxMaxDeliveryAttempts: 10,
      outboxDeliveryBaseBackoffMs: 500,
      outboxDeliveryMaxBackoffMs: 60000,
      outboxMaxPayloadBytes: 65536,
      outboxRetentionMaxAgeMs: 604800000,
      outboxRetentionBatchSize: 500,
      outboxRetentionEveryNTicks: 60,
    });
  });

  it('loads valid configuration when valid overrides are provided', () => {
    const config = loadConfig({
      NODE_ENV: 'production',
      LOG_LEVEL: 'warn',
      API_PORT: '8080',
      DATABASE_URL: 'postgresql://custom:custom@localhost:5433/custom_db',
      REDIS_URL: 'redis://custom:custom@localhost:6380',
      WORKER_HEARTBEAT_INTERVAL_MS: '2000',
      WORKER_HEARTBEAT_TTL_SECONDS: '10',
      WORKER_JOB_LEASE_DURATION_MS: '45000',
      WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS: '15000',
      DEFAULT_DOCKER_IMAGE: 'node:20-alpine',
      DEFAULT_EXECUTION_TIMEOUT_MS: '120000',
      MAX_EXECUTION_TIMEOUT_MS: '600000',
      MAX_OUTPUT_BYTES: '2097152',
      DOCKER_HOST: 'tcp://127.0.0.1:2375',
      DEFAULT_MAX_ATTEMPTS: '3',
      MAX_JOB_ATTEMPTS: '20',
      DEFAULT_RETRY_BASE_DELAY_MS: '2000',
      MAX_RETRY_BACKOFF_MS: '1800000',
      FAIRNESS_AGING_INTERVAL_MS: '30000',
      FAIRNESS_AGE_BONUS_STEP: '25',
      FAIRNESS_MAX_AGE_BONUS: '1000',
    });
    expect(config).toEqual({
      nodeEnv: 'production',
      logLevel: 'warn',
      apiPort: 8080,
      databaseUrl: 'postgresql://custom:custom@localhost:5433/custom_db',
      redisUrl: 'redis://custom:custom@localhost:6380',
      workerHeartbeatIntervalMs: 2000,
      workerHeartbeatTtlSeconds: 10,
      workerJobLeaseDurationMs: 45000,
      workerJobLeaseRenewalIntervalMs: 15000,
      defaultDockerImage: 'node:20-alpine',
      defaultExecutionTimeoutMs: 120000,
      maxExecutionTimeoutMs: 600000,
      maxOutputBytes: 2097152,
      dockerHost: 'tcp://127.0.0.1:2375',
      defaultMaxAttempts: 3,
      maxJobAttempts: 20,
      defaultRetryBaseDelayMs: 2000,
      maxRetryBackoffMs: 1800000,
      fairnessAgingIntervalMs: 30000,
      fairnessAgeBonusStep: 25,
      fairnessMaxAgeBonus: 1000,
      outboxDispatchPollIntervalMs: 1000,
      outboxDispatchBatchSize: 100,
      outboxClaimTimeoutMs: 60000,
      outboxPublishTimeoutMs: 10000,
      outboxMaxDeliveryAttempts: 10,
      outboxDeliveryBaseBackoffMs: 500,
      outboxDeliveryMaxBackoffMs: 60000,
      outboxMaxPayloadBytes: 65536,
      outboxRetentionMaxAgeMs: 604800000,
      outboxRetentionBatchSize: 500,
      outboxRetentionEveryNTicks: 60,
    });
  });

  it('fails clearly when NODE_ENV is invalid', () => {
    expect(() => {
      loadConfig({
        NODE_ENV: 'staging' as unknown as string,
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when LOG_LEVEL is invalid', () => {
    expect(() => {
      loadConfig({
        LOG_LEVEL: 'verbose',
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when API_PORT is not a number', () => {
    expect(() => {
      loadConfig({
        API_PORT: 'not-a-number',
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when API_PORT is out of range', () => {
    expect(() => {
      loadConfig({
        API_PORT: '70000',
      });
    }).toThrow(ConfigValidationError);

    expect(() => {
      loadConfig({
        API_PORT: '0',
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when WORKER_HEARTBEAT_TTL_SECONDS is less than or equal to interval', () => {
    expect(() => {
      loadConfig({
        WORKER_HEARTBEAT_INTERVAL_MS: '10000', // 10s
        WORKER_HEARTBEAT_TTL_SECONDS: '5', // 5s < 10s
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when WORKER_JOB_LEASE_DURATION_MS is less than or equal to renewal interval', () => {
    expect(() => {
      loadConfig({
        WORKER_JOB_LEASE_DURATION_MS: '10000',
        WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS: '15000', // 15s > 10s
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when MAX_EXECUTION_TIMEOUT_MS is less than DEFAULT_EXECUTION_TIMEOUT_MS', () => {
    expect(() => {
      loadConfig({
        DEFAULT_EXECUTION_TIMEOUT_MS: '120000',
        MAX_EXECUTION_TIMEOUT_MS: '60000', // 60s < 120s
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when MAX_JOB_ATTEMPTS is less than DEFAULT_MAX_ATTEMPTS', () => {
    expect(() => {
      loadConfig({
        DEFAULT_MAX_ATTEMPTS: '5',
        MAX_JOB_ATTEMPTS: '2',
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when MAX_RETRY_BACKOFF_MS is less than DEFAULT_RETRY_BASE_DELAY_MS', () => {
    expect(() => {
      loadConfig({
        DEFAULT_RETRY_BASE_DELAY_MS: '5000',
        MAX_RETRY_BACKOFF_MS: '2000',
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when FAIRNESS_AGING_INTERVAL_MS is less than minimum', () => {
    expect(() => {
      loadConfig({
        FAIRNESS_AGING_INTERVAL_MS: '500', // < 1000ms
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when FAIRNESS_AGE_BONUS_STEP is less than 1', () => {
    expect(() => {
      loadConfig({
        FAIRNESS_AGE_BONUS_STEP: '0',
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when FAIRNESS_MAX_AGE_BONUS exceeds limit', () => {
    expect(() => {
      loadConfig({
        FAIRNESS_MAX_AGE_BONUS: '3000', // > 2000
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when FAIRNESS_MAX_AGE_BONUS is less than FAIRNESS_AGE_BONUS_STEP', () => {
    expect(() => {
      loadConfig({
        FAIRNESS_AGE_BONUS_STEP: '100',
        FAIRNESS_MAX_AGE_BONUS: '50',
      });
    }).toThrow(ConfigValidationError);
  });

  describe('outbox configuration (PR 21)', () => {
    it('applies conservative outbox defaults', () => {
      const cfg = loadConfig({});
      expect(cfg.outboxDispatchPollIntervalMs).toBe(1000);
      expect(cfg.outboxDispatchBatchSize).toBe(100);
      expect(cfg.outboxClaimTimeoutMs).toBe(60000);
      expect(cfg.outboxPublishTimeoutMs).toBe(10000);
      expect(cfg.outboxMaxDeliveryAttempts).toBe(10);
      expect(cfg.outboxDeliveryBaseBackoffMs).toBe(500);
      expect(cfg.outboxDeliveryMaxBackoffMs).toBe(60000);
      expect(cfg.outboxMaxPayloadBytes).toBe(65536);
      expect(cfg.outboxRetentionMaxAgeMs).toBe(604800000);
      expect(cfg.outboxRetentionBatchSize).toBe(500);
      expect(cfg.outboxRetentionEveryNTicks).toBe(60);
    });

    it('parses overrides', () => {
      const cfg = loadConfig({
        OUTBOX_DISPATCH_BATCH_SIZE: '250',
        OUTBOX_RETENTION_MAX_AGE_MS: '0',
      });
      expect(cfg.outboxDispatchBatchSize).toBe(250);
      expect(cfg.outboxRetentionMaxAgeMs).toBe(0);
    });

    it('rejects claim timeout below publish timeout + poll interval', () => {
      expect(() =>
        loadConfig({
          OUTBOX_CLAIM_TIMEOUT_MS: '5000',
          OUTBOX_PUBLISH_TIMEOUT_MS: '10000',
          OUTBOX_DISPATCH_POLL_INTERVAL_MS: '1000',
        }),
      ).toThrow(/OUTBOX_CLAIM_TIMEOUT_MS/);
    });

    it('rejects max backoff below base backoff', () => {
      expect(() =>
        loadConfig({
          OUTBOX_DELIVERY_BASE_BACKOFF_MS: '5000',
          OUTBOX_DELIVERY_MAX_BACKOFF_MS: '1000',
        }),
      ).toThrow(/OUTBOX_DELIVERY_MAX_BACKOFF_MS/);
    });

    it('rejects batch size over 1000 and delivery attempts over 100', () => {
      expect(() => loadConfig({ OUTBOX_DISPATCH_BATCH_SIZE: '5000' })).toThrow(
        /OUTBOX_DISPATCH_BATCH_SIZE/,
      );
      expect(() => loadConfig({ OUTBOX_MAX_DELIVERY_ATTEMPTS: '500' })).toThrow(
        /OUTBOX_MAX_DELIVERY_ATTEMPTS/,
      );
    });
  });
});
