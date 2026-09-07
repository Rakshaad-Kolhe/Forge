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
});
