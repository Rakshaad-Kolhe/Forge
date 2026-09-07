import { describe, expect, it, vi } from 'vitest';
import type { RedisClient } from '@forge/redis';
import {
  QueueError,
  QueueMessageNotFoundError,
  QueueOperationError,
  QueueUnavailableError,
  QueueValidationError,
} from './errors.js';
import { getInFlightKey, getMessagesKey, getMetadataKey, getReadyKey } from './keys.js';
import { createJobQueue } from './queue.js';

describe('JobQueue Unit Tests', () => {
  const createMockRedisClient = (): RedisClient => {
    return {
      status: 'ready',
      connect: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
      healthCheck: vi.fn().mockResolvedValue(true),
      getRawClient: vi.fn(),
      get: vi.fn(),
      set: vi.fn(),
      del: vi.fn(),
      exists: vi.fn(),
      expire: vi.fn(),
      ttl: vi.fn(),
      getJson: vi.fn(),
      setJson: vi.fn(),
      setNx: vi.fn(),
      incr: vi.fn(),
      decr: vi.fn(),
      eval: vi.fn(),
    };
  };

  describe('Queue Key Conventions', () => {
    it('builds standard namespaced keys compliant with @forge/redis conventions', () => {
      expect(getReadyKey('jobs')).toBe('forge:queue:jobs:ready');
      expect(getMessagesKey('jobs')).toBe('forge:queue:jobs:messages');
      expect(getInFlightKey('jobs')).toBe('forge:queue:jobs:in_flight');
      expect(getMetadataKey('jobs')).toBe('forge:queue:jobs:meta');
    });

    it('rejects empty queue names in key generation', () => {
      expect(() => getReadyKey('')).toThrow('Redis key part at index 0 cannot be empty');
      expect(() => getReadyKey('   ')).toThrow('Redis key part at index 0 cannot be empty');
    });
  });

  describe('Queue Construction Options Validation', () => {
    it('throws QueueValidationError on empty queue name', () => {
      const mockClient = createMockRedisClient();
      expect(() => createJobQueue(mockClient, { queueName: '' })).toThrow(QueueValidationError);
      expect(() => createJobQueue(mockClient, { queueName: '   ' })).toThrow(QueueValidationError);
    });

    it('throws QueueValidationError on invalid defaultVisibilityTimeoutSeconds', () => {
      const mockClient = createMockRedisClient();
      expect(() =>
        createJobQueue(mockClient, {
          queueName: 'jobs',
          defaultVisibilityTimeoutSeconds: -5,
        }),
      ).toThrow(QueueValidationError);
      expect(() =>
        createJobQueue(mockClient, {
          queueName: 'jobs',
          defaultVisibilityTimeoutSeconds: 0,
        }),
      ).toThrow(QueueValidationError);
    });

    it('throws QueueValidationError on invalid maxPayloadSizeBytes', () => {
      const mockClient = createMockRedisClient();
      expect(() =>
        createJobQueue(mockClient, {
          queueName: 'jobs',
          maxPayloadSizeBytes: -100,
        }),
      ).toThrow(QueueValidationError);
      expect(() =>
        createJobQueue(mockClient, {
          queueName: 'jobs',
          maxPayloadSizeBytes: 0,
        }),
      ).toThrow(QueueValidationError);
    });
  });

  describe('Enqueue Validation', () => {
    it('rejects empty or missing jobId', async () => {
      const mockClient = createMockRedisClient();
      const queue = createJobQueue(mockClient, { queueName: 'test-q' });

      await expect(
        queue.enqueue({
          jobId: '',
          pipelineRunId: 'run-1',
          stepName: 'build',
        }),
      ).rejects.toThrow(QueueValidationError);

      await expect(
        queue.enqueue({
          jobId: '   ',
          pipelineRunId: 'run-1',
          stepName: 'build',
        }),
      ).rejects.toThrow(QueueValidationError);
    });

    it('rejects empty or missing pipelineRunId', async () => {
      const mockClient = createMockRedisClient();
      const queue = createJobQueue(mockClient, { queueName: 'test-q' });

      await expect(
        queue.enqueue({
          jobId: 'job-1',
          pipelineRunId: '',
          stepName: 'build',
        }),
      ).rejects.toThrow(QueueValidationError);
    });

    it('rejects empty or missing stepName', async () => {
      const mockClient = createMockRedisClient();
      const queue = createJobQueue(mockClient, { queueName: 'test-q' });

      await expect(
        queue.enqueue({
          jobId: 'job-1',
          pipelineRunId: 'run-1',
          stepName: '',
        }),
      ).rejects.toThrow(QueueValidationError);
    });

    it('rejects invalid attemptNumber', async () => {
      const mockClient = createMockRedisClient();
      const queue = createJobQueue(mockClient, { queueName: 'test-q' });

      await expect(
        queue.enqueue({
          jobId: 'job-1',
          pipelineRunId: 'run-1',
          stepName: 'build',
          attemptNumber: 0,
        }),
      ).rejects.toThrow(QueueValidationError);

      await expect(
        queue.enqueue({
          jobId: 'job-1',
          pipelineRunId: 'run-1',
          stepName: 'build',
          attemptNumber: -1,
        }),
      ).rejects.toThrow(QueueValidationError);

      await expect(
        queue.enqueue({
          jobId: 'job-1',
          pipelineRunId: 'run-1',
          stepName: 'build',
          attemptNumber: 1.5,
        }),
      ).rejects.toThrow(QueueValidationError);
    });

    it('rejects payloads exceeding configured maxPayloadSizeBytes', async () => {
      const mockClient = createMockRedisClient();
      const queue = createJobQueue(mockClient, {
        queueName: 'test-q',
        maxPayloadSizeBytes: 50, // very small limit to trigger error
      });

      await expect(
        queue.enqueue({
          jobId: 'job-1234567890',
          pipelineRunId: 'run-1234567890',
          stepName: 'build-step-with-a-very-long-name-that-exceeds-limit',
        }),
      ).rejects.toThrow(QueueValidationError);
    });
  });

  describe('Error Taxonomy', () => {
    it('instantiates all queue error classes with correct inheritance and cause tracking', () => {
      const cause = new Error('Original root cause');
      const baseErr = new QueueError('Base error', cause);
      expect(baseErr).toBeInstanceOf(Error);
      expect(baseErr).toBeInstanceOf(QueueError);
      expect(baseErr.name).toBe('QueueError');
      expect(baseErr.cause).toBe(cause);
      expect(baseErr.stack).toContain('Original root cause');

      const valErr = new QueueValidationError('Validation failed', cause);
      expect(valErr).toBeInstanceOf(QueueError);
      expect(valErr.name).toBe('QueueValidationError');

      const unavailErr = new QueueUnavailableError('Redis is down', cause);
      expect(unavailErr).toBeInstanceOf(QueueError);
      expect(unavailErr.name).toBe('QueueUnavailableError');

      const opErr = new QueueOperationError('Eval failed', cause);
      expect(opErr).toBeInstanceOf(QueueError);
      expect(opErr.name).toBe('QueueOperationError');

      const notFoundErr = new QueueMessageNotFoundError('msg-999');
      expect(notFoundErr).toBeInstanceOf(QueueError);
      expect(notFoundErr.name).toBe('QueueMessageNotFoundError');
      expect(notFoundErr.message).toContain('msg-999');
    });
  });
});
