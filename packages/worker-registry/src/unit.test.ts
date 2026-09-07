import { describe, expect, it, vi } from 'vitest';
import type { WorkerRepository } from '@forge/database';
import {
  WorkerError,
  WorkerHeartbeatError,
  WorkerNotFoundError,
  WorkerRegistrationError,
  WorkerValidationError,
} from './errors.js';
import { getWorkerHeartbeatKey } from './keys.js';
import { createWorkerRegistry } from './registry.js';
import { createWorkerId, type WorkerHeartbeatStore, type WorkerStatus } from './types.js';

describe('Worker Registry Unit Tests', () => {
  const createMockRepo = (): WorkerRepository => ({
    save: vi.fn().mockResolvedValue(undefined),
    findById: vi.fn().mockResolvedValue(null),
    list: vi.fn().mockResolvedValue([]),
    updateStatus: vi.fn().mockResolvedValue(true),
    delete: vi.fn().mockResolvedValue(true),
  });

  const createMockHeartbeatStore = (): WorkerHeartbeatStore => ({
    recordHeartbeat: vi.fn().mockResolvedValue(undefined),
    isAlive: vi.fn().mockResolvedValue(true),
    getHeartbeat: vi.fn().mockResolvedValue(null),
    getTtl: vi.fn().mockResolvedValue(15),
    removeHeartbeat: vi.fn().mockResolvedValue(undefined),
  });

  describe('WorkerId Factory', () => {
    it('creates branded WorkerId from non-empty string', () => {
      const id = createWorkerId('worker-123');
      expect(id).toBe('worker-123');
    });

    it('trims whitespace and rejects empty string', () => {
      expect(() => createWorkerId('')).toThrow('WorkerId cannot be empty');
      expect(() => createWorkerId('   ')).toThrow('WorkerId cannot be empty');
    });
  });

  describe('Redis Key Conventions', () => {
    it('builds standard key forge:worker:{id}:heartbeat', () => {
      expect(getWorkerHeartbeatKey('w-101')).toBe('forge:worker:w-101:heartbeat');
      expect(getWorkerHeartbeatKey('node-alpha')).toBe('forge:worker:node-alpha:heartbeat');
    });
  });

  describe('Registration Input Validation', () => {
    it('rejects empty workerId override if provided', async () => {
      const registry = createWorkerRegistry(createMockRepo(), createMockHeartbeatStore());

      await expect(
        registry.register({
          workerId: '   ',
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: 4, memoryBytes: 8192 },
        }),
      ).rejects.toThrow(WorkerValidationError);
    });

    it('rejects invalid worker lifecycle status', async () => {
      const registry = createWorkerRegistry(createMockRepo(), createMockHeartbeatStore());

      await expect(
        registry.register({
          status: 'RUNNING' as unknown as WorkerStatus, // RUNNING is a job status, not a worker status!
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: 4, memoryBytes: 8192 },
        }),
      ).rejects.toThrow(WorkerValidationError);
    });

    it('rejects empty executors list or empty executor names', async () => {
      const registry = createWorkerRegistry(createMockRepo(), createMockHeartbeatStore());

      await expect(
        registry.register({
          capabilities: { executors: [] },
          resources: { cpuCores: 4, memoryBytes: 8192 },
        }),
      ).rejects.toThrow(WorkerValidationError);

      await expect(
        registry.register({
          capabilities: { executors: [''] },
          resources: { cpuCores: 4, memoryBytes: 8192 },
        }),
      ).rejects.toThrow(WorkerValidationError);
    });

    it('rejects invalid CPU resource numbers', async () => {
      const registry = createWorkerRegistry(createMockRepo(), createMockHeartbeatStore());

      await expect(
        registry.register({
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: 0, memoryBytes: 8192 },
        }),
      ).rejects.toThrow(WorkerValidationError);

      await expect(
        registry.register({
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: -2, memoryBytes: 8192 },
        }),
      ).rejects.toThrow(WorkerValidationError);

      await expect(
        registry.register({
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: 2.5, memoryBytes: 8192 },
        }),
      ).rejects.toThrow(WorkerValidationError);
    });

    it('rejects invalid memory resource numbers', async () => {
      const registry = createWorkerRegistry(createMockRepo(), createMockHeartbeatStore());

      await expect(
        registry.register({
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: 4, memoryBytes: 0 },
        }),
      ).rejects.toThrow(WorkerValidationError);

      await expect(
        registry.register({
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: 4, memoryBytes: -1024 },
        }),
      ).rejects.toThrow(WorkerValidationError);
    });

    it('rejects negative GPU count', async () => {
      const registry = createWorkerRegistry(createMockRepo(), createMockHeartbeatStore());

      await expect(
        registry.register({
          capabilities: { executors: ['shell'] },
          resources: { cpuCores: 4, memoryBytes: 8192, gpuCount: -1 },
        }),
      ).rejects.toThrow(WorkerValidationError);
    });
  });

  describe('Error Taxonomy', () => {
    it('instantiates all error classes with correct inheritance and cause tracking', () => {
      const cause = new Error('Root cause details');

      const baseErr = new WorkerError('Base worker error', cause);
      expect(baseErr).toBeInstanceOf(Error);
      expect(baseErr).toBeInstanceOf(WorkerError);
      expect(baseErr.name).toBe('WorkerError');
      expect(baseErr.cause).toBe(cause);
      expect(baseErr.stack).toContain('Root cause details');

      const valErr = new WorkerValidationError('Invalid worker input', cause);
      expect(valErr).toBeInstanceOf(WorkerError);
      expect(valErr.name).toBe('WorkerValidationError');

      const notFoundErr = new WorkerNotFoundError('w-missing');
      expect(notFoundErr).toBeInstanceOf(WorkerError);
      expect(notFoundErr.name).toBe('WorkerNotFoundError');
      expect(notFoundErr.message).toContain('w-missing');

      const regErr = new WorkerRegistrationError('Registration failed in DB', cause);
      expect(regErr).toBeInstanceOf(WorkerError);
      expect(regErr.name).toBe('WorkerRegistrationError');

      const hbErr = new WorkerHeartbeatError('Redis heartbeat write timed out', cause);
      expect(hbErr).toBeInstanceOf(WorkerError);
      expect(hbErr.name).toBe('WorkerHeartbeatError');
    });
  });
});
