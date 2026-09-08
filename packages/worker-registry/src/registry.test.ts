import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  createDatabasePool,
  DEFAULT_DATABASE_URL,
  PgWorkerRepository,
  resetDatabase,
  runMigrations,
  type DatabasePool,
  type WorkerRepository,
} from '@forge/database';
import { createRedisClient, DEFAULT_REDIS_URL, type RedisClient } from '@forge/redis';
import { createWorkerHeartbeatStore } from './heartbeat.js';
import { createWorkerRegistry } from './registry.js';
import { createWorkerId, type WorkerHeartbeatStore, type WorkerRegistry } from './types.js';

describe('Real PostgreSQL & Redis WorkerRegistry Integration Tests', () => {
  let pool: DatabasePool;
  let redisClient: RedisClient;
  let workerRepo: WorkerRepository;
  let heartbeatStore: WorkerHeartbeatStore;
  let registry: WorkerRegistry;

  beforeAll(async () => {
    // 1. Setup real PostgreSQL
    pool = createDatabasePool({ connectionString: DEFAULT_DATABASE_URL });
    await runMigrations(pool);
    workerRepo = new PgWorkerRepository(pool);

    // 2. Setup real Redis
    redisClient = createRedisClient({
      url: DEFAULT_REDIS_URL,
      connectTimeoutMillis: 5000,
      maxRetriesPerRequest: 2,
    });
    await redisClient.connect();

    // 3. Setup registry
    heartbeatStore = createWorkerHeartbeatStore(redisClient, { defaultTtlSeconds: 5 });
    registry = createWorkerRegistry(workerRepo, heartbeatStore, { heartbeatTtlSeconds: 5 });
  });

  afterAll(async () => {
    await redisClient.close();
    await pool.close();
  });

  beforeEach(async () => {
    await resetDatabase(pool);
    await runMigrations(pool);
  });

  afterEach(async () => {
    // Targeted cleanup of test worker heartbeat keys in Redis
    const raw = redisClient.getRawClient();
    const keys = await raw.keys('forge:worker:*');
    if (keys.length > 0) {
      await raw.del(...keys);
    }
  });

  describe('Worker Registration & Durable Persistence', () => {
    it('registers a worker in PostgreSQL and creates an initial heartbeat in Redis', async () => {
      const worker = await registry.register({
        workerId: 'worker-primary-1',
        hostname: 'node-01.forge.internal',
        capabilities: { executors: ['shell', 'docker'] },
        resources: { cpuCores: 8, memoryBytes: 16 * 1024 * 1024 * 1024, gpuCount: 1 },
      });

      expect(worker.workerId).toBe('worker-primary-1');
      expect(worker.status).toBe('READY');
      expect(worker.hostname).toBe('node-01.forge.internal');
      expect(worker.capabilities.executors).toEqual(['shell', 'docker']);
      expect(worker.resources.cpuCores).toBe(8);

      // Verify PostgreSQL state
      const dbRecord = await workerRepo.findById('worker-primary-1');
      expect(dbRecord).not.toBeNull();
      expect(dbRecord!.status).toBe('READY');
      expect(dbRecord!.hostname).toBe('node-01.forge.internal');
      expect(dbRecord!.resources.cpuCores).toBe(8);

      // Verify Redis heartbeat state
      const isAlive = await heartbeatStore.isAlive(createWorkerId('worker-primary-1'));
      expect(isAlive).toBe(true);

      const ttl = await heartbeatStore.getTtl(createWorkerId('worker-primary-1'));
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(5);

      // Verify getWorker composite view
      const info = await registry.getWorker(createWorkerId('worker-primary-1'));
      expect(info).not.toBeNull();
      expect(info!.liveness).toBe('ALIVE');
      expect(info!.lastHeartbeatAt).toBeDefined();
    });

    it('handles idempotent repeated registration without duplicate records', async () => {
      const id = createWorkerId('worker-idempotent');

      // First registration
      await registry.register({
        workerId: id,
        hostname: 'host-v1',
        capabilities: { executors: ['shell'] },
        resources: { cpuCores: 4, memoryBytes: 8192 },
      });

      // Second registration with updated resources and hostname
      await registry.register({
        workerId: id,
        hostname: 'host-v2',
        capabilities: { executors: ['shell', 'docker'] },
        resources: { cpuCores: 8, memoryBytes: 16384 },
      });

      // Single record in PostgreSQL
      const workers = await workerRepo.list();
      expect(workers.length).toBe(1);
      expect(workers[0]!.id).toBe(id);
      expect(workers[0]!.hostname).toBe('host-v2');
      expect(workers[0]!.executors).toEqual(['shell', 'docker']);
      expect(workers[0]!.resources.cpuCores).toBe(8);

      // Heartbeat still alive
      expect(await heartbeatStore.isAlive(id)).toBe(true);
    });
  });

  describe('Heartbeat Renewal & High-Frequency Coordination', () => {
    it('refreshes heartbeat TTL in Redis without issuing database writes', async () => {
      const id = createWorkerId('worker-heartbeat-test');
      await registry.register({
        workerId: id,
        capabilities: { executors: ['shell'] },
        resources: { cpuCores: 2, memoryBytes: 4096 },
      });

      const initialRecord = await workerRepo.findById(id);
      expect(initialRecord).not.toBeNull();

      // Wait a short duration to let TTL decrement
      await new Promise((resolve) => setTimeout(resolve, 1100));

      const ttlBefore = await heartbeatStore.getTtl(id);
      expect(ttlBefore).toBeLessThanOrEqual(4);

      // Refresh heartbeat
      await registry.heartbeat(id);

      const ttlAfter = await heartbeatStore.getTtl(id);
      expect(ttlAfter).toBeGreaterThan(ttlBefore);

      // Verify PostgreSQL record was NOT touched
      const recordAfter = await workerRepo.findById(id);
      expect(recordAfter!.updatedAt.getTime()).toBe(initialRecord!.updatedAt.getTime());
    });
  });

  describe('Crash Simulation, Expiry & Stale Worker Detection', () => {
    it('detects a crashed worker as STALE after TTL expires while preserving durable record', async () => {
      // Use 1-second TTL for fast test turnaround
      const shortLivedStore = createWorkerHeartbeatStore(redisClient, { defaultTtlSeconds: 1 });
      const fastRegistry = createWorkerRegistry(workerRepo, shortLivedStore, {
        heartbeatTtlSeconds: 1,
      });

      const id = createWorkerId('worker-crash-simulation');
      await fastRegistry.register({
        workerId: id,
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 8192 },
      });

      // Immediately after registration, worker is ALIVE
      let info = await fastRegistry.getWorker(id);
      expect(info).not.toBeNull();
      expect(info!.liveness).toBe('ALIVE');

      // Simulate crash: worker stops sending heartbeats
      // Wait for 1-second TTL to expire
      await new Promise((resolve) => setTimeout(resolve, 1200));

      // Worker is now STALE in Redis
      expect(await shortLivedStore.isAlive(id)).toBe(false);

      info = await fastRegistry.getWorker(id);
      expect(info).not.toBeNull();
      expect(info!.liveness).toBe('STALE');
      expect(info!.lastHeartbeatAt).toBeNull();

      // PostgreSQL record is strictly preserved
      const dbRecord = await workerRepo.findById(id);
      expect(dbRecord).not.toBeNull();
      expect(dbRecord!.status).toBe('READY');

      // Reconnection: worker comes back online and sends heartbeat
      await fastRegistry.heartbeat(id);

      info = await fastRegistry.getWorker(id);
      expect(info!.liveness).toBe('ALIVE');
      expect(info!.lastHeartbeatAt).not.toBeNull();
    });
  });

  describe('Graceful Deregistration', () => {
    it('deregisters a worker: removes Redis heartbeat and marks PostgreSQL status OFFLINE', async () => {
      const id = createWorkerId('worker-graceful');
      await registry.register({
        workerId: id,
        capabilities: { executors: ['shell'] },
        resources: { cpuCores: 2, memoryBytes: 4096 },
      });

      expect(await heartbeatStore.isAlive(id)).toBe(true);

      // Graceful shutdown
      await registry.deregister(id);

      // Redis heartbeat immediately purged
      expect(await heartbeatStore.isAlive(id)).toBe(false);

      // PostgreSQL record updated to OFFLINE
      const dbRecord = await workerRepo.findById(id);
      expect(dbRecord).not.toBeNull();
      expect(dbRecord!.status).toBe('OFFLINE');

      // getWorker reports STALE and OFFLINE
      const info = await registry.getWorker(id);
      expect(info!.worker.status).toBe('OFFLINE');
      expect(info!.liveness).toBe('STALE');
    });
  });

  describe('Multiple Concurrent Workers & Isolation', () => {
    it('maintains isolated heartbeat states across multiple logical workers', async () => {
      const shortLivedStore = createWorkerHeartbeatStore(redisClient, { defaultTtlSeconds: 1 });
      const customRegistry = createWorkerRegistry(workerRepo, shortLivedStore, {
        heartbeatTtlSeconds: 1,
      });

      const workerA = createWorkerId('worker-A');
      const workerB = createWorkerId('worker-B');

      await customRegistry.register({
        workerId: workerA,
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 8192 },
      });

      await customRegistry.register({
        workerId: workerB,
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 8, memoryBytes: 16384 },
      });

      expect(await shortLivedStore.isAlive(workerA)).toBe(true);
      expect(await shortLivedStore.isAlive(workerB)).toBe(true);

      // Simulate worker A crashing while worker B keeps sending heartbeats
      for (let i = 0; i < 3; i++) {
        await new Promise((resolve) => setTimeout(resolve, 400));
        await customRegistry.heartbeat(workerB);
      }

      // Worker A has exceeded 1s without heartbeat -> STALE
      // Worker B was refreshed -> ALIVE
      const infoA = await customRegistry.getWorker(workerA);
      const infoB = await customRegistry.getWorker(workerB);

      expect(infoA!.liveness).toBe('STALE');
      expect(infoB!.liveness).toBe('ALIVE');
    });

    it('lists workers with status and liveness filtering', async () => {
      const w1 = createWorkerId('w-ready-alive');
      const w2 = createWorkerId('w-ready-stale');
      const w3 = createWorkerId('w-offline');

      await registry.register({
        workerId: w1,
        capabilities: { executors: ['shell'] },
        resources: { cpuCores: 2, memoryBytes: 2048 },
      });

      await registry.register({
        workerId: w2,
        capabilities: { executors: ['shell'] },
        resources: { cpuCores: 2, memoryBytes: 2048 },
      });

      await registry.register({
        workerId: w3,
        capabilities: { executors: ['shell'] },
        resources: { cpuCores: 2, memoryBytes: 2048 },
      });

      // Expire w2 heartbeat manually
      await heartbeatStore.removeHeartbeat(w2);

      // Deregister w3
      await registry.deregister(w3);

      // List all
      const all = await registry.listWorkers();
      expect(all.length).toBe(3);

      // Filter by liveness ALIVE
      const aliveOnly = await registry.listWorkers({ liveness: 'ALIVE' });
      expect(aliveOnly.length).toBe(1);
      expect(aliveOnly[0]!.worker.workerId).toBe(w1);

      // Filter by liveness STALE
      const staleOnly = await registry.listWorkers({ liveness: 'STALE' });
      expect(staleOnly.length).toBe(2);

      // Filter by status OFFLINE
      const offlineOnly = await registry.listWorkers({ status: 'OFFLINE' });
      expect(offlineOnly.length).toBe(1);
      expect(offlineOnly[0]!.worker.workerId).toBe(w3);
    });

    it('heartbeat loss marks worker as STALE without mutating database status (PostgreSQL remains authority)', async () => {
      const lostWorkerId = createWorkerId('worker-lost-heartbeat');

      await registry.register({
        workerId: lostWorkerId,
        hostname: 'node-lost-1',
        capabilities: { executors: ['docker'] },
        resources: { cpuCores: 4, memoryBytes: 4096 },
      });

      // Initially ALIVE and READY in both Redis and PostgreSQL
      const initialInfo = await registry.getWorker(lostWorkerId);
      expect(initialInfo).not.toBeNull();
      expect(initialInfo!.liveness).toBe('ALIVE');
      expect(initialInfo!.worker.status).toBe('READY');

      // Expire heartbeat from Redis (simulate network partition / crash)
      await heartbeatStore.removeHeartbeat(lostWorkerId);

      // Verify: Worker registry now reports STALE liveness
      const staleInfo = await registry.getWorker(lostWorkerId);
      expect(staleInfo).not.toBeNull();
      expect(staleInfo!.liveness).toBe('STALE');

      // Crucial architectural guarantee: PostgreSQL record is NOT mutated by Redis heartbeat expiration.
      // Worker status in DB is STILL READY, not destroyed or modified.
      const dbWorker = await workerRepo.findById(lostWorkerId);
      expect(dbWorker).not.toBeNull();
      expect(dbWorker!.status).toBe('READY');

      // Scheduler will filter this worker out from candidate selection because liveness === 'STALE',
      // but lease recovery remains strictly tied to PostgreSQL worker_leases.expires_at.
    });
  });
});
