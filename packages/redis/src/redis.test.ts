import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createRedisClient } from './client.js';
import { DEFAULT_REDIS_URL } from './config.js';
import { RedisConnectionError } from './errors.js';
import { createRedisKey } from './keys.js';
import type { RedisClient } from './types.js';

describe('Real Redis Integration Tests', () => {
  let client: RedisClient;
  let runNamespace: string;

  beforeAll(async () => {
    client = createRedisClient({
      url: DEFAULT_REDIS_URL,
      connectTimeoutMillis: 5000,
      maxRetriesPerRequest: 2,
    });

    await client.connect();
  });

  afterAll(async () => {
    await client.close();
  });

  beforeEach(async () => {
    // Deterministic isolation prefix: forge:test:{timestamp}_{random}
    runNamespace = `test_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  });

  afterEach(async () => {
    // Clean up all keys created in this test's unique namespace without using FLUSHALL
    const raw = client.getRawClient();
    const pattern = `forge:${runNamespace}:*`;
    const keys = await raw.keys(pattern);
    if (keys.length > 0) {
      await raw.del(...keys);
    }
  });

  describe('Connection Lifecycle & Health Checking', () => {
    it('successfully connects and reports ready status', () => {
      expect(client.status).toBe('ready');
    });

    it('returns true on healthCheck() when Redis is healthy', async () => {
      const healthy = await client.healthCheck();
      expect(healthy).toBe(true);
    });

    it('rejects connection and reports healthCheck() failure when Redis is unavailable', async () => {
      // Connect to non-existent port with zero retries
      const deadClient = createRedisClient({
        url: 'redis://127.0.0.1:59999',
        connectTimeoutMillis: 1000,
        maxRetriesPerRequest: 0,
        retryStrategy: () => null,
      });

      await expect(deadClient.connect()).rejects.toThrow(RedisConnectionError);

      const healthy = await deadClient.healthCheck();
      expect(healthy).toBe(false);

      await deadClient.disconnect();
    });

    it('supports idempotent connect() calls', async () => {
      await expect(client.connect()).resolves.not.toThrow();
      expect(client.status).toBe('ready');
    });
  });

  describe('Low-Level Key-Value Primitives', () => {
    it('stores, reads, checks existence, and deletes string keys', async () => {
      const key = createRedisKey(runNamespace, 'key1');

      // Key initially does not exist
      expect(await client.get(key)).toBeNull();
      expect(await client.exists(key)).toBe(0);

      // Set key
      const setOk = await client.set(key, 'forge-value-1');
      expect(setOk).toBe(true);

      // Read key
      const value = await client.get(key);
      expect(value).toBe('forge-value-1');
      expect(await client.exists(key)).toBe(1);

      // Delete key
      const deletedCount = await client.del(key);
      expect(deletedCount).toBe(1);
      expect(await client.get(key)).toBeNull();
      expect(await client.exists(key)).toBe(0);
    });

    it('respects conditional SET options (NX and XX)', async () => {
      const key = createRedisKey(runNamespace, 'cond_key');

      // NX on non-existent key: succeeds
      const firstSet = await client.set(key, 'initial', { ifNotExists: true });
      expect(firstSet).toBe(true);

      // NX on existing key: fails
      const secondSet = await client.set(key, 'overwrite', { ifNotExists: true });
      expect(secondSet).toBe(false);
      expect(await client.get(key)).toBe('initial');

      // XX on existing key: succeeds
      const thirdSet = await client.set(key, 'updated', { ifExists: true });
      expect(thirdSet).toBe(true);
      expect(await client.get(key)).toBe('updated');

      // XX on non-existent key: fails
      const nonExistentKey = createRedisKey(runNamespace, 'non_existent_key');
      const fourthSet = await client.set(nonExistentKey, 'val', { ifExists: true });
      expect(fourthSet).toBe(false);
    });
  });

  describe('TTL & Expiration Primitives', () => {
    it('sets key TTL via EX option and verifies decreasing TTL', async () => {
      const key = createRedisKey(runNamespace, 'ttl_test');

      await client.set(key, 'expiring_value', { ttlSeconds: 30 });

      const ttl = await client.ttl(key);
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(30);
    });

    it('sets key expiration explicitly using expire() command', async () => {
      const key = createRedisKey(runNamespace, 'expire_test');

      await client.set(key, 'persistent_initially');
      expect(await client.ttl(key)).toBe(-1); // -1 = persistent (no expiration)

      const expireOk = await client.expire(key, 20);
      expect(expireOk).toBe(true);

      const ttl = await client.ttl(key);
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(20);
    });

    it('removes keys upon TTL expiration', async () => {
      const key = createRedisKey(runNamespace, 'short_ttl');

      // 1-second expiration
      await client.set(key, 'ephemeral', { ttlSeconds: 1 });

      // Poll until expiration completes (bounded by 2.5s)
      const start = Date.now();
      let remainingValue: string | null = 'ephemeral';

      while (Date.now() - start < 2500) {
        remainingValue = await client.get(key);
        if (remainingValue === null) {
          break;
        }
        await new Promise((r) => setTimeout(r, 100));
      }

      expect(remainingValue).toBeNull();
      expect(await client.ttl(key)).toBe(-2); // -2 = key does not exist
    });
  });

  describe('Atomic Coordination Primitives', () => {
    it('implements atomic SETNX mutual exclusion', async () => {
      const lockKey = createRedisKey(runNamespace, 'atomic_claim');

      // Client A attempts to claim key
      const claimA = await client.setNx(lockKey, 'worker-A', 10);
      expect(claimA).toBe(true);

      // Client B attempts to claim same key concurrently
      const claimB = await client.setNx(lockKey, 'worker-B', 10);
      expect(claimB).toBe(false);

      // Owner remains worker-A
      expect(await client.get(lockKey)).toBe('worker-A');
    });

    it('atomically increments and decrements numeric counters', async () => {
      const counterKey = createRedisKey(runNamespace, 'counter');

      expect(await client.incr(counterKey)).toBe(1);
      expect(await client.incr(counterKey)).toBe(2);
      expect(await client.incr(counterKey)).toBe(3);

      expect(await client.decr(counterKey)).toBe(2);
      expect(await client.decr(counterKey)).toBe(1);
    });

    it('atomically executes Lua scripts via eval()', async () => {
      const key = createRedisKey(runNamespace, 'lua_test');
      await client.set(key, 'initial-lua-value');

      // Atomic conditional update Lua script:
      // If current value == ARGV[1], update to ARGV[2] and return 1; else return 0
      const script = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
          redis.call("set", KEYS[1], ARGV[2])
          return 1
        else
          return 0
        end
      `;

      // Attempt matching update
      const resultMatch = await client.eval(script, 1, key, 'initial-lua-value', 'updated-by-lua');
      expect(resultMatch).toBe(1);
      expect(await client.get(key)).toBe('updated-by-lua');

      // Attempt non-matching update
      const resultMismatch = await client.eval(script, 1, key, 'wrong-value', 'should-not-set');
      expect(resultMismatch).toBe(0);
      expect(await client.get(key)).toBe('updated-by-lua');
    });
  });

  describe('Structured JSON Primitives', () => {
    it('stores and retrieves complex JSON payloads faithfully', async () => {
      const key = createRedisKey(runNamespace, 'structured');

      interface JobDescriptor {
        jobId: string;
        step: string;
        dependencies: string[];
        attempt: number;
        metadata: { priority: string; timeoutSec: number };
      }

      const descriptor: JobDescriptor = {
        jobId: 'job-ci-build',
        step: 'compile',
        dependencies: ['setup', 'lint'],
        attempt: 1,
        metadata: {
          priority: 'high',
          timeoutSec: 300,
        },
      };

      await client.setJson(key, descriptor, { ttlSeconds: 60 });

      const loaded = await client.getJson<JobDescriptor>(key);
      expect(loaded).toEqual(descriptor);
    });

    it('returns null when getJson is called on non-existent key', async () => {
      const nonExistent = createRedisKey(runNamespace, 'no_json');
      const loaded = await client.getJson(nonExistent);
      expect(loaded).toBeNull();
    });
  });
});
