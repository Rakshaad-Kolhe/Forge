import { describe, expect, it } from 'vitest';
import { createRedisClient } from './client.js';
import { DEFAULT_REDIS_URL } from './config.js';
import { sanitizeRedisUrl, SerializationError } from './errors.js';
import { createRedisKey, parseRedisKey } from './keys.js';
import { deserializeJson, serializeJson } from './serialization.js';

describe('Redis Foundation Unit Tests', () => {
  describe('createRedisKey & parseRedisKey', () => {
    it('constructs standardized namespaced keys', () => {
      const key = createRedisKey('coordination', 'job', '123');
      expect(key).toBe('forge:coordination:job:123');
    });

    it('rejects empty namespace', () => {
      expect(() => createRedisKey('')).toThrow('Redis key namespace cannot be empty');
      expect(() => createRedisKey('   ')).toThrow('Redis key namespace cannot be empty');
    });

    it('rejects empty key parts', () => {
      expect(() => createRedisKey('ns', 'part1', '')).toThrow(
        'Redis key part at index 1 cannot be empty',
      );
    });

    it('parses standardized keys back to components', () => {
      const parsed = parseRedisKey('forge:locks:resource:456');
      expect(parsed).toEqual({
        prefix: 'forge',
        namespace: 'locks',
        parts: ['resource', '456'],
      });
    });

    it('rejects non-forge keys in parseRedisKey', () => {
      expect(() => parseRedisKey('other:prefix:key')).toThrow('Invalid Forge Redis key format');
      expect(() => parseRedisKey('singletoken')).toThrow('Invalid Forge Redis key format');
    });
  });

  describe('sanitizeRedisUrl', () => {
    it('masks passwords in standard redis URIs', () => {
      const sanitized = sanitizeRedisUrl('redis://:supersecret@127.0.0.1:6379');
      expect(sanitized).toBe('redis://:***@127.0.0.1:6379');
      expect(sanitized).not.toContain('supersecret');
    });

    it('masks passwords with username in redis URIs', () => {
      const sanitized = sanitizeRedisUrl('redis://admin:mypassword@redis.internal:6379/2');
      expect(sanitized).toBe('redis://admin:***@redis.internal:6379/2');
      expect(sanitized).not.toContain('mypassword');
    });

    it('preserves clean URLs without credentials', () => {
      const sanitized = sanitizeRedisUrl('redis://127.0.0.1:6379');
      expect(sanitized).toBe('redis://127.0.0.1:6379');
    });
  });

  describe('serializeJson & deserializeJson', () => {
    it('roundtrips valid plain objects and arrays deterministically', () => {
      const payload = {
        id: 'job-1',
        step: 'build',
        retries: 3,
        active: true,
        tags: ['ci', 'fast'],
      };

      const encoded = serializeJson(payload);
      expect(typeof encoded).toBe('string');

      const decoded = deserializeJson<typeof payload>(encoded);
      expect(decoded).toEqual(payload);
    });

    it('rejects undefined values with SerializationError', () => {
      expect(() => serializeJson(undefined)).toThrow(SerializationError);
      expect(() => serializeJson({ key: undefined })).toThrow(SerializationError);
    });

    it('rejects functions and symbols with SerializationError', () => {
      expect(() => serializeJson({ fn: () => {} })).toThrow(SerializationError);
      expect(() => serializeJson({ sym: Symbol('test') })).toThrow(SerializationError);
    });

    it('rejects NaN and Infinity with SerializationError', () => {
      expect(() => serializeJson({ value: NaN })).toThrow(SerializationError);
      expect(() => serializeJson({ value: Infinity })).toThrow(SerializationError);
    });

    it('rejects BigInt values with SerializationError', () => {
      expect(() => serializeJson({ count: BigInt(100) })).toThrow(SerializationError);
    });

    it('rejects non-plain class instances with SerializationError', () => {
      class CustomWorkerPayload {
        constructor(public name: string) {}
      }
      expect(() => serializeJson(new CustomWorkerPayload('worker-1'))).toThrow(SerializationError);
    });

    it('rejects invalid JSON syntax on deserialization with SerializationError', () => {
      expect(() => deserializeJson('not-valid-json{')).toThrow(SerializationError);
    });
  });

  describe('createRedisClient factory', () => {
    it('initializes in lazy wait status without immediate network connection', () => {
      const client = createRedisClient({
        url: DEFAULT_REDIS_URL,
        lazyConnect: true,
      });

      expect(client.status).toBe('wait');
      client.disconnect();
    });
  });
});
