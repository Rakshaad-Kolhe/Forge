import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createRedisClient, DEFAULT_REDIS_URL, type RedisClient } from '@forge/redis';
import { QueueUnavailableError, QueueValidationError } from './errors.js';
import { createJobQueue } from './queue.js';
import type { JobQueue } from './types.js';

describe('Real Redis JobQueue Integration Tests', () => {
  let redisClient: RedisClient;
  let testQueueName: string;
  let queue: JobQueue;

  beforeAll(async () => {
    redisClient = createRedisClient({
      url: DEFAULT_REDIS_URL,
      connectTimeoutMillis: 5000,
      maxRetriesPerRequest: 2,
    });
    await redisClient.connect();
  });

  afterAll(async () => {
    await redisClient.close();
  });

  beforeEach(async () => {
    // Unique isolated queue namespace per test
    testQueueName = `test_q_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    queue = createJobQueue(redisClient, {
      queueName: testQueueName,
      defaultVisibilityTimeoutSeconds: 5,
    });
  });

  afterEach(async () => {
    // Targeted cleanup of keys in this test queue's namespace (no FLUSHALL)
    const raw = redisClient.getRawClient();
    const pattern = `forge:queue:${testQueueName}:*`;
    const keys = await raw.keys(pattern);
    if (keys.length > 0) {
      await raw.del(...keys);
    }
  });

  describe('Basic Lifecycle: Enqueue, Dequeue, Depth, and ACK', () => {
    it('enqueues a message, inspects depth, dequeues, and acknowledges cleanly', async () => {
      expect(await queue.depth()).toBe(0);
      expect(await queue.inFlightCount()).toBe(0);

      const enqueueResult = await queue.enqueue({
        jobId: 'job-101',
        pipelineRunId: 'run-501',
        stepName: 'test',
        attemptNumber: 1,
      });

      expect(enqueueResult.messageId).toMatch(/^msg_/);
      expect(enqueueResult.deduplicated).toBe(false);
      expect(await queue.depth()).toBe(1);
      expect(await queue.inFlightCount()).toBe(0);

      // Dequeue
      const delivery = await queue.dequeue();
      expect(delivery).not.toBeNull();
      expect(delivery!.message.messageId).toBe(enqueueResult.messageId);
      expect(delivery!.message.jobId).toBe('job-101');
      expect(delivery!.message.pipelineRunId).toBe('run-501');
      expect(delivery!.message.stepName).toBe('test');
      expect(delivery!.message.attemptNumber).toBe(1);
      expect(delivery!.deliveryCount).toBe(1);
      expect(delivery!.deliveryId).toMatch(/^del_/);

      // Depth is now 0 (message is in-flight, not ready)
      expect(await queue.depth()).toBe(0);
      expect(await queue.inFlightCount()).toBe(1);

      // Acknowledge
      const ackResult = await queue.acknowledge(delivery!.message.messageId);
      expect(ackResult).toBe(true);

      // Both depth and in-flight are 0 after ACK
      expect(await queue.depth()).toBe(0);
      expect(await queue.inFlightCount()).toBe(0);
    });

    it('returns null when dequeuing from an empty queue without error', async () => {
      expect(await queue.depth()).toBe(0);
      const delivery = await queue.dequeue();
      expect(delivery).toBeNull();
    });

    it('handles idempotent acknowledgement deterministically', async () => {
      const enqueueResult = await queue.enqueue({
        jobId: 'job-ack',
        pipelineRunId: 'run-ack',
        stepName: 'lint',
      });

      const delivery = await queue.dequeue();
      expect(delivery).not.toBeNull();
      expect(delivery!.message.messageId).toBe(enqueueResult.messageId);

      // First ACK succeeds
      const firstAck = await queue.acknowledge(delivery!.message.messageId);
      expect(firstAck).toBe(true);

      // Second ACK returns false without throwing
      const secondAck = await queue.acknowledge(delivery!.message.messageId);
      expect(secondAck).toBe(false);
    });
  });

  describe('FIFO Ordering Guarantees', () => {
    it('strictly preserves FIFO order for sequential enqueues [A, B, C, D]', async () => {
      const jobs = [
        { id: 'job-A', step: 'step-A' },
        { id: 'job-B', step: 'step-B' },
        { id: 'job-C', step: 'step-C' },
        { id: 'job-D', step: 'step-D' },
      ];

      const enqueuedIds: string[] = [];
      for (const item of jobs) {
        const res = await queue.enqueue({
          jobId: item.id,
          pipelineRunId: 'run-fifo',
          stepName: item.step,
        });
        enqueuedIds.push(res.messageId);
      }

      expect(await queue.depth()).toBe(4);

      const dequeuedIds: string[] = [];
      for (let i = 0; i < jobs.length; i++) {
        const delivery = await queue.dequeue();
        expect(delivery).not.toBeNull();
        dequeuedIds.push(delivery!.message.messageId);
        expect(delivery!.message.jobId).toBe(jobs[i]!.id);
        await queue.acknowledge(delivery!.message.messageId);
      }

      // Exact order match
      expect(dequeuedIds).toEqual(enqueuedIds);
      expect(await queue.depth()).toBe(0);
    });
  });

  describe('At-Least-Once Delivery: Crash & Unacknowledged Message Recovery', () => {
    it('reclaims an unacknowledged message after visibility expiration and redelivers with incremented deliveryCount', async () => {
      // Use a 1-second visibility timeout for fast test turnaround
      const enqueueResult = await queue.enqueue({
        jobId: 'job-crash',
        pipelineRunId: 'run-crash',
        stepName: 'deploy',
      });

      // Consumer 1 dequeues with 1-second visibility timeout
      const delivery1 = await queue.dequeue({ visibilityTimeoutSeconds: 1 });
      expect(delivery1).not.toBeNull();
      expect(delivery1!.message.messageId).toBe(enqueueResult.messageId);
      expect(delivery1!.deliveryCount).toBe(1);

      // Consumer 1 crashes! (Never calls acknowledge)
      // Immediately after crash, message is in-flight, not ready
      expect(await queue.depth()).toBe(0);
      expect(await queue.inFlightCount()).toBe(1);

      // Before timeout expires, reclaim finds nothing
      const prematureReclaim = await queue.reclaimExpired();
      expect(prematureReclaim).toBe(0);

      // Wait for visibility timeout to expire (> 1000ms)
      await new Promise((resolve) => setTimeout(resolve, 1100));

      // Now reclaim moves expired message back to ready list
      const reclaimedCount = await queue.reclaimExpired();
      expect(reclaimedCount).toBe(1);
      expect(await queue.depth()).toBe(1);
      expect(await queue.inFlightCount()).toBe(0);

      // Consumer 2 dequeues the recovered message
      const delivery2 = await queue.dequeue();
      expect(delivery2).not.toBeNull();
      expect(delivery2!.message.messageId).toBe(enqueueResult.messageId);
      expect(delivery2!.message.jobId).toBe('job-crash');
      expect(delivery2!.deliveryCount).toBe(2); // deliveryCount incremented!

      // Consumer 2 acknowledges
      await queue.acknowledge(delivery2!.message.messageId);

      // Verify clean final state
      expect(await queue.depth()).toBe(0);
      expect(await queue.inFlightCount()).toBe(0);
    });

    it('does NOT reclaim messages that were acknowledged before visibility expiration', async () => {
      await queue.enqueue({
        jobId: 'job-ack-first',
        pipelineRunId: 'run-1',
        stepName: 'build',
      });

      const delivery = await queue.dequeue({ visibilityTimeoutSeconds: 1 });
      expect(delivery).not.toBeNull();

      // Normal acknowledgement
      await queue.acknowledge(delivery!.message.messageId);

      // Wait past timeout
      await new Promise((resolve) => setTimeout(resolve, 1100));

      const reclaimed = await queue.reclaimExpired();
      expect(reclaimed).toBe(0);
      expect(await queue.depth()).toBe(0);
    });
  });

  describe('Competing Consumers Concurrency', () => {
    it('dispatches ready messages across competing consumers without duplicate delivery', async () => {
      const messageCount = 10;
      for (let i = 0; i < messageCount; i++) {
        await queue.enqueue({
          jobId: `job-worker-${i}`,
          pipelineRunId: 'run-compete',
          stepName: 'compile',
        });
      }

      expect(await queue.depth()).toBe(messageCount);

      // Simulate 3 concurrent consumers competing for the 10 messages
      const consumerA: string[] = [];
      const consumerB: string[] = [];
      const consumerC: string[] = [];

      const consumeLoop = async (collector: string[]) => {
        while (true) {
          const delivery = await queue.dequeue();
          if (!delivery) {
            break;
          }
          collector.push(delivery.message.messageId);
          await queue.acknowledge(delivery.message.messageId);
        }
      };

      await Promise.all([consumeLoop(consumerA), consumeLoop(consumerB), consumeLoop(consumerC)]);

      const allDelivered = [...consumerA, ...consumerB, ...consumerC];
      expect(allDelivered.length).toBe(messageCount);

      // Verify no message was delivered to more than one consumer
      const uniqueDelivered = new Set(allDelivered);
      expect(uniqueDelivered.size).toBe(messageCount);
      expect(await queue.depth()).toBe(0);
      expect(await queue.inFlightCount()).toBe(0);
    });
  });

  describe('Duplicate Enqueue Semantics', () => {
    it('allows multiple messages for the same jobId with unique auto-generated messageIds', async () => {
      const res1 = await queue.enqueue({
        jobId: 'job-duplicate-test',
        pipelineRunId: 'run-dup',
        stepName: 'build',
      });

      const res2 = await queue.enqueue({
        jobId: 'job-duplicate-test',
        pipelineRunId: 'run-dup',
        stepName: 'build',
      });

      expect(res1.messageId).not.toBe(res2.messageId);
      expect(await queue.depth()).toBe(2);

      const d1 = await queue.dequeue();
      const d2 = await queue.dequeue();
      expect(d1!.message.messageId).toBe(res1.messageId);
      expect(d2!.message.messageId).toBe(res2.messageId);

      await queue.acknowledge(d1!.message.messageId);
      await queue.acknowledge(d2!.message.messageId);
    });

    it('deduplicates when explicit customMessageId already exists in queue', async () => {
      const customId = 'fixed-custom-message-id-123';

      const res1 = await queue.enqueue({
        jobId: 'job-custom',
        pipelineRunId: 'run-custom',
        stepName: 'test',
        customMessageId: customId,
      });

      expect(res1.messageId).toBe(customId);
      expect(res1.deduplicated).toBe(false);

      // Re-enqueue with the exact same customMessageId
      const res2 = await queue.enqueue({
        jobId: 'job-custom',
        pipelineRunId: 'run-custom',
        stepName: 'test',
        customMessageId: customId,
      });

      expect(res2.messageId).toBe(customId);
      expect(res2.deduplicated).toBe(true);

      // Depth is still 1 (not duplicated)
      expect(await queue.depth()).toBe(1);

      const delivery = await queue.dequeue();
      expect(delivery!.message.messageId).toBe(customId);
      await queue.acknowledge(delivery!.message.messageId);
    });
  });

  describe('Connection Failure & Error Propagation', () => {
    it('throws QueueUnavailableError on dequeue when Redis is closed, never returning null', async () => {
      const disconnectedClient = createRedisClient({
        url: 'redis://127.0.0.1:19999', // Non-existent port
        connectTimeoutMillis: 1000,
        maxRetriesPerRequest: 0,
      });

      const disconnectedQueue = createJobQueue(disconnectedClient, {
        queueName: 'disconnected-q',
      });

      await expect(disconnectedQueue.dequeue()).rejects.toThrow(QueueUnavailableError);
    });

    it('throws QueueValidationError when dequeue has invalid visibility timeout', async () => {
      await expect(queue.dequeue({ visibilityTimeoutSeconds: -1 })).rejects.toThrow(
        QueueValidationError,
      );
      await expect(queue.dequeue({ visibilityTimeoutSeconds: 0 })).rejects.toThrow(
        QueueValidationError,
      );
    });

    it('throws QueueValidationError when acknowledge is called with empty ID', async () => {
      await expect(queue.acknowledge('')).rejects.toThrow(QueueValidationError);
      await expect(queue.acknowledge('   ')).rejects.toThrow(QueueValidationError);
    });
  });
});
