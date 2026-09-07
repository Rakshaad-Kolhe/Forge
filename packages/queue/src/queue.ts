import { randomUUID } from 'node:crypto';
import type { Logger } from '@forge/logging';
import { deserializeJson, serializeJson, type RedisClient } from '@forge/redis';
import {
  QueueError,
  QueueOperationError,
  QueueUnavailableError,
  QueueValidationError,
} from './errors.js';
import { getInFlightKey, getMessagesKey, getMetadataKey, getReadyKey } from './keys.js';
import type {
  DequeueOptions,
  EnqueueResult,
  JobQueue,
  JobQueueOptions,
  QueueDelivery,
  QueueMessage,
  QueueMessageInput,
  ReclaimOptions,
} from './types.js';

const DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 30;
const DEFAULT_MAX_PAYLOAD_SIZE_BYTES = 64 * 1024; // 64 KB
const DEFAULT_RECLAIM_BATCH_SIZE = 50;

/**
 * Atomic Lua script for FIFO enqueue.
 *
 * KEYS[1]: readyKey
 * KEYS[2]: messagesKey
 * KEYS[3]: metaKey
 * ARGV[1]: messageId
 * ARGV[2]: payloadJson
 * ARGV[3]: enqueuedAt
 *
 * Returns 1 if enqueued, 0 if messageId already exists in messagesKey.
 */
const ENQUEUE_LUA_SCRIPT = `
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], cjson.encode({ deliveryCount = 0, enqueuedAt = ARGV[3] }))
redis.call('LPUSH', KEYS[1], ARGV[1])
return 1
`;

/**
 * Atomic Lua script for FIFO dequeue with visibility timeout.
 *
 * KEYS[1]: readyKey
 * KEYS[2]: messagesKey
 * KEYS[3]: inFlightKey
 * KEYS[4]: metaKey
 * ARGV[1]: nowMs
 * ARGV[2]: visibilityTimeoutMs
 * ARGV[3]: nowIso
 * ARGV[4]: deliveryId
 *
 * Returns table { messageId, payloadJson, deliveryCountStr, expiresAtMsStr, deliveredAtIso, deliveryId }
 * or nil if ready list is empty.
 */
const DEQUEUE_LUA_SCRIPT = `
local messageId = redis.call('RPOP', KEYS[1])
if not messageId then
  return nil
end

local payload = redis.call('HGET', KEYS[2], messageId)
if not payload then
  redis.call('ZREM', KEYS[3], messageId)
  redis.call('HDEL', KEYS[4], messageId)
  return nil
end

local metaRaw = redis.call('HGET', KEYS[4], messageId)
local deliveryCount = 1
local enqueuedAt = ARGV[3]
if metaRaw then
  local meta = cjson.decode(metaRaw)
  if meta.deliveryCount then
    deliveryCount = meta.deliveryCount + 1
  end
  if meta.enqueuedAt then
    enqueuedAt = meta.enqueuedAt
  end
end

local updatedMeta = cjson.encode({
  deliveryCount = deliveryCount,
  enqueuedAt = enqueuedAt,
  deliveredAt = ARGV[3],
  deliveryId = ARGV[4]
})
redis.call('HSET', KEYS[4], messageId, updatedMeta)

local expiresAtMs = tonumber(ARGV[1]) + tonumber(ARGV[2])
redis.call('ZADD', KEYS[3], expiresAtMs, messageId)

return { messageId, payload, tostring(deliveryCount), tostring(expiresAtMs), ARGV[3], ARGV[4] }
`;

/**
 * Atomic Lua script for explicit acknowledgement.
 *
 * KEYS[1]: inFlightKey
 * KEYS[2]: messagesKey
 * KEYS[3]: metaKey
 * ARGV[1]: messageId
 *
 * Returns 1 if removed from in_flight or messages, 0 if not found / already acknowledged.
 */
const ACK_LUA_SCRIPT = `
local removedInFlight = redis.call('ZREM', KEYS[1], ARGV[1])
local removedMsg = redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])

if removedInFlight > 0 or removedMsg > 0 then
  return 1
else
  return 0
end
`;

/**
 * Atomic Lua script for reclaiming expired in-flight messages.
 *
 * KEYS[1]: inFlightKey
 * KEYS[2]: readyKey
 * KEYS[3]: messagesKey
 * KEYS[4]: metaKey
 * ARGV[1]: nowMs
 * ARGV[2]: batchSize
 *
 * Scans in_flight where score <= nowMs, moves valid messages back to ready list with RPUSH
 * so that retried messages get priority at the head of the pop queue.
 * Returns the count of reclaimed messages.
 */
const RECLAIM_LUA_SCRIPT = `
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local count = 0
for _, messageId in ipairs(expired) do
  local exists = redis.call('HEXISTS', KEYS[3], messageId)
  if exists == 1 then
    redis.call('ZREM', KEYS[1], messageId)
    redis.call('RPUSH', KEYS[2], messageId)
    count = count + 1
  else
    redis.call('ZREM', KEYS[1], messageId)
    redis.call('HDEL', KEYS[4], messageId)
  end
end
return count
`;

class RedisJobQueue implements JobQueue {
  public readonly queueName: string;
  private readonly defaultVisibilityTimeoutSeconds: number;
  private readonly maxPayloadSizeBytes: number;
  private readonly readyKey: string;
  private readonly messagesKey: string;
  private readonly inFlightKey: string;
  private readonly metadataKey: string;

  constructor(
    private readonly redisClient: RedisClient,
    options: JobQueueOptions,
    private readonly logger?: Logger,
  ) {
    const trimmedName = options.queueName?.trim();
    if (!trimmedName) {
      throw new QueueValidationError('Queue name cannot be empty');
    }

    if (
      options.defaultVisibilityTimeoutSeconds !== undefined &&
      (options.defaultVisibilityTimeoutSeconds <= 0 ||
        !Number.isFinite(options.defaultVisibilityTimeoutSeconds))
    ) {
      throw new QueueValidationError('defaultVisibilityTimeoutSeconds must be a positive number');
    }

    if (
      options.maxPayloadSizeBytes !== undefined &&
      (options.maxPayloadSizeBytes <= 0 || !Number.isFinite(options.maxPayloadSizeBytes))
    ) {
      throw new QueueValidationError('maxPayloadSizeBytes must be a positive number');
    }

    this.queueName = trimmedName;
    this.defaultVisibilityTimeoutSeconds =
      options.defaultVisibilityTimeoutSeconds ?? DEFAULT_VISIBILITY_TIMEOUT_SECONDS;
    this.maxPayloadSizeBytes = options.maxPayloadSizeBytes ?? DEFAULT_MAX_PAYLOAD_SIZE_BYTES;

    this.readyKey = getReadyKey(this.queueName);
    this.messagesKey = getMessagesKey(this.queueName);
    this.inFlightKey = getInFlightKey(this.queueName);
    this.metadataKey = getMetadataKey(this.queueName);
  }

  public async enqueue(input: QueueMessageInput): Promise<EnqueueResult> {
    this.validateMessageInput(input);

    const messageId =
      input.customMessageId && input.customMessageId.trim()
        ? input.customMessageId.trim()
        : `msg_${randomUUID()}`;

    const message: QueueMessage = {
      messageId,
      jobId: input.jobId.trim(),
      pipelineRunId: input.pipelineRunId.trim(),
      stepName: input.stepName.trim(),
      attemptNumber: input.attemptNumber ?? 1,
      enqueuedAt: new Date().toISOString(),
    };

    let payloadJson: string;
    try {
      payloadJson = serializeJson(message);
    } catch (err) {
      throw new QueueValidationError(
        `Failed to serialize queue message: ${(err as Error).message}`,
        err as Error,
      );
    }

    const payloadBytes = Buffer.byteLength(payloadJson, 'utf8');
    if (payloadBytes > this.maxPayloadSizeBytes) {
      throw new QueueValidationError(
        `Queue message payload size (${payloadBytes} bytes) exceeds configured maximum limit of ${this.maxPayloadSizeBytes} bytes`,
      );
    }

    try {
      const result = await this.redisClient.eval(
        ENQUEUE_LUA_SCRIPT,
        3,
        this.readyKey,
        this.messagesKey,
        this.metadataKey,
        message.messageId,
        payloadJson,
        message.enqueuedAt,
      );

      const deduplicated = result === 0;

      return {
        messageId: message.messageId,
        enqueuedAt: message.enqueuedAt,
        deduplicated,
      };
    } catch (err) {
      this.handleError(err, 'enqueue');
    }
  }

  public async dequeue(options?: DequeueOptions): Promise<QueueDelivery | null> {
    const visibilitySeconds =
      options?.visibilityTimeoutSeconds ?? this.defaultVisibilityTimeoutSeconds;

    if (visibilitySeconds <= 0 || !Number.isFinite(visibilitySeconds)) {
      throw new QueueValidationError('visibilityTimeoutSeconds must be a positive number');
    }

    const nowMs = Date.now();
    const visibilityTimeoutMs = Math.round(visibilitySeconds * 1000);
    const nowIso = new Date(nowMs).toISOString();
    const deliveryId = `del_${randomUUID()}`;

    try {
      const result = (await this.redisClient.eval(
        DEQUEUE_LUA_SCRIPT,
        4,
        this.readyKey,
        this.messagesKey,
        this.inFlightKey,
        this.metadataKey,
        nowMs,
        visibilityTimeoutMs,
        nowIso,
        deliveryId,
      )) as [string, string, string, string, string, string] | null;

      if (!result) {
        return null;
      }

      const [, payloadJson, deliveryCountStr, expiresAtMsStr, deliveredAtIso, delId] = result;
      const message = deserializeJson<QueueMessage>(payloadJson);
      const deliveryCount = Number(deliveryCountStr);
      const visibilityExpiresAt = new Date(Number(expiresAtMsStr)).toISOString();

      return {
        message,
        deliveryId: delId,
        deliveredAt: deliveredAtIso,
        visibilityExpiresAt,
        deliveryCount,
      };
    } catch (err) {
      this.handleError(err, 'dequeue');
    }
  }

  public async acknowledge(messageId: string): Promise<boolean> {
    const trimmedId = messageId?.trim();
    if (!trimmedId) {
      throw new QueueValidationError('messageId cannot be empty');
    }

    try {
      const result = await this.redisClient.eval(
        ACK_LUA_SCRIPT,
        3,
        this.inFlightKey,
        this.messagesKey,
        this.metadataKey,
        trimmedId,
      );

      return result === 1;
    } catch (err) {
      this.handleError(err, 'acknowledge');
    }
  }

  public async depth(): Promise<number> {
    try {
      const raw = this.redisClient.getRawClient();
      return await raw.llen(this.readyKey);
    } catch (err) {
      this.handleError(err, 'depth');
    }
  }

  public async inFlightCount(): Promise<number> {
    try {
      const raw = this.redisClient.getRawClient();
      return await raw.zcard(this.inFlightKey);
    } catch (err) {
      this.handleError(err, 'inFlightCount');
    }
  }

  public async reclaimExpired(options?: ReclaimOptions): Promise<number> {
    const batchSize = options?.batchSize ?? DEFAULT_RECLAIM_BATCH_SIZE;
    if (batchSize <= 0 || !Number.isFinite(batchSize)) {
      throw new QueueValidationError('batchSize must be a positive integer');
    }

    const nowMs = Date.now();

    try {
      const reclaimedCount = (await this.redisClient.eval(
        RECLAIM_LUA_SCRIPT,
        4,
        this.inFlightKey,
        this.readyKey,
        this.messagesKey,
        this.metadataKey,
        nowMs,
        batchSize,
      )) as number;

      return reclaimedCount;
    } catch (err) {
      this.handleError(err, 'reclaimExpired');
    }
  }

  public async close(): Promise<void> {
    await this.redisClient.close();
  }

  private validateMessageInput(input: QueueMessageInput): void {
    if (!input || typeof input !== 'object') {
      throw new QueueValidationError('Queue message input must be an object');
    }

    if (!input.jobId || typeof input.jobId !== 'string' || !input.jobId.trim()) {
      throw new QueueValidationError('jobId must be a non-empty string');
    }

    if (
      !input.pipelineRunId ||
      typeof input.pipelineRunId !== 'string' ||
      !input.pipelineRunId.trim()
    ) {
      throw new QueueValidationError('pipelineRunId must be a non-empty string');
    }

    if (!input.stepName || typeof input.stepName !== 'string' || !input.stepName.trim()) {
      throw new QueueValidationError('stepName must be a non-empty string');
    }

    if (input.attemptNumber !== undefined) {
      if (
        typeof input.attemptNumber !== 'number' ||
        !Number.isInteger(input.attemptNumber) ||
        input.attemptNumber < 1
      ) {
        throw new QueueValidationError('attemptNumber must be a positive integer (>= 1)');
      }
    }
  }

  private handleError(err: unknown, operation: string): never {
    if (err instanceof QueueError) {
      throw err;
    }

    const error = err as Error;
    const msg = error?.message ?? String(err);

    this.logger?.error('Queue operation failed', {
      queue: this.queueName,
      operation,
      error: msg,
    });

    if (
      msg.includes('Connection is closed') ||
      msg.includes('connection is closed') ||
      msg.includes('ECONNREFUSED') ||
      msg.includes('ENOTFOUND') ||
      msg.includes('unreachable') ||
      msg.includes('offline') ||
      msg.includes('Stream is not readable') ||
      msg.includes('Redis service is currently unavailable') ||
      msg.includes('max retries per request limit') ||
      msg.includes('MaxRetriesPerRequestError') ||
      error?.name === 'RedisConnectionError' ||
      error?.name === 'RedisUnavailableError'
    ) {
      throw new QueueUnavailableError(
        `Queue "${this.queueName}" is unavailable during ${operation}: ${msg}`,
        error,
      );
    }

    throw new QueueOperationError(
      `Queue "${this.queueName}" operation "${operation}" failed: ${msg}`,
      error,
    );
  }
}

/**
 * Factory function creating a new JobQueue instance.
 */
export function createJobQueue(
  redisClient: RedisClient,
  options: JobQueueOptions,
  logger?: Logger,
): JobQueue {
  return new RedisJobQueue(redisClient, options, logger);
}
