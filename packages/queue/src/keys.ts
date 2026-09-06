import { createRedisKey } from '@forge/redis';

/**
 * Returns the Redis key for the FIFO ready list.
 * Messages are pushed onto this list via LPUSH and popped via RPOP.
 * Key convention: forge:queue:{queueName}:ready
 */
export function getReadyKey(queueName: string): string {
  return createRedisKey('queue', queueName, 'ready');
}

/**
 * Returns the Redis key for the message payload hash.
 * Stores JSON-serialized payloads mapped by messageId.
 * Key convention: forge:queue:{queueName}:messages
 */
export function getMessagesKey(queueName: string): string {
  return createRedisKey('queue', queueName, 'messages');
}

/**
 * Returns the Redis key for the in-flight sorted set.
 * Maps messageId to visibility expiration timestamp in milliseconds.
 * Key convention: forge:queue:{queueName}:in_flight
 */
export function getInFlightKey(queueName: string): string {
  return createRedisKey('queue', queueName, 'in_flight');
}

/**
 * Returns the Redis key for delivery metadata hash.
 * Stores delivery counters, enqueue timestamps, and delivery IDs mapped by messageId.
 * Key convention: forge:queue:{queueName}:meta
 */
export function getMetadataKey(queueName: string): string {
  return createRedisKey('queue', queueName, 'meta');
}
