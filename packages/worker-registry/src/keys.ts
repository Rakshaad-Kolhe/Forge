import { createRedisKey } from '@forge/redis';

/**
 * Returns the standardized Redis key for a worker's transient heartbeat.
 * Key convention: forge:worker:{workerId}:heartbeat
 */
export function getWorkerHeartbeatKey(workerId: string): string {
  return createRedisKey('worker', workerId, 'heartbeat');
}
