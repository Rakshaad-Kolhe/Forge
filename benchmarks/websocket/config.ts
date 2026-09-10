/**
 * Static configuration for the WebSocket gateway latency benchmark.
 *
 * Mirrors the shape of `benchmarks/outbox/config.ts`: one frozen object, no wall-clock or
 * `Math.random()` seeding of the workload. Measures the end-to-end path
 *
 *   RedisEventPublisher.publish(event)  ->  Redis Pub/Sub  ->  gateway fan-out  ->  ws client receipt
 *
 * against a live Redis (`docker compose up -d`). No PostgreSQL, no Docker executor.
 */
export interface WebsocketBenchmarkConfig {
  /** Redis URL (env override, else the dev default). */
  readonly redisUrl: string;
  /** Shared-secret used for the benchmark gateway handshake. */
  readonly authToken: string;
  /** Fan-out sizes: number of subscribed clients receiving each published event. */
  readonly fanoutSizes: readonly number[];
  /** Measured lifecycle events published per fan-out size. */
  readonly lifecycleEvents: number;
  /** Measured bounded `JobLogChunk` events published per fan-out size. */
  readonly logChunkEvents: number;
  /** Bytes in each benchmarked `JobLogChunk` payload (a realistic bounded chunk). */
  readonly logChunkBytes: number;
  /** Unrecorded warmup events per phase. */
  readonly warmupEvents: number;
  /** Per-event receipt timeout (ms) — a miss fails the run loudly, never retried. */
  readonly receiptTimeoutMs: number;
}

export const websocketBenchmarkConfig: WebsocketBenchmarkConfig = {
  redisUrl: process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6379',
  authToken: 'benchmark-secret-token',
  fanoutSizes: [1, 10, 50, 100] as const,
  lifecycleEvents: 200,
  logChunkEvents: 100,
  logChunkBytes: 64 * 1024,
  warmupEvents: 20,
  receiptTimeoutMs: 5000,
};
