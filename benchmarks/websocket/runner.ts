/**
 * WebSocket gateway latency benchmark.
 *
 *   npx tsx benchmarks/websocket/runner.ts
 *
 * For each fan-out size it stands up a real gateway (real `ws` server) on an ephemeral
 * port, connects N real `ws` clients all subscribed to one run, and publishes events
 * through the real `RedisEventPublisher` -> Redis Pub/Sub path. Per event it measures the
 * wall-clock delay from just before `publish()` to the moment the *first* subscribed
 * client receives it, and separately the delay until *all* N clients have received it
 * (full fan-out). Requires a reachable Redis; it fails loudly if Redis is down and never
 * retries to force completion.
 *
 * Reported numbers are measurements, not guarantees. They characterise this machine and
 * this loopback; they are not a production-scale or throughput claim.
 */
import * as os from 'node:os';
import * as path from 'node:path';
import * as fs from 'node:fs/promises';
import { once } from 'node:events';
import { execSync } from 'node:child_process';
import { performance } from 'node:perf_hooks';
import { WebSocket } from 'ws';
import { loadConfig } from '@forge/config';
import { createLogger } from '@forge/logging';
import { createForgeEvent, type ForgeEvent } from '@forge/events';
import { RedisEventPublisher, RedisEventSubscriber } from '@forge/realtime';
import { createRedisPubSub } from '@forge/redis';
import {
  AllowAuthenticatedAuthorizer,
  createGateway,
  gatewayConfigFromAppConfig,
  type RealtimeGateway,
} from '@forge/realtime-gateway';
import { calculateStats, type SampleStats } from '../scheduler/utils/timer.js';
import type { EnvironmentInfo } from '../scheduler/utils/reporter.js';
import { websocketBenchmarkConfig as cfg } from './config.js';

const RUN_ID = 'bench-run';
const CHANNEL = `forge:bench:ws:${Date.now()}`;

interface PhaseResult {
  readonly phase: 'lifecycle' | 'logchunk';
  readonly fanout: number;
  readonly events: number;
  readonly payloadBytes: number;
  readonly firstReceiptMs: SampleStats;
  readonly fullFanoutMs: SampleStats;
  readonly throughputEventsPerSec: number;
  readonly rssDeltaBytes: number;
  readonly cpuUserMicros: number;
  readonly cpuSystemMicros: number;
  readonly clientCloses: number;
}

interface WebsocketBenchmarkReport {
  readonly environment: EnvironmentInfo;
  readonly configuration: typeof cfg;
  readonly results: readonly PhaseResult[];
  readonly generatedAt: string;
}

function lifecycleEvent(seq: number): ForgeEvent {
  return createForgeEvent('JobSucceeded', {
    correlation: {
      run_id: RUN_ID,
      job_id: `${RUN_ID}-job-${seq}`,
      attempt_id: 'a',
      worker_id: 'w',
    },
    payload: {
      job_id: `${RUN_ID}-job-${seq}`,
      attempt_id: 'a',
      worker_id: 'w',
      attempt_number: 1,
      duration_ms: seq,
      exit_code: 0,
    },
  });
}

function logChunkEvent(seq: number): ForgeEvent {
  return createForgeEvent('JobLogChunk', {
    correlation: { run_id: RUN_ID, job_id: `${RUN_ID}-job`, attempt_id: 'a' },
    payload: {
      job_id: `${RUN_ID}-job`,
      attempt_id: 'a',
      sequence: seq,
      stream: 'stdout',
      chunk: 'x'.repeat(cfg.logChunkBytes),
      byte_offset: seq * cfg.logChunkBytes,
      truncated: false,
      final: false,
    },
  });
}

async function collectEnvironmentInfo(redisVersion: string): Promise<EnvironmentInfo> {
  let gitCommit = 'unknown';
  let gitBranch = 'unknown';
  try {
    gitCommit = execSync('git rev-parse HEAD', { encoding: 'utf-8' }).trim();
    gitBranch = execSync('git rev-parse --abbrev-ref HEAD', { encoding: 'utf-8' }).trim();
  } catch {
    /* git may be unavailable */
  }
  const cpus = os.cpus();
  return {
    gitCommit,
    gitBranch,
    platform: process.platform,
    arch: process.arch,
    cpuModel: cpus[0]?.model ?? 'unknown',
    cpuCores: cpus.length,
    totalMemoryBytes: os.totalmem(),
    freeMemoryBytes: os.freemem(),
    nodeVersion: process.version,
    postgresVersion: 'not-applicable',
    redisVersion,
    timestamp: new Date().toISOString(),
  };
}

async function startGateway(): Promise<{
  gateway: RealtimeGateway;
  port: number;
  publisher: RedisEventPublisher;
  stop: () => Promise<void>;
}> {
  const logger = createLogger({
    service: 'realtime-gateway',
    environment: 'test',
    minLevel: 'error',
  });
  const appConfig = loadConfig({
    WEBSOCKET_AUTH_TOKEN: cfg.authToken,
    WEBSOCKET_MAX_CONNECTIONS: '512',
    WEBSOCKET_MAX_PENDING_MESSAGES: '2048',
    REALTIME_REDIS_CHANNEL: CHANNEL,
    REDIS_URL: cfg.redisUrl,
  });

  const subPubSub = createRedisPubSub({ url: cfg.redisUrl }, logger);
  await subPubSub.connect();
  const subscriber = new RedisEventSubscriber({ pubsub: subPubSub, channel: CHANNEL, logger });

  const gateway = createGateway({
    config: { ...gatewayConfigFromAppConfig(appConfig), port: 0 },
    subscriber,
    authorizer: new AllowAuthenticatedAuthorizer(),
    logger,
  });
  await gateway.start();
  const port = gateway.address()?.port;
  if (!port) {
    throw new Error('benchmark gateway failed to bind a port');
  }

  const pubPubSub = createRedisPubSub({ url: cfg.redisUrl }, logger);
  await pubPubSub.connect();
  const publisher = new RedisEventPublisher({ pubsub: pubPubSub, channel: CHANNEL, logger });

  return {
    gateway,
    port,
    publisher,
    stop: async () => {
      await gateway.stop();
      await pubPubSub.close();
      await subPubSub.close();
    },
  };
}

async function connectClients(port: number, count: number): Promise<WebSocket[]> {
  const clients: WebSocket[] = [];
  for (let i = 0; i < count; i += 1) {
    const ws = new WebSocket(`ws://127.0.0.1:${port}`, {
      headers: { authorization: `Bearer ${cfg.authToken}` },
    });
    // Attach the message handler BEFORE `open` so the gateway's immediate `ready` frame is
    // never dropped (it is sent synchronously as the connection is accepted).
    const subscribed = new Promise<void>((resolve) => {
      const onMsg = (data: unknown): void => {
        const msg = JSON.parse(String(data)) as { type: string };
        if (msg.type === 'ready') {
          ws.send(JSON.stringify({ type: 'subscribe', v: 1, target: { kind: 'run', id: RUN_ID } }));
        } else if (msg.type === 'subscribed') {
          ws.off('message', onMsg);
          resolve();
        }
      };
      ws.on('message', onMsg);
    });
    await once(ws, 'open');
    await subscribed;
    clients.push(ws);
  }
  return clients;
}

interface PendingReceipt {
  firstAt?: number;
  remaining: number;
  resolveAll: () => void;
}

async function runPhase(
  phase: 'lifecycle' | 'logchunk',
  fanout: number,
  make: (seq: number) => ForgeEvent,
  eventCount: number,
): Promise<PhaseResult> {
  const gw = await startGateway();
  const publisher = gw.publisher;
  const clients = await connectClients(gw.port, fanout);

  const pending = new Map<string, PendingReceipt>();
  for (const ws of clients) {
    ws.on('message', (data) => {
      const msg = JSON.parse(String(data)) as { type: string; event?: { event_id: string } };
      if (msg.type !== 'event' || !msg.event) return;
      const p = pending.get(msg.event.event_id);
      if (!p) return;
      if (p.firstAt === undefined) {
        p.firstAt = performance.now();
      }
      p.remaining -= 1;
      if (p.remaining === 0) {
        p.resolveAll();
      }
    });
  }

  const publishOne = async (seq: number): Promise<{ first: number; all: number }> => {
    const event = make(seq);
    let resolveAll!: () => void;
    const allPromise = new Promise<void>((r) => (resolveAll = r));
    const entry: PendingReceipt = { remaining: fanout, resolveAll };
    pending.set(event.event_id, entry);

    const timeout = new Promise<never>((_, reject) =>
      setTimeout(
        () =>
          reject(
            new Error(`event ${event.event_id} not received within ${cfg.receiptTimeoutMs}ms`),
          ),
        cfg.receiptTimeoutMs,
      ).unref(),
    );

    const t0 = performance.now();
    await publisher.publish(event);
    await Promise.race([allPromise, timeout]);
    pending.delete(event.event_id);
    return {
      first: (entry.firstAt ?? performance.now()) - t0,
      all: performance.now() - t0,
    };
  };

  // Warmup (unrecorded).
  for (let i = 0; i < cfg.warmupEvents; i += 1) {
    await publishOne(900_000 + i);
  }

  const firstSamples: number[] = [];
  const allSamples: number[] = [];
  let closes = 0;
  for (const ws of clients) ws.on('close', () => (closes += 1));

  const memBefore = process.memoryUsage();
  const cpuBefore = process.cpuUsage();
  const wallStart = performance.now();

  for (let i = 0; i < eventCount; i += 1) {
    const { first, all } = await publishOne(i);
    firstSamples.push(first);
    allSamples.push(all);
  }

  const wallMs = performance.now() - wallStart;
  const cpu = process.cpuUsage(cpuBefore);
  const mem = process.memoryUsage();

  for (const ws of clients) ws.close();
  await gw.stop();

  return {
    phase,
    fanout,
    events: eventCount,
    payloadBytes:
      phase === 'logchunk' ? cfg.logChunkBytes : Buffer.byteLength(JSON.stringify(make(0))),
    firstReceiptMs: calculateStats(firstSamples),
    fullFanoutMs: calculateStats(allSamples),
    throughputEventsPerSec: Number(((eventCount / wallMs) * 1000).toFixed(1)),
    rssDeltaBytes: mem.rss - memBefore.rss,
    cpuUserMicros: cpu.user,
    cpuSystemMicros: cpu.system,
    clientCloses: closes,
  };
}

function formatConsole(report: WebsocketBenchmarkReport): string {
  const L: string[] = [];
  L.push('================================================================================');
  L.push('                  FORGE V2 WEBSOCKET GATEWAY BENCHMARK REPORT                   ');
  L.push('================================================================================');
  L.push(`Date:       ${report.generatedAt}`);
  L.push(`Commit:     ${report.environment.gitCommit} (${report.environment.gitBranch})`);
  L.push(`Platform:   ${report.environment.platform} ${report.environment.arch}`);
  L.push(`CPU:        ${report.environment.cpuModel} (${report.environment.cpuCores} cores)`);
  L.push(`Node.js:    ${report.environment.nodeVersion}`);
  L.push(`Redis:      ${report.environment.redisVersion}`);
  L.push('================================================================================\n');

  const headers = [
    'Phase',
    'Fan-out',
    'Events',
    'Bytes',
    'first mean',
    'first p50',
    'first p95',
    'first p99',
    'first min',
    'first max',
    'full p95',
    'evt/s',
    'RSS Δ MB',
  ];
  const rows = report.results.map((r) => [
    r.phase,
    String(r.fanout),
    String(r.events),
    String(r.payloadBytes),
    r.firstReceiptMs.mean.toFixed(3),
    r.firstReceiptMs.median.toFixed(3),
    r.firstReceiptMs.p95.toFixed(3),
    r.firstReceiptMs.p99.toFixed(3),
    r.firstReceiptMs.min.toFixed(3),
    r.firstReceiptMs.max.toFixed(3),
    r.fullFanoutMs.p95.toFixed(3),
    r.throughputEventsPerSec.toLocaleString('en-US'),
    (r.rssDeltaBytes / (1024 * 1024)).toFixed(2),
  ]);
  const w = headers.map((h, i) => Math.max(h.length, ...rows.map((row) => row[i]!.length)));
  const line = (cells: readonly string[]): string =>
    '| ' + cells.map((c, i) => c.padEnd(w[i]!)).join(' | ') + ' |';
  L.push(line(headers));
  L.push('| ' + w.map((x) => '-'.repeat(x)).join(' | ') + ' |');
  for (const row of rows) L.push(line(row));
  L.push('\nLatency is publish() -> first client receipt (ms). "full p95" is publish() -> all');
  L.push('N clients received. Measurements only — not a production-scale or throughput claim.');
  L.push('\n================================================================================\n');
  return L.join('\n');
}

export async function runWebsocketBenchmark(): Promise<WebsocketBenchmarkReport> {
  // Fail loudly if Redis is unreachable — never retried to force completion.
  const probe = createRedisPubSub({ url: cfg.redisUrl, maxRetriesPerRequest: 1 });
  try {
    await probe.connect();
  } catch (err) {
    throw new Error(
      `WebSocket benchmark requires a reachable Redis at ${cfg.redisUrl} (docker compose up -d): ${
        err instanceof Error ? err.message : String(err)
      }`,
    );
  } finally {
    await probe.close();
  }

  const environment = await collectEnvironmentInfo('reachable');
  const results: PhaseResult[] = [];

  for (const fanout of cfg.fanoutSizes) {
    console.log(`-> lifecycle events, fan-out ${fanout} ...`);
    results.push(await runPhase('lifecycle', fanout, lifecycleEvent, cfg.lifecycleEvents));
  }
  for (const fanout of cfg.fanoutSizes) {
    console.log(`-> JobLogChunk events (${cfg.logChunkBytes} B), fan-out ${fanout} ...`);
    results.push(await runPhase('logchunk', fanout, logChunkEvent, cfg.logChunkEvents));
  }

  const report: WebsocketBenchmarkReport = {
    environment,
    configuration: cfg,
    results,
    generatedAt: new Date().toISOString(),
  };

  console.log(formatConsole(report));

  const reportPath = path.resolve(
    process.cwd(),
    'benchmarks',
    'reports',
    'websocket-benchmark-report.json',
  );
  await fs.mkdir(path.dirname(reportPath), { recursive: true });
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2), 'utf-8');
  console.log(`Benchmark report JSON saved to: ${reportPath}\n`);
  return report;
}

const isDirectRun =
  process.argv[1] &&
  (process.argv[1].endsWith('runner.ts') || process.argv[1].endsWith('runner.js'));

if (isDirectRun) {
  runWebsocketBenchmark()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error(
        'WebSocket benchmark execution failed:',
        err instanceof Error ? (err.stack ?? err.message) : String(err),
      );
      process.exit(1);
    });
}
