import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import { beforeAll, describe, expect, it } from 'vitest';
import { DockerExecutor } from './docker-executor.js';

const execAsync = promisify(exec);

describe('DockerExecutor Concurrency Suite (Real Parallel Containers)', () => {
  const dockerHost = process.env.DOCKER_HOST ?? 'tcp://127.0.0.1:2375';
  let executor: DockerExecutor;
  let dockerAvailable = false;

  beforeAll(async () => {
    executor = new DockerExecutor({
      dockerHost,
      defaultImage: 'alpine:3.19',
      defaultTimeoutMs: 60000,
      user: '1000:1000',
    });

    dockerAvailable = await executor.isAvailable();
  });

  async function runConcurrentJobs(batchSize: number, label: string) {
    if (!dockerAvailable) return;

    const startTime = Date.now();
    const jobs = Array.from({ length: batchSize }, (_, i) => ({
      jobId: `bench-conc-${label}-${i}-${Date.now()}`,
      attemptId: `att-${i}`,
      workerId: `worker-conc-${i}`,
      payloadTag: `payload-${label}-${i}-${Math.random().toString(36).slice(2, 8)}`,
    }));

    const results = await Promise.all(
      jobs.map((job) =>
        executor.execute({
          jobId: job.jobId,
          attemptId: job.attemptId,
          workerId: job.workerId,
          command: `echo "OUTPUT=${job.payloadTag}" && sleep 0.2`,
        }),
      ),
    );

    const totalDurationMs = Date.now() - startTime;

    // 1. Verify every execution succeeded
    for (let i = 0; i < batchSize; i++) {
      const res = results[i];
      const job = jobs[i];
      expect(res).toBeDefined();
      if (!res || !job) continue;
      expect(res.status).toBe('SUCCEEDED');
      expect(res.exitCode).toBe(0);
      expect(res.stdout).toContain(`OUTPUT=${job.payloadTag}`);

      // Verify no cross-talk: output of job i must NOT contain other job payloads
      for (let j = 0; j < batchSize; j++) {
        if (i !== j) {
          const otherJob = jobs[j];
          if (otherJob) {
            expect(res.stdout).not.toContain(`OUTPUT=${otherJob.payloadTag}`);
          }
        }
      }
    }

    // 2. Verify zero residual containers remain
    const { stdout } = await execAsync(
      `docker ps -a --filter "name=forge-exec-bench-conc-${label}-" --format "{{.Names}}"`,
      {
        env: { ...process.env, DOCKER_HOST: dockerHost },
      },
    );
    expect(stdout.trim()).toBe('');

    return {
      batchSize,
      totalDurationMs,
      avgPerJobMs: Math.round(totalDurationMs / batchSize),
    };
  }

  it('runs 2 concurrent Docker executions with strict isolation and zero container leaks', async () => {
    if (!dockerAvailable) return;
    const metrics = await runConcurrentJobs(2, 'batch2');
    expect(metrics?.batchSize).toBe(2);
  });

  it('runs 5 concurrent Docker executions with strict isolation and zero container leaks', async () => {
    if (!dockerAvailable) return;
    const metrics = await runConcurrentJobs(5, 'batch5');
    expect(metrics?.batchSize).toBe(5);
  });

  it('runs 10 concurrent Docker executions with strict isolation and zero container leaks', async () => {
    if (!dockerAvailable) return;
    const metrics = await runConcurrentJobs(10, 'batch10');
    expect(metrics?.batchSize).toBe(10);
  });
});
