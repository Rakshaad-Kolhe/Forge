import {
  createJobAttemptId,
  createJobId,
  createPipelineId,
  createPipelineRunId,
  Job,
  JobAttempt,
  Pipeline,
  PipelineRun,
} from '@forge/pipeline';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDatabasePool } from '../client.js';
import { DEFAULT_DATABASE_URL } from '../config.js';
import { ConstraintViolationError, PersistenceError } from '../errors.js';
import { resetDatabase, runMigrations } from '../migrations/migrator.js';
import type { DatabasePool } from '../types.js';
import { PgJobAttemptRepository } from './pg-job-attempt-repository.js';
import { PgJobRepository } from './pg-job-repository.js';
import { PgPipelineRepository } from './pg-pipeline-repository.js';
import { PgPipelineRunRepository } from './pg-pipeline-run-repository.js';

describe('PostgreSQL Repositories Integration Tests', () => {
  let pool: DatabasePool;
  let pipelineRepo: PgPipelineRepository;
  let pipelineRunRepo: PgPipelineRunRepository;
  let jobRepo: PgJobRepository;
  let jobAttemptRepo: PgJobAttemptRepository;

  beforeAll(async () => {
    pool = createDatabasePool({
      connectionString: DEFAULT_DATABASE_URL,
    });
    await resetDatabase(pool);
    await runMigrations(pool);

    pipelineRepo = new PgPipelineRepository(pool);
    pipelineRunRepo = new PgPipelineRunRepository(pool);
    jobRepo = new PgJobRepository(pool);
    jobAttemptRepo = new PgJobAttemptRepository(pool);
  });

  afterAll(async () => {
    await resetDatabase(pool);
    await pool.close();
  });

  beforeEach(async () => {
    // Clean tables between tests to ensure deterministic isolation
    await pool.query('DELETE FROM job_attempts;');
    await pool.query('DELETE FROM jobs;');
    await pool.query('DELETE FROM pipeline_runs;');
    await pool.query('DELETE FROM pipelines;');
  });

  describe('PgPipelineRepository', () => {
    it('creates, saves, reads back, and validates a pipeline with complete DAG', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-build-test',
        name: 'Build and Test',
        steps: [
          { name: 'lint', command: 'npm run lint' },
          { name: 'build', command: 'npm run build', dependsOn: ['lint'] },
          { name: 'test', command: 'npm test', dependsOn: ['build'] },
        ],
      });

      await pipelineRepo.save(pipeline);

      const loaded = await pipelineRepo.findById(pipeline.id);
      expect(loaded).not.toBeNull();
      expect(loaded!.id).toBe(pipeline.id);
      expect(loaded!.name).toBe('Build and Test');
      expect(loaded!.getSteps()).toHaveLength(3);
      expect(loaded!.getSteps()[0]?.name).toBe('lint');
      expect(loaded!.getSteps()[1]?.name).toBe('build');
      expect(loaded!.getSteps()[1]?.dependsOn).toEqual(['lint']);
      expect(loaded!.getSteps()[2]?.name).toBe('test');
      expect(loaded!.getSteps()[2]?.dependsOn).toEqual(['build']);

      // Verify DAG ordering is preserved deterministically
      const topoSteps = loaded!.getDag().getTopologicalOrder();
      expect(topoSteps).toEqual(['lint', 'build', 'test']);

      // Verify JSON representation equivalence
      expect(loaded!.toJSON()).toEqual(pipeline.toJSON());
    });

    it('returns null when pipeline is not found', async () => {
      const nonExistent = await pipelineRepo.findById(createPipelineId('does-not-exist'));
      expect(nonExistent).toBeNull();
    });

    it('lists all stored pipelines', async () => {
      const p1 = new Pipeline({
        id: 'pipe-1',
        name: 'Pipeline 1',
        steps: [{ name: 's1', command: 'echo 1' }],
      });
      const p2 = new Pipeline({
        id: 'pipe-2',
        name: 'Pipeline 2',
        steps: [{ name: 's2', command: 'echo 2' }],
      });

      await pipelineRepo.save(p1);
      await pipelineRepo.save(p2);

      const list = await pipelineRepo.list();
      expect(list).toHaveLength(2);
      expect(list.map((p) => p.id)).toContain('pipe-1');
      expect(list.map((p) => p.id)).toContain('pipe-2');
    });

    it('deletes an existing pipeline and returns false when deleting non-existent', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-to-delete',
        name: 'To Delete',
        steps: [{ name: 'step-1', command: 'echo delete' }],
      });

      await pipelineRepo.save(pipeline);
      const deleted = await pipelineRepo.delete(pipeline.id);
      expect(deleted).toBe(true);

      const missing = await pipelineRepo.findById(pipeline.id);
      expect(missing).toBeNull();

      const deletedAgain = await pipelineRepo.delete(pipeline.id);
      expect(deletedAgain).toBe(false);
    });

    it('prevents deleting pipeline when existing pipeline runs reference it', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-protected',
        name: 'Protected Pipeline',
        steps: [{ name: 's1', command: 'echo 1' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-fk-1'), pipeline);
      await pipelineRunRepo.save(run);

      await expect(pipelineRepo.delete(pipeline.id)).rejects.toThrow(ConstraintViolationError);
    });
  });

  describe('PgPipelineRunRepository', () => {
    it('creates, saves, and rehydrates a pipeline run with constituent jobs', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-run-test',
        name: 'CI Workflow',
        steps: [
          { name: 'setup', command: 'npm ci' },
          { name: 'compile', command: 'npm run build', dependsOn: ['setup'] },
        ],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-101'), pipeline);
      expect(run.status).toBe('PENDING');
      expect(run.getJobs()).toHaveLength(2);

      await pipelineRunRepo.save(run);

      const loadedRun = await pipelineRunRepo.findById(run.id);
      expect(loadedRun).not.toBeNull();
      expect(loadedRun!.id).toBe(run.id);
      expect(loadedRun!.pipelineId).toBe(pipeline.id);
      expect(loadedRun!.pipelineName).toBe('CI Workflow');
      expect(loadedRun!.status).toBe('PENDING');

      // Verify constituent jobs rehydrated
      const loadedJobs = loadedRun!.getJobs();
      expect(loadedJobs).toHaveLength(2);
      expect(loadedJobs[0]?.stepName).toBe('setup');
      expect(loadedJobs[0]?.command).toBe('npm ci');
      expect(loadedJobs[1]?.stepName).toBe('compile');
      expect(loadedJobs[1]?.dependsOn).toEqual(['setup']);
    });

    it('updates pipeline run lifecycle status and timestamps through valid state transitions', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-status-test',
        name: 'Status Test',
        steps: [{ name: 'step', command: 'echo status' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-status-1'), pipeline);
      await pipelineRunRepo.save(run);

      expect(run.status).toBe('PENDING');

      // Valid transition: PENDING -> QUEUED
      run.markQueued();
      await pipelineRunRepo.save(run);
      let current = await pipelineRunRepo.findById(run.id);
      expect(current!.status).toBe('QUEUED');

      // Valid transition: QUEUED -> RUNNING
      const startedAt = new Date().toISOString();
      run.start(startedAt);
      await pipelineRunRepo.save(run);
      current = await pipelineRunRepo.findById(run.id);
      expect(current!.status).toBe('RUNNING');
      expect(current!.startedAt).toBeDefined();

      // Valid transition: RUNNING -> SUCCEEDED
      run.transitionTo('SUCCEEDED');
      await pipelineRunRepo.save(run);
      current = await pipelineRunRepo.findById(run.id);
      expect(current!.status).toBe('SUCCEEDED');
      expect(current!.finishedAt).toBeDefined();
    });

    it('rejects terminal state regression (SUCCEEDED -> RUNNING) and preserves persistent state', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-terminal-test',
        name: 'Terminal Test',
        steps: [{ name: 'step', command: 'echo terminal' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-terminal-1'), pipeline);
      run.markQueued();
      run.start();
      run.transitionTo('SUCCEEDED');
      await pipelineRunRepo.save(run);

      // Verify it is SUCCEEDED in database
      const saved = await pipelineRunRepo.findById(run.id);
      expect(saved!.status).toBe('SUCCEEDED');

      // Create a mutated run object attempting to regress to RUNNING
      const regressedRun = new PipelineRun({
        id: run.id,
        pipelineId: pipeline.id,
        pipelineName: pipeline.name,
        initialStatus: 'RUNNING',
      });

      // Attempting to save regressed run must throw InvalidStateTransitionError
      await expect(pipelineRunRepo.save(regressedRun)).rejects.toThrow();

      // Database must remain SUCCEEDED
      const afterAttempt = await pipelineRunRepo.findById(run.id);
      expect(afterAttempt!.status).toBe('SUCCEEDED');
    });

    it('rejects invalid state jumps (PENDING -> SUCCEEDED) and preserves persistent state', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-invalid-jump',
        name: 'Invalid Jump Test',
        steps: [{ name: 'step', command: 'echo jump' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-jump-1'), pipeline);
      await pipelineRunRepo.save(run);
      expect(run.status).toBe('PENDING');

      // Attempt to save run directly transitioning to SUCCEEDED
      const jumpedRun = new PipelineRun({
        id: run.id,
        pipelineId: pipeline.id,
        pipelineName: pipeline.name,
        initialStatus: 'SUCCEEDED',
      });

      await expect(pipelineRunRepo.save(jumpedRun)).rejects.toThrow();

      const afterAttempt = await pipelineRunRepo.findById(run.id);
      expect(afterAttempt!.status).toBe('PENDING');
    });

    it('allows same-state idempotent saves without error', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-idempotent',
        name: 'Idempotent Test',
        steps: [{ name: 'step', command: 'echo idemp' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-idemp-1'), pipeline);
      run.markQueued();
      run.start();
      await pipelineRunRepo.save(run);

      // Save again with same RUNNING status
      await expect(pipelineRunRepo.save(run)).resolves.not.toThrow();

      const loaded = await pipelineRunRepo.findById(run.id);
      expect(loaded!.status).toBe('RUNNING');
    });

    it('lists pipeline runs by pipelineId', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-multiple-runs',
        name: 'Multi Run',
        steps: [{ name: 'step', command: 'echo multi' }],
      });
      await pipelineRepo.save(pipeline);

      const r1 = PipelineRun.create(createPipelineRunId('run-m-1'), pipeline);
      const r2 = PipelineRun.create(createPipelineRunId('run-m-2'), pipeline);
      await pipelineRunRepo.save(r1);
      await pipelineRunRepo.save(r2);

      const runs = await pipelineRunRepo.findByPipelineId(pipeline.id);
      expect(runs).toHaveLength(2);
      expect(runs.map((r) => r.id)).toContain('run-m-1');
      expect(runs.map((r) => r.id)).toContain('run-m-2');
    });
  });

  describe('Domain Reconstruction Integrity', () => {
    it('throws PersistenceError when rehydrating a pipeline with cyclic DAG from database', async () => {
      // Directly insert malformed steps containing a cyclic dependency into PostgreSQL
      const cyclicSteps = JSON.stringify([
        { name: 'step-a', command: 'echo a', dependsOn: ['step-b'] },
        { name: 'step-b', command: 'echo b', dependsOn: ['step-a'] },
      ]);

      await pool.query(
        `INSERT INTO pipelines (id, name, steps, created_at, updated_at)
         VALUES ('pipe-corrupt-cycle', 'Corrupt Cycle', $1::jsonb, NOW(), NOW());`,
        [cyclicSteps],
      );

      await expect(pipelineRepo.findById(createPipelineId('pipe-corrupt-cycle'))).rejects.toThrow(
        PersistenceError,
      );
    });

    it('throws PersistenceError when rehydrating a pipeline with non-existent dependency', async () => {
      const brokenSteps = JSON.stringify([
        { name: 'step-a', command: 'echo a', dependsOn: ['non-existent-step'] },
      ]);

      await pool.query(
        `INSERT INTO pipelines (id, name, steps, created_at, updated_at)
         VALUES ('pipe-corrupt-dep', 'Corrupt Dep', $1::jsonb, NOW(), NOW());`,
        [brokenSteps],
      );

      await expect(pipelineRepo.findById(createPipelineId('pipe-corrupt-dep'))).rejects.toThrow(
        PersistenceError,
      );
    });
  });

  describe('PgJobRepository', () => {
    it('enforces UNIQUE(pipeline_run_id, step_name) duplicate step invariant', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-job-dup',
        name: 'Duplicate Step Test',
        steps: [{ name: 'build', command: 'echo build' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-job-dup'), pipeline);
      await pipelineRunRepo.save(run);

      // Attempt to save a second job with same stepName in the same run
      const duplicateJob = new Job({
        id: createJobId('job-second-duplicate'),
        pipelineRunId: run.id,
        stepName: 'build',
        command: 'echo other-build',
      });

      await expect(jobRepo.save(duplicateJob)).rejects.toThrow(ConstraintViolationError);
    });

    it('throws ConstraintViolationError when saving job for non-existent pipeline run', async () => {
      const orphanJob = new Job({
        id: createJobId('job-orphan'),
        pipelineRunId: createPipelineRunId('run-non-existent'),
        stepName: 'test',
        command: 'echo orphan',
      });

      await expect(jobRepo.save(orphanJob)).rejects.toThrow(ConstraintViolationError);
    });

    it('validates state transitions and rejects terminal state regression on jobs', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-job-status',
        name: 'Job Status Pipeline',
        steps: [{ name: 'task', command: 'echo task' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-job-stat'), pipeline);
      await pipelineRunRepo.save(run);

      const job = run.getJobs()[0]!;
      expect(job.status).toBe('PENDING');

      // Valid: PENDING -> QUEUED
      job.markQueued();
      await jobRepo.save(job);
      let updated = await jobRepo.findById(job.id);
      expect(updated!.status).toBe('QUEUED');

      // Valid: QUEUED -> RUNNING
      job.start();
      await jobRepo.save(job);
      updated = await jobRepo.findById(job.id);
      expect(updated!.status).toBe('RUNNING');

      // Valid: RUNNING -> SUCCEEDED
      job.succeed();
      await jobRepo.save(job);
      updated = await jobRepo.findById(job.id);
      expect(updated!.status).toBe('SUCCEEDED');

      // Terminal regression: attempt to save job as RUNNING
      const regressedJob = new Job({
        id: job.id,
        pipelineRunId: run.id,
        stepName: job.stepName,
        command: job.command,
        initialStatus: 'RUNNING',
      });

      await expect(jobRepo.save(regressedJob)).rejects.toThrow();

      // Verify database state remains SUCCEEDED
      const afterRegress = await jobRepo.findById(job.id);
      expect(afterRegress!.status).toBe('SUCCEEDED');
    });

    it('persists and reconstructs job execution requirements accurately', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-job-reqs',
        name: 'Job Requirements Pipeline',
        steps: [
          {
            name: 'heavy-task',
            command: 'cargo build --release',
            requirements: {
              executor: 'docker',
              cpuCores: 8,
              memoryBytes: 16 * 1024 * 1024 * 1024,
              gpuCount: 2,
            },
          },
        ],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-job-reqs-1'), pipeline);
      await pipelineRunRepo.save(run);

      const job = run.getJobs()[0]!;
      expect(job.requirements).toEqual({
        executor: 'docker',
        cpuCores: 8,
        memoryBytes: 16 * 1024 * 1024 * 1024,
        gpuCount: 2,
      });

      // Find via findById
      const loadedJob = await jobRepo.findById(job.id);
      expect(loadedJob).not.toBeNull();
      expect(loadedJob!.requirements).toEqual({
        executor: 'docker',
        cpuCores: 8,
        memoryBytes: 16 * 1024 * 1024 * 1024,
        gpuCount: 2,
      });

      // Find via findByPipelineRunId
      const loadedJobs = await jobRepo.findByPipelineRunId(run.id);
      expect(loadedJobs).toHaveLength(1);
      expect(loadedJobs[0]!.requirements).toEqual({
        executor: 'docker',
        cpuCores: 8,
        memoryBytes: 16 * 1024 * 1024 * 1024,
        gpuCount: 2,
      });
    });
  });

  describe('PgJobAttemptRepository', () => {
    it('preserves multiple sequential attempts immutably without overwriting history', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-attempts',
        name: 'Attempts Pipeline',
        steps: [{ name: 'flaky-step', command: 'npm test' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-attempts'), pipeline);
      await pipelineRunRepo.save(run);

      const job = run.getJobs()[0]!;

      // Attempt 1: starts and fails
      const attempt1 = job.createAttempt();
      attempt1.start();
      attempt1.fail(1, 'Network timeout connecting to upstream');
      await jobAttemptRepo.save(attempt1);

      // Attempt 2: starts and succeeds
      const attempt2 = job.createAttempt();
      attempt2.start();
      attempt2.succeed(0);
      await jobAttemptRepo.save(attempt2);

      // Verify both attempts exist in database
      const loadedAttempts = await jobAttemptRepo.findByJobId(job.id);
      expect(loadedAttempts).toHaveLength(2);

      // Attempt 1 historical integrity
      expect(loadedAttempts[0]?.attemptNumber).toBe(1);
      expect(loadedAttempts[0]?.status).toBe('FAILED');
      expect(loadedAttempts[0]?.exitCode).toBe(1);
      expect(loadedAttempts[0]?.failureReason).toBe('Network timeout connecting to upstream');

      // Attempt 2 historical integrity
      expect(loadedAttempts[1]?.attemptNumber).toBe(2);
      expect(loadedAttempts[1]?.status).toBe('SUCCEEDED');
      expect(loadedAttempts[1]?.exitCode).toBe(0);
      expect(loadedAttempts[1]?.failureReason).toBeUndefined();

      // Verify full job rehydration reattaches both attempts
      const loadedJob = await jobRepo.findById(job.id);
      expect(loadedJob!.attempts).toHaveLength(2);
      expect(loadedJob!.attempts[0]?.status).toBe('FAILED');
      expect(loadedJob!.attempts[1]?.status).toBe('SUCCEEDED');
      expect(loadedJob!.currentAttempt?.attemptNumber).toBe(2);
    });

    it('rejects duplicate attempt number for the same job with ConstraintViolationError', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-dup-attempt',
        name: 'Dup Attempt Pipeline',
        steps: [{ name: 'step', command: 'echo step' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-dup-att'), pipeline);
      await pipelineRunRepo.save(run);

      const job = run.getJobs()[0]!;

      const att1 = new JobAttempt({
        id: createJobAttemptId('att-1'),
        jobId: job.id,
        attemptNumber: 1,
      });
      await jobAttemptRepo.save(att1);

      // Create a different attempt record with same (job_id, attempt_number)
      // Directly inserting into PostgreSQL to test the unique constraint
      await expect(
        pool.query(
          `INSERT INTO job_attempts (id, job_id, attempt_number, status)
           VALUES ('att-conflict', $1, 1, 'PENDING');`,
          [job.id],
        ),
      ).rejects.toThrow();
    });

    it('validates state transitions and rejects terminal state regression on job attempts', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-att-sm',
        name: 'Attempt SM Pipeline',
        steps: [{ name: 'step', command: 'echo step' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-att-sm'), pipeline);
      await pipelineRunRepo.save(run);

      const job = run.getJobs()[0]!;
      const attempt = job.createAttempt();
      await jobAttemptRepo.save(attempt);
      expect(attempt.status).toBe('PENDING');

      // Valid: PENDING -> RUNNING
      attempt.start();
      await jobAttemptRepo.save(attempt);
      let loaded = await jobAttemptRepo.findById(attempt.id);
      expect(loaded!.status).toBe('RUNNING');

      // Valid: RUNNING -> SUCCEEDED
      attempt.succeed(0);
      await jobAttemptRepo.save(attempt);
      loaded = await jobAttemptRepo.findById(attempt.id);
      expect(loaded!.status).toBe('SUCCEEDED');

      // Terminal regression: attempt to save attempt as RUNNING
      const regressedAttempt = new JobAttempt({
        id: attempt.id,
        jobId: job.id,
        attemptNumber: attempt.attemptNumber,
        initialStatus: 'RUNNING',
      });

      await expect(jobAttemptRepo.save(regressedAttempt)).rejects.toThrow();

      // Verify database state remains SUCCEEDED
      const afterRegress = await jobAttemptRepo.findById(attempt.id);
      expect(afterRegress!.status).toBe('SUCCEEDED');
    });

    it('rejects invalid state jumps on job attempts (PENDING -> SUCCEEDED)', async () => {
      const pipeline = new Pipeline({
        id: 'pipe-att-jump',
        name: 'Attempt Jump Pipeline',
        steps: [{ name: 'step', command: 'echo step' }],
      });
      await pipelineRepo.save(pipeline);

      const run = PipelineRun.create(createPipelineRunId('run-att-jump'), pipeline);
      await pipelineRunRepo.save(run);

      const job = run.getJobs()[0]!;
      const attempt = job.createAttempt();
      await jobAttemptRepo.save(attempt);

      // Attempt invalid jump: PENDING -> SUCCEEDED
      const jumpedAttempt = new JobAttempt({
        id: attempt.id,
        jobId: job.id,
        attemptNumber: attempt.attemptNumber,
        initialStatus: 'SUCCEEDED',
      });

      await expect(jobAttemptRepo.save(jumpedAttempt)).rejects.toThrow();

      const afterAttempt = await jobAttemptRepo.findById(attempt.id);
      expect(afterAttempt!.status).toBe('PENDING');
    });
  });
});
