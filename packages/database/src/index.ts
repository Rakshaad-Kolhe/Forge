// Client and Pool
export { createDatabasePool } from './client.js';

// Configuration
export { DEFAULT_DATABASE_URL, resolveDatabaseConfig } from './config.js';

// Errors
export {
  ConstraintViolationError,
  DatabaseConnectionError,
  DatabaseError,
  EntityNotFoundError,
  MigrationError,
  OutboxPayloadError,
  PersistenceError,
  sanitizeConnectionString,
} from './errors.js';

// Migrations
export {
  DEAD_LETTER_JOBS_SQL,
  INITIAL_SCHEMA_SQL,
  MIGRATIONS,
  resetDatabase,
  runMigrations,
  WORKER_LEASES_SQL,
  WORKER_REGISTRY_SQL,
} from './migrations/migrator.js';

// Repositories - Contracts
export type {
  DeadLetterFilter,
  DeadLetterRepository,
} from './repositories/contracts/dead-letter-repository.contract.js';
export type { JobAttemptRepository } from './repositories/contracts/job-attempt-repository.contract.js';
export type { JobRepository } from './repositories/contracts/job-repository.contract.js';
export type { PipelineRepository } from './repositories/contracts/pipeline-repository.contract.js';
export type { PipelineRunRepository } from './repositories/contracts/pipeline-run-repository.contract.js';
export type { WorkerLeaseRepository } from './repositories/contracts/worker-lease-repository.contract.js';
export type {
  WorkerLifecycleStatus,
  WorkerRecord,
  WorkerRepository,
  WorkerResourcesRecord,
} from './repositories/contracts/worker-repository.contract.js';

// Repositories - PostgreSQL Implementations
export { PgDeadLetterRepository } from './repositories/pg-dead-letter-repository.js';
export { PgJobAttemptRepository } from './repositories/pg-job-attempt-repository.js';
export { PgJobRepository } from './repositories/pg-job-repository.js';
export { PgPipelineRepository } from './repositories/pg-pipeline-repository.js';
export { PgPipelineRunRepository } from './repositories/pg-pipeline-run-repository.js';
export { PgWorkerLeaseRepository } from './repositories/pg-worker-lease-repository.js';
export { PgWorkerRepository } from './repositories/pg-worker-repository.js';

// Recovery Services
export { LeaseRecoveryService, type RecoveryLogger } from './lease-recovery-service.js';

// Transactions
export { type TransactionContext, withTransaction } from './transaction.js';

// Types
export type {
  DatabaseClient,
  DatabaseConfig,
  DatabasePool,
  DeadLetterJobRow,
  JobAttemptRow,
  JobRow,
  OutboxEventRow,
  PipelineRow,
  PipelineRunRow,
  WorkerLeaseRow,
} from './types.js';
