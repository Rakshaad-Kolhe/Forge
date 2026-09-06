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
  PersistenceError,
  sanitizeConnectionString,
} from './errors.js';

// Migrations
export {
  INITIAL_SCHEMA_SQL,
  MIGRATIONS,
  resetDatabase,
  runMigrations,
} from './migrations/migrator.js';

// Repositories - Contracts
export type { JobAttemptRepository } from './repositories/contracts/job-attempt-repository.contract.js';
export type { JobRepository } from './repositories/contracts/job-repository.contract.js';
export type { PipelineRepository } from './repositories/contracts/pipeline-repository.contract.js';
export type { PipelineRunRepository } from './repositories/contracts/pipeline-run-repository.contract.js';

// Repositories - PostgreSQL Implementations
export { PgJobAttemptRepository } from './repositories/pg-job-attempt-repository.js';
export { PgJobRepository } from './repositories/pg-job-repository.js';
export { PgPipelineRepository } from './repositories/pg-pipeline-repository.js';
export { PgPipelineRunRepository } from './repositories/pg-pipeline-run-repository.js';

// Transactions
export { type TransactionContext, withTransaction } from './transaction.js';

// Types
export type {
  DatabaseClient,
  DatabaseConfig,
  DatabasePool,
  JobAttemptRow,
  JobRow,
  PipelineRow,
  PipelineRunRow,
} from './types.js';
