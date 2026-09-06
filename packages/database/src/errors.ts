/**
 * Sanitizes connection strings to ensure passwords and credentials are never logged or exposed.
 */
export function sanitizeConnectionString(connectionString: string): string {
  try {
    const url = new URL(connectionString);
    if (url.password) {
      url.password = '***';
    }
    return url.toString();
  } catch {
    // Fallback regex if URL constructor fails
    return connectionString.replace(/:\/\/(.*?):(.*?)@/, '://$1:***@');
  }
}

/**
 * Base error class for all @forge/database errors.
 */
export abstract class DatabaseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Thrown when connecting to the PostgreSQL server fails.
 */
export class DatabaseConnectionError extends DatabaseError {
  public readonly cause?: Error;

  constructor(message: string, cause?: Error) {
    super(message);
    this.cause = cause;
  }
}

/**
 * Thrown when an expected entity does not exist in the database.
 */
export class EntityNotFoundError extends DatabaseError {
  public readonly entityType: string;
  public readonly entityId: string;

  constructor(entityType: string, entityId: string) {
    super(`${entityType} with ID "${entityId}" not found`);
    this.entityType = entityType;
    this.entityId = entityId;
  }
}

/**
 * Thrown when a unique constraint or foreign key constraint is violated.
 */
export class ConstraintViolationError extends DatabaseError {
  public readonly constraint?: string;
  public readonly detail?: string;

  constructor(message: string, constraint?: string, detail?: string) {
    super(message);
    this.constraint = constraint;
    this.detail = detail;
  }
}

/**
 * Thrown when general persistence, domain mapping, or deserialization fails.
 */
export class PersistenceError extends DatabaseError {
  public readonly cause?: Error;

  constructor(message: string, cause?: Error) {
    super(message);
    this.cause = cause;
  }
}

/**
 * Thrown when database migrations fail to execute or verify.
 */
export class MigrationError extends DatabaseError {
  public readonly migrationName?: string;
  public readonly cause?: Error;

  constructor(message: string, migrationName?: string, cause?: Error) {
    super(message);
    this.migrationName = migrationName;
    this.cause = cause;
  }
}
