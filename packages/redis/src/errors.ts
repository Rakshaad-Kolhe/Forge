/**
 * Base error class for all Redis-related errors in Forge.
 */
export class RedisError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = 'RedisError';
    if (cause && cause.stack) {
      this.stack = `${this.stack}\nCaused by: ${cause.stack}`;
    }
  }
}

/**
 * Thrown when establishing or maintaining a Redis connection fails.
 */
export class RedisConnectionError extends RedisError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'RedisConnectionError';
  }
}

/**
 * Thrown when a Redis command fails execution.
 */
export class RedisCommandError extends RedisError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'RedisCommandError';
  }
}

/**
 * Thrown when attempting an operation while Redis is disconnected or unavailable.
 */
export class RedisUnavailableError extends RedisError {
  constructor(message: string = 'Redis service is currently unavailable', cause?: Error) {
    super(message, cause);
    this.name = 'RedisUnavailableError';
  }
}

/**
 * Thrown when serialization or deserialization of structured Redis values fails.
 */
export class SerializationError extends RedisError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'SerializationError';
  }
}

/**
 * Strips sensitive credentials (passwords, auth tokens) from Redis connection URLs.
 * Safe for logging and diagnostic output.
 */
export function sanitizeRedisUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.password) {
      parsed.password = '***';
    }
    return parsed.toString();
  } catch {
    // If URL parsing fails, mask any user:pass@ pattern via regex fallback
    return url.replace(/\/\/([^:]+):([^@]+)@/, '//$1:***@');
  }
}
