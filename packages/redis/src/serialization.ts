import { SerializationError } from './errors.js';

/**
 * Validates that an input value is strictly JSON-serializable without silent type loss.
 * Explicitly rejects undefined, functions, symbols, NaN, and Infinity.
 */
function validateJsonSafe(value: unknown, path: string = 'root'): void {
  if (value === undefined) {
    throw new SerializationError(`Cannot serialize undefined value at "${path}" to JSON`);
  }

  if (typeof value === 'function' || typeof value === 'symbol') {
    throw new SerializationError(
      `Cannot serialize unsupported type "${typeof value}" at "${path}" to JSON`,
    );
  }

  if (typeof value === 'number' && (!Number.isFinite(value) || Number.isNaN(value))) {
    throw new SerializationError(
      `Cannot serialize non-finite number "${value}" at "${path}" to JSON`,
    );
  }

  if (typeof value === 'bigint') {
    throw new SerializationError(`Cannot serialize BigInt at "${path}" without custom encoder`);
  }

  if (value !== null && typeof value === 'object') {
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        validateJsonSafe(value[i], `${path}[${i}]`);
      }
    } else if (value.constructor && value.constructor.name !== 'Object') {
      // Check for Dates, custom classes, etc.
      if (value instanceof Date) {
        // Dates are serializable to ISO string in standard JSON
        return;
      }
      throw new SerializationError(
        `Cannot serialize class instance of "${value.constructor.name}" at "${path}"`,
      );
    } else {
      for (const [key, val] of Object.entries(value)) {
        validateJsonSafe(val, `${path}.${key}`);
      }
    }
  }
}

/**
 * Serializes a JSON-safe value to a string.
 *
 * @throws {SerializationError} If value contains undefined, functions, NaN, Infinity, or unhandled class instances.
 */
export function serializeJson(value: unknown): string {
  try {
    validateJsonSafe(value);
    return JSON.stringify(value);
  } catch (err) {
    if (err instanceof SerializationError) {
      throw err;
    }
    throw new SerializationError(
      `Failed to serialize value to JSON: ${(err as Error).message}`,
      err as Error,
    );
  }
}

/**
 * Deserializes a raw JSON string into a typed object.
 *
 * @throws {SerializationError} If the string is malformed or invalid JSON.
 */
export function deserializeJson<T>(raw: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch (err) {
    throw new SerializationError(
      `Failed to parse JSON string: ${(err as Error).message}`,
      err as Error,
    );
  }
}
