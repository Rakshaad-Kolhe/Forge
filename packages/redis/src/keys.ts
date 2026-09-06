/**
 * Standard key namespace prefix for Forge V2 in Redis.
 */
export const FORGE_KEY_PREFIX = 'forge';

/**
 * Standard delimiter separating key segments.
 */
export const FORGE_KEY_DELIMITER = ':';

/**
 * Builds a standardized, namespaced Redis key following the convention:
 * `forge:{namespace}:{part1}:{part2}:...`
 *
 * @param namespace - Logical coordination category (e.g. 'test', 'lock', 'temp')
 * @param parts - Sub-identifiers identifying the specific resource
 *
 * Escaping assumptions:
 * - Empty string or whitespace-only segments are rejected to prevent malformed keys.
 * - Non-alphanumeric characters (excluding standard dashes, dots, underscores) are preserved
 *   as standard Redis binary-safe UTF-8 strings.
 */
export function createRedisKey(namespace: string, ...parts: (string | number)[]): string {
  const trimmedNamespace = namespace.trim();
  if (!trimmedNamespace) {
    throw new Error('Redis key namespace cannot be empty');
  }

  const validParts = parts.map((part, index) => {
    const str = String(part).trim();
    if (!str) {
      throw new Error(`Redis key part at index ${index} cannot be empty`);
    }
    return str;
  });

  return [FORGE_KEY_PREFIX, trimmedNamespace, ...validParts].join(FORGE_KEY_DELIMITER);
}

/**
 * Parses a Forge key into its constituent segments.
 */
export function parseRedisKey(key: string): {
  prefix: string;
  namespace: string;
  parts: string[];
} {
  const segments = key.split(FORGE_KEY_DELIMITER);
  if (segments.length < 2 || segments[0] !== FORGE_KEY_PREFIX) {
    throw new Error(`Invalid Forge Redis key format: "${key}"`);
  }

  const prefix = segments[0];
  const namespace = segments[1] ?? '';
  const parts = segments.slice(2);

  return { prefix, namespace, parts };
}
