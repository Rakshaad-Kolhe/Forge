/**
 * WebSocket `Origin` validation.
 *
 * Browsers always send `Origin` on the upgrade request; a non-browser client (CLI,
 * service) typically omits it. Policy:
 *
 * - `Origin` present  → must exactly match a configured allowlist entry.
 * - `Origin` absent   → allowed (non-browser); the handshake still requires the auth token.
 * - allowlist empty   → every browser `Origin` is rejected. `*` is never accepted.
 */
export function isOriginAllowed(origin: string | undefined, allowlist: readonly string[]): boolean {
  if (origin === undefined || origin === '') {
    return true;
  }
  if (origin === 'null') {
    // Opaque origin (sandboxed iframe, file://). Treat as browser-supplied and untrusted.
    return false;
  }
  return allowlist.includes(origin);
}
