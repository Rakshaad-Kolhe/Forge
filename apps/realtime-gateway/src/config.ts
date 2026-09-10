/**
 * Projects the shared {@link AppConfig} into the gateway's own option shape and enforces
 * the one gateway-specific invariant the global config deliberately does not: the
 * handshake secret must be present. `loadConfig()` is called by every service, so an
 * empty `WEBSOCKET_AUTH_TOKEN` cannot be a hard error there — it is enforced here, at
 * gateway startup only.
 */
import type { AppConfig } from '@forge/contracts';

export interface GatewayConfig {
  readonly port: number;
  readonly authToken: string;
  readonly originAllowlist: readonly string[];
  readonly channel: string;
  readonly maxConnections: number;
  readonly maxSubscriptionsPerConnection: number;
  readonly maxPendingMessages: number;
  readonly maxMessageBytes: number;
  readonly heartbeatIntervalMs: number;
  /** Upper bound on the graceful-shutdown wait before straggler sockets are terminated. */
  readonly shutdownGraceMs: number;
}

export class GatewayConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'GatewayConfigError';
  }
}

export function gatewayConfigFromAppConfig(config: AppConfig): GatewayConfig {
  const authToken = config.websocketAuthToken ?? '';
  if (authToken.trim().length === 0) {
    throw new GatewayConfigError(
      'WEBSOCKET_AUTH_TOKEN is required to start the realtime gateway (it authenticates the WebSocket handshake). Set it in the environment.',
    );
  }
  return {
    port: config.websocketPort,
    authToken,
    originAllowlist: config.websocketOriginAllowlist,
    channel: config.realtimeRedisChannel,
    maxConnections: config.websocketMaxConnections,
    maxSubscriptionsPerConnection: config.websocketMaxSubscriptionsPerConnection,
    maxPendingMessages: config.websocketMaxPendingMessages,
    maxMessageBytes: config.websocketMaxMessageBytes,
    heartbeatIntervalMs: config.websocketHeartbeatIntervalMs,
    shutdownGraceMs: 5000,
  };
}
