import { describe, expect, it } from 'vitest';
import { loadConfig } from '@forge/config';
import { GatewayConfigError, gatewayConfigFromAppConfig } from './config.js';

describe('gatewayConfigFromAppConfig', () => {
  it('throws when the handshake secret is unset or blank', () => {
    expect(() => gatewayConfigFromAppConfig(loadConfig({}))).toThrow(GatewayConfigError);
    expect(() => gatewayConfigFromAppConfig(loadConfig({ WEBSOCKET_AUTH_TOKEN: '   ' }))).toThrow(
      GatewayConfigError,
    );
  });

  it('maps AppConfig fields to the gateway option shape', () => {
    const cfg = gatewayConfigFromAppConfig(
      loadConfig({
        WEBSOCKET_AUTH_TOKEN: 'secret',
        WEBSOCKET_PORT: '4321',
        WEBSOCKET_MAX_CONNECTIONS: '10',
        WEBSOCKET_MAX_SUBSCRIPTIONS_PER_CONNECTION: '4',
        WEBSOCKET_MAX_PENDING_MESSAGES: '8',
        WEBSOCKET_MAX_MESSAGE_BYTES: '2048',
        WEBSOCKET_HEARTBEAT_INTERVAL_MS: '15000',
        WEBSOCKET_ORIGIN_ALLOWLIST: 'https://a.example',
        REALTIME_REDIS_CHANNEL: 'forge:realtime:events',
      }),
    );
    expect(cfg).toEqual({
      port: 4321,
      authToken: 'secret',
      originAllowlist: ['https://a.example'],
      channel: 'forge:realtime:events',
      maxConnections: 10,
      maxSubscriptionsPerConnection: 4,
      maxPendingMessages: 8,
      maxMessageBytes: 2048,
      heartbeatIntervalMs: 15000,
      shutdownGraceMs: 5000,
    });
  });
});
