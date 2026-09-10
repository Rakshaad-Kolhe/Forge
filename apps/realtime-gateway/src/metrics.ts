/**
 * Cheap in-process counters. Not Prometheus — a snapshot hook for future observability.
 */
export type GatewayCounterKey =
  | 'connectionsOpened'
  | 'connectionsRejected'
  | 'eventsReceived'
  | 'eventsDelivered'
  | 'eventsFiltered'
  | 'protocolErrors'
  | 'authorizationDenied'
  | 'slowConsumerDisconnects'
  | 'redisDisconnects';

export type GatewayMetricsSnapshot = Record<GatewayCounterKey, number> & {
  activeConnections: number;
  activeSubscriptions: number;
};

export class GatewayMetrics {
  private readonly counters: Record<GatewayCounterKey, number> = {
    connectionsOpened: 0,
    connectionsRejected: 0,
    eventsReceived: 0,
    eventsDelivered: 0,
    eventsFiltered: 0,
    protocolErrors: 0,
    authorizationDenied: 0,
    slowConsumerDisconnects: 0,
    redisDisconnects: 0,
  };

  constructor(
    private readonly activeConnections: () => number,
    private readonly activeSubscriptions: () => number,
  ) {}

  public incr(key: GatewayCounterKey, by = 1): void {
    this.counters[key] += by;
  }

  public snapshot(): GatewayMetricsSnapshot {
    return {
      activeConnections: this.activeConnections(),
      activeSubscriptions: this.activeSubscriptions(),
      ...this.counters,
    };
  }
}
