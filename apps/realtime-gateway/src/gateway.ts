/**
 * The Forge realtime WebSocket gateway.
 *
 *   Redis Pub/Sub → EventSubscriber → RealtimeGateway → subscribed WebSocket clients
 *
 * Owns an HTTP server (upgrade only), a `ws` server in `noServer` mode, a per-instance
 * {@link ConnectionRegistry}, and one Redis subscription. It has **no database access** and
 * is not authoritative for anything: a client that misses events reconnects and refreshes
 * state through the API.
 *
 * Handshake pipeline:  origin allowlist → shared-secret auth → instance capacity → accept.
 * Per-connection:      bounded subscriptions, bounded outbound queue (slow-consumer drop),
 *                      ping/pong liveness.
 * Shutdown:            stop accepting → stop Redis fan-out → close sockets → bounded wait
 *                      → terminate stragglers → close HTTP server.
 */
import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Duplex } from 'node:stream';
import { WebSocketServer, type WebSocket } from 'ws';
import type { EventSubscriber, ForgeEvent, Unsubscribe } from '@forge/events';
import type { Logger } from '@forge/logging';
import type { SubscriptionAuthorizer } from './authorization.js';
import { authenticateHandshake } from './auth.js';
import type { GatewayConfig } from './config.js';
import { GatewayConnection } from './connection.js';
import { GatewayMetrics } from './metrics.js';
import { isOriginAllowed } from './origin.js';
import { CLOSE_CODES, parseClientMessage, type SubscriptionTarget } from './protocol.js';
import { ConnectionRegistry } from './registry.js';

/** An {@link EventSubscriber} that also owns transport lifecycle (e.g. `RedisEventSubscriber`). */
export interface LifecycleEventSubscriber extends EventSubscriber {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface RealtimeGatewayOptions {
  readonly config: GatewayConfig;
  readonly subscriber: LifecycleEventSubscriber;
  readonly authorizer: SubscriptionAuthorizer;
  readonly logger: Logger;
  readonly metrics?: GatewayMetrics;
}

export class RealtimeGateway {
  private readonly config: GatewayConfig;
  private readonly subscriber: LifecycleEventSubscriber;
  private readonly authorizer: SubscriptionAuthorizer;
  private readonly logger: Logger;
  private readonly registry: ConnectionRegistry;
  private readonly metrics: GatewayMetrics;
  private readonly httpServer: Server;
  private readonly wss: WebSocketServer;

  private accepting = false;
  private started = false;
  private stopping?: Promise<void>;
  private eventUnsub?: Unsubscribe;
  private heartbeatTimer?: NodeJS.Timeout;

  constructor(options: RealtimeGatewayOptions) {
    this.config = options.config;
    this.subscriber = options.subscriber;
    this.authorizer = options.authorizer;
    this.logger = options.logger;
    this.registry = new ConnectionRegistry(this.config.maxConnections);
    this.metrics =
      options.metrics ??
      new GatewayMetrics(
        () => this.registry.size,
        () => this.registry.subscriptionCount,
      );

    this.httpServer = createServer((_req, res) => {
      res.writeHead(426, { 'content-type': 'text/plain' });
      res.end('Upgrade Required');
    });
    this.wss = new WebSocketServer({
      noServer: true,
      maxPayload: this.config.maxMessageBytes,
      handleProtocols: (protocols) => (protocols.has('forge.v1') ? 'forge.v1' : false),
    });
    this.httpServer.on('upgrade', (req, socket, head) => {
      this.handleUpgrade(req, socket as Duplex, head);
    });
  }

  public getMetrics(): GatewayMetrics {
    return this.metrics;
  }

  /** Bound address once {@link start} has resolved (useful in tests with port 0). */
  public address(): AddressInfo | null {
    const addr = this.httpServer.address();
    return addr && typeof addr === 'object' ? addr : null;
  }

  public async start(): Promise<void> {
    if (this.started) {
      return;
    }
    this.started = true;

    await this.subscriber.start();
    this.eventUnsub = this.subscriber.subscribe((event) => {
      this.onEvent(event);
    });

    await new Promise<void>((resolve, reject) => {
      const onError = (err: Error): void => reject(err);
      this.httpServer.once('error', onError);
      this.httpServer.listen(this.config.port, () => {
        this.httpServer.removeListener('error', onError);
        resolve();
      });
    });

    this.accepting = true;
    this.heartbeatTimer = setInterval(() => {
      this.registry.forEach((connection) => connection.heartbeatTick());
    }, this.config.heartbeatIntervalMs);
    this.heartbeatTimer.unref();

    this.logger.info('websocket.gateway_started', {
      port: this.address()?.port ?? this.config.port,
      max_connections: this.config.maxConnections,
    });
  }

  public async stop(): Promise<void> {
    if (!this.started) {
      return;
    }
    if (this.stopping) {
      return this.stopping;
    }
    this.stopping = (async () => {
      this.accepting = false;

      this.eventUnsub?.();
      this.eventUnsub = undefined;
      await this.subscriber.stop();

      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = undefined;
      }

      this.registry.forEach((connection) => {
        connection.send({ type: 'closing', v: 1, reason: 'SERVER_SHUTTING_DOWN' });
      });
      this.registry.closeAll(CLOSE_CODES.GOING_AWAY, 'server shutting down');

      const deadline = Date.now() + this.config.shutdownGraceMs;
      while (this.registry.size > 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 25));
      }
      if (this.registry.size > 0) {
        this.logger.warn('websocket.shutdown_forced', { remaining: this.registry.size });
        this.registry.terminateAll();
      }

      await new Promise<void>((resolve) => this.wss.close(() => resolve()));
      await new Promise<void>((resolve) => this.httpServer.close(() => resolve()));

      this.started = false;
      this.logger.info('websocket.gateway_stopped', this.metrics.snapshot());
    })();
    return this.stopping;
  }

  private onEvent(event: ForgeEvent): void {
    this.metrics.incr('eventsReceived');
    const result = this.registry.dispatch(event);
    if (result.delivered > 0) {
      this.metrics.incr('eventsDelivered', result.delivered);
    } else {
      this.metrics.incr('eventsFiltered');
    }
  }

  private rejectUpgrade(socket: Duplex, status: number, reason: string): void {
    const body = `${status} ${reason}`;
    socket.write(
      `HTTP/1.1 ${status} ${reason}\r\n` +
        'Connection: close\r\n' +
        'Content-Type: text/plain\r\n' +
        `Content-Length: ${Buffer.byteLength(body)}\r\n` +
        '\r\n' +
        body,
    );
    socket.destroy();
  }

  private handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void {
    if (!this.accepting) {
      this.metrics.incr('connectionsRejected');
      this.rejectUpgrade(socket, 503, 'Service Unavailable');
      return;
    }

    const origin = req.headers['origin'];
    if (!isOriginAllowed(origin, this.config.originAllowlist)) {
      this.metrics.incr('connectionsRejected');
      this.logger.warn('websocket.origin_rejected', { origin: origin ?? null });
      this.rejectUpgrade(socket, 403, 'Forbidden Origin');
      return;
    }

    const auth = authenticateHandshake(req, this.config.authToken);
    if (!auth.ok) {
      this.metrics.incr('connectionsRejected');
      this.logger.warn('websocket.authentication_failed', { reason: auth.reason });
      this.rejectUpgrade(socket, 401, 'Unauthorized');
      return;
    }

    if (this.registry.atCapacity) {
      this.metrics.incr('connectionsRejected');
      this.logger.warn('websocket.capacity_reached', { max: this.config.maxConnections });
      this.rejectUpgrade(socket, 503, 'Connection Limit Reached');
      return;
    }

    this.wss.handleUpgrade(req, socket, head, (ws) => {
      this.acceptConnection(ws, auth.principal.id);
    });
  }

  private acceptConnection(ws: WebSocket, principalId: string): void {
    const id = randomUUID();
    const connection = new GatewayConnection({
      id,
      socket: ws,
      maxPendingMessages: this.config.maxPendingMessages,
      maxSubscriptions: this.config.maxSubscriptionsPerConnection,
      maxMessageBytes: this.config.maxMessageBytes,
      heartbeatIntervalMs: this.config.heartbeatIntervalMs,
      logger: this.logger,
      onClose: (closed) => {
        this.registry.remove(closed);
        if (closed.slowConsumerDisconnect) {
          this.metrics.incr('slowConsumerDisconnects');
        }
        this.logger.info('websocket.connection_closed', {
          connection_id: closed.id,
          slow_consumer: closed.slowConsumerDisconnect,
        });
      },
    });

    if (!this.registry.add(connection)) {
      connection.close(CLOSE_CODES.GOING_AWAY, 'capacity');
      this.metrics.incr('connectionsRejected');
      return;
    }

    this.metrics.incr('connectionsOpened');
    this.logger.info('websocket.connection_opened', {
      connection_id: id,
      principal_id: principalId,
    });

    connection.onMessage((raw) => {
      void this.handleClientMessage(connection, principalId, raw);
    });
    connection.sendReady();
  }

  private async handleClientMessage(
    connection: GatewayConnection,
    principalId: string,
    raw: string,
  ): Promise<void> {
    const parsed = parseClientMessage(raw);
    if (!parsed.ok) {
      this.metrics.incr('protocolErrors');
      connection.sendError(parsed.code, parsed.reason);
      return;
    }

    const message = parsed.message;
    if (message.type === 'ping') {
      connection.sendPong();
      return;
    }

    const target: SubscriptionTarget = message.target;

    if (message.type === 'subscribe') {
      let allowed: boolean;
      try {
        allowed = await this.authorizer.authorize({ id: principalId }, target);
      } catch (err) {
        this.logger.error('websocket.authorization_error', {
          connection_id: connection.id,
          error: err instanceof Error ? err.message : String(err),
        });
        connection.sendError('FORBIDDEN', 'authorization check failed', target);
        return;
      }
      if (!allowed) {
        this.metrics.incr('authorizationDenied');
        this.logger.warn('websocket.authorization_denied', {
          connection_id: connection.id,
          target_kind: target.kind,
        });
        connection.sendError('FORBIDDEN', 'not authorized for this resource', target);
        return;
      }
      if (!this.registry.subscribe(connection, target)) {
        connection.sendError(
          'SUBSCRIPTION_LIMIT',
          `subscription limit (${this.config.maxSubscriptionsPerConnection}) reached`,
          target,
        );
        return;
      }
      this.logger.debug('websocket.subscription_added', {
        connection_id: connection.id,
        target_kind: target.kind,
      });
      connection.sendAck('subscribed', target);
      return;
    }

    // message.type === 'unsubscribe'
    this.registry.unsubscribe(connection, target);
    this.logger.debug('websocket.subscription_removed', {
      connection_id: connection.id,
      target_kind: target.kind,
    });
    connection.sendAck('unsubscribed', target);
  }
}

export function createGateway(options: RealtimeGatewayOptions): RealtimeGateway {
  return new RealtimeGateway(options);
}
