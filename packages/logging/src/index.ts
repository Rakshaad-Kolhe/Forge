import type { LogEntry, LogLevel, ServiceName } from '@forge/contracts';

const LOG_LEVEL_SEVERITY: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export interface LoggerOptions {
  service: ServiceName | string;
  environment?: string;
  minLevel?: LogLevel;
  writeFn?: (output: string) => void;
}

export interface Logger {
  debug(message: string, context?: Record<string, unknown>): void;
  info(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, context?: Record<string, unknown>): void;
  child(defaultContext: Record<string, unknown>): Logger;
}

export class StructuredLogger implements Logger {
  private readonly service: string;
  private readonly environment: string;
  private readonly minLevel: LogLevel;
  private readonly writeFn: (output: string) => void;
  private readonly defaultContext: Record<string, unknown>;

  constructor(options: LoggerOptions, defaultContext: Record<string, unknown> = {}) {
    this.service = options.service;
    this.environment = options.environment ?? process.env['NODE_ENV'] ?? 'development';
    this.minLevel = options.minLevel ?? ((process.env['LOG_LEVEL'] as LogLevel) || 'info');
    this.writeFn = options.writeFn ?? ((out: string) => process.stdout.write(out + '\n'));
    this.defaultContext = defaultContext;
  }

  private shouldLog(level: LogLevel): boolean {
    const minSeverity = LOG_LEVEL_SEVERITY[this.minLevel] ?? 20;
    const currentSeverity = LOG_LEVEL_SEVERITY[level] ?? 20;
    return currentSeverity >= minSeverity;
  }

  private formatOutput(entry: LogEntry): string {
    if (this.environment === 'production') {
      return JSON.stringify(entry);
    }

    const ctx =
      entry.context && Object.keys(entry.context).length > 0
        ? ` ${JSON.stringify(entry.context)}`
        : '';
    const reqId = entry.request_id ? ` [req:${entry.request_id}]` : '';
    return `[${entry.timestamp}] ${entry.level.toUpperCase()} [${entry.service}]${reqId}: ${entry.message}${ctx}`;
  }

  private log(level: LogLevel, message: string, context?: Record<string, unknown>): void {
    if (!this.shouldLog(level)) {
      return;
    }

    const mergedContext = { ...this.defaultContext, ...context };
    const requestId =
      typeof mergedContext['request_id'] === 'string'
        ? (mergedContext['request_id'] as string)
        : undefined;

    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      level,
      service: this.service,
      message,
      environment: this.environment,
      ...(requestId ? { request_id: requestId } : {}),
      ...(Object.keys(mergedContext).length > 0 ? { context: mergedContext } : {}),
    };

    this.writeFn(this.formatOutput(entry));
  }

  public debug(message: string, context?: Record<string, unknown>): void {
    this.log('debug', message, context);
  }

  public info(message: string, context?: Record<string, unknown>): void {
    this.log('info', message, context);
  }

  public warn(message: string, context?: Record<string, unknown>): void {
    this.log('warn', message, context);
  }

  public error(message: string, context?: Record<string, unknown>): void {
    this.log('error', message, context);
  }

  public child(defaultContext: Record<string, unknown>): Logger {
    return new StructuredLogger(
      {
        service: this.service,
        environment: this.environment,
        minLevel: this.minLevel,
        writeFn: this.writeFn,
      },
      { ...this.defaultContext, ...defaultContext },
    );
  }
}

export function createLogger(options: LoggerOptions): Logger {
  return new StructuredLogger(options);
}
