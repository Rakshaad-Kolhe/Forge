# Redis Coordination Foundation

## 1. Overview & Architectural Role

Forge V2 is an event-driven, self-hosted distributed CI/CD orchestration engine. Its persistence and state management is partitioned cleanly across two dedicated tiers:

```text
       ┌────────────────────────┐
       │       PostgreSQL       │
       │  AUTHORITATIVE STATE   │
       │   - Pipelines          │
       │   - Pipeline Runs      │
       │   - Jobs               │
       │   - Job Attempts       │
       │   - Audit & History    │
       └────────────────────────┘

                   ▲
                   │ (Authoritative reconciliation)
                   ▼

       ┌────────────────────────┐
       │         Redis          │
       │ TRANSIENT COORDINATION │
       │   - Job Queues (Future)│
       │   - Ephemeral Leases   │
       │   - Distributed Sync   │
       │   - Atomic Claims      │
       └────────────────────────┘
```

### The Non-Negotiable Contract

> **PostgreSQL is the durable source of truth.**  
> **Redis is strictly a transient distributed coordination mechanism.**

Losing, restarting, or flushing Redis must **never** corrupt, rewrite, or erase historical execution records in PostgreSQL. When Redis restarts or recovers from downtime, the scheduling plane reconstructs transient coordination state (e.g. active queues, unassigned jobs) from authoritative records in PostgreSQL.

---

## 2. Ownership Boundaries

### What Redis Owns (Transient Coordination)

1. **Ephemeral State**: Information that expires or can be recreated (e.g. heartbeat tracking, worker claim locks, execution stages).
2. **Low-Latency Atomic Operations**: Fast single-threaded primitive synchronization (`SET ... NX EX`, atomic `INCR`/`DECR`, custom Lua scripts).
3. **Short-Lived Keys with TTLs**: Automatically garbage-collected locks and coordination markers.
4. **Future Task Buffering**: Low-latency queues for staging executable jobs awaiting worker pickup (implemented in subsequent PRs).

### What Redis Does NOT Own (Durable Truth)

1. **Pipeline Definitions**: Pipelines, steps, commands, and DAG topologies reside exclusively in PostgreSQL.
2. **Execution History**: Pipeline runs, jobs, attempts, logs, exit codes, and timestamps reside in PostgreSQL.
3. **Terminal State Records**: Completed jobs and runs are sealed in PostgreSQL; Redis records for those entities are purged or allowed to expire.
4. **Permanent Audit Trails**: No audit records or historical metrics are stored exclusively in Redis.

---

## 3. Package Structure (`@forge/redis`)

The `@forge/redis` package is an infrastructure package providing low-level connection management and primitives:

```text
packages/redis/
├── package.json
├── tsconfig.json
└── src/
    ├── client.ts         # Managed client wrapper around ioredis
    ├── config.ts         # RedisConfig interface and default constants
    ├── errors.ts         # Diagnostic error hierarchy and credential sanitization
    ├── keys.ts           # Standard key namespace generator (forge:{namespace}:{...})
    ├── serialization.ts  # Deterministic JSON encoder/decoder with type validation
    ├── types.ts          # RedisClient interface and primitive method signatures
    ├── index.ts          # Public exports
    ├── client.test.ts    # Unit tests for key builder, errors, and serialization
    └── redis.test.ts     # Real Redis integration tests
```

---

## 4. Connection Lifecycle & Management

Forge encapsulates the Redis connection via `createRedisClient(config, logger)`:

```text
[Unconnected] ──► client.connect() ──► [Connecting] ──► [Ready]
                                                              │
                     client.close() / client.disconnect() ◄───┘
```

- **Lazy Connection**: Clients default to `lazyConnect: true`, allowing components to initialize without forcing immediate socket connections.
- **Connection Idempotency**: Calling `connect()` on a ready or connecting client is a safe no-op.
- **Graceful Shutdown**: `client.close()` sends `QUIT`, waiting for in-flight commands before terminating the socket. If `QUIT` fails or times out, `disconnect()` forcibly releases socket handles.
- **Credential Protection**: Connection URLs are passed through `sanitizeRedisUrl()`, masking sensitive authentication credentials (`redis://user:***@host:port`) across all structured log entries and error causes.

---

## 5. Health Checking

Health checking is implemented via an active `healthCheck()` operation:

```typescript
const isHealthy = await redisClient.healthCheck();
```

- Executes an explicit Redis `PING` over the wire.
- Returns `true` if and only if Redis responds with `PONG`.
- Returns `false` deterministically if the socket is closed, unreachable, or unresponsive (never silently succeeds when Redis is down).

---

## 6. Generic Coordination Primitives

The package exposes minimal generic primitives for upcoming queue, lock, and lease implementations:

| Method                           | Description                                                                                                           |
| :------------------------------- | :-------------------------------------------------------------------------------------------------------------------- |
| `get(key)`                       | Retrieves raw string value stored at `key`.                                                                           |
| `set(key, value, options)`       | Stores string value with optional TTL (`ttlSeconds`, `ttlMillis`) and conditions (`ifNotExists: NX`, `ifExists: XX`). |
| `del(...keys)`                   | Deletes one or more keys and returns the count of deleted keys.                                                       |
| `exists(...keys)`                | Returns the count of existing keys.                                                                                   |
| `expire(key, seconds)`           | Sets a timeout on an existing key.                                                                                    |
| `ttl(key)`                       | Returns remaining TTL in seconds (-1 = persistent, -2 = missing).                                                     |
| `getJson<T>(key)`                | Retrieves and deserializes a JSON payload.                                                                            |
| `setJson<T>(key, val, options)`  | Validates, serializes, and stores structured JSON.                                                                    |
| `setNx(key, value, ttlSeconds)`  | Atomic mutual exclusion primitive (`SET key value [EX ttl] NX`).                                                      |
| `incr(key)` / `decr(key)`        | Atomically increments or decrements an integer counter.                                                               |
| `eval(script, numkeys, ...args)` | Atomically evaluates an arbitrary Lua script on the Redis engine.                                                     |

> [!WARNING]
> `setNx` is an atomic low-level primitive, **not** a full distributed lock implementation. Distributed locks, leases, renew loops, and job queues are future scope and must not be inferred from primitives alone.

---

## 7. Key Naming Foundation

Keys are structured deterministically using `createRedisKey`:

```text
forge:{namespace}:{identifier1}:{identifier2}
```

- **Prefix**: `forge` (constant).
- **Delimiter**: `:` (standard Redis namespace delimiter).
- **Namespace Validation**: Empty or whitespace-only segments throw immediate errors.

---

## 8. Serialization Convention

Structured values stored in Redis follow strict JSON serialization via `serializeJson` and `deserializeJson`:

- Plain objects, arrays, strings, numbers, booleans, and null are supported.
- `undefined`, `functions`, `symbols`, `NaN`, `Infinity`, `BigInt`, and arbitrary custom class instances are explicitly rejected with `SerializationError` to prevent silent corruption.

---

## 9. Failure Semantics & Reconnection

- **Disconnected / Unavailable**: Calling commands while disconnected or when Redis is unreachable throws typed `RedisConnectionError` or `RedisUnavailableError`.
- **Command Failure**: Syntax errors, bad script execution, or type mismatches throw typed `RedisCommandError`.
- **Reconnection**: Handled via `ioredis` with bounded exponential backoff. Transition events (`connect`, `ready`, `close`, `reconnecting`, `end`) are logged via `@forge/logging` without noisy per-command logs.

---

## 10. Local Development Infrastructure

Local Redis is declared in `docker-compose.yml`:

```yaml
redis:
  image: redis:7.2-alpine
  container_name: forge-redis
  restart: unless-stopped
  ports:
    - '6379:6379'
  healthcheck:
    test: ['CMD', 'redis-cli', 'ping']
    interval: 5s
    timeout: 5s
    retries: 5
```

For development and test environments, Redis runs on `127.0.0.1:6379`.
Integration tests isolate test keys under unique run prefixes (`forge:test_{timestamp}_{rand}:*`) and clean up after execution without invoking destructive commands like `FLUSHALL`.
