# Architecture — Container Executor & Sandboxed Job Execution

## 1. Overview & Core Objective

The **Executor** is Forge V2's execution plane abstraction. While previous PRs established reliable FIFO queuing (PR 07), worker registration and liveness (PR 08), capability and resource matching (PR 09), deterministic scheduling (PR 10), priority ordering (PR 11), and distributed worker leases (PR 12), PR 13 establishes the actual execution of claimed jobs:

```text
Claimed Job
    ↓
Worker Ownership Validation
    ↓
Job / JobAttempt State Transition (RUNNING)
    ↓
Background Lease Renewal Loop
    ↓
Executor Abstraction (DockerExecutor)
    ↓
Isolated Ephemeral Workspace & Non-Root Container
    ↓
Supervised Execution (Timeout, Cancellation, Resource Limits, Bounded Log Capture)
    ↓
Exit Code & Result Classification
    ↓
Guaranteed Cleanup (Container & Ephemeral Workspace)
    ↓
Transactional Persistence (PostgreSQL ACID Transaction)
    ↓
Authoritative Lease Release
```

---

## 2. Core Invariants

1. **One-Way Ephemeral Lifecycle**: An execution environment (container or workspace) is created dynamically for a single job attempt and is **never reused** across attempts or distinct jobs.
2. **Non-Root Execution**: User commands inside the container execute as an unprivileged UID:GID (`--user 1000:1000` by default). The container process never runs as privileged root.
3. **Strict Host Isolation**: Containers run with standard bridge networking, without `--privileged` capabilities, without mounting the host Docker socket (`/var/run/docker.sock`), and without mounting host filesystems outside the dedicated temporary workspace.
4. **Hard Timeout Enforcement**: The supervisor strictly enforces execution timeouts by issuing a graceful `docker stop -t 2` followed by a forced `docker kill` if needed. Timed-out executions are classified explicitly as `TIMED_OUT`.
5. **Bounded Output Capture**: Process stdout and stderr are captured up to a configurable maximum byte threshold (`MAX_OUTPUT_BYTES`). If exceeded, streams are truncated without unbounded memory growth and marked with `truncated: true`.
6. **Lease Synchronization & Split-Brain Elimination**: Before execution begins, the worker validates that it holds the current active lease. During execution, the lease is renewed periodically. If lease ownership is lost (e.g. `LEASE_EXPIRED` or `LEASE_OWNER_MISMATCH`), the running container is immediately aborted to prevent duplicate concurrent execution across workers.
7. **Guaranteed Cleanup**: Ephemeral workspaces and created containers are cleaned up via `finally` blocks regardless of outcome (`SUCCEEDED`, `FAILED`, `TIMED_OUT`, `CANCELLED`, or startup error). Cleanup errors do not erase primary execution results.
8. **Transactional Persistence Integrity**: Job state transitions and attempt updates pass through the domain state machine and are persisted transactionally in PostgreSQL before the distributed lease is released.

---

## 3. Executor Architecture

The `Executor` interface decouples the worker daemon from container engine implementation details:

```text
               ┌───────────────────────┐
               │   Executor Interface  │
               └───────────┬───────────┘
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
  ┌─────────────────────┐     ┌─────────────────────┐
  │   DockerExecutor    │     │ KubernetesExecutor  │
  │ (Local Daemon/Sock) │     │ (Future Phase)      │
  └─────────────────────┘     └─────────────────────┘
```

### The `Executor` Contract (`@forge/contracts`)

```ts
export interface Executor {
  readonly name: string;
  execute(context: ExecutionContext): Promise<ExecutionResult>;
  isAvailable(): Promise<boolean>;
}
```

### Context & Result Contracts

```ts
export interface ExecutionContext {
  readonly jobId: string;
  readonly attemptId: string;
  readonly workerId: string;
  readonly command: string;
  readonly image?: string;
  readonly environment?: Readonly<Record<string, string>>;
  readonly workingDirectory?: string;
  readonly cpuCores?: number;
  readonly memoryBytes?: number;
  readonly timeoutMs?: number;
  readonly abortSignal?: AbortSignal;
}

export interface ExecutionResult {
  readonly status: 'SUCCEEDED' | 'FAILED' | 'TIMED_OUT' | 'CANCELLED';
  readonly exitCode: number | null;
  readonly startedAt: Date;
  readonly finishedAt: Date;
  readonly durationMs: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly truncated: boolean;
  readonly failureReason?: string;
}
```

---

## 4. Isolation & Security Controls

| Threat Vector                            | Isolation Control                                                                                                                          |
| :--------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------- |
| **Host Privilege Escalation**            | No `--privileged` flag; container runs with default seccomp profiles.                                                                      |
| **Container Breakout via Root**          | Unprivileged UID:GID (`--user 1000:1000`) enforced on container startup.                                                                   |
| **Docker-in-Docker Socket Exploitation** | Docker socket (`/var/run/docker.sock`) is never mounted into user containers.                                                              |
| **Host Filesystem Snooping**             | Host filesystem is not accessible; only the temporary workspace is bind-mounted (`/workspace`).                                            |
| **Host Environment Leaks**               | Host `process.env` is never forwarded into the container; only explicitly declared job environment variables are passed.                   |
| **Host Shell Injection**                 | Docker CLI commands are spawned with structured array arguments (`spawn('docker', args)`), preventing shell command injection on the host. |
| **Resource Starvation**                  | CPU limits mapped to `--cpus`; memory quotas mapped to `--memory`.                                                                         |

---

## 5. Resource Mapping

| Resource Requirement | Docker CLI Flag     | Validation & Constraints                                                                                                                              |
| :------------------- | :------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cpuCores`           | `--cpus=<float>`    | Must be positive finite number (`> 0`).                                                                                                               |
| `memoryBytes`        | `--memory=<bytes>b` | Must be positive finite number; clamped to Docker's 6MB minimum floor (`6291456b`).                                                                   |
| `gpuCount`           | _Deferred_          | GPU capacity metadata is recognized by the scheduler, but GPU runtime execution is marked **unverified / deferred** without NVIDIA container toolkit. |

---

## 6. Distributed Lease Synchronization & Ownership Lifecycle

```text
Worker Claims Job Lease (PR 12)
       │
       ▼
1. Validate Active Lease in PostgreSQL
   (Ensure lease.status == ACTIVE && lease.workerId == this.workerId)
       │
       ▼
2. Create JobAttempt & Transition Job to RUNNING
   (Persist RUNNING status to PostgreSQL)
       │
       ▼
3. Start Periodic Background Lease Renewal
   (Runs every WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS)
   ├── Success: Lease expiry extended in PostgreSQL
   └── Lost / Expired: Abort running container immediately
       │
       ▼
4. Execute via DockerExecutor
   (Spawn container, supervise timeout & abort signal, collect exit status)
       │
       ▼
5. Stop Background Lease Renewal Timer
       │
       ▼
6. Apply Terminal Domain State Machine Transitions
   (RUNNING -> SUCCEEDED | FAILED | TIMED_OUT | CANCELLED)
       │
       ▼
7. Transactional Persistence via PostgreSQL
   (Commit Job and JobAttempt terminal state in single ACID transaction)
       │
       ▼
8. Authoritative Lease Release in PostgreSQL
   (Transition lease status to RELEASED)
```

---

## 7. Failure Handling Matrix

| Failure Mode                            | Worker & Executor Behavior                                                                          | Persistent State Outcome                                                              |
| :-------------------------------------- | :-------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------ |
| **Docker CLI Missing / Unreachable**    | Throws `DockerUnavailableError` or logs startup failure; skips container execution.                 | `Job` & `JobAttempt` marked `FAILED`; lease released.                                 |
| **Container Command Exits 0**           | Captures exit code 0; collects stdout/stderr.                                                       | `Job` & `JobAttempt` marked `SUCCEEDED`; lease released.                              |
| **Container Command Exits Non-Zero**    | Captures non-zero exit code; records failure message.                                               | `Job` & `JobAttempt` marked `FAILED`; lease released.                                 |
| **Execution Exceeds Timeout**           | Issues `docker stop -t 2` followed by `docker kill`; sets status `TIMED_OUT`.                       | `Job` & `JobAttempt` marked `TIMED_OUT`; lease released.                              |
| **Execution Cancelled via AbortSignal** | Issues `docker stop -t 1`; sets status `CANCELLED`.                                                 | `Job` & `JobAttempt` marked `CANCELLED`; lease released.                              |
| **Lease Renewal Definitive Failure**    | Detects `LEASE_EXPIRED` or `LEASE_OWNER_MISMATCH`; aborts running container immediately.            | `Job` & `JobAttempt` marked `FAILED` (ownership lost); skips releasing unowned lease. |
| **PostgreSQL Persistence Failure**      | Database transaction rolls back; error is rethrown. Primary result is not falsely marked succeeded. | Retains uncommitted state; lease expires naturally.                                   |
| **Workspace Cleanup Error**             | Cleanup error logged with target `workspace`; primary result preserved.                             | Primary result unaffected.                                                            |
