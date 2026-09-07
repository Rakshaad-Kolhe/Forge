# PR 13: Container Executor & Sandboxed Job Execution

## Summary

This pull request implements **PR 13: Container Executor & Sandboxed Job Execution** for Forge V2. It introduces Forge's execution plane abstraction (`Executor`) and a production-oriented container execution engine (`DockerExecutor` in `@forge/executor`), enabling workers to safely execute claimed jobs inside isolated, disposable, non-root Docker containers with explicit resource constraints, hard wall-clock timeouts, bounded log capture, and transactional result persistence in PostgreSQL.

---

## Key Architectural Decisions & Guarantees

1. **Pluggable Executor Abstraction (`@forge/executor`)**:
   - Establishes the `Executor` contract in `@forge/contracts` decoupling the worker daemon and scheduling plane from container engine specifics.
   - Initial implementation: `DockerExecutor` leveraging the Docker daemon via structured process spawning (`spawn('docker', args)`), completely eliminating host-shell interpolation and command-injection vulnerabilities.
   - Architected for seamless future extension to `KubernetesExecutor` (Pods and Jobs).

2. **Ephemeral Lifecycle & Non-Root Execution**:
   - Enforces a strict one-way lifecycle: fresh ephemeral workspace created per execution attempt (`os.tmpdir()/forge-workspaces/<id>`), bind-mounted to `/workspace`, and destroyed in `finally` teardown.
   - User commands execute under unprivileged UID:GID (`--user 1000:1000` by default), preventing containerized processes from running as root.
   - Container isolation: `--network bridge`, `--rm=false` (for controlled exit code and log capture before removal), no `--privileged` mode, no host Docker socket mount (`/var/run/docker.sock`), and no sensitive host filesystem mounts.

3. **Resource Enforcement & Timeout Supervision**:
   - Resource mapping: `JobRequirements.cpuCores` mapped to `--cpus=<float>`; `JobRequirements.memoryBytes` mapped to `--memory=<bytes>b` (enforcing Docker's 6MB minimum floor).
   - GPU execution runtime is explicitly marked as **not verified / deferred** in accordance with Section 18, avoiding false runtime claims.
   - Hard wall-clock timeout supervision: sends graceful `docker stop -t 2` followed by `docker kill` if needed, classifying outcomes explicitly as `TIMED_OUT`.
   - Cancellation support: `AbortSignal` triggers container teardown and returns `CANCELLED`.

4. **Bounded Output Capture & Truncation Protection**:
   - `OutputCollector` streams and captures stdout and stderr up to `MAX_OUTPUT_BYTES` (default 1MB).
   - If output exceeds the threshold, streams are truncated and marked with `truncated: true`, preventing unbounded memory consumption.

5. **Distributed Lease Synchronization & Split-Brain Elimination**:
   - Pre-execution validation: worker verifies it holds the active, unexpired lease in PostgreSQL before launching the container.
   - Periodic lease renewal: background timer periodically extends lease duration during long-running tasks.
   - Split-brain abort policy: if renewal fails definitively (`LEASE_EXPIRED` or `LEASE_OWNER_MISMATCH`), the running container is immediately aborted, eliminating duplicate execution risks.
   - Transactional persistence: `Job` and `JobAttempt` transitions pass through domain state machines and are persisted via PostgreSQL ACID transactions before the worker lease is released.

---

## What Was Implemented

### 1. Contracts Package (`packages/contracts`)

- Added `ExecutionStatus = 'SUCCEEDED' | 'FAILED' | 'TIMED_OUT' | 'CANCELLED'`.
- Added `ExecutionResult`, `ExecutionContext`, and `Executor` interfaces.
- Extended `AppConfig` with `defaultDockerImage`, `defaultExecutionTimeoutMs`, `maxExecutionTimeoutMs`, `maxOutputBytes`, and `dockerHost`.

### 2. Configuration Package (`packages/config`)

- Added `DEFAULT_DOCKER_IMAGE` (default `'alpine:3.19'`).
- Added `DEFAULT_EXECUTION_TIMEOUT_MS` (default `60000`, min `1000`).
- Added `MAX_EXECUTION_TIMEOUT_MS` (default `1800000`, min `1000`).
- Added `MAX_OUTPUT_BYTES` (default `1048576`, min `1024`).
- Added optional `DOCKER_HOST`.
- Refinement rule: `MAX_EXECUTION_TIMEOUT_MS >= DEFAULT_EXECUTION_TIMEOUT_MS`.
- Unit tests covering default values, overrides, and refinement failures.

### 3. Dedicated Executor Package (`packages/executor`)

- New package `@forge/executor` with composite TypeScript project references.
- `DockerExecutor`: Production container execution engine with non-root UID enforcement, structured CLI arguments, and guaranteed teardown.
- `workspace.ts`: Ephemeral workspace management with cross-platform Windows drive (`C:\...`) to POSIX WSL mount (`/mnt/c/...`) path translation.
- `resource-mapper.ts`: Validates and maps CPU cores and memory limits to Docker CLI flags; marks GPU execution deferred.
- `output-stream.ts`: Bounded stdout and stderr capture.
- Errors: `ExecutionError`, `DockerUnavailableError`, `ContainerStartupError`, `ExecutionTimeoutError`, `ExecutionCancelledError`, `CleanupError`.
- 14 unit tests in `docker-executor.test.ts`.
- 9 live Docker integration tests in `docker-executor.integration.test.ts`.

### 4. Worker Service Integration (`apps/worker`)

- Extended `WorkerShell` with `executeJob`:
  - Pre-execution lease ownership verification.
  - Initial `RUNNING` domain state transition and PostgreSQL persistence.
  - Background periodic lease renewal.
  - Abort on definitive lease loss.
  - Container execution via `Executor`.
  - Terminal domain state transitions (`SUCCEEDED`, `FAILED`, `TIMED_OUT`, `CANCELLED`).
  - Transactional persistence via `withTransaction`.
  - Authoritative lease release.
  - Graceful shutdown aborts running containers before releasing leases and deregistering.
- Unit tests covering full lifecycle, timeout, and lease-loss abort.
- Live integration tests covering end-to-end claim, execution, persistence, and lease release against real PostgreSQL and Docker engines.

### 5. Documentation (`docs/architecture/`)

- Created `docs/architecture/executor.md` specifying executor architecture, container isolation, security boundaries, and failure handling.
- Updated `overview.md`, `glossary.md`, `invariants.md`, and `README.md`.

---

## Verification & Quality Gates

```bash
npm run format:check  # Passed: All files use Prettier code style
npm run lint          # Passed: 0 errors, 0 warnings across all workspaces
npm run typecheck     # Passed: Clean compilation across 14 project references
npm test              # Passed: 33 test files passed, 355 tests passed
npm run build         # Passed: Clean production build across all packages and apps
```
