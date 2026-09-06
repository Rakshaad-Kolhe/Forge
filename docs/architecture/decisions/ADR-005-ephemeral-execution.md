# ADR-005: Ephemeral Execution Environments

## Status

Accepted

## Context

CI/CD workloads execute arbitrary, user-defined shell scripts, source code, dependency managers, and compilation tools. In multi-tenant, self-hosted environments, executing these workloads introduces severe security, stability, and reproducibility hazards:

- **Cross-Build Contamination**: Residual files, background daemon processes, modified environment variables, or global package caches left by one build can silently alter or corrupt the execution of subsequent builds.
- **Security & Multi-Tenant Isolation**: Untrusted pull requests could execute malicious code that accesses host filesystem secrets, queries internal network services, or tampers with the CI engine itself.
- **Resource Exhaustion**: A single runaway process (e.g. an infinite memory allocation or fork-bomb) could crash the host worker node, affecting all other concurrent builds on that host.
- **State Drift**: Long-lived environments suffer from configuration drift over time ("it worked on worker A but failed on worker B"), destroying reproducible CI builds.

To guarantee repeatable, isolated, and secure execution, Forge V2 requires a formal execution model that treats execution environments as strictly ephemeral, isolated, and disposable.

---

## Decision

All CI/CD jobs in Forge V2 must execute inside **isolated, disposable, and ephemeral execution environments**.

No user-defined job commands may execute directly in the host worker's operating system environment.

### The Executor Abstraction

Forge establishes the `Executor` abstraction to decouple task execution from specific container engines:

```text
               ┌───────────────────────┐
               │   Executor Interface  │
               └───────────┬───────────┘
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
  ┌─────────────────────┐     ┌─────────────────────┐
  │   DockerExecutor    │     │ KubernetesExecutor  │
  │ (Local Daemon/Sock) │     │ (K8s Pods / Jobs)   │
  └─────────────────────┘     └─────────────────────┘
```

- `DockerExecutor`: Uses the local Docker/Containerd engine on the worker host to spawn a clean container for each job.
- `KubernetesExecutor`: Spawns a dedicated, isolated Kubernetes Pod for each job, terminating the Pod upon completion.

### The Ephemeral Execution Lifecycle

Every job execution attempt follows a strict, one-way lifecycle:

```text
          Claimed Job
               │
               ▼
  1. Prepare Isolated Workspace
     (Create fresh temporary disk directory, checkout code, stage caches)
               │
               ▼
  2. Create Execution Environment
     (Provision fresh container / Pod with resource constraints)
               │
               ▼
  3. Execute Commands
     (Stream stdout/stderr, enforce timeouts, monitor health)
               │
               ▼
  4. Collect Results & Artifacts
     (Extract exit codes, harvest declared build artifacts, store logs)
               │
               ▼
  5. Terminate Environment
     (Kill container / pod, terminate lingering background processes)
               │
               ▼
  6. Cleanup Workspace
     (Purge temporary directories, unmount volumes, reclaim disk space)
```

The execution environment is **never reused** across different jobs. When a job completes—whether through success, failure, or cancellation—the execution environment is destroyed.

---

## The Core Distinction: Worker vs. Execution Environment

```text
┌────────────────────────────────────────────────────────┐
│          Long-Lived Worker Daemon (apps/worker)        │
│  - Statically provisioned host service                 │
│  - Communicates with Scheduler & Redis                 │
│  - Manages machine lifecycle and Executor instances    │
│  - Never runs untrusted user scripts directly          │
└──────────────────────────┬─────────────────────────────┘
                           │ spawns & supervises
                           ▼
┌────────────────────────────────────────────────────────┐
│      Ephemeral Execution Environment (Container/Pod)   │
│  - Created dynamically for a single job attempt        │
│  - Runs user commands with restricted permissions      │
│  - Destroyed immediately upon attempt completion       │
└────────────────────────────────────────────────────────┘
```

A worker process is long-lived and trusted; the execution environment it spawns is temporary and untrusted.

---

## Security Principles & Isolation Controls

Future executor implementations must incorporate the following isolation principles:

1. **Non-Root Execution**: Container processes should execute as an unprivileged UID/GID whenever possible to restrict host escape vulnerabilities.
2. **Resource Constraints (cgroups)**: Strict CPU limits, memory quotas, and swap constraints to prevent single-job resource starvation.
3. **Hard Execution Timeouts**: Strict wall-clock limits enforced by the supervisor; jobs exceeding timeouts are forcibly killed (`SIGKILL`).
4. **Filesystem Isolation**: Jobs operate in dedicated workspace bind-mounts. Sensitive host filesystems (`/etc`, `/proc`, `/sys`) must remain read-only or masked.
5. **Network Boundaries**: Default network sandboxing, preventing untrusted jobs from querying internal infrastructure metadata endpoints.
6. **Guaranteed Teardown**: Teardown hooks must run in `finally` blocks, ensuring that even crashed jobs do not leave orphaned containers or dangling disk allocations.

_(Note: These controls represent architectural intent for future PRs and are not implemented in PR 02.)_

---

## Rationale

- **Reproducibility**: Every build starts from a known, pristine container image. A build never fails due to state left behind by a previous build.
- **Security Isolation**: Multi-tenant builds and untrusted pull requests are sandboxed within isolated container boundaries, protecting host credentials and node integrity.
- **Declarative Environments**: Developers can declare exact runtime images (e.g. `node:20-alpine`, `golang:1.24`, `python:3.12`) in their pipeline definitions without requiring the host worker to install multiple competing toolchains.

---

## Consequences

### Positive

- **Deterministic Results**: Zero cross-build pollution or configuration drift.
- **Safe Multi-Tenancy**: Untrusted user code cannot compromise host workers or access sibling job workspaces.
- **Portable Runtimes**: Works equally well on single-node Docker hosts and large-scale Kubernetes clusters through the unified `Executor` abstraction.

### Negative / Trade-offs

- **Container Startup Overhead**: Spawning a fresh container or Kubernetes Pod adds startup latency (typically 500ms–3s) compared to executing bare metal scripts directly on the host shell.
- **Cache Management Overhead**: Because filesystems are destroyed after each run, caching mechanisms (e.g., package manager caches) require explicit caching layers and volume staging.

---

## Invariants

1. User-defined CI job commands must never run directly within the host worker's operating system environment.
2. An execution environment (container or pod) must never be shared across distinct jobs or reused across attempts.
3. When a job completes, fails, or is cancelled, its execution environment and temporary workspace must be completely destroyed.
4. The worker daemon itself must remain isolated from the containerized user processes it supervises.

---

## Failure Behavior

- **Container Engine Failure**: If Docker or Kubernetes fails to launch the environment, the attempt is marked as an infrastructure failure and reported clearly to the scheduler.
- **Out of Memory (OOM) Kill**: If a job exceeds its memory quota, the executor captures the OOM event, marks the attempt as failed with an explicit `OOMKilled` reason, and cleans up the container.
- **Job Cancellation**: If a user cancels a running job, the worker issues an immediate `SIGTERM` followed by a `SIGKILL` to the container, and initiates complete workspace cleanup.

---

## Alternatives Considered

### 1. Bare Metal / Direct Host Execution

- _Trade-off_: Lowest latency, zero container overhead, direct access to host toolchains.
- _Why Rejected_: High risk of cross-build contamination, security vulnerabilities, impossible multi-version toolchain management, and vulnerability to catastrophic host corruption from rogue user scripts (`rm -rf /`).

### 2. Long-Lived Per-Project Containers

- _Trade-off_: Faster subsequent builds because dependencies remain pre-installed in the container.
- _Why Rejected_: Hidden state drift. Builds break unexpectedly when files are modified across runs, causing non-reproducible CI failures that are notoriously difficult for engineers to debug.

### 3. Persistent Build Virtual Machines

- _Trade-off_: High security isolation with hypervisor boundaries.
- _Why Rejected_: High provisioning latency (tens of seconds per VM) and heavy compute overhead make VMs impractical for lightweight, rapid CI feedback loops in comparison to lightweight containers.

---

## Implementation Implications

- PR 01 and PR 02 contain zero Docker, Kubernetes, or container engine dependencies.
- PR 05 will define the `Executor` TypeScript interface in the worker workspace and introduce the `DockerExecutor` implementation.
- Workspace cleanup logic must be hardened with robust filesystem permissions handling to account for files created by non-root users inside containers.

---

## Validation

- Verified that PR 01 and PR 02 introduce no Docker SDK, Kubernetes client, or container execution code.
- The architectural lifecycle and boundary definitions are documented and agreed upon.

---

## Related Decisions

- [ADR-001: Explicit Service and Package Boundaries](ADR-001-service-boundaries.md)
- [ADR-002: PostgreSQL as Source of Truth](ADR-002-postgresql-source-of-truth.md)
- [ADR-004: At-Least-Once Delivery and Idempotent State Transitions](ADR-004-at-least-once-delivery.md)
