# Forge V2 — Architecture Glossary

This glossary defines standard, non-negotiable terminology for Forge V2. All future implementation PRs, code symbols, database models, and documentation must adhere to these definitions to prevent semantic drift.

---

### Pipeline

The declarative definition of an automated workflow (parsed from a pipeline configuration file such as YAML). A pipeline defines directed acyclic graph (DAG) relationships between jobs, trigger conditions (branches, tags, webhooks), and environment parameters. It represents static configuration, not an execution instance.

### Pipeline Run

A specific execution instance of a Pipeline triggered by a user, webhook, or schedule. A Pipeline Run tracks the overall status (e.g., `pending`, `running`, `passed`, `failed`, `cancelled`), start and completion timestamps, triggering commit SHA, and the collection of Jobs comprising the execution graph.

### Job

A distinct node within a Pipeline DAG representing a cohesive unit of work to be executed on a single worker node (e.g., "lint", "test", "build-frontend"). A Job defines dependencies on predecessor jobs, required container images, environment variables, commands, and target runner criteria.

### Job Attempt

A specific physical execution attempt of a Job. If a Job fails due to an infrastructure outage or timeout and is retried, a new Job Attempt is created with an incremented attempt number (`attempt_id`). All logs, worker allocations, execution durations, and exit codes belong to a specific Job Attempt.

### Worker

A long-lived host daemon (`apps/worker`) running on an execution host or virtual machine. The Worker registers with the cluster, emits periodic heartbeats, claims Jobs from the scheduling layer, supervises the local Executor, streams execution logs, and reports attempt completion.

### Scheduler

The centralized or clustered control plane service (`apps/scheduler`) responsible for evaluating DAG dependencies, maintaining queue priority ordering, allocating worker leases, detecting timed-out attempts, and dispatching executable work.

### Executor

The pluggable runtime abstraction (`Executor` interface) within a Worker responsible for spawning, supervising, and cleaning up an isolated, disposable execution environment (e.g. `DockerExecutor`, `KubernetesExecutor`).

### Lease

An ephemeral, time-bounded exclusive lock held by a Worker over a Job in Redis (e.g., using atomic key-value expiration). A lease authorizes a specific Worker to execute a Job Attempt. If the Worker crashes and fails to renew the lease heartbeat, the lease expires, allowing the Scheduler to safely reassign the Job.

### Queue

The transient, in-memory FIFO and priority ordering mechanism hosted in Redis used to stage executable Jobs awaiting assignment to available Workers.

### Source of Truth

The authoritative, durable persistent data store (PostgreSQL) where all historical records, user configurations, pipeline runs, job statuses, audit trails, and artifact metadata are durably committed under ACID guarantees.

### Event

A discrete notification representing a notable change of state within the system (e.g., `job.queued`, `job.started`, `job.completed`, `worker.heartbeat`). Events are used for inter-service signaling and WebSocket UI streaming; they are derived from or accompany state transitions.

### Idempotency

The property whereby an operation can be applied multiple times without changing the result beyond the initial application. In Forge, all state transitions in PostgreSQL (such as updating job outcomes or marking runs complete) must be strictly idempotent against duplicate message delivery or delayed acknowledgments.

### At-Least-Once

The distributed delivery guarantee ensuring that every message or task will be delivered to a recipient at least once, but may be redelivered in the event of network partitions or node failures. Forge relies on At-Least-Once delivery paired with Idempotent State Transitions.

### Ephemeral Execution

The architectural requirement that every Job Attempt executes inside a pristine, completely isolated, and disposable container or pod environment that is constructed immediately prior to execution and completely destroyed upon attempt completion.

### Artifact

A durable binary asset or set of files generated during a Job Attempt (e.g., compiled binaries, test coverage reports, distribution tarballs). Artifacts are uploaded to blob storage upon job completion, and their metadata and checksums are recorded in the PostgreSQL Source of Truth.

### Cache

A transient storage mechanism used to accelerate repetitive build operations (such as language dependency directories or build compiler caches) across job executions. Caches are strictly disposable; the loss of a cache degrades build speed but must never cause build failures or incorrect outcomes.

### Operational Eligibility

The dual-predicate condition requiring a worker node to be both in durable `status = READY` (PostgreSQL) and transient `liveness = ALIVE` (unexpired heartbeat in Redis) before being considered as a candidate for job placement.

### DeterministicFirstEligible

The baseline worker selection policy for Forge V2 that sorts candidate workers matching declared job requirements into a canonical, locale-independent sequence based on ascending `workerId` code-points and selects the first worker in that ordering.

### ScheduleDecision

The typed, explainable result produced by the scheduler service. It explicitly differentiates successful placements (`SCHEDULED`, identifying the selected worker, candidate counts, and job priority) from placement failures (`UNSCHEDULABLE`, detailing why no candidate matched and job priority).

### Job Priority

A bounded integer assigned to a pipeline step or job (`[-1000, 1000]`, default `0`) indicating its scheduling precedence. Higher numerical values indicate higher scheduling urgency. Enforced via domain validation and PostgreSQL check constraints.

### HighestPriorityFirst

The baseline deterministic priority scheduling policy in Forge V2. Ready jobs are ordered strictly by descending priority (`priority DESC`), with ties broken deterministically by ascending job ID (`jobId ASC`) alphanumeric code-point order.

### Non-Blocking Unschedulable Semantics

The scheduling invariant that an unschedulable higher-priority job (e.g., unsatisfiable CPU/memory constraints) must never block eligible lower-priority jobs in a batch from being evaluated and assigned to compatible workers.

### Worker Lease

A renewable, time-bounded distributed ownership record persisted authoritatively in PostgreSQL (`worker_leases`) that grants a specific worker the exclusive right to execute a job for a specified duration.

### Active Lease Exclusivity

The fundamental distributed systems invariant enforcing that at most one active lease (`status = 'ACTIVE'`) may exist for a given job at any point in time. Enforced authoritatively via PostgreSQL partial unique index `uq_worker_leases_active_job`.

### Lease Expiration

The automatic transition of lease validity when current database time exceeds `expires_at`. An expired lease can no longer be renewed or released by its original owner, and permits a replacement worker to claim the job.

### Lease Renewal

The atomic operation extending an active lease's `expires_at` timestamp by its recorded owner prior to expiration, ensuring long-running tasks retain ownership without interruption.

### Lease Conflict

The condition where a worker attempts to claim or schedule a job that is already held by another active, unexpired lease owner. Results in status `CONFLICT` or `UNSCHEDULABLE` with reason `LEASE_CONFLICT`.

### Reclaim Expired Leases

A background or reactive sweep operation that scans the PostgreSQL database for leases with `status = 'ACTIVE'` and `expires_at <= NOW()` and transitions their status to `'EXPIRED'`.

### DockerExecutor

The production-oriented container execution implementation of `Executor` (`@forge/executor`) that executes claimed jobs inside isolated, disposable non-root containers using the Docker engine.

### Execution Context

The typed specification passed to an Executor detailing the target command, container image, environment variables, CPU/memory constraints, and abort signals for a specific job attempt.

### Execution Result

The structured outcome returned by an Executor upon attempt completion, encapsulating execution status (`SUCCEEDED`, `FAILED`, `TIMED_OUT`, `CANCELLED`), exit code, duration, timestamps, and bounded stdout/stderr output.

### Ephemeral Workspace

A unique, isolated temporary filesystem directory created per execution attempt on the worker host, bind-mounted into the container at `/workspace`, and guaranteed to be destroyed upon attempt completion.

### Non-Root Execution

The security requirement and runtime enforcement ensuring that containerized user processes run as an unprivileged user (`--user 1000:1000` by default) rather than privileged root inside the container.
