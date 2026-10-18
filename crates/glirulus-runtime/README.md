# glirulus-runtime

Execution, checkpointing and distributed workers for Glirulus workflows.

`InProcessRunner` executes without persistence. `CheckpointingRunner` saves
execution progress through a persistence backend. `WorkflowClient` submits and
controls workflows, while `PooledWorker` claims and executes available work.
`LiveWorkflowIndex` supports definition registration for an existing worker;
`WorkerHandle::shutdown_with_timeout` bounds shutdown and reaps worker tasks.

## Complete example

From the repository root:

```sh
scripts/check.sh example
```

[examples/basic.rs](examples/basic.rs) is the complete program and also supplies
the crate's rustdoc quick start. It builds an increment task with the public Core
builder, calls `CheckpointingRunner::run` with instance `basic` and input `41`,
requires completion, reads the snapshot through `SnapshotStore`, decodes
`final_output_bytes()` with `JsonCodec` and prints `42`. The checker prints a
command heading in addition to the program's output.

The example uses current-thread Tokio and enables only Runtime's `json` feature.
It needs neither procedural macros nor rkyv. Its `InMemoryBackend` is process-local
and does not survive restarts; use [glirulus-postgres](../glirulus-postgres/README.md)
for PostgreSQL-backed durability.

## Features and consumers

The default features are `json`, `rkyv` and `macros`. At least one codec must be
enabled; `otel` adds optional OpenTelemetry integration. The reduced JSON profile
uses `default-features = false, features = ["json"]`.

Macro consumers declare both `glirulus-core` and `glirulus-runtime` at the matching
version because generated code names both crates directly. Runtime reexports
`#[task]`, `workflow!` and `BranchKey` when `macros` is enabled. See
[glirulus-macros](../glirulus-macros/README.md) for the DSL and generated contracts.
Stable cohort version: [1.0.0 on crates.io](https://crates.io/crates/glirulus-runtime/1.0.0). See the
[versioned API documentation](https://docs.rs/glirulus-runtime/1.0.0/glirulus_runtime/).

The generic runtime preserves existing snapshot codecs, definition/task hashes
and continuation behavior. Business rules, authorization, application effects
and optional caller-owned wait arbitration belong to the consuming application.
See `NOTICE` for exact upstream and generic-fix provenance, `LICENSE` for the MIT
grant, and the repository README for actual qualification and publication status.
