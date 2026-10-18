# glirulus-runtime

Execution, checkpointing and distributed workers for Glirulus workflows.

`InProcessRunner` executes without persistence. `CheckpointingRunner` saves
execution progress through a persistence backend. `WorkflowClient` submits and
controls workflows, while `PooledWorker` claims and executes available work.
The live `WorkflowIndex` supports definition registration for existing workers;
`WorkerHandle::shutdown_with_timeout` bounds shutdown and reaps worker tasks.

The default features are `json`, `rkyv` and `macros`. At least one codec must be
enabled; `otel` adds optional OpenTelemetry integration. The reduced JSON profile
uses `default-features = false, features = ["json"]`.

Macro consumers declare both `glirulus-core` and `glirulus-runtime` at the matching
version because generated code names both crates directly. Runtime reexports
`#[task]`, `workflow!` and `BranchKey` when `macros` is enabled. For example:

```rust
use glirulus_runtime::{task, workflow};

#[task(id = "increment")]
async fn increment(input: u32) -> u32 {
    input + 1
}

let workflow = workflow! {
    name: "example",
    steps: [increment],
}?;
```

The generic runtime preserves existing snapshot codecs, definition/task hashes
and continuation behavior. Business rules, authorization, application effects
and native wait arbitration belong to the consuming application. This source
is a prerelease preparation; publication and qualification status belong to the
repository README. See `NOTICE` for exact upstream and generic-fix provenance,
`LICENSE` for the MIT grant, and API rustdoc for full contracts.
