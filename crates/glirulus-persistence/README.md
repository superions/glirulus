# glirulus-persistence

Storage contracts and an in-memory backend for the
[Glirulus](https://github.com/superions/glirulus) durable workflow engine,
derived from Sayiir 1.0.0.

`SnapshotStore` reads and writes snapshots. `SignalStore` supplies cancellation
and pause primitives. `PersistentBackend` combines those two traits through a
blanket implementation. `TaskClaimStore` and `TaskResultStore` add distributed
claiming and retained task outputs. Custom backends implement the relevant
focused traits rather than implementing the blanket supertrait directly.

Registry publication is pending. These version declarations describe the planned
cohort; use the source checkout until release acceptance.

```toml
[dependencies]
glirulus-core = "=1.0.0-glirulus.1"
glirulus-persistence = "=1.0.0-glirulus.1"
```

For an existing async context:

```rust
use glirulus_core::{DefinitionHash, snapshot::WorkflowSnapshot};
use glirulus_persistence::{BackendError, InMemoryBackend, SnapshotStore};

async fn example() -> Result<(), BackendError> {
    let backend = InMemoryBackend::new();
    let mut snapshot = WorkflowSnapshot::new(
        "instance-123",
        DefinitionHash::from("workflow-hash"),
    );
    backend.save_snapshot(&mut snapshot).await?;
    let _loaded = backend.load_snapshot("instance-123").await?;
    Ok(())
}
```

`InMemoryBackend` is process-local storage for tests and prototypes; it does not
survive restarts. `glirulus-postgres` provides the durable PostgreSQL backend.
Claim fencing and effect acknowledgement remain responsibilities of the relevant
backend and consuming application; a claim alone does not promise exactly-once
external effects.

Versioned API documentation after publication: [docs.rs/glirulus-persistence](https://docs.rs/glirulus-persistence).
The original MIT grant and fork attribution are in [LICENSE](LICENSE);
[NOTICE](NOTICE) identifies the source archive.
