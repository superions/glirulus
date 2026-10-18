# glirulus-core

Core types and traits for the [Glirulus](https://github.com/superions/glirulus)
durable workflow engine, derived from Sayiir 1.0.0.

This crate models workflows, task registries, codecs and execution snapshots.
It does not provide persistence or execute workflows. `WorkflowBuilder` assembles
continuation trees; `CoreTask` describes task inputs, outputs and metadata;
`WorkflowSnapshot` retains execution position and completed task output.

Stable cohort version: [1.0.0](https://crates.io/crates/glirulus-core/1.0.0).
Use matching versions of all Glirulus packages.

```toml
[dependencies]
glirulus-core = "=1.0.0"
```

The default `tokio` feature supplies task-local workflow context. Disable defaults
for a runtime-independent model. Optional `rkyv` support adds archive traits;
concrete JSON and rkyv codecs live in `glirulus-runtime`.

Use `glirulus-persistence` for storage traits, `glirulus-runtime` for execution,
and `glirulus-macros` for `#[task]`, `workflow!` and `BranchKey`. Consumers of
those macros declare Core directly because generated paths name `glirulus_core`.

For a complete JSON workflow using this builder, run `scripts/check.sh example`
from the repository root; [Runtime's basic example](../glirulus-runtime/examples/basic.rs)
prints `42` using process-local storage.

The inherited snapshot magic, format versions and definition/task identities
are preserved. Changing a Rust crate name does not migrate persisted data.

Version-specific API reference: [docs.rs/glirulus-core/1.0.0](https://docs.rs/glirulus-core/1.0.0/glirulus_core/).
The original MIT grant and fork attribution are in [LICENSE](LICENSE);
[NOTICE](NOTICE) identifies the source archive.
