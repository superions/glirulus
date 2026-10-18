# glirulus-core

Core types and traits for the [Glirulus](https://github.com/superions/glirulus)
durable workflow engine, derived from Sayiir 1.0.0.

This crate models workflows, task registries, codecs and execution snapshots.
It does not provide persistence or execute workflows. `WorkflowBuilder` assembles
continuation trees; `CoreTask` describes task inputs, outputs and metadata;
`WorkflowSnapshot` retains execution position and completed task output.

```toml
[dependencies]
glirulus-core = "=1.0.0-glirulus.1"
```

The default `tokio` feature supplies task-local workflow context. Disable defaults
for a runtime-independent model. Optional `rkyv` support adds archive traits;
concrete JSON and rkyv codecs live in `glirulus-runtime`.

Use `glirulus-persistence` for storage traits, `glirulus-runtime` for execution,
and `glirulus-macros` for `#[task]`, `workflow!` and `BranchKey`. Consumers of
those macros declare Core directly because generated paths name `glirulus_core`.

The inherited snapshot magic, format versions and definition/task identities
are preserved. Changing a Rust crate name does not migrate persisted data.

API documentation: [docs.rs/glirulus-core](https://docs.rs/glirulus-core).
The original MIT grant and fork attribution are in [LICENSE](LICENSE);
[NOTICE](NOTICE) identifies the source archive.
