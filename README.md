# Glirulus

**Durable workflows embedded in your Rust application.**

<img src="artwork/automation.webp" alt="A woodland mascot beside documents and an automation gear" width="640">

Build typed workflows, checkpoint their progress and resume them through a
persistence backend. Glirulus runs inside your application: start with in-memory
execution, then use your PostgreSQL database and pooled workers for durable,
distributed processing. No separate workflow server is required.

Glirulus is an independently maintained fork of
[Sayiir](https://github.com/sayiir/sayiir), maintained by **Daniel Scharf**.
It keeps the original engine model and adds generic integration and recovery
improvements developed while embedding workflows in Myoxus.

## Lightweight by design

Embed the library without an additional orchestration server, message broker or
management UI. Choose the packages you need, reuse your Tokio runtime and attach
an existing SQLx PostgreSQL pool. The reduced JSON profile leaves macros and rkyv
disabled; OpenTelemetry is opt-in.

## What you can build

- Typed task pipelines with branches, parallel forks/joins, loops and child workflows.
- Durable waits for signals and delays, checkpointed task results, retries and backoff.
- Single-process runners or pooled workers sharing a persistence backend.
- Workflows built through the Rust builder API or optional `#[task]`, `workflow!`
  and `BranchKey` macros.
- JSON or rkyv snapshots, with optional OpenTelemetry integration.

The application owns business rules, authorization and external effects.
External effects need application-level idempotency; workflow checkpoints do
not promise exactly-once delivery. In-memory storage is process-local and loses
state on restart. PostgreSQL supplies durable storage.

## Start with 41 → 42

For a minimal JSON consumer:

```toml
[dependencies]
glirulus-core = { version = "=1.0.0", default-features = false, features = ["tokio"] }
glirulus-runtime = { version = "=1.0.0", default-features = false, features = ["json"] }
tokio = { version = "1", features = ["rt", "macros"] }
```

The [complete example](crates/glirulus-runtime/examples/basic.rs) builds an
increment task, runs input `41` with `CheckpointingRunner`, reads the completed
snapshot through `SnapshotStore` and decodes its final output as `42`.
It uses current-thread Tokio and `InMemoryBackend`. From a source checkout:

```sh
scripts/check.sh example
```

Runtime defaults enable `json`, `rkyv` and `macros`; at least one codec is required.
The reduced profile above enables JSON alone. Macro consumers declare both Core
and Runtime because generated code refers to their direct crate names.

## What Glirulus adds

This comparison uses the published **Sayiir 1.0.0 archives**, preserved in the
[original source baseline](https://github.com/superions/glirulus/tree/4a4340481d32a84dd45ef6c5c8fdaba88a3637ef).
It does not describe later Sayiir releases. The workflow builder, macros, codecs,
checkpointing, PostgreSQL backend and basic worker model are inherited features.

| Capability | Original Sayiir 1.0.0 | Glirulus 1.0.0 |
| --- | --- | --- |
| Typed workflows, macros, JSON/rkyv, PostgreSQL checkpoints | Included | Retained |
| Attach an existing PostgreSQL pool without library DDL | Connection setup runs library migrations; no attach-only constructor | `from_pool_without_migrations`; caller supplies the schema |
| Snapshot admission/restart and correlated-event delivery in a caller's transaction | No corresponding caller-transaction APIs | Explicit APIs preserve the caller's commit boundary |
| Correlated-event replay identity | No correlated UUID/acceptance-time API | UUID, acceptance time and exact bytes retained across redelivery |
| Application-owned wait decisions | No `WaitArbitrator` hook | Optional arbitration before snapshot locks on the same connection |
| Register definitions in a running worker | No `LiveWorkflowIndex` | Shared live registration for the existing worker |
| Bounded worker shutdown with task reaping | No `shutdown_with_timeout` | Completion handle and bounded shutdown |

Recovery fixes also cover zero/exhausted retries, continuation from retained task
output, JSON branch routing and consecutive signal/delay parking with buffered
events. Eligible definition filtering happens before polling limits. A rkyv-only
consumer no longer implicitly requires the JSON fallback. See
[UPSTREAM.md](UPSTREAM.md) and package `NOTICE` files for the exact source history
and changes.

Existing `sayiir_workflow_*` SQL objects, `sayiir_task_ready`, snapshot framing and
definition/task identities remain storage contracts. Library-managed databases
append migration `010` to the unchanged original nine migrations. Attaching a
caller-managed pool runs no DDL; the caller must already provide the matching
schema and retain its own migration authority.

## Packages

All five packages use version **1.0.0** and exact sibling version requirements.

| Package | Purpose | API reference |
| --- | --- | --- |
| [`glirulus-core`](https://crates.io/crates/glirulus-core/1.0.0) | Workflow definitions, builders, task contracts and snapshots | [Docs.rs](https://docs.rs/glirulus-core/1.0.0/glirulus_core/) |
| [`glirulus-persistence`](https://crates.io/crates/glirulus-persistence/1.0.0) | Storage traits and in-memory backend | [Docs.rs](https://docs.rs/glirulus-persistence/1.0.0/glirulus_persistence/) |
| [`glirulus-macros`](https://crates.io/crates/glirulus-macros/1.0.0) | Task, workflow and branch-key generation | [Docs.rs](https://docs.rs/glirulus-macros/1.0.0/glirulus_macros/) |
| [`glirulus-runtime`](https://crates.io/crates/glirulus-runtime/1.0.0) | Runners, checkpoints, pooled workers and codecs | [Docs.rs](https://docs.rs/glirulus-runtime/1.0.0/glirulus_runtime/) |
| [`glirulus-postgres`](https://crates.io/crates/glirulus-postgres/1.0.0) | PostgreSQL persistence and transactional integration | [Docs.rs](https://docs.rs/glirulus-postgres/1.0.0/glirulus_postgres/) |

## Development and licensing

Contributions of all sizes are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md).

Use a current Rust toolchain supporting edition 2024. The standalone workspace
has its own lockfile and requires no Myoxus checkout. `scripts/check.sh all` runs
the existing library/doctest, macro, feature and isolated PostgreSQL checks;
PostgreSQL checks require Docker. The Linux checker bounds commands, honors
caller Cargo job limits and removes its labeled disposable containers.
`scripts/release.sh` owns source/package checks and explicit cohort publication.

The Rust crates and documentation are [MIT licensed](LICENSE), retaining
**Copyright (c) 2026 Sayiir** and Daniel Scharf's modification attribution.
Package-local `LICENSE` and `NOTICE` files preserve those grants and provenance.
The illustration is separately licensed under
[AGPL-3.0-or-later](artwork/LICENSE), with its source and attribution in
[artwork/NOTICE](artwork/NOTICE).

*Glirulus* is the genus of the Japanese dormouse, continuing the woodland naming
family of Myoxus and Virgultum.
