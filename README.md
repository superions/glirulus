# Glirulus

Glirulus is an independently maintained, Rust-focused fork of
[Sayiir](https://github.com/sayiir/sayiir), an embeddable durable workflow engine.
It is maintained by **Daniel Scharf**, including generic engine fixes used by
[Myoxus](https://github.com/superions/myoxus).

**Status: renamed source fork; registry release pending.** The five packages use
`1.0.0-glirulus.1`. Standalone qualification is complete; registry publication
is a separate step. No package release is claimed by this source checkpoint.

## Packages and boundary

| Package | Rust library | Role |
| --- | --- | --- |
| `glirulus-core` | `glirulus_core` | Workflow definitions, task contracts, hashes and snapshots |
| `glirulus-persistence` | `glirulus_persistence` | Backend traits and in-memory persistence |
| `glirulus-macros` | `glirulus_macros` | `#[task]`, `workflow!` and `BranchKey` generation |
| `glirulus-runtime` | `glirulus_runtime` | Runners, checkpointing and pooled workers |
| `glirulus-postgres` | `glirulus_postgres` | PostgreSQL persistence and transactional admission |

Runtime's default features are `json`, `rkyv` and `macros`; `otel` is optional.
At least one serialization codec must be enabled. A reduced JSON consumer uses
Core with defaults disabled plus `tokio`, and Runtime with defaults disabled
plus `json`. A macro consumer declares both Core and Runtime: generated task
and workflow code refers to their direct public crate names.

Generic engine fixes include attaching an existing PostgreSQL pool without
AutoDDL, eligible-definition filtering, caller-owned snapshot/restart/event
transactions, admission fencing and correlated-event consumption; live
workflow registration, bounded worker shutdown, retry exhaustion, continuation
from retained task output, JSON branch routing and consecutive signal/delay
parking. A consuming application can arbitrate a native wait within the same
transaction before snapshot locks. Business rules, authorization, native
receipts, outbox writes and the application's migration ledger remain with the
consumer; this library does not implement them.

The source takeover preserves the snapshot format, definition hashes, SQL
objects named `sayiir_workflow_*` and the `sayiir_task_ready` channel. These are
persisted contracts, not package branding. External webhook delivery remains
at-least-once. This fork does not claim exactly-once external effects.

Library-managed PostgreSQL schemas append migration `010` to the unchanged
upstream `001`–`009` chain for generic wait/event projections. Applications
attaching an already provisioned pool keep their own migration authority;
attachment never runs DDL. This source takeover does not change the Myoxus
migration ledger or rewrite its data.

## Standalone verification

Use a current Rust toolchain supporting edition 2024 and Cargo. The workspace
has one committed root lockfile and needs no Myoxus checkout or tooling.

```sh
scripts/check.sh build
scripts/check.sh all
```

`build` is the baseline library build. `all` runs inherited library tests and
doctests once, the external macro API consuming test, feature compilation and
the PostgreSQL integration target. `tests`, `features` and `postgres` also run
those groups separately. Disabling defaults still selects JSON explicitly for
Runtime, whose codec-free configuration is intentionally unsupported; separate
package-specific Core/Tokio, Runtime/JSON and Runtime/rkyv checks prevent
PostgreSQL test dependencies from masking the reduced profile.

PostgreSQL integration uses disposable Docker containers rather than an
application database. It requires access to a local Docker daemon. The Linux
checker requires Bash, GNU timeout, setsid and flock, retains caller Cargo job
limits, owns only this checkout's target directory, bounds each command and
removes its labeled test containers and anonymous volumes after completion or
interruption.

## Release preparation

The Linux release entry point additionally requires Python 3.11 or later. Review
and commit all release inputs first, including the release scripts. Select that
full commit SHA explicitly for normal Cargo packaging:

```sh
scripts/release.sh source
scripts/release.sh package --revision <reviewed-full-commit-sha>
```

Both commands require a clean checkout and locked dependency resolution. They
share the checker's exclusive target lock and command limits. Packaging uses
Cargo's normal verification without dirty-tree or verification bypasses. The
archive review checks all five exact package versions and sibling edges,
committed source, generated VCS identity, attribution, README files and SQL
migrations. Its checksums are recorded outside the archives in ignored
`target/release-cohort/` evidence and cannot silently change for the same
reviewed revision.

These commands do not publish packages, create tags or publish GitHub releases.
Registry account/name access, dependency-order publication, downloaded archive
comparison, an external registry consumer and versioned Docs.rs output remain
release acceptance requirements. A partial package cohort is not a release;
already published versions must remain immutable. Registry installation is not
available until the complete cohort has passed that acceptance.

## Qualified source checkpoint

Qualification on 2026-10-18 passed 373 inherited library tests, 30 executable
doctests, one public macro consumer and all 55 PostgreSQL integration tests.
The 19 inherited ignored documentation fragments were not counted as executed
checks. Default, no-default JSON and all-feature workspace compilation passed,
as did independent Core/Tokio and Runtime/JSON and Runtime/rkyv profiles.
A temporary bidirectional old/new JSON snapshot, continuation, hash and ordered
event-codec comparison passed in the consumer's JSON feature context. That
phase-bound diagnostic is removed after acceptance; it is not a permanent suite
or evidence of application-level persisted-run acceptance.

## Upstream and licensing

[UPSTREAM.md](UPSTREAM.md) records all five exact Sayiir 1.0.0 archive identities,
the independently verified PostgreSQL integration target and the exact generic
Myoxus handoff revision. Preparation history and the attributable source
baseline remain in Git; no upstream or Myoxus Git parents have been merged.

Every package retains the complete original MIT grant, **Copyright (c) 2026
Sayiir**, and Daniel Scharf's Glirulus modification attribution. Package-local
`NOTICE` files identify the original sources and maintenance boundary. The
original repository documentation is licensed under [MIT](LICENSE), copyright
2026 Daniel Scharf. Glirulus does not represent the Sayiir maintainers.

*Glirulus* is the scientific genus of the Japanese dormouse, continuing the
woodland naming family of Myoxus and Virgultum.
