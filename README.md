# Glirulus

Glirulus is an independently maintained, Rust-focused fork of
[Sayiir](https://github.com/sayiir/sayiir), an embeddable durable workflow engine.
It is maintained by **Daniel Scharf**, including generic engine fixes used by
[Myoxus](https://github.com/superions/myoxus).

**Status: renamed source fork; registry release pending.** The five packages use
`1.0.0-glirulus.1`. Initial engine qualification is recorded below; registry
publication is a separate step. No package release is claimed by this source
checkpoint.

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
parking. Applications may install an optional `WaitArbitrator` to resolve
accepted outcomes, absolute deadlines and stop decisions on the same transaction
connection before the engine locks a snapshot. The application owns that policy;
caller-owned transaction APIs also leave commit to the caller. Other backend
operations retain their own transaction boundary. Event UUIDs, acceptance times
and exact payload bytes identify correlated deliveries; consumed identities remain
available for replay.
Business rules, authorization, effect acknowledgement, event delivery and the
application's migration ledger remain with the consumer.

The source takeover preserves the snapshot format, definition hashes, SQL
objects named `sayiir_workflow_*` and the `sayiir_task_ready` channel. These are
persisted contracts, not package branding. External webhook delivery remains
at-least-once. This fork does not claim exactly-once external effects.

Library-managed PostgreSQL schemas append migration `010` to the unchanged
upstream `001`–`009` chain for generic wait/event projections. Applications
attaching an already provisioned pool keep their own migration authority;
attachment never runs DDL. This source takeover does not change the Myoxus
migration ledger or rewrite its data.

## Run a complete workflow

From a source checkout, run:

```sh
scripts/check.sh example
```

The [complete example](crates/glirulus-runtime/examples/basic.rs) builds a JSON
integer increment task, runs input `41` with `CheckpointingRunner`, reads the
completed snapshot through `SnapshotStore` and decodes its final output. The
program prints `42`; the checker also prints its command heading. It uses a
current-thread Tokio runtime and the reduced JSON feature profile, without
requiring the macro or rkyv features.

The example's `InMemoryBackend` is process-local and loses its state on restart.
Use [`glirulus-postgres`](crates/glirulus-postgres/README.md) for PostgreSQL-backed
durability. Registry installation remains pending release acceptance; the
checkout example is the runnable entry point in the meantime.

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

Before publishing, establish that the existing secure Cargo credential belongs
to the intended crates.io account and has permission to publish all five names.
The public registry checks cannot prove credential identity or token scopes.
After verifying that prerequisite, explicitly select publication:

```sh
scripts/release.sh publish --revision <reviewed-full-commit-sha> --owner <crates-io-login> --account-verified
```

`--account-verified` records the operator's confirmation; it performs no
automated account verification. Never put a token in command arguments. The
publisher checks all five names and any existing versions before writing, then
publishes Core, Persistence, Macros, Runtime and PostgreSQL in that order. It
repackages each missing version against the real registry and compares it with
the pinned cohort before upload, then compares registry checksums and downloaded
archives against those same original bytes. Resuming skips only versions whose
bytes match; an unexpected owner,
yanked version or different archive blocks publication. A source correction
after partial publication requires a new coherent version cohort.

No mode creates commits, tags or GitHub releases. An external registry consumer
with reduced JSON and default macro profiles, versioned Docs.rs output for all
five packages and the GitHub prerelease remain release acceptance requirements.
The release tag must identify the reviewed package commit; a later documentation
status update must leave it intact. A partial package cohort is not a release,
and registry installation is not accepted until the complete cohort passes.

## Qualified source checkpoint

After the pre-release implementation refactor on 2026-10-18, all 373 inherited
library tests, 30 doctests (including compilation-only examples) and the public
macro consumer passed. The 19 inherited ignored documentation fragments were
not counted as passed checks. Default, no-default JSON and all-feature workspace
compilation passed, as did independent Core/Tokio, Runtime/JSON and Runtime/rkyv
profiles. The complete basic example printed `42`; its shared Runtime doctest
also executed successfully.

The earlier 55 PostgreSQL integration tests remain accepted evidence: executable
PostgreSQL code and migrations are unchanged by this refactor. A temporary
pre/post-refactor comparison passed for public type identities, rehydrated
continuation structures and definition hashes, a stable framed snapshot and
ordered JSON payload bytes in the consuming JSON feature context. Temporary
comparison sources and targets are removed at acceptance; these checks are not
a permanent suite or application-level persisted-run acceptance.

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
