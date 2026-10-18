# Upstream provenance

This records the attributable Sayiir source baseline and the renamed Glirulus
fork with its generic engine fixes. Registry publication remains a separate step.

## Original project

- Project: [Sayiir](https://github.com/sayiir/sayiir)
- Original license: [MIT](https://github.com/sayiir/sayiir/blob/7e6318225398ad7a418efb866156d2cc327ee43f/LICENSE)
- Original copyright: Copyright (c) 2026 Sayiir
- Reference crate version: **1.0.0**
- Source commit recorded by the reference archives:
  [`7e6318225398ad7a418efb866156d2cc327ee43f`](https://github.com/sayiir/sayiir/commit/7e6318225398ad7a418efb866156d2cc327ee43f)

## Reference archives

The authoritative inputs are the published crates.io archives, downloaded and
verified by their complete SHA-256 checksums:

| Original crate 1.0.0 | Final directory | Archive SHA-256 |
| --- | --- | --- |
| [`sayiir-core`](https://static.crates.io/crates/sayiir-core/sayiir-core-1.0.0.crate) | `crates/glirulus-core` | `59026c3d3594059fa7314c11edf35a5cc489acf8a2a022904dfcafd46248ad28` |
| [`sayiir-persistence`](https://static.crates.io/crates/sayiir-persistence/sayiir-persistence-1.0.0.crate) | `crates/glirulus-persistence` | `6e66b5bad8268ae6717213078e391ff2b9488ea3aaac3e18a87ce9d1744e5c6a` |
| [`sayiir-macros`](https://static.crates.io/crates/sayiir-macros/sayiir-macros-1.0.0.crate) | `crates/glirulus-macros` | `f0a8982ba58b85f72e3e5f1c1d9da1ff23bcd7ee37106dd11c5fa95357d2ade8` |
| [`sayiir-runtime`](https://static.crates.io/crates/sayiir-runtime/sayiir-runtime-1.0.0.crate) | `crates/glirulus-runtime` | `fb257d3c08a51588607ea53dba9d6cdd0724c3ce5ff814f1ba35c39e5f471eb3` |
| [`sayiir-postgres`](https://static.crates.io/crates/sayiir-postgres/sayiir-postgres-1.0.0.crate) | `crates/glirulus-postgres` | `42bb11cb0ed558787c59517d12819c1213261ed1b7bd498ec358868be53f3633` |

The PostgreSQL archive's `.cargo_vcs_info.json` records `dirty: true`. The
recorded commit therefore does not establish that its packaged source is an
unchanged checkout of that commit. The archive checksums identify the actual
reference bytes.

## Source import

Source and useful embedded tests were imported from those exact archives in
baseline commit `4a4340481d32a84dd45ef6c5c8fdaba88a3637ef`, which retains the
original Sayiir package/library identities. That immutable commit also retains
the original authoring manifests and archived VCS records. The current packages
are renamed to `glirulus-*` / `glirulus_*` at stable `1.0.0`, with exact
sibling path-plus-version edges and the original feature intent. Stale imported
`Cargo.toml.orig` and `.cargo_vcs_info.json` files are removed from active crate
roots so Cargo can generate current package provenance at release. The single
root `Cargo.lock` owns resolution; archived per-crate lockfiles are not active.
Trailing whitespace on blank lines in Persistence’s inherited README was removed
without changing its examples or prose.

The PostgreSQL archive contains and explicitly declares `tests/integration.rs`,
but omits its development dependencies. The test bytes were independently
verified against the recorded source commit; the corresponding development
dependency definitions were restored from
[`sayiir-postgres/` at the recorded source commit](https://github.com/sayiir/sayiir/tree/7e6318225398ad7a418efb866156d2cc327ee43f/sayiir-postgres).
The original integration source SHA-256 is
`820d72eaffa21f7a4ec89c700ff627b155f57e622267e04bd445107e459d9688`;
the corresponding source manifest SHA-256 is
`923818dfdff66e349ec968d55f9824673680a2b66398857055ce2fe77278c617`.
Its explicit Cargo target is retained despite `autotests = false`. The only
harness repair labels containers when `GLIRULUS_TEST_RUN_ID` is set, so the
bounded standalone checker can remove exactly its disposable resources after
an interruption. Assertions and test cases are unchanged.

The complete original MIT grant in every crate was verified against the shared
upstream `LICENSE` at that same commit (SHA-256
`11ae41f8a2eb5d9ed7673c09010d90e1d3b6724df166c5405cdda83b16a8af9a`).
All five published archives omit that file; using
the shared grant restores the actual upstream permission and authorship.
Glirulus modifications are attributed to Daniel Scharf without
replacing `Copyright (c) 2026 Sayiir`.

Generic engine changes belong in Glirulus. Consumer-specific business rules,
authorization and native data writes remain with their consuming application.

## Generic engine handoff

The complete PostgreSQL and Runtime generic source trees are taken from the
reviewed, clean source at pushed Myoxus revision
`fd7353183f4f1a566394c42bfe0bbef0b942a350`, under
`server/crates/infrastructure/src/workflow_runtime/extensions/`.
Its exact Git tree is `1c4cdeffeabe2947f2ac2bb925fb258cf98fa554`. This includes
all generic fixes in that checkpoint's `NOTICE`, including the most recent
consecutive signal/delay parking and buffered-event drain. Source paths and
public reexports are renamed coherently; persisted identifiers are retained.

The PostgreSQL delta retains pool attachment without AutoDDL, filtering eligible
definitions before limits, caller-owned snapshot/restart/event transactions,
snapshot admission fencing, correlated-event identity consumption and a
caller-supplied native wait arbitration hook before snapshot locks. The Runtime
delta retains live definition registration, bounded worker completion/shutdown,
zero/exhausted retry handling, continuation from retained task output, already
advanced signal checkpoints, JSON branch routing/input and declared-selector
dispatch, `routingEmpty`, and parking consecutive signal/delay controls while
draining buffered events. Package-local notices describe those generic changes.
The fork additionally gates the JSON loop-envelope fallback on its optional
`json` feature, so an independent `rkyv`-only consumer compiles without implicitly
enabling JSON. The binary envelope path and JSON-enabled behavior are unchanged.
The fork also restores uncorrelated FIFO buffer consumption outside a parked
wait only when no native arbitrator is installed, and atomically flushes terminal
signal task output before terminal state discards its task map. Correlated
identities, native arbitration, completion timestamps and caller transaction
ownership are retained. Existing inherited PostgreSQL tests protect both fixes.

Myoxus's typed Application port, native `waits.rs` arbitration, receipts,
authorization, assistance modes, outbox and migration ledger remain outside this
repository. No operational files, credentials, private native payloads, TODOs,
learnings, business modules or Myoxus Git parents are imported.

The `sayiir_workflow_*` SQL names, `sayiir_task_ready`, snapshot envelope and
codec identities, task/definition hashes and event bytes remain storage
contracts. Myoxus already provisions the required generic schema and continues
to attach without AutoDDL; no Myoxus migration or data rewrite accompanies the
rename. For library-managed standalone databases, Glirulus adds migration
`010_correlated_event_wait_projection.sql`: four nullable generic wait/event
projection columns and the correlated-event identity index required by the
ported code. The nine original migrations remain byte-for-byte unchanged.
This additive library migration was authored under MIT from the generic engine
contracts; no native schema, application ledger or business migration is copied.
Private old/new JSON snapshot/continuation/hash comparison is bounded to
qualification; it is not an additional permanent test suite.

## Pre-release maintenance boundaries

Before the first registry publication, construction and continuation behavior
was separated into private Core implementation modules, and the pooled worker
into actor, claimed dispatch and continuation progression modules. Public types
remain in their original defining modules. The split retains the engine model,
public API, generic bounds, serialized contracts and execution order; it does
not replace the recorded generic handoff with a second implementation.

The Runtime package includes a complete `examples/basic.rs` entry point using
JSON and process-local in-memory persistence. Its explicit Cargo target and
archive inclusion are part of release-input validation. Registry publication,
downloaded-package consumption and application-level persisted-run acceptance
remain separate from this source refactoring.
