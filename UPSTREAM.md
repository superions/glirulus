# Upstream provenance

This records the imported upstream source baseline. It is not yet the renamed
Glirulus engine, a port of Myoxus fixes, or a registry release.

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

Source and useful embedded tests come from those exact archives. The original
package/library names and upstream dependency intent are retained at this
baseline. Normalized manifests are repaired only for standalone workspace and
packaging use: exact sibling path-plus-version edges, restored PostgreSQL dev
dependencies, current repository metadata and included legal material. Original
`Cargo.toml.orig` files remain provenance inputs, not active manifests; per-crate
archive lockfiles are not activated as workspace locks. The root `Cargo.lock`
owns resolution. Trailing whitespace on blank lines in Persistence’s inherited
README is removed; its examples and prose are unchanged.

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
Glirulus packaging modifications are attributed to Daniel Scharf without
replacing `Copyright (c) 2026 Sayiir`.

Generic engine changes belong in Glirulus. Consumer-specific business rules,
authorization and native data writes remain with their consuming application.
No Myoxus source, operational files, credentials, native business modules or Git
history are imported into this baseline.
