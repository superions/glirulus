# Upstream provenance

This is a preparation record for the planned source import. No upstream
implementation code is included in this repository yet.

## Original project

- Project: [Sayiir](https://github.com/sayiir/sayiir)
- Original license: [MIT](https://github.com/sayiir/sayiir/blob/7e6318225398ad7a418efb866156d2cc327ee43f/LICENSE)
- Original copyright: Copyright (c) 2026 Sayiir
- Reference crate version: **1.0.0**
- Source commit recorded by the reference archives:
  [`7e6318225398ad7a418efb866156d2cc327ee43f`](https://github.com/sayiir/sayiir/commit/7e6318225398ad7a418efb866156d2cc327ee43f)

## Reference archives

The intended reference inputs for the runtime and PostgreSQL backend are the
published crates.io archives, identified by their complete SHA-256 checksums:

| Crate | Archive | SHA-256 |
| --- | --- | --- |
| `sayiir-runtime` 1.0.0 | [Download](https://static.crates.io/crates/sayiir-runtime/sayiir-runtime-1.0.0.crate) | `fb257d3c08a51588607ea53dba9d6cdd0724c3ce5ff814f1ba35c39e5f471eb3` |
| `sayiir-postgres` 1.0.0 | [Download](https://static.crates.io/crates/sayiir-postgres/sayiir-postgres-1.0.0.crate) | `42bb11cb0ed558787c59517d12819c1213261ed1b7bd498ec358868be53f3633` |

The PostgreSQL archive's `.cargo_vcs_info.json` records `dirty: true`. The
recorded commit therefore does not establish that its packaged source is an
unchanged checkout of that commit. The archive checksums identify the actual
reference bytes.

## Source import

The first implementation import must record the files and dependencies actually
adopted, retain their original license and copyright notices, and distinguish
upstream source from Glirulus changes. Additional upstream components need their
own source identities; the two archives above do not describe the entire
transitive dependency graph.

Generic engine changes belong in Glirulus. Consumer-specific business rules,
authorization and native data writes remain with their consuming application.

The reference identities above are provenance, not a claim of compatibility,
release qualification or a published Glirulus package.
