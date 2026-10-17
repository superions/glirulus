# Glirulus

Glirulus is being prepared as an independently maintained, Rust-focused fork of
[Sayiir](https://github.com/sayiir/sayiir), an embeddable durable workflow engine.
It is maintained by **Daniel Scharf** for use in
[Myoxus](https://github.com/superions/myoxus).

**Status: repository preparation only.** This repository contains project
documentation and a license. Implementation code has not been imported. There
are no buildable crates, releases or published Glirulus packages yet.

## Purpose

The intended scope is a durable workflow runtime and PostgreSQL persistence,
with particular attention to:

- resuming checkpointed work without repeating acknowledged effects;
- atomic checkpoint and event transitions;
- explicit signal, wait and deadline semantics;
- predictable retries and worker shutdown.

These describe the planned scope, not capabilities delivered by this repository
at its current stage. Business rules, authorization and application-specific
writes belong to the consuming application.

## Name

*Glirulus* is the scientific genus name of the Japanese dormouse,
[*Glirulus japonicus*](https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value=584949).
The name continues the woodland and undergrowth naming family of Myoxus and
Virgultum.

## Upstream relationship

Sayiir is the original project. Glirulus is an independent maintenance effort;
this repository does not represent the Sayiir maintainers.

The planned starting point is the published Sayiir **1.0.0** crate sources.
[UPSTREAM.md](UPSTREAM.md) records the reference archives, checksums and original
licensing. This preparation repository currently has its own documentation-only
history; upstream source and history have not been imported.

Generic corrections should remain small enough to offer upstream independently.
Future imports and upstream updates will need an explicit provenance record and
validation before release.

## License

The original Glirulus repository documentation is licensed under
[MIT](LICENSE), copyright 2026 Daniel Scharf.

Sayiir is MIT-licensed, copyright 2026 Sayiir. Its original copyright and
permission notices must be retained when upstream material is imported. See
[UPSTREAM.md](UPSTREAM.md).
