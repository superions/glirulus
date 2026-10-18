# Glirulus

Glirulus is being prepared as an independently maintained, Rust-focused fork of
[Sayiir](https://github.com/sayiir/sayiir), an embeddable durable workflow engine.
It is maintained by **Daniel Scharf** for use in
[Myoxus](https://github.com/superions/myoxus).

**Status: imported upstream source baseline.** The five Sayiir 1.0.0 crates
are wired into a standalone Cargo workspace. This baseline deliberately retains
the original package and library names; the final directories are already named
`crates/glirulus-*`. No Glirulus package or release has been published, and the
Myoxus generic engine fixes have not been ported yet.

## Purpose

The intended scope is a durable workflow runtime and PostgreSQL persistence,
with particular attention to:

- resuming checkpointed work without repeating acknowledged effects;
- atomic checkpoint and event transitions;
- explicit signal, wait and deadline semantics;
- predictable retries and worker shutdown.

The upstream implementation is the starting point for that maintenance work,
not a claim that the later fixes or release qualification are complete. Business
rules, authorization and application-specific writes belong to the consuming
application.

## Name

*Glirulus* is the scientific genus name of the Japanese dormouse,
[*Glirulus japonicus*](https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value=584949).
The name continues the woodland and undergrowth naming family of Myoxus and
Virgultum.

## Upstream relationship

Sayiir is the original project. Glirulus is an independent maintenance effort;
this repository does not represent the Sayiir maintainers.

The source baseline is the published Sayiir **1.0.0** crate archives.
[UPSTREAM.md](UPSTREAM.md) records all five exact archive identities, the
independently verified PostgreSQL integration target and original licensing.
The original Glirulus preparation history is preserved; upstream Git parents
have not been merged.

Generic corrections should remain small enough to offer upstream independently.
Future imports and upstream updates will need an explicit provenance record and
validation before release.

## Standalone verification

Use a current Rust toolchain supporting edition 2024 and Cargo. The workspace
has one committed root lockfile and needs no Myoxus checkout or tooling.

```sh
scripts/check.sh build
```

The `tests`, `features` and `postgres` modes separate inherited lib/doctests,
feature compilation and the restored PostgreSQL integration target. The `all`
mode runs those three once in that order. PostgreSQL integration uses disposable
Docker containers, not an existing application database; it requires access to
a local Docker daemon. Caller Cargo job limits are retained. Each command is
bounded and owned test containers are removed on completion or interruption.

## License

The original Glirulus repository documentation is licensed under
[MIT](LICENSE), copyright 2026 Daniel Scharf.

Sayiir is MIT-licensed, copyright 2026 Sayiir. The complete original grant is
retained in each crate alongside Daniel Scharf’s modification attribution. See
[UPSTREAM.md](UPSTREAM.md).
