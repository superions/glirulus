# glirulus-postgres

PostgreSQL persistence for Glirulus workflow snapshots, task results, claims,
signals and correlated events. The backend is generic over the snapshot codec.

`PostgresBackend::connect`, `connect_with_options` and `connect_with` run the
included library migrations. Applications that own schema migration use
`from_pool_without_migrations`; this attaches the pool without running DDL.
The application remains responsible for supplying the matching schema.

Caller-owned transaction APIs support snapshot admission/restart and correlated
event acceptance without committing the caller's transaction. An optional
`WaitArbitrator` resolves accepted outcomes, absolute deadlines and stop decisions
on the same connection before engine snapshot locks. Application acceptance/stop
locks must follow that ordering; the application owns the policy. Caller-owned
transaction APIs leave commit to the caller; other backend operations retain
their own transaction boundary. The observed deadline is a possibly stale input,
not acceptance authority. Without an arbitrator, the backend uses the parked
wait's deadline and buffered events.

Correlated events use stable UUID identities, caller acceptance times and exact
payload bytes. Redelivery of the same identity must agree on signal name, time
and bytes; consumed identities remain available for replay. Eligible definition
filtering happens before polling limits, and buffered/due waits advance through
the same signal projection used by event admission.

Existing `sayiir_workflow_*` SQL object names and the `sayiir_task_ready` channel
are deliberately retained for persisted compatibility. Renaming the Rust crate
does not change these persisted identities. The original nine library migrations
are unchanged; standalone migration 010 adds the correlated-event columns and
active-signal projection required by the generic engine fixes. The library
migration chain is separate from any application's migration ledger.
Caller-managed schemas must already supply these columns and the event identity
index before attachment; attaching a pool never runs library migrations.

The supported minimum PostgreSQL major version is 13. Inherited integration
tests use disposable PostgreSQL 13 and 18 containers and reject PostgreSQL 12.
The standalone repository check runner labels these containers for bounded
cleanup. Integration tests do not connect to an application database.

For a complete process-local workflow entry point, see
[Runtime's basic example](../glirulus-runtime/examples/basic.rs) and run
`scripts/check.sh example` from the repository root. PostgreSQL connections use
this crate's backend in place of that example's process-local storage.

This source is a prerelease preparation; registry installation remains pending.
Publication and qualification status belong to the repository README. See `NOTICE` for exact upstream and generic-fix
provenance, `LICENSE` for the MIT grant, and API rustdoc for full contracts.
