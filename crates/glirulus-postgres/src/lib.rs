//! PostgreSQL persistence backend for the Glirulus workflow engine.
//!
//! This crate provides [`PostgresBackend`], a production-grade implementation of
//! [`SnapshotStore`](glirulus_persistence::SnapshotStore),
//! [`SignalStore`](glirulus_persistence::SignalStore), and
//! [`TaskClaimStore`](glirulus_persistence::TaskClaimStore) backed by PostgreSQL via
//! [`sqlx`].
//!
//! # Features
//!
//! - **Codec-generic**: Serialise snapshots with any codec (JSON for debuggability,
//!   rkyv/bincode for speed). The data column is always `BYTEA`.
//! - **ACID transactions**: Composite signal operations (`check_and_cancel`,
//!   `check_and_pause`, `unpause`) use single Postgres transactions with
//!   `SELECT … FOR UPDATE` for true atomicity.
//! - **Snapshot history**: Every checkpoint is appended to an immutable history
//!   table for debugging, auditing, and future replay.
//! - **Observability-ready**: Indexed metadata columns (`status`, `current_task_id`,
//!   `completed_task_count`, `error`, timestamps) plus a denormalised
//!   `sayiir_workflow_tasks` table enable monitoring without deserialising blobs.
//! - **Distributed task claiming**: `TaskClaimStore` with TTL-based claims,
//!   expired-claim replacement, and soft worker bias.
//!
//! # Schema ownership
//!
//! Library-managed connections run the included migration chain, including the
//! additive correlated-event identity and active-signal projection migration.
//! [`PostgresBackend::from_pool_without_migrations`] performs no DDL; its caller
//! must supply the matching schema through the application's own migration path.
//! Existing `sayiir_workflow_*` SQL identities and `sayiir_task_ready` are retained.
//!
//! An optional [`WaitArbitrator`] resolves caller-owned accepted outcomes,
//! deadlines and stop decisions before snapshot locks on the same transaction
//! connection. The application owns acceptance policy and its locks; transaction
//! ownership stays with the invoking backend operation or its caller. Correlated
//! events retain UUID, acceptance time and exact bytes.
//!
//! # PostgreSQL version support
//!
//! Minimum supported version: **PostgreSQL 13**. The schema uses
//! `INSERT … ON CONFLICT DO UPDATE` (9.5+) and `ALTER TABLE … ADD COLUMN IF NOT
//! EXISTS` (9.6+). Integration tests exercise PostgreSQL 13 and 18 and reject 12.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use glirulus_postgres::PostgresBackend;
//! use glirulus_runtime::serialization::JsonCodec;
//! use glirulus_persistence::SnapshotStore;
//! use glirulus_core::snapshot::WorkflowSnapshot;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let backend = PostgresBackend::<JsonCodec>::connect("postgresql://localhost/sayiir").await?;
//!
//! let mut snapshot = WorkflowSnapshot::new("order-123", glirulus_core::DefinitionHash::from("hash-abc"));
//! backend.save_snapshot(&mut snapshot).await?;
//!
//! let loaded = backend.load_snapshot("order-123").await?;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]
#![deny(clippy::pedantic)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr
)]
// PostgreSQL is a proper noun, not inline code.
#![allow(clippy::doc_markdown)]

mod backend;
mod error;
mod history;
mod signal_store;
mod snapshot_store;
mod task_claim_store;
mod task_result_store;
mod wakeup;

pub use backend::{PoolOptions, PostgresBackend};
pub use signal_store::{WaitArbitrator, WaitFuture, WaitResolution};
pub use wakeup::wakeup_drops_total;

/// Per-instance child tables — i.e. everything that holds rows keyed by
/// `instance_id` other than `sayiir_workflow_snapshots` itself. Source
/// of truth for both `delete_snapshot`'s cleanup loop and the
/// benchmark's `reset_sayiir_tables` truncate.
pub const WORKFLOW_CHILD_TABLES: &[&str] = &[
    "sayiir_workflow_snapshot_history",
    "sayiir_workflow_tasks",
    "sayiir_workflow_events",
    "sayiir_workflow_signals",
    "sayiir_workflow_claims",
];
