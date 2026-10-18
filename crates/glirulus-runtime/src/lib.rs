#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]
#![deny(clippy::pedantic)]
#![forbid(unsafe_code)]
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

//! Workflow runtime for executing durable workflows.
//!
//! This crate provides the execution engine that drives [`glirulus_core`] workflows
//! to completion. It re-exports the core crate, persistence layer, and proc-macros
//! for convenient access. Macro consumers also declare Core because generated
//! code refers directly to its public crate path:
//!
//! ```toml
//! [dependencies]
//! glirulus-core = "=1.0.0-glirulus.1"
//! glirulus-runtime = "=1.0.0-glirulus.1"
//! ```
//!
//! # Execution Strategies
//!
//! | Scenario | Use |
//! |----------|-----|
//! | Single server, crash recovery needed | [`CheckpointingRunner`] |
//! | Multiple workers, horizontal scaling | [`PooledWorker`] |
//! | Simple in-memory execution | [`InProcessRunner`] |
//!
//! # Quick Example
//!
//! ```rust,no_run
//! use glirulus_runtime::{CheckpointingRunner, WorkflowRunner, workflow, task};
//! use glirulus_runtime::persistence::InMemoryBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let backend = InMemoryBackend::new();
//! let runner = CheckpointingRunner::new(backend);
//!
//! // Run a workflow with automatic checkpointing
//! // let status = runner.run(&workflow, "instance-123", input).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Re-exports
//!
//! This crate re-exports key items for convenience:
//!
//! - [`glirulus_persistence`] — as [`persistence`]
//! - [`task`] and [`workflow!`] — from `glirulus-macros`
//! - [`task_context!`] — context macro from `glirulus-core`
//!
//! # Cargo Features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `json` | **yes** | JSON serialization codec ([`serde_json`]) |
//! | `rkyv` | **yes** | Zero-copy binary codec ([`rkyv`]) |
//! | `macros` | **yes** | Proc-macro re-exports (`#[task]`, `workflow!`) from `glirulus-macros` |
//! | `otel` | no | OpenTelemetry integration — W3C trace context propagation across workers, OTLP span export, and [`trace_context::init_tracing`] / [`trace_context::shutdown_tracing`] helpers |
//!
//! At least one of `json` or `rkyv` must be enabled (enforced at compile time).
//!
//! Enable `otel` in your bindings or application to get distributed trace
//! propagation via the `trace_parent` field on snapshots, and a ready-made
//! subscriber setup controlled by `OTEL_EXPORTER_OTLP_ENDPOINT`,
//! `OTEL_SERVICE_NAME`, and `RUST_LOG` environment variables.
//!
//! For the full README with architecture diagrams and detailed configuration,
//! see the [crate README](https://crates.io/crates/glirulus-runtime).

#[cfg(not(any(feature = "json", feature = "rkyv")))]
compile_error!(
    "at least one serialization codec must be enabled: enable the `json` or `rkyv` feature"
);

mod client;
pub mod error;
pub mod execution;
pub mod prelude;
mod runner;
pub mod serialization;
#[cfg(feature = "otel")]
pub mod trace_context;
pub mod worker;

// Re-exports
pub use client::WorkflowClient;
pub use error::RuntimeError;
pub use execution::{
    PrepareRunOutcome, ResumeOutcome, check_existing_instance, execute_continuation_async,
    execute_continuation_sync, execute_continuation_with_checkpointing, finalize_execution,
    prepare_resume, prepare_run, serialize_branch_results,
};
pub use runner::WorkflowRunExt;
pub use runner::WorkflowRunner;
pub use runner::distributed::CheckpointingRunner;
pub use runner::in_process::InProcessRunner;
pub use worker::{
    ExternalTaskExecutor, ExternalWorkflow, LiveWorkflowIndex, PooledWorker, PooledWorkerBuilder,
    WorkerHandle, WorkflowIndex, WorkflowRegistry,
};

pub use glirulus_core::branch_key::BranchKey;
pub use glirulus_core::deps::{Deps, DepsBuilder, DepsInjectable, MissingDep};
pub use glirulus_core::task_context;
pub use glirulus_core::workflow::ConflictPolicy;
#[cfg(feature = "macros")]
pub use glirulus_macros::{BranchKey, task, workflow};
pub use glirulus_persistence as persistence;
