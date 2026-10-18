//! Convenience re-exports for common usage.
//!
//! ```rust
//! use glirulus_runtime::prelude::*;
//! ```

// Runtime types
pub use crate::{
    CheckpointingRunner, InProcessRunner, PooledWorker, PooledWorkerBuilder, RuntimeError,
    WorkerHandle, WorkflowRunExt, WorkflowRunner,
};

// Codecs
#[cfg(feature = "json")]
pub use crate::serialization::JsonCodec;
#[cfg(feature = "rkyv")]
pub use crate::serialization::RkyvCodec;

// Codec traits
pub use glirulus_core::codec::{Decoder, Encoder, EnvelopeCodec};

// Core workflow types (from glirulus-core)
pub use glirulus_core::branch_key::BranchKey;
pub use glirulus_core::branch_results::NamedBranchResults;
pub use glirulus_core::context::WorkflowContext;
pub use glirulus_core::deps::{Deps, DepsBuilder, DepsInjectable, MissingDep};
pub use glirulus_core::error::BoxError;
pub use glirulus_core::registry::TaskRegistry;
pub use glirulus_core::workflow::{Workflow, WorkflowBuilder, WorkflowStatus};

// Persistence (from glirulus-persistence)
pub use glirulus_persistence::{InMemoryBackend, PersistentBackend};

// Macros (from glirulus-macros)
#[cfg(feature = "macros")]
pub use glirulus_macros::{BranchKey, task, workflow};
