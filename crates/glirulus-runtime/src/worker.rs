//! Pooled worker for distributed, multi-worker workflow execution.
//!
//! A pooled worker is part of a worker pool that collaboratively executes workflows.
//! Each worker polls the backend for available tasks, claims them (to prevent duplicates),
//! executes them, and updates the snapshot. Multiple workers can process tasks from
//! the same workflow instance in parallel.
//!
//! **Use this when**: You need horizontal scaling with multiple workers processing
//! tasks concurrently across machines or processes.
//!
//! **Use [`CheckpointingRunner`](crate::runner::distributed::CheckpointingRunner) instead when**:
//! You want a single process to run an entire workflow with crash recovery.

use bytes::Bytes;
use glirulus_core::error::BoxError;
use glirulus_core::registry::TaskRegistry;
use glirulus_core::workflow::{Workflow, WorkflowContinuation};
use glirulus_persistence::{PersistentBackend, TaskClaimStore};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

// Keep nominal types here so implementation splits preserve their defining paths.
mod actor;
mod continuation;
mod dispatch;

/// A list of workflow definitions keyed by their definition hash.
pub type WorkflowRegistry<C, Input, M> =
    Vec<(glirulus_core::DefinitionHash, Arc<Workflow<C, Input, M>>)>;

/// Workflow definition for binding-friendly worker API.
///
/// Contains only the structural information (definition hash + continuation tree)
/// needed by `PooledWorker` for position tracking, completion detection, retry
/// policies, and timeouts. Task execution is delegated to an external executor.
#[derive(Clone)]
pub struct ExternalWorkflow {
    /// The continuation tree describing the workflow structure.
    pub continuation: Arc<WorkflowContinuation>,
    /// Per-workflow `TaskId → metadata` index. Built once when the workflow
    /// is registered; replaces the O(N) tree walk + per-node SHA-256 that
    /// the worker would otherwise do for every task dispatch.
    pub task_index: Arc<glirulus_core::TaskIndex>,
    /// Human-readable workflow name (for log/error display and FFI task executor).
    pub workflow_id: Arc<str>,
    /// Optional JSON-encoded workflow-level metadata.
    pub metadata_json: Option<Arc<str>>,
}

/// Workflow index keyed by definition hash for O(1) lookup during task dispatch.
pub type WorkflowIndex = HashMap<glirulus_core::DefinitionHash, ExternalWorkflow>;

/// Live definition registration without restarting a pooled worker. Definitions
/// are cloned before awaiting execution; no registry lock crosses an await.
#[derive(Clone, Default)]
pub struct LiveWorkflowIndex(Arc<std::sync::RwLock<WorkflowIndex>>);

impl LiveWorkflowIndex {
    /// Register or replace a definition for subsequent worker task dispatches.
    pub fn insert(&self, hash: glirulus_core::DefinitionHash, workflow: ExternalWorkflow) {
        self.0
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(hash, workflow);
    }

    fn get(&self, hash: &glirulus_core::DefinitionHash) -> Option<ExternalWorkflow> {
        self.0
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(hash)
            .cloned()
    }
}

impl From<WorkflowIndex> for LiveWorkflowIndex {
    fn from(index: WorkflowIndex) -> Self {
        Self(Arc::new(std::sync::RwLock::new(index)))
    }
}

impl WorkflowLookup for LiveWorkflowIndex {
    fn contains_definition_hash(&self, hash: &glirulus_core::DefinitionHash) -> bool {
        self.get(hash).is_some()
    }
}

/// Definition-hash containment over either [`WorkflowIndex`] (`HashMap`,
/// O(1)) or [`WorkflowRegistry`] (`Vec`, O(n)). Lets `can_handle_hint`
/// accept any worker-registered workflows collection uniformly instead
/// of taking a hand-rolled closure per call site.
pub(crate) trait WorkflowLookup {
    fn contains_definition_hash(&self, hash: &glirulus_core::DefinitionHash) -> bool;
}

impl WorkflowLookup for WorkflowIndex {
    fn contains_definition_hash(&self, hash: &glirulus_core::DefinitionHash) -> bool {
        self.contains_key(hash)
    }
}

impl<C, Input, M> WorkflowLookup for WorkflowRegistry<C, Input, M> {
    fn contains_definition_hash(&self, hash: &glirulus_core::DefinitionHash) -> bool {
        self.iter().any(|(k, _)| k == hash)
    }
}

/// External task executor function signature.
///
/// Receives the task ID and input bytes, returns the output bytes.
/// Used by language bindings (Python, Node.js) to delegate task execution
/// to the host language's runtime.
pub type ExternalTaskExecutor = Arc<
    dyn Fn(
            &str,
            Bytes,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, BoxError>> + Send>>
        + Send
        + Sync,
>;

/// Internal command sent from [`WorkerHandle`] to the actor loop.
enum WorkerCommand {
    Shutdown,
}

struct WorkerHandleInner<B> {
    backend: Arc<B>,
    shutdown_tx: mpsc::Sender<WorkerCommand>,
    join_handle:
        tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), crate::error::RuntimeError>>>>,
}

/// A cloneable handle for interacting with a running [`PooledWorker`].
///
/// Obtained from [`PooledWorker::spawn`]. The handle is cheap to clone and can
/// be shared across tasks. Dropping **all** handles triggers a graceful
/// shutdown of the worker (equivalent to calling [`shutdown`](Self::shutdown)).
pub struct WorkerHandle<B> {
    inner: Arc<WorkerHandleInner<B>>,
}

/// A pooled worker that claims and executes tasks from a shared backend.
///
/// `PooledWorker` is designed for horizontal scaling: multiple workers can run
/// across different machines/processes, all polling the same backend for tasks.
/// Task claiming with TTL prevents duplicate execution while allowing automatic
/// recovery when workers crash.
///
/// # When to Use
///
/// - **Horizontal scaling**: Multiple workers process tasks concurrently
/// - **Fault tolerance**: Failed workers' tasks are automatically reclaimed
/// - **Load balancing**: Tasks distributed across available workers
///
/// For single-process execution with checkpointing, use
/// [`CheckpointingRunner`](crate::runner::distributed::CheckpointingRunner).
///
/// # Example
///
/// ```rust,no_run
/// # use glirulus_runtime::prelude::*;
/// # use std::sync::Arc;
/// # use std::time::Duration;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let backend = InMemoryBackend::new();
/// let registry = TaskRegistry::new();
/// let worker = PooledWorker::new("worker-1", backend, registry);
///
/// let ctx = WorkflowContext::new("my-wf", Arc::new(JsonCodec), Arc::new(()));
/// let workflow = WorkflowBuilder::new(ctx)
///     .then("step1", |i: u32| async move { Ok(i + 1) })
///     .build()?;
/// let workflows = vec![(*workflow.definition_hash(), Arc::new(workflow))];
///
/// // Spawn the worker and get a handle for lifecycle control
/// let handle = worker.spawn(Duration::from_secs(1), workflows);
/// // ... later ...
/// handle.shutdown();
/// handle.join().await?;
/// # Ok(())
/// # }
/// ```
pub struct PooledWorker<B> {
    worker_id: String,
    backend: Arc<B>,
    #[allow(unused)]
    registry: Arc<TaskRegistry>,
    claim_ttl: Option<Duration>,
    batch_size: NonZeroUsize,
    aging_interval: Duration,
    tags: Vec<String>,
}

impl<B> PooledWorker<B>
where
    B: PersistentBackend + TaskClaimStore + 'static,
{
    /// Create a new worker node.
    ///
    /// # Parameters
    ///
    /// - `worker_id`: Unique identifier for this worker node
    /// - `backend`: The persistent backend to use
    /// - `registry`: Task registry containing all task implementations
    ///
    /// # Heartbeat
    ///
    /// Heartbeats are derived automatically from `claim_ttl` (TTL / 2).
    /// With the default 5-minute TTL, heartbeats fire every 2.5 minutes.
    ///
    pub fn new(worker_id: impl Into<String>, backend: B, registry: TaskRegistry) -> Self {
        Self {
            worker_id: worker_id.into(),
            backend: Arc::new(backend),
            registry: Arc::new(registry),
            claim_ttl: Some(Duration::from_mins(5)), // Default 5 minutes
            batch_size: NonZeroUsize::MIN,           // Default: fetch one task at a time (1)
            aging_interval: Duration::from_mins(5),  // Default 5 minutes
            tags: vec![],
        }
    }

    /// Set the TTL for task claims.
    ///
    /// `None` means "never expires" — a crashed worker holding such a
    /// claim pins the workflow undispatchable until the claim row is
    /// manually released. Prefer a finite TTL (default 5 minutes) so
    /// crashed-worker recovery happens via the eligibility predicate's
    /// `expires_at > now()` check.
    #[must_use]
    pub fn with_claim_ttl(mut self, ttl: Option<Duration>) -> Self {
        if ttl.is_none() {
            tracing::warn!(
                "PooledWorker::with_claim_ttl(None) disables claim expiry; \
                 a crashed worker will pin its workflow until manual release"
            );
        }
        self.claim_ttl = ttl;
        self
    }

    /// Set the aging interval for priority-based scheduling.
    ///
    /// Lower-priority tasks that have been waiting longer than this interval
    /// get their effective priority boosted, preventing starvation.
    /// Default: 5 minutes (300 seconds).
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero.
    #[must_use]
    pub fn with_aging_interval(mut self, interval: Duration) -> Self {
        assert!(!interval.is_zero(), "aging interval must be non-zero");
        self.aging_interval = interval;
        self
    }

    /// Set the number of tasks to fetch per poll (default: 1).
    ///
    /// With `batch_size=1`, the worker fetches one task, executes it, then polls again.
    /// Other workers can pick up remaining tasks immediately.
    ///
    /// Higher values reduce polling overhead but may cause workers to hold task IDs
    /// they won't process immediately (though other workers can still claim them).
    #[must_use]
    pub fn with_batch_size(mut self, size: NonZeroUsize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set affinity tags for this worker.
    ///
    /// When tags are set, the worker only picks up tasks whose tags are a
    /// subset of the worker's tags (or tasks with no tags). When no tags are
    /// set (the default), the worker accepts all tasks.
    #[must_use]
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Get a reference to the backend.
    #[must_use]
    pub fn backend(&self) -> &Arc<B> {
        &self.backend
    }

    /// Create a builder with sensible defaults.
    ///
    /// By default, the worker ID is derived from `{hostname}-{pid}`.
    /// Override with [`PooledWorkerBuilder::worker_id`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use glirulus_runtime::prelude::*;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Auto-generated worker ID from hostname + PID
    /// let worker = PooledWorker::builder(InMemoryBackend::new(), TaskRegistry::new()).build();
    ///
    /// // Or override with explicit ID
    /// let worker = PooledWorker::builder(InMemoryBackend::new(), TaskRegistry::new())
    ///     .worker_id("custom-worker-1")
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(backend: B, registry: TaskRegistry) -> PooledWorkerBuilder<B> {
        PooledWorkerBuilder {
            worker_id: None,
            backend,
            registry,
            claim_ttl: Some(Duration::from_mins(5)),
            batch_size: NonZeroUsize::MIN,
            aging_interval: Duration::from_mins(5),
            tags: vec![],
        }
    }

}

/// Generate a default worker ID from `{hostname}-{pid}`.
fn default_worker_id() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
    format!("{host}-{}", std::process::id())
}

/// Builder for [`PooledWorker`] with sensible defaults.
///
/// By default, derives the worker ID from `{hostname}-{pid}`.
/// Override with [`worker_id`](Self::worker_id).
///
/// Created via [`PooledWorker::builder`].
pub struct PooledWorkerBuilder<B> {
    worker_id: Option<String>,
    backend: B,
    registry: TaskRegistry,
    claim_ttl: Option<Duration>,
    batch_size: NonZeroUsize,
    aging_interval: Duration,
    tags: Vec<String>,
}

impl<B> PooledWorkerBuilder<B>
where
    B: PersistentBackend + TaskClaimStore + 'static,
{
    /// Set an explicit worker ID.
    ///
    /// If not called, the ID is auto-generated from `{hostname}-{pid}`.
    #[must_use]
    pub fn worker_id(mut self, id: impl Into<String>) -> Self {
        self.worker_id = Some(id.into());
        self
    }

    /// Set the TTL for task claims (default: 5 minutes).
    #[must_use]
    pub fn claim_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.claim_ttl = ttl;
        self
    }

    /// Set the number of tasks to fetch per poll (default: 1).
    #[must_use]
    pub fn batch_size(mut self, size: NonZeroUsize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the aging interval for priority-based scheduling (default: 300s).
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero.
    #[must_use]
    pub fn aging_interval(mut self, interval: Duration) -> Self {
        assert!(!interval.is_zero(), "aging interval must be non-zero");
        self.aging_interval = interval;
        self
    }

    /// Set affinity tags for this worker.
    ///
    /// When tags are set, the worker only picks up tasks whose tags are a
    /// subset of the worker's tags (or tasks with no tags). When no tags are
    /// set (the default), the worker accepts all tasks.
    #[must_use]
    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Build the [`PooledWorker`].
    ///
    /// If no `worker_id` was set, generates one from `{hostname}-{pid}`.
    #[must_use]
    pub fn build(self) -> PooledWorker<B> {
        let worker_id = self.worker_id.unwrap_or_else(default_worker_id);
        PooledWorker {
            worker_id,
            backend: Arc::new(self.backend),
            registry: Arc::new(self.registry),
            claim_ttl: self.claim_ttl,
            batch_size: self.batch_size,
            aging_interval: self.aging_interval,
            tags: self.tags,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::serialization::JsonCodec;
    use glirulus_core::registry::TaskRegistry;
    use glirulus_core::snapshot::{SignalKind, WorkflowSnapshot};
    use glirulus_persistence::{InMemoryBackend, SignalStore, SnapshotStore};

    type EmptyWorkflows = WorkflowRegistry<JsonCodec, (), ()>;

    fn make_worker() -> PooledWorker<InMemoryBackend> {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();
        PooledWorker::new("test-worker", backend, registry)
    }

    #[tokio::test]
    async fn test_spawn_and_shutdown() {
        let worker = make_worker();
        let handle = worker.spawn(Duration::from_millis(50), EmptyWorkflows::new());

        handle.shutdown();

        let result = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
        assert!(result.is_ok(), "Worker should exit cleanly after shutdown");
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_handle_is_clone_and_send() {
        let worker = make_worker();
        let handle = worker.spawn(Duration::from_millis(50), EmptyWorkflows::new());

        let handle2 = handle.clone();
        let remote = tokio::spawn(async move {
            handle2.shutdown();
        });
        remote.await.ok();

        let result = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
        assert!(result.is_ok_and(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn test_cancel_via_client() {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();

        // Create a workflow snapshot so store_signal can validate it
        let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
        backend.save_snapshot(&mut snapshot).await.unwrap();

        let worker = PooledWorker::new("test-worker", backend, registry);
        let handle = worker.spawn(Duration::from_millis(50), EmptyWorkflows::new());

        // Cancel via WorkflowClient instead of handle
        let client = crate::WorkflowClient::from_shared(std::sync::Arc::clone(handle.backend()));
        client
            .cancel(
                "wf-1",
                Some("test reason".to_string()),
                Some("tester".to_string()),
            )
            .await
            .unwrap();

        // Verify the signal was stored
        let signal = handle
            .backend()
            .get_signal("wf-1", SignalKind::Cancel)
            .await;
        assert!(signal.is_ok_and(|s| s.is_some()));

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(5), handle.join())
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn test_builder_auto_generates_worker_id() {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();
        let worker = PooledWorker::builder(backend, registry).build();

        // Should contain PID
        let pid = std::process::id().to_string();
        assert!(
            worker.worker_id.contains(&pid),
            "Auto-generated ID '{}' should contain PID '{}'",
            worker.worker_id,
            pid
        );
    }

    #[test]
    fn test_builder_explicit_worker_id() {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();
        let worker = PooledWorker::builder(backend, registry)
            .worker_id("my-worker")
            .build();

        assert_eq!(worker.worker_id, "my-worker");
    }

    #[test]
    fn test_builder_custom_settings() {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();
        let worker = PooledWorker::builder(backend, registry)
            .worker_id("w1")
            .claim_ttl(Some(Duration::from_mins(2)))
            .batch_size(NonZeroUsize::new(8).unwrap())
            .build();

        assert_eq!(worker.worker_id, "w1");
        assert_eq!(worker.claim_ttl, Some(Duration::from_mins(2)));
        assert_eq!(worker.batch_size.get(), 8);
    }

    #[tokio::test]
    async fn test_dropped_handle_shuts_down_worker() {
        let worker = make_worker();
        let handle = worker.spawn(Duration::from_millis(50), EmptyWorkflows::new());

        // Extract the join handle before dropping so we can still await completion
        let join_handle = handle.inner.join_handle.lock().await.take().unwrap();
        drop(handle);

        let result = tokio::time::timeout(Duration::from_secs(5), join_handle)
            .await
            .ok()
            .and_then(Result::ok);
        assert!(
            result.is_some(),
            "Worker should exit when all handles are dropped"
        );
        assert!(result.is_some_and(|r| r.is_ok()));
    }
}
