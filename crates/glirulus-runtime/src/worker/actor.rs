//! Worker actor polling, admission and lifecycle.

use super::{
    ExternalTaskExecutor, LiveWorkflowIndex, PooledWorker, WorkerCommand, WorkerHandle,
    WorkerHandleInner, WorkflowIndex, WorkflowLookup, WorkflowRegistry,
};
use glirulus_core::codec::{Codec, EnvelopeCodec, sealed};
use glirulus_core::snapshot::{SignalKind, SignalRequest};
use glirulus_persistence::{PersistentBackend, TaskClaimStore, TaskWakeupHint};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

/// Fallback poll-tick interval, phase-shifted per worker so a fleet spawned
/// together doesn't fire the `find_available_tasks` scan in lockstep (a
/// synchronized DB herd). Offset is `hash(worker_id) % poll_interval`. Keeps
/// the default `Burst` missed-tick behavior: a worker that falls behind catches
/// up immediately, which is what drives saturated back-to-back pickup.
fn staggered_poll_interval(worker_id: &str, poll_interval: Duration) -> time::Interval {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    worker_id.hash(&mut hasher);
    let period_ns = u64::try_from(poll_interval.as_nanos())
        .unwrap_or(u64::MAX)
        .max(1);
    let offset = Duration::from_nanos(hasher.finish() % period_ns);
    time::interval_at(time::Instant::now() + offset, poll_interval)
}

impl<B> Clone for WorkerHandle<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<B> WorkerHandle<B> {
    /// Observe termination without taking ownership of the join future.
    pub fn is_finished(&self) -> bool {
        self.inner.join_handle.try_lock().ok().is_some_and(|guard| {
            guard
                .as_ref()
                .is_none_or(tokio::task::JoinHandle::is_finished)
        })
    }

    /// Bound shutdown and reap the aborted task before returning on expiry.
    pub async fn shutdown_with_timeout(
        &self,
        deadline: Duration,
    ) -> Result<(), crate::error::RuntimeError> {
        self.shutdown();
        let Some(mut handle) = self.inner.join_handle.lock().await.take() else {
            return Ok(());
        };
        match time::timeout(deadline, &mut handle).await {
            Ok(result) => result?,
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                Err(glirulus_persistence::BackendError::Backend(
                    "worker shutdown deadline exceeded".into(),
                )
                .into())
            }
        }
    }

    /// Request a graceful shutdown of the worker.
    ///
    /// The worker will finish its current task (if any) and then exit.
    /// This is a non-async, fire-and-forget operation — errors are ignored
    /// (the actor may have already stopped).
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown_tx.try_send(WorkerCommand::Shutdown);
    }

    /// Wait for the worker task to finish.
    ///
    /// The first caller gets the real result; subsequent callers get `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker task panicked or returned an error.
    pub async fn join(&self) -> Result<(), crate::error::RuntimeError> {
        let jh = self.inner.join_handle.lock().await.take();
        match jh {
            Some(jh) => Ok(jh.await??),
            None => Ok(()),
        }
    }

    /// Get a reference to the backend.
    #[must_use]
    pub fn backend(&self) -> &Arc<B> {
        &self.inner.backend
    }
}

impl<B> PooledWorker<B>
where
    B: PersistentBackend + TaskClaimStore + 'static,
{
    /// Request cancellation of a workflow.
    ///
    /// This requests cancellation of the specified workflow instance.
    /// Running tasks will complete, but no new tasks will be started.
    ///
    /// # Parameters
    ///
    /// - `instance_id`: The workflow instance ID to cancel
    /// - `reason`: Optional reason for the cancellation
    /// - `cancelled_by`: Optional identifier of who requested the cancellation
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be cancelled (not found or in terminal state).
    pub async fn cancel_workflow(
        &self,
        instance_id: &str,
        reason: Option<String>,
        cancelled_by: Option<String>,
    ) -> Result<(), crate::error::RuntimeError> {
        self.backend
            .store_signal(
                instance_id,
                SignalKind::Cancel,
                SignalRequest::new(reason, cancelled_by),
            )
            .await?;

        Ok(())
    }

    /// Request pausing of a workflow.
    ///
    /// This requests pausing of the specified workflow instance.
    /// Running tasks will complete, but no new tasks will be started.
    ///
    /// # Parameters
    ///
    /// - `instance_id`: The workflow instance ID to pause
    /// - `reason`: Optional reason for the pause
    /// - `paused_by`: Optional identifier of who requested the pause
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be paused (not found or in terminal/paused state).
    pub async fn pause_workflow(
        &self,
        instance_id: &str,
        reason: Option<String>,
        paused_by: Option<String>,
    ) -> Result<(), crate::error::RuntimeError> {
        self.backend
            .store_signal(
                instance_id,
                SignalKind::Pause,
                SignalRequest::new(reason, paused_by),
            )
            .await?;

        Ok(())
    }

    /// Spawn the worker as a background task and return a handle.
    ///
    /// Consumes `self`, creates an internal command channel, and spawns the
    /// actor loop on the Tokio runtime. Returns a cloneable [`WorkerHandle`]
    /// for lifecycle control — call [`WorkerHandle::shutdown`] to request
    /// graceful shutdown and [`WorkerHandle::join`] to await completion.
    ///
    /// The worker runs until:
    /// - [`WorkerHandle::shutdown`] is called, or
    /// - All clones of the handle are dropped, or
    /// - A fatal backend error occurs.
    ///
    /// # Parameters
    ///
    /// - `poll_interval`: How often to poll for new tasks
    /// - `workflows`: Map of workflow definition hash to workflow
    #[must_use]
    pub fn spawn<C, Input, M>(
        self,
        poll_interval: Duration,
        workflows: WorkflowRegistry<C, Input, M>,
    ) -> WorkerHandle<B>
    where
        Input: Send + Sync + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        let (tx, rx) = mpsc::channel(1);
        let backend = Arc::clone(&self.backend);
        let join_handle =
            tokio::spawn(async move { self.run_actor_loop(poll_interval, workflows, rx).await });
        WorkerHandle {
            inner: Arc::new(WorkerHandleInner {
                backend,
                shutdown_tx: tx,
                join_handle: tokio::sync::Mutex::new(Some(join_handle)),
            }),
        }
    }

    /// Spawn the worker with an external executor and return a handle.
    ///
    /// Like [`spawn`](Self::spawn) but instead of executing tasks via typed
    /// `Workflow` closures, delegates all task execution to the provided
    /// `executor`. This is used by language bindings (Python, Node.js) where
    /// task functions live in the host language.
    ///
    /// # Parameters
    ///
    /// - `poll_interval`: How often to poll for new tasks
    /// - `workflows`: Workflow definitions (hash + continuation tree)
    /// - `executor`: Closure that executes a task by ID given input bytes
    #[must_use]
    pub fn spawn_with_executor(
        self,
        poll_interval: Duration,
        workflows: WorkflowIndex,
        executor: ExternalTaskExecutor,
    ) -> WorkerHandle<B> {
        self.spawn_with_live_executor(poll_interval, workflows.into(), executor)
    }

    /// External execution with immutable definitions registered during intake.
    pub fn spawn_with_live_executor(
        self,
        poll_interval: Duration,
        workflows: LiveWorkflowIndex,
        executor: ExternalTaskExecutor,
    ) -> WorkerHandle<B> {
        let (tx, rx) = mpsc::channel(1);
        let backend = Arc::clone(&self.backend);
        let join_handle = tokio::spawn(async move {
            self.run_external_actor_loop(poll_interval, workflows, executor, rx)
                .await
        });
        WorkerHandle {
            inner: Arc::new(WorkerHandleInner {
                backend,
                shutdown_tx: tx,
                join_handle: tokio::sync::Mutex::new(Some(join_handle)),
            }),
        }
    }

    /// Actor loop for external executor mode.
    async fn run_external_actor_loop(
        &self,
        poll_interval: Duration,
        workflows: LiveWorkflowIndex,
        executor: ExternalTaskExecutor,
        mut cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) -> Result<(), crate::error::RuntimeError> {
        let mut interval = staggered_poll_interval(&self.worker_id, poll_interval);

        loop {
            let hint = tokio::select! {
                biased;

                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(WorkerCommand::Shutdown) | None => {
                            tracing::info!(worker_id = %self.worker_id, "Worker shutting down");
                            return Ok(());
                        }
                    }
                }
                _ = interval.tick() => {
                    tracing::trace!(worker_id = %self.worker_id, "fallback poll tick");
                    None
                }
                hint = self.backend.wait_for_wakeup(poll_interval) => {
                    let hint = hint?;
                    tracing::debug!(
                        worker_id = %self.worker_id,
                        has_hint = hint.is_some(),
                        "wakeup notification",
                    );
                    hint
                }
            };

            if let Some(h) = hint.as_ref()
                && !self.can_handle_hint(h, &workflows)
            {
                // This optimization cuts PG load proportional to NOTIFY volume x fleet-tag-fragmentation.
                tracing::trace!(
                    worker_id = %self.worker_id,
                    instance_id = %h.instance_id,
                    "skipping wakeup, hint not handleable here",
                );
                continue;
            }

            if let Some(h) = hint.as_ref() {
                match self.try_hinted_execute(h, &workflows, &executor).await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(ref e) if e.is_timeout() => {
                        tracing::error!(worker_id = %self.worker_id, error = %e, "Task timed out — worker shutting down");
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                }
            }

            let available_tasks = self
                .backend
                .find_available_tasks(
                    &self.worker_id,
                    self.batch_size.get(),
                    chrono::Duration::from_std(self.aging_interval)
                        .unwrap_or(chrono::Duration::MAX),
                    &self.tags,
                )
                .await?;

            for task in available_tasks {
                if let Ok(WorkerCommand::Shutdown) | Err(mpsc::error::TryRecvError::Disconnected) =
                    cmd_rx.try_recv()
                {
                    tracing::info!(worker_id = %self.worker_id, "Worker shutting down mid-batch");
                    return Ok(());
                }

                if let Some(ext_wf) = workflows.get(&task.workflow_definition_hash) {
                    match self
                        .execute_external_task(
                            &ext_wf,
                            &task.workflow_definition_hash,
                            &executor,
                            &task,
                        )
                        .await
                    {
                        Err(ref e) if e.is_timeout() => {
                            tracing::error!(
                                worker_id = %self.worker_id,
                                error = %e,
                                "Task timed out — worker shutting down"
                            );
                            return Ok(());
                        }
                        Ok(_) => {
                            tracing::info!(worker_id = %self.worker_id, "completed task");
                        }
                        Err(e) => {
                            tracing::error!(
                                worker_id = %self.worker_id,
                                error = %e,
                                "task execution failed"
                            );
                        }
                    }
                }
            }
        }
    }

    /// True if the worker has the hint's workflow registered and its
    /// tag set covers the hint's tags.
    fn can_handle_hint(&self, hint: &TaskWakeupHint, workflows: &impl WorkflowLookup) -> bool {
        let hash = glirulus_core::DefinitionHash::from_bytes(hint.definition_hash);
        if !workflows.contains_definition_hash(&hash) {
            return false;
        }
        hint.tags.iter().all(|t| self.tags.contains(t))
    }

    /// `Ok(true)` if the hinted task was claimed and executed (or its
    /// execution failed non-fatally), `Ok(false)` to fall through to the
    /// full polling scan, `Err(e)` on a fatal task timeout.
    async fn try_hinted_execute(
        &self,
        hint: &TaskWakeupHint,
        workflows: &LiveWorkflowIndex,
        executor: &ExternalTaskExecutor,
    ) -> Result<bool, crate::error::RuntimeError> {
        let Some(task) = self.backend.find_hinted_task(hint).await? else {
            return Ok(false);
        };
        let Some(ext_wf) = workflows.get(&task.workflow_definition_hash) else {
            return Ok(false);
        };
        match self
            .execute_external_task(&ext_wf, &task.workflow_definition_hash, executor, &task)
            .await
        {
            Err(e) if e.is_timeout() => return Err(e),
            Ok(_) => tracing::info!(worker_id = %self.worker_id, "completed hinted task"),
            Err(e) => {
                tracing::error!(worker_id = %self.worker_id, error = %e, "hinted task execution failed");
            }
        }
        Ok(true)
    }

    /// The actor loop: poll for tasks, execute them, respond to shutdown.
    ///
    async fn run_actor_loop<C, Input, M>(
        &self,
        poll_interval: Duration,
        workflows: WorkflowRegistry<C, Input, M>,
        mut cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) -> Result<(), crate::error::RuntimeError>
    where
        Input: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        let mut interval = staggered_poll_interval(&self.worker_id, poll_interval);

        loop {
            // In-process mode only filters irrelevant wakes; the direct-
            // claim shortcut lives on the external-executor path.
            let hint = tokio::select! {
                biased;

                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(WorkerCommand::Shutdown) | None => {
                            tracing::info!(worker_id = %self.worker_id, "Worker shutting down");
                            return Ok(());
                        }
                    }
                }
                _ = interval.tick() => {
                    tracing::trace!(worker_id = %self.worker_id, "fallback poll tick");
                    None
                }
                hint = self.backend.wait_for_wakeup(poll_interval) => {
                    let hint = hint?;
                    tracing::debug!(
                        worker_id = %self.worker_id,
                        has_hint = hint.is_some(),
                        "wakeup notification",
                    );
                    hint
                }
            };

            if let Some(h) = hint.as_ref()
                && !self.can_handle_hint(h, &workflows)
            {
                // This optimization cuts PG load proportional to NOTIFY volume x fleet-tag-fragmentation.
                tracing::trace!(
                    worker_id = %self.worker_id,
                    instance_id = %h.instance_id,
                    "skipping wakeup, hint not handleable here",
                );
                continue;
            }

            let available_tasks = self
                .backend
                .find_available_tasks(
                    &self.worker_id,
                    self.batch_size.get(),
                    chrono::Duration::from_std(self.aging_interval)
                        .unwrap_or(chrono::Duration::MAX),
                    &self.tags,
                )
                .await?;

            for task in available_tasks {
                if let Ok(WorkerCommand::Shutdown) | Err(mpsc::error::TryRecvError::Disconnected) =
                    cmd_rx.try_recv()
                {
                    tracing::info!(worker_id = %self.worker_id, "Worker shutting down mid-batch");
                    return Ok(());
                }

                if let Some((_, workflow)) = workflows
                    .iter()
                    .find(|(hash, _)| *hash == task.workflow_definition_hash)
                // hash and task.workflow_definition_hash are both DefinitionHash
                {
                    match self.execute_task(workflow.as_ref(), task).await {
                        Err(ref e) if e.is_timeout() => {
                            tracing::error!(
                                worker_id = %self.worker_id,
                                error = %e,
                                "Task timed out — worker shutting down"
                            );
                            return Ok(());
                        }
                        Ok(_) => {
                            tracing::info!(worker_id = %self.worker_id, "completed task");
                        }
                        Err(e) => {
                            tracing::error!(
                                worker_id = %self.worker_id,
                                error = %e,
                                "task execution failed"
                            );
                        }
                    }
                }
            }
        }
    }
}
