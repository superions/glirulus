//! Claimed task execution, deadlines and checkpoint settlement.

use super::{ExternalTaskExecutor, ExternalWorkflow, PooledWorker};
use bytes::Bytes;
use futures::FutureExt;
use glirulus_core::codec::{Codec, EnvelopeCodec, sealed};
use glirulus_core::context::{TaskExecutionContext, with_task_context};
use glirulus_core::error::WorkflowError;
use glirulus_core::snapshot::{TaskDeadline, WorkflowSnapshot};
use glirulus_core::task_claim::AvailableTask;
use glirulus_core::workflow::{Workflow, WorkflowContinuation, WorkflowStatus};
use glirulus_persistence::{PersistentBackend, TaskClaimStore};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tokio::time;

/// Owns a claimed task and provides explicit release methods.
///
/// No `Drop` impl — callers must explicitly call `release()` or `release_quietly()`.
struct ActiveTaskClaim<'a, B> {
    backend: &'a B,
    instance_id: std::sync::Arc<str>,
    task_id: glirulus_core::TaskId,
    worker_id: String,
}

impl<B: TaskClaimStore> ActiveTaskClaim<'_, B> {
    /// Release the claim, propagating backend errors.
    async fn release(self) -> Result<(), crate::error::RuntimeError> {
        self.backend
            .release_task_claim(&self.instance_id, &self.task_id, &self.worker_id)
            .await?;
        Ok(())
    }

    /// Release the claim, silently ignoring errors. Use for error/panic paths.
    async fn release_quietly(self) {
        let _ = self.release().await;
    }
}

/// Outcome of running a task through `execute_with_deadline`.
enum ExecutionOutcome {
    /// Task completed successfully.
    Success(Bytes),
    /// Task execution returned an error.
    TaskError(crate::error::RuntimeError),
    /// Task panicked.
    Panic(Box<dyn std::any::Any + Send>),
    /// Heartbeat detected an expired deadline (active cancellation).
    Timeout(crate::error::RuntimeError),
}

/// Extract a human-readable message from a panic payload.
fn extract_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Task panicked with unknown payload".to_string()
    }
}

impl<B> PooledWorker<B>
where
    B: PersistentBackend + TaskClaimStore + 'static,
{
    /// Execute a single task using an external executor.
    #[tracing::instrument(
        name = "workflow",
        skip_all,
        fields(
            worker_id = %self.worker_id,
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            definition_hash = %definition_hash,
        ),
    )]
    pub(super) async fn execute_external_task(
        &self,
        ext_wf: &ExternalWorkflow,
        definition_hash: &glirulus_core::DefinitionHash,
        executor: &ExternalTaskExecutor,
        available_task: &AvailableTask,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError> {
        // Link current span to the workflow's trace context (cross-worker propagation)
        #[cfg(feature = "otel")]
        if let Some(ref tp) = available_task.trace_parent {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let remote_ctx = crate::trace_context::context_from_trace_parent(tp);
            let _ = tracing::Span::current().set_parent(remote_ctx);
        }

        // Workers that lose the claim race drop the `available_task`
        // (and its `Arc<WorkflowSnapshot>`) cheaply — keep the deep
        // clone strictly past the claim_task gate.
        let already_completed = Self::validate_task_preconditions(
            definition_hash,
            &ext_wf.task_index,
            Some(&ext_wf.continuation),
            available_task,
            &available_task.snapshot,
        )?;
        let Some(claim) = self.claim_task(available_task).await? else {
            return Ok(WorkflowStatus::InProgress);
        };

        if let Some(status) = self.check_post_claim_guards(available_task).await? {
            claim.release_quietly().await;
            return Ok(status);
        }

        // Past the claim gate — now pay for the deep clone exactly once.
        let mut snapshot = (*available_task.snapshot).clone();
        if !already_completed
            && Self::park_dispatched_control(
                &ext_wf.continuation,
                &available_task.task_id,
                &mut snapshot,
            )?
        {
            self.backend.save_snapshot(&mut snapshot).await?;
            self.drain_pending_signal(&available_task.instance_id, &mut snapshot)
                .await?;
            claim.release().await?;
            return Ok(WorkflowStatus::InProgress);
        }
        if already_completed {
            return self
                .resume_checkpointed_task(
                    &ext_wf.continuation,
                    available_task,
                    &mut snapshot,
                    claim,
                )
                .await;
        }

        tracing::debug!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            "Executing task (external)"
        );

        let execution_result = self
            .execute_with_deadline_ext(ext_wf, executor, available_task, &mut snapshot, &claim)
            .await;

        self.settle_execution_result_ext(
            execution_result,
            &ext_wf.continuation,
            &ext_wf.task_index,
            available_task,
            &mut snapshot,
            claim,
        )
        .await
    }

    /// Run the external executor with an optional deadline.
    async fn execute_with_deadline_ext(
        &self,
        ext_wf: &ExternalWorkflow,
        executor: &ExternalTaskExecutor,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: &ActiveTaskClaim<'_, B>,
    ) -> ExecutionOutcome {
        let task_id = available_task.task_id;
        let input = available_task.input.clone();
        // Resolve the declared external task or Branch selector name, retaining
        // the index fast path for ordinary tasks.
        let indexed_meta = ext_wf.task_index.get(&task_id);
        let task_name =
            Self::dispatch_task_name(&ext_wf.task_index, Some(&ext_wf.continuation), &task_id)
                .unwrap_or_else(|| Arc::from(task_id.to_hex()));

        let deadline = if let Some(timeout) =
            indexed_meta.and_then(glirulus_core::TaskNodeMetadata::timeout)
        {
            snapshot.set_task_deadline(task_id, timeout);
            snapshot.refresh_task_deadline();
            // Deadline lives in-process for `run_with_heartbeat`; it
            // is persisted on the next save_task_result (which holds
            // a FOR UPDATE lock and so can't race with check_and_*).
            // A bare save_snapshot here would clobber a
            // check_and_cancel that landed between
            // check_post_claim_guards and now, silently resurrecting
            // a cancelled workflow into InProgress.
            snapshot.task_deadline.clone()
        } else {
            None
        };

        let task_ctx = TaskExecutionContext {
            workflow_id: Arc::clone(&ext_wf.workflow_id),
            instance_id: Arc::clone(&available_task.instance_id),
            task_id: Arc::clone(&task_name),
            metadata: ext_wf.task_index.build_task_metadata(&task_id),
            workflow_metadata_json: ext_wf.metadata_json.clone(),
        };

        let execution_future = with_task_context(task_ctx, executor(&task_name, input));

        let heartbeat_result = self
            .run_with_heartbeat(
                claim,
                deadline.as_ref(),
                AssertUnwindSafe(execution_future).catch_unwind(),
            )
            .await;

        snapshot.clear_task_deadline();

        match heartbeat_result {
            Err(timeout_err) => ExecutionOutcome::Timeout(timeout_err),
            Ok(Err(panic_payload)) => ExecutionOutcome::Panic(panic_payload),
            Ok(Ok(Err(e))) => ExecutionOutcome::TaskError(e.into()),
            Ok(Ok(Ok(output))) => ExecutionOutcome::Success(output),
        }
    }

    /// Settle execution result for external executor mode.
    #[tracing::instrument(
        name = "settle_result",
        skip_all,
        fields(worker_id = %self.worker_id, instance_id = %available_task.instance_id, task_id = %available_task.task_id),
    )]
    async fn settle_execution_result_ext(
        &self,
        outcome: ExecutionOutcome,
        continuation: &WorkflowContinuation,
        task_index: &glirulus_core::TaskIndex,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: ActiveTaskClaim<'_, B>,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError> {
        tracing::debug!("settling execution result");
        match outcome {
            ExecutionOutcome::Timeout(err) => {
                if let Ok(Some(status)) = self
                    .try_schedule_retry(task_index, available_task, snapshot, &err.to_string())
                    .await
                {
                    claim.release_quietly().await;
                    return Ok(status);
                }

                tracing::warn!(
                    instance_id = %available_task.instance_id,
                    task_id = %available_task.task_id,
                    error = %err,
                    "Task timed out via heartbeat — marking workflow failed, shutting down"
                );
                snapshot.mark_failed(err.to_string());
                let _ = self.backend.save_snapshot(snapshot).await;
                claim.release_quietly().await;
                Err(err)
            }
            ExecutionOutcome::Panic(panic_payload) => {
                let panic_msg = extract_panic_message(&panic_payload);

                if let Ok(Some(status)) = self
                    .try_schedule_retry(task_index, available_task, snapshot, &panic_msg)
                    .await
                {
                    claim.release_quietly().await;
                    return Ok(status);
                }

                tracing::error!(
                    instance_id = %available_task.instance_id,
                    task_id = %available_task.task_id,
                    panic = %panic_msg,
                    "Task panicked - releasing claim"
                );
                snapshot.mark_failed(panic_msg.clone());
                self.backend.save_snapshot(snapshot).await?;
                claim.release_quietly().await;
                Err(WorkflowError::TaskPanicked(panic_msg).into())
            }
            ExecutionOutcome::TaskError(e) => {
                if let Ok(Some(status)) = self
                    .try_schedule_retry(task_index, available_task, snapshot, &e.to_string())
                    .await
                {
                    claim.release_quietly().await;
                    return Ok(status);
                }

                tracing::error!(
                    instance_id = %available_task.instance_id,
                    task_id = %available_task.task_id,
                    error = %e,
                    "Task execution failed"
                );
                snapshot.mark_failed(e.to_string());
                self.backend.save_snapshot(snapshot).await?;
                claim.release_quietly().await;
                Err(e)
            }
            ExecutionOutcome::Success(output) => {
                snapshot.clear_retry_state(&available_task.task_id);
                self.commit_task_result(
                    continuation,
                    available_task,
                    snapshot,
                    output.clone(),
                    claim,
                )
                .await?;
                self.determine_post_task_status(continuation, available_task, snapshot, output)
                    .await
            }
        }
    }

    /// Load cancellation status from a snapshot.
    ///
    /// Attempts to load the snapshot and extract cancellation details.
    /// Returns `WorkflowStatus::Cancelled` with either the extracted details or defaults.
    pub(super) async fn load_cancelled_status(&self, instance_id: &str) -> WorkflowStatus {
        if let Ok(snapshot) = self.backend.load_snapshot(instance_id).await
            && let Some((reason, cancelled_by)) = snapshot.state.cancellation_details()
        {
            return WorkflowStatus::Cancelled {
                reason,
                cancelled_by,
            };
        }
        WorkflowStatus::Cancelled {
            reason: None,
            cancelled_by: None,
        }
    }

    /// Load paused status from a snapshot.
    ///
    /// Attempts to load the snapshot and extract pause details.
    /// Returns `WorkflowStatus::Paused` with either the extracted details or defaults.
    pub(super) async fn load_paused_status(&self, instance_id: &str) -> WorkflowStatus {
        if let Ok(snapshot) = self.backend.load_snapshot(instance_id).await
            && let Some((reason, paused_by)) = snapshot.state.pause_details()
        {
            return WorkflowStatus::Paused { reason, paused_by };
        }
        WorkflowStatus::Paused {
            reason: None,
            paused_by: None,
        }
    }

    /// Execute a single task from an available task.
    ///
    /// This claims the task, executes it, updates the snapshot, and releases the claim.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The task cannot be claimed
    /// - The workflow definition hash doesn't match
    /// - Task execution fails
    /// - Snapshot update fails
    #[tracing::instrument(
        name = "workflow",
        skip_all,
        fields(
            worker_id = %self.worker_id,
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            definition_hash = %available_task.workflow_definition_hash,
        ),
    )]
    pub async fn execute_task<C, Input, M>(
        &self,
        workflow: &Workflow<C, Input, M>,
        available_task: AvailableTask,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError>
    where
        Input: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        // Link current span to the workflow's trace context (cross-worker propagation)
        #[cfg(feature = "otel")]
        if let Some(ref tp) = available_task.trace_parent {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let remote_ctx = crate::trace_context::context_from_trace_parent(tp);
            let _ = tracing::Span::current().set_parent(remote_ctx);
        }

        // 1. Use the snapshot the dispatch SELECT already decoded.
        // Lost-race workers drop the `Arc<WorkflowSnapshot>` cheaply —
        // keep the deep clone strictly past the claim_task gate.
        let already_completed = Self::validate_task_preconditions(
            workflow.definition_hash(),
            workflow.task_index(),
            None,
            &available_task,
            &available_task.snapshot,
        )?;
        let Some(claim) = self.claim_task(&available_task).await? else {
            return Ok(WorkflowStatus::InProgress);
        };

        // 3. Post-claim guards (cancel/pause)
        if let Some(status) = self.check_post_claim_guards(&available_task).await? {
            claim.release_quietly().await;
            return Ok(status);
        }

        // Past the claim gate — now pay for the deep clone exactly once.
        let mut snapshot = (*available_task.snapshot).clone();
        if !already_completed
            && Self::park_dispatched_control(
                workflow.continuation(),
                &available_task.task_id,
                &mut snapshot,
            )?
        {
            self.backend.save_snapshot(&mut snapshot).await?;
            self.drain_pending_signal(&available_task.instance_id, &mut snapshot)
                .await?;
            claim.release().await?;
            return Ok(WorkflowStatus::InProgress);
        }
        if already_completed {
            return self
                .resume_checkpointed_task(
                    workflow.continuation(),
                    &available_task,
                    &mut snapshot,
                    claim,
                )
                .await;
        }

        tracing::debug!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            "Executing task"
        );

        // 4. Execute with deadline + heartbeat, then settle the result
        let execution_result = self
            .execute_with_deadline(workflow, &available_task, &mut snapshot, &claim)
            .await;

        self.settle_execution_result(
            execution_result,
            workflow,
            &available_task,
            &mut snapshot,
            claim,
        )
        .await
    }

    /// Run the task future with an optional deadline, returning the panic-wrapped result.
    ///
    /// Sets a deadline on the snapshot (if the task has a timeout), persists it,
    /// then runs the future inside `run_with_heartbeat`. On heartbeat-level timeout
    /// the task future is dropped and an `Err` is returned.
    async fn execute_with_deadline<C, Input, M>(
        &self,
        workflow: &Workflow<C, Input, M>,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: &ActiveTaskClaim<'_, B>,
    ) -> ExecutionOutcome
    where
        Input: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        let continuation = workflow.continuation();
        let task_index = workflow.task_index();
        let task_id = available_task.task_id;
        // If this task is the entry of a Fork's join, rebuild the
        // `NamedBranchResults` envelope the join body deserialises into
        // `BranchOutputs`. The PooledWorker dispatch path otherwise
        // hands the join the LAST branch's output (whatever
        // `get_last_task_output` saw), which fails to deserialise as
        // soon as any branch result is read by id. The in-process
        // runner builds this through `resolve_join`; for the worker
        // path we have to do it explicitly because each task is its
        // own dispatch and there is no in-memory `branch_results`.
        let input = match Self::find_fork_branches_for_join(continuation, &task_id) {
            Some(branches) => {
                let build = || -> Result<Bytes, crate::error::RuntimeError> {
                    let mut results = Vec::with_capacity(branches.len());
                    for branch in branches {
                        // The join envelope keys results by `branch.id()`
                        // (the branch entry node) so user code can look
                        // up `results["branch-a"]`, but the actual output
                        // is stored under the branch's TERMINAL task —
                        // for multi-step branches the entry task's
                        // output is an intermediate value, not the
                        // branch's "result". TaskId::from(branch.id())
                        // works only for single-Task branches and either
                        // returns the wrong bytes (entry task's output)
                        // or surfaces a TaskNotFound that hides the real
                        // problem.
                        let branch_name = branch.id().to_string();
                        let terminal_tid = glirulus_core::TaskId::from(branch.terminal_task_id());
                        let output = snapshot
                            .get_task_result_bytes(&terminal_tid)
                            .ok_or_else(|| WorkflowError::TaskNotFound(branch_name.clone()))?;
                        results.push((branch_name, output));
                    }
                    crate::execution::serialize_branch_results(&results, workflow.codec().as_ref())
                };
                match build() {
                    Ok(bytes) => bytes,
                    Err(e) => return ExecutionOutcome::TaskError(e),
                }
            }
            None => available_task.input.clone(),
        };
        // Resolve human-readable task name from the prebuilt index for the
        // task context exposed to user code (no tree walk, no rehashing).
        let indexed_meta = task_index.get(&task_id);
        let task_name: Arc<str> =
            indexed_meta.map_or_else(|| Arc::from(task_id.to_hex()), |m| Arc::clone(m.name()));

        // Set deadline if task has a timeout configured
        let deadline = if let Some(timeout) =
            indexed_meta.and_then(glirulus_core::TaskNodeMetadata::timeout)
        {
            snapshot.set_task_deadline(task_id, timeout);
            snapshot.refresh_task_deadline();
            // Deadline lives in-process for `run_with_heartbeat`; it
            // is persisted on the next save_task_result (which holds
            // a FOR UPDATE lock and so can't race with check_and_*).
            // A bare save_snapshot here would clobber a
            // check_and_cancel that landed between
            // check_post_claim_guards and now, silently resurrecting
            // a cancelled workflow into InProgress.
            snapshot.task_deadline.clone()
        } else {
            None
        };

        let task_ctx = TaskExecutionContext {
            workflow_id: Arc::from(workflow.context().workflow_id()),
            instance_id: Arc::clone(&available_task.instance_id),
            task_id: Arc::clone(&task_name),
            metadata: task_index.build_task_metadata(&task_id),
            workflow_metadata_json: workflow.context().metadata_json.clone(),
        };

        let execution_future = with_task_context(task_ctx, async move {
            Self::execute_task_by_id(continuation, &task_name, input).await
        });

        let heartbeat_result = self
            .run_with_heartbeat(
                claim,
                deadline.as_ref(),
                AssertUnwindSafe(execution_future).catch_unwind(),
            )
            .await;

        snapshot.clear_task_deadline();

        match heartbeat_result {
            Err(timeout_err) => ExecutionOutcome::Timeout(timeout_err),
            Ok(Err(panic_payload)) => ExecutionOutcome::Panic(panic_payload),
            Ok(Ok(Err(e))) => ExecutionOutcome::TaskError(e),
            Ok(Ok(Ok(output))) => ExecutionOutcome::Success(output),
        }
    }

    /// Try to schedule a retry for a failed task.
    ///
    /// Looks up the retry policy in the task index. If retries are available,
    /// records the retry state on the snapshot, clears the deadline, saves the
    /// snapshot, releases the claim, and returns `Ok(Some(InProgress))`.
    /// Otherwise returns `Ok(None)` (caller falls through to existing error handling).
    async fn try_schedule_retry(
        &self,
        task_index: &glirulus_core::TaskIndex,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        error_msg: &str,
    ) -> Result<Option<WorkflowStatus>, crate::error::RuntimeError> {
        let Some(policy) = task_index.retry_policy(&available_task.task_id) else {
            return Ok(None);
        };

        if policy.max_retries == 0 || snapshot.retries_exhausted(&available_task.task_id) {
            return Ok(None);
        }

        let next_retry_at = snapshot.record_retry(
            available_task.task_id,
            policy,
            error_msg,
            Some(&self.worker_id),
        );
        snapshot.clear_task_deadline();
        let _ = self.backend.save_snapshot(snapshot).await;

        tracing::info!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            attempt = snapshot.get_retry_state(&available_task.task_id).map_or(0, |rs| rs.attempts),
            max_retries = policy.max_retries,
            %next_retry_at,
            "Scheduling retry"
        );

        Ok(Some(WorkflowStatus::InProgress))
    }

    /// Settle the outcome of task execution: persist results or errors, release claim.
    #[tracing::instrument(
        name = "settle_result",
        skip_all,
        fields(worker_id = %self.worker_id, instance_id = %available_task.instance_id, task_id = %available_task.task_id),
    )]
    async fn settle_execution_result<C, Input, M>(
        &self,
        outcome: ExecutionOutcome,
        workflow: &Workflow<C, Input, M>,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: ActiveTaskClaim<'_, B>,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError>
    where
        Input: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        tracing::debug!("settling execution result");
        match outcome {
            ExecutionOutcome::Timeout(err) => {
                if let Ok(Some(status)) = self
                    .try_schedule_retry(
                        workflow.task_index(),
                        available_task,
                        snapshot,
                        &err.to_string(),
                    )
                    .await
                {
                    claim.release_quietly().await;
                    return Ok(status);
                }

                tracing::warn!(
                    instance_id = %available_task.instance_id,
                    task_id = %available_task.task_id,
                    error = %err,
                    "Task timed out via heartbeat — marking workflow failed, shutting down"
                );
                snapshot.mark_failed(err.to_string());
                let _ = self.backend.save_snapshot(snapshot).await;
                claim.release_quietly().await;
                Err(err)
            }
            ExecutionOutcome::Panic(panic_payload) => {
                let panic_msg = extract_panic_message(&panic_payload);

                if let Ok(Some(status)) = self
                    .try_schedule_retry(workflow.task_index(), available_task, snapshot, &panic_msg)
                    .await
                {
                    claim.release_quietly().await;
                    return Ok(status);
                }

                tracing::error!(
                    instance_id = %available_task.instance_id,
                    task_id = %available_task.task_id,
                    panic = %panic_msg,
                    "Task panicked - releasing claim"
                );
                claim.release_quietly().await;
                Err(WorkflowError::TaskPanicked(panic_msg).into())
            }
            ExecutionOutcome::TaskError(e) => {
                if let Ok(Some(status)) = self
                    .try_schedule_retry(
                        workflow.task_index(),
                        available_task,
                        snapshot,
                        &e.to_string(),
                    )
                    .await
                {
                    claim.release_quietly().await;
                    return Ok(status);
                }

                tracing::error!(
                    instance_id = %available_task.instance_id,
                    task_id = %available_task.task_id,
                    error = %e,
                    "Task execution failed"
                );
                claim.release_quietly().await;
                Err(e)
            }
            ExecutionOutcome::Success(output) => {
                snapshot.clear_retry_state(&available_task.task_id);
                self.commit_task_result(
                    workflow.continuation(),
                    available_task,
                    snapshot,
                    output.clone(),
                    claim,
                )
                .await?;
                // After saving the body task result, resolve any parent loop
                // nodes so that is_workflow_complete() can detect loop completion.
                Self::resolve_loop_completions(
                    workflow.continuation(),
                    snapshot,
                    self.backend.as_ref(),
                )
                .await?;
                self.determine_post_task_status(
                    workflow.continuation(),
                    available_task,
                    snapshot,
                    output,
                )
                .await
            }
        }
    }

    /// Acquire a claim on the task, returning an `ActiveTaskClaim`.
    ///
    /// Returns `None` if already claimed by another worker.
    async fn claim_task(
        &self,
        available_task: &AvailableTask,
    ) -> Result<Option<ActiveTaskClaim<'_, B>>, crate::error::RuntimeError> {
        let claim = self
            .backend
            .claim_task(
                &available_task.instance_id,
                &available_task.task_id,
                &self.worker_id,
                self.claim_ttl
                    .and_then(|d| chrono::Duration::from_std(d).ok()),
            )
            .await?;

        if claim.is_some() {
            tracing::debug!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Claim successful"
            );
            Ok(Some(ActiveTaskClaim {
                backend: &self.backend,
                instance_id: Arc::clone(&available_task.instance_id),
                task_id: available_task.task_id,
                worker_id: self.worker_id.clone(),
            }))
        } else {
            tracing::debug!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Task was already claimed by another worker"
            );
            Ok(None)
        }
    }

    /// Check cancel/pause guards after claiming.
    ///
    /// Returns `Some(status)` if the workflow is cancelled or paused
    /// (caller should release claim and return status).
    /// Returns `None` if execution should proceed.
    async fn check_post_claim_guards(
        &self,
        available_task: &AvailableTask,
    ) -> Result<Option<WorkflowStatus>, crate::error::RuntimeError> {
        if self
            .backend
            .check_and_cancel(&available_task.instance_id, Some(available_task.task_id))
            .await?
        {
            tracing::info!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Workflow was cancelled, releasing claim"
            );
            return Ok(Some(
                self.load_cancelled_status(&available_task.instance_id)
                    .await,
            ));
        }

        if self
            .backend
            .check_and_pause(&available_task.instance_id)
            .await?
        {
            tracing::info!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Workflow was paused, releasing claim"
            );
            return Ok(Some(
                self.load_paused_status(&available_task.instance_id).await,
            ));
        }

        Ok(None)
    }

    /// Execute a future while periodically extending the task claim.
    ///
    /// If a `deadline` is provided, the heartbeat tick also checks whether the
    /// deadline has expired. If it has, the task future is dropped (active
    /// cancellation) and a `TaskTimedOut` error is returned.
    #[tracing::instrument(
        name = "task",
        skip_all,
        fields(worker_id = %self.worker_id, instance_id = %claim.instance_id, task_id = %claim.task_id),
    )]
    async fn run_with_heartbeat<F, T>(
        &self,
        claim: &ActiveTaskClaim<'_, B>,
        deadline: Option<&TaskDeadline>,
        future: F,
    ) -> Result<T, crate::error::RuntimeError>
    where
        F: std::future::Future<Output = T>,
    {
        tracing::debug!("running task with heartbeat");
        let Some(ttl) = self.claim_ttl else {
            return Ok(future.await);
        };
        let Some(chrono_ttl) = chrono::Duration::from_std(ttl).ok() else {
            return Ok(future.await);
        };

        let interval_duration = ttl / 2;
        let mut heartbeat_timer = time::interval(interval_duration);
        heartbeat_timer.tick().await; // skip first immediate tick

        tokio::pin!(future);

        loop {
            tokio::select! {
                result = &mut future => break Ok(result),
                _ = heartbeat_timer.tick() => {
                    // Check deadline during heartbeat
                    if let Some(dl) = deadline
                        && chrono::Utc::now() >= dl.deadline
                    {
                        tracing::warn!(
                            instance_id = %claim.instance_id,
                            task_id = %dl.task_id,
                            "Task deadline expired during heartbeat, cancelling"
                        );
                        return Err(WorkflowError::TaskTimedOut {
                            task_id: dl.task_id,
                            timeout: std::time::Duration::from_millis(dl.timeout_ms),
                        }
                        .into());
                    }

                    tracing::trace!(
                        instance_id = %claim.instance_id,
                        task_id = %claim.task_id,
                        "Extending task claim via heartbeat"
                    );
                    if let Err(e) = self.backend
                        .extend_task_claim(
                            &claim.instance_id,
                            &claim.task_id,
                            &claim.worker_id,
                            chrono_ttl,
                        )
                        .await
                    {
                        tracing::warn!(
                            instance_id = %claim.instance_id,
                            task_id = %claim.task_id,
                            error = %e,
                            "Failed to extend task claim"
                        );
                    }
                }
            }
        }
    }

    /// A crash can persist the final task result before workflow completion.
    /// Continue that checkpoint under a real claim without repeating its effect.
    async fn resume_checkpointed_task(
        &self,
        continuation: &WorkflowContinuation,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: ActiveTaskClaim<'_, B>,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError> {
        let output = snapshot
            .get_task_result(&available_task.task_id)
            .map(|result| result.output.clone())
            .ok_or_else(|| WorkflowError::TaskNotFound(available_task.task_id.to_hex()))?;
        snapshot.clear_retry_state(&available_task.task_id);
        self.commit_task_result(
            continuation,
            available_task,
            snapshot,
            output.clone(),
            claim,
        )
        .await?;
        Self::resolve_loop_completions(continuation, snapshot, self.backend.as_ref()).await?;
        self.determine_post_task_status(continuation, available_task, snapshot, output)
            .await
    }

    /// Persist task result and release the claim.
    async fn commit_task_result(
        &self,
        continuation: &WorkflowContinuation,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        output: Bytes,
        claim: ActiveTaskClaim<'_, B>,
    ) -> Result<(), crate::error::RuntimeError> {
        snapshot.mark_task_completed(available_task.task_id, output);
        tracing::debug!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            "Task completed"
        );

        Self::update_position_after_task(continuation, &available_task.task_id, snapshot)?;
        #[cfg(feature = "otel")]
        {
            snapshot.trace_parent = crate::trace_context::current_trace_parent();
        }
        self.backend.save_snapshot(snapshot).await?;

        // If we just entered AtSignal, drain any event that was
        // buffered while the snapshot was still at AtTask. The
        // race: send_event takes the lock during this worker's task
        // body (status='InProgress', position=AtTask), sees no
        // AtSignal to resume, INSERTs into sayiir_workflow_events,
        // and commits. Then this save_snapshot advances to AtSignal,
        // but PooledWorker has no AwaitSignal poll loop, so the
        // workflow would sit forever with a matching event sitting
        // unconsumed in the buffer. Closing the race here keeps the
        // signal-driven dispatch path single-source (send_event is
        // still primary; this is the worker's belt-and-suspenders).
        self.drain_pending_signal(&available_task.instance_id, snapshot)
            .await?;

        claim.release().await?;
        Ok(())
    }

    /// If `snapshot` is parked at `AtSignal { signal_name }` and a
    /// matching event sits buffered in `sayiir_workflow_events`,
    /// consume it and advance the snapshot to its next position. Loops
    /// in case advancement lands on another `AtSignal` whose buffered
    /// event is also already waiting.
    async fn drain_pending_signal(
        &self,
        instance_id: &Arc<str>,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(), crate::error::RuntimeError> {
        loop {
            let (signal_id, signal_name, next_task_id) = match &snapshot.state {
                glirulus_core::snapshot::WorkflowSnapshotState::InProgress {
                    position:
                        glirulus_core::snapshot::ExecutionPosition::AtSignal {
                            signal_id,
                            signal_name,
                            next_task_id,
                            ..
                        },
                    ..
                } => (*signal_id, signal_name.clone(), *next_task_id),
                _ => return Ok(()),
            };

            let payload = self
                .backend
                .consume_event(instance_id, &signal_name)
                .await?;
            if crate::execution::refresh_consumed_signal(self.backend.as_ref(), snapshot, signal_id)
                .await?
            {
                continue;
            }
            let Some(payload) = payload else {
                return Ok(());
            };

            tracing::debug!(
                instance_id = %instance_id,
                %signal_name,
                "draining buffered signal that landed during the AtTask→AtSignal transition"
            );
            snapshot.mark_task_completed(signal_id, payload.clone());
            if let Some(next_id) = next_task_id {
                snapshot.update_position(glirulus_core::snapshot::ExecutionPosition::AtTask {
                    task_id: next_id,
                });
            } else {
                snapshot.mark_completed(payload);
            }
            self.backend.save_snapshot(snapshot).await?;
        }
    }

    /// Determine workflow status after a task completes.
    ///
    /// Checks cancel/pause guards and workflow completion.
    async fn determine_post_task_status(
        &self,
        continuation: &WorkflowContinuation,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        output: Bytes,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError> {
        // Check for cancellation after task completion
        if self
            .backend
            .check_and_cancel(&available_task.instance_id, None)
            .await?
        {
            tracing::info!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Workflow was cancelled after task completion"
            );
            return Ok(self
                .load_cancelled_status(&available_task.instance_id)
                .await);
        }

        // Check for pause after task completion
        if self
            .backend
            .check_and_pause(&available_task.instance_id)
            .await?
        {
            tracing::info!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Workflow was paused after task completion"
            );
            return Ok(self.load_paused_status(&available_task.instance_id).await);
        }

        if Self::is_workflow_complete(continuation, snapshot) {
            tracing::info!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Workflow complete"
            );
            snapshot.mark_completed(output);
            self.backend.save_snapshot(snapshot).await?;
            Ok(WorkflowStatus::Completed)
        } else {
            tracing::debug!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Task completed, workflow continues"
            );
            Ok(WorkflowStatus::InProgress)
        }
    }
}
