//! Worker continuation lookup, dispatch positioning and checkpoint progression.

use super::PooledWorker;
use bytes::Bytes;
use glirulus_core::codec::LoopDecision;
use glirulus_core::error::{CodecError, WorkflowError};
use glirulus_core::snapshot::{ExecutionPosition, TaskHint, WorkflowSnapshot};
use glirulus_core::task_claim::AvailableTask;
use glirulus_core::workflow::WorkflowContinuation;
use glirulus_persistence::{PersistentBackend, TaskClaimStore};
use std::pin::Pin;
use std::sync::Arc;

impl<B> PooledWorker<B>
where
    B: PersistentBackend + TaskClaimStore + 'static,
{
    /// Core 1.0's index omits Branch selectors. The external executor dispatches
    /// them by name, so only that path supplies its retained continuation.
    pub(super) fn dispatch_task_name(
        task_index: &glirulus_core::TaskIndex,
        continuation: Option<&WorkflowContinuation>,
        task_id: &glirulus_core::TaskId,
    ) -> Option<Arc<str>> {
        task_index.name(task_id).cloned().or_else(|| {
            continuation?.iter_nodes().find_map(|node| {
                (node.kind == glirulus_core::workflow::NodeKind::Branch
                    && glirulus_core::TaskId::from(node.id) == *task_id)
                    .then(|| Arc::from(node.id))
            })
        })
    }

    /// Validate task preconditions without side effects.
    ///
    /// Checks definition hash match, task existence in continuation,
    /// and whether the current task has a retained result.
    /// `Ok(true)` requires checkpoint continuation after claiming, never execution.
    pub(super) fn validate_task_preconditions(
        definition_hash: &glirulus_core::DefinitionHash,
        task_index: &glirulus_core::TaskIndex,
        continuation: Option<&WorkflowContinuation>,
        available_task: &AvailableTask,
        snapshot: &WorkflowSnapshot,
    ) -> Result<bool, crate::error::RuntimeError> {
        if available_task.workflow_definition_hash != *definition_hash {
            return Err(WorkflowError::DefinitionMismatch {
                expected: *definition_hash,
                found: available_task.workflow_definition_hash,
            }
            .into());
        }

        if Self::dispatch_task_name(task_index, continuation, &available_task.task_id).is_none() {
            tracing::error!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Task does not exist in workflow"
            );
            return Err(WorkflowError::TaskNotFound(available_task.task_id.to_hex()).into());
        }

        if snapshot.get_task_result(&available_task.task_id).is_some() {
            tracing::debug!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Task already completed, resuming checkpoint"
            );
            return Ok(true);
        }

        Ok(false)
    }

    /// If `task_id` is the entry task of a `Fork`'s join continuation,
    /// return the slice of branches whose outputs feed the join. The
    /// `PooledWorker` dispatches each task with `available_task.input`
    /// set to the previous task's output — for a join, that's just one
    /// branch's output, not the `NamedBranchResults` the join body
    /// expects. Callers use this to rebuild the join input from
    /// `snapshot.completed_tasks` before invoking the join task.
    pub(super) fn find_fork_branches_for_join<'a>(
        continuation: &'a WorkflowContinuation,
        task_id: &glirulus_core::TaskId,
    ) -> Option<&'a [Arc<WorkflowContinuation>]> {
        match continuation {
            WorkflowContinuation::Task { next, .. }
            | WorkflowContinuation::Delay { next, .. }
            | WorkflowContinuation::AwaitSignal { next, .. } => next
                .as_deref()
                .and_then(|n| Self::find_fork_branches_for_join(n, task_id)),
            WorkflowContinuation::Fork { branches, join, .. } => {
                if let Some(join_cont) = join {
                    let join_first = glirulus_core::TaskId::from(join_cont.first_task_id());
                    if join_first == *task_id {
                        return Some(&branches[..]);
                    }
                    if let Some(b) = Self::find_fork_branches_for_join(join_cont, task_id) {
                        return Some(b);
                    }
                }
                for branch in branches {
                    if let Some(b) = Self::find_fork_branches_for_join(branch, task_id) {
                        return Some(b);
                    }
                }
                None
            }
            WorkflowContinuation::Branch {
                branches,
                default,
                next,
                ..
            } => {
                for branch_cont in branches.values() {
                    if let Some(b) = Self::find_fork_branches_for_join(branch_cont, task_id) {
                        return Some(b);
                    }
                }
                if let Some(def) = default
                    && let Some(b) = Self::find_fork_branches_for_join(def, task_id)
                {
                    return Some(b);
                }
                next.as_deref()
                    .and_then(|n| Self::find_fork_branches_for_join(n, task_id))
            }
            WorkflowContinuation::Loop { body, next, .. } => {
                Self::find_fork_branches_for_join(body, task_id).or_else(|| {
                    next.as_deref()
                        .and_then(|n| Self::find_fork_branches_for_join(n, task_id))
                })
            }
            WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                Self::find_fork_branches_for_join(child, task_id).or_else(|| {
                    next.as_deref()
                        .and_then(|n| Self::find_fork_branches_for_join(n, task_id))
                })
            }
        }
    }

    /// A persisted signal/delay advancement names its next node by ID. If that
    /// head is another control node, park it under the normal dispatch claim;
    /// it has no task body and must never reach the external task executor.
    pub(super) fn park_dispatched_control(
        continuation: &WorkflowContinuation,
        task_id: &glirulus_core::TaskId,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<bool, crate::error::RuntimeError> {
        match Self::find_continuation(continuation, task_id) {
            Some(
                head @ (WorkflowContinuation::AwaitSignal { .. }
                | WorkflowContinuation::Delay { .. }),
            ) => {
                Self::set_position_at(head, snapshot)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Locate a dispatchable task/control head without choosing a Branch arm.
    fn find_continuation<'a>(
        continuation: &'a WorkflowContinuation,
        task_id: &glirulus_core::TaskId,
    ) -> Option<&'a WorkflowContinuation> {
        match continuation {
            WorkflowContinuation::Task { id, next, .. }
            | WorkflowContinuation::Delay { id, next, .. }
            | WorkflowContinuation::AwaitSignal { id, next, .. } => {
                if glirulus_core::TaskId::from(id.as_str()) == *task_id {
                    Some(continuation)
                } else {
                    next.as_deref()
                        .and_then(|next| Self::find_continuation(next, task_id))
                }
            }
            WorkflowContinuation::Fork { branches, join, .. } => branches
                .iter()
                .find_map(|branch| Self::find_continuation(branch, task_id))
                .or_else(|| {
                    join.as_deref()
                        .and_then(|next| Self::find_continuation(next, task_id))
                }),
            WorkflowContinuation::Branch {
                id,
                branches,
                default,
                next,
                ..
            } => {
                if glirulus_core::TaskId::from(id.as_str()) == *task_id {
                    return Some(continuation);
                }
                branches
                    .values()
                    .find_map(|branch| Self::find_continuation(branch, task_id))
                    .or_else(|| {
                        default
                            .as_deref()
                            .and_then(|branch| Self::find_continuation(branch, task_id))
                    })
                    .or_else(|| {
                        next.as_deref()
                            .and_then(|next| Self::find_continuation(next, task_id))
                    })
            }
            WorkflowContinuation::Loop { body, next, .. } => Self::find_continuation(body, task_id)
                .or_else(|| {
                    next.as_deref()
                        .and_then(|next| Self::find_continuation(next, task_id))
                }),
            WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                Self::find_continuation(child, task_id).or_else(|| {
                    next.as_deref()
                        .and_then(|next| Self::find_continuation(next, task_id))
                })
            }
        }
    }

    /// Tree-walk predicate used to select the containing execution subtree.
    fn find_task_id_in_continuation(
        continuation: &WorkflowContinuation,
        task_id: &glirulus_core::TaskId,
    ) -> bool {
        Self::find_continuation(continuation, task_id).is_some()
    }

    /// Execute a task by ID from the workflow continuation (iterative, no boxing).
    #[allow(clippy::manual_async_fn)]
    pub(super) fn execute_task_by_id<'a>(
        continuation: &'a WorkflowContinuation,
        task_id: &'a str,
        input: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, crate::error::RuntimeError>> + Send + 'a
    {
        async move {
            let task_id_hash = glirulus_core::TaskId::from(task_id);
            let task_id = &task_id_hash;
            let mut current = continuation;

            loop {
                match current {
                    WorkflowContinuation::Task { id, func, next, .. } => {
                        if glirulus_core::TaskId::from(id.as_str()) == *task_id {
                            let func = func
                                .as_ref()
                                .ok_or_else(|| WorkflowError::TaskNotImplemented(id.clone()))?;
                            return Ok(func.run(input).await?);
                        } else if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::Delay { next, .. }
                    | WorkflowContinuation::AwaitSignal { next, .. } => {
                        // Skip over delay/signal nodes when searching for a task
                        if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::Fork { branches, join, .. } => {
                        // Check branches
                        let mut found_in_branch = false;
                        for branch in branches {
                            if Self::find_task_id_in_continuation(branch, task_id) {
                                current = branch;
                                found_in_branch = true;
                                break;
                            }
                        }
                        if found_in_branch {
                            continue;
                        }
                        // Check join
                        if let Some(join_cont) = join {
                            current = join_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::Branch {
                        branches,
                        default,
                        next,
                        ..
                    } => {
                        // Search branch sub-continuations for the task
                        let mut found = false;
                        for branch_cont in branches.values() {
                            if Self::find_task_id_in_continuation(branch_cont, task_id) {
                                current = branch_cont;
                                found = true;
                                break;
                            }
                        }
                        if found {
                            continue;
                        }
                        if let Some(def) = default
                            && Self::find_task_id_in_continuation(def, task_id)
                        {
                            current = def;
                            continue;
                        }
                        if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::Loop { body, next, .. } => {
                        if Self::find_task_id_in_continuation(body, task_id) {
                            current = body;
                            continue;
                        }
                        if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                        if Self::find_task_id_in_continuation(child, task_id) {
                            current = child;
                            continue;
                        }
                        if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                }
            }
        }
    }

    /// Set the snapshot's execution position to reflect "execution is now at
    /// the head of `cont`".
    ///
    /// For Task heads (and Branch / Fork / Loop / `ChildWorkflow` which all
    /// start with a Task-shaped node), set `AtTask(first_task_hint)`. For
    /// `Delay` / `AwaitSignal` heads, enter the corresponding park state
    /// directly — using `first_task_hint(...).id` would name the Delay or
    /// Signal node itself, but `execute_task_by_id` skips past those when
    /// dispatching, so the worker would loop on a phantom task ID.
    fn set_position_at(
        cont: &WorkflowContinuation,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(), crate::error::RuntimeError> {
        use crate::execution::control_flow::{compute_signal_timeout, compute_wake_at};
        match cont {
            WorkflowContinuation::Delay { id, duration, next } => {
                let wake_at = compute_wake_at(duration)?;
                let entered_at = chrono::Utc::now();
                let next_hint = next.as_deref().map(WorkflowContinuation::first_task_hint);
                let next_task_id = next_hint.as_ref().map(|h| h.id);
                // Update task_priority/task_tags so backends advancing
                // AtDelay -> AtTask inherit the next task's routing hints
                // (mirrors save_park_checkpoint).
                snapshot.set_task_hint(next_hint.as_ref().unwrap_or(&TaskHint::default()));
                let delay_id = glirulus_core::TaskId::from(id.as_str());
                snapshot.update_position(ExecutionPosition::AtDelay {
                    delay_id,
                    entered_at,
                    wake_at,
                    next_task_id,
                });
                // Mirror the executor's behaviour when it parks at a delay:
                // mark the delay node as completed with the passthrough input
                // (which is the *last* completed task's output, sitting at the
                // top of `completed_tasks`) so downstream nodes see the same
                // continuous output chain.
                let passthrough = snapshot.get_last_task_output().unwrap_or_default();
                snapshot.mark_task_completed(delay_id, passthrough);
            }
            WorkflowContinuation::AwaitSignal {
                id,
                signal_name,
                timeout,
                next,
            } => {
                let wake_at = compute_signal_timeout(timeout.as_ref());
                let next_hint = next.as_deref().map(WorkflowContinuation::first_task_hint);
                let next_task_id = next_hint.as_ref().map(|h| h.id);
                // Update task_priority/task_tags so backends advancing
                // AtSignal -> AtTask inherit the next task's routing hints
                // (mirrors save_park_checkpoint).
                snapshot.set_task_hint(next_hint.as_ref().unwrap_or(&TaskHint::default()));
                snapshot.update_position(ExecutionPosition::AtSignal {
                    signal_id: glirulus_core::TaskId::from(id.as_str()),
                    signal_name: signal_name.clone(),
                    wake_at,
                    next_task_id,
                });
            }
            _ => {
                let hint = cont.first_task_hint();
                snapshot.update_position(ExecutionPosition::AtTask { task_id: hint.id });
                snapshot.set_task_hint(&hint);
            }
        }
        Ok(())
    }

    /// Update execution position after a task completes.
    pub(super) fn update_position_after_task(
        continuation: &WorkflowContinuation,
        completed_task_id: &glirulus_core::TaskId,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(), crate::error::RuntimeError> {
        match continuation {
            WorkflowContinuation::Task { id, next, .. }
            | WorkflowContinuation::Delay { id, next, .. }
            | WorkflowContinuation::AwaitSignal { id, next, .. } => {
                if glirulus_core::TaskId::from(id.as_str()) == *completed_task_id {
                    if let Some(next_cont) = next.as_deref() {
                        Self::set_position_at(next_cont, snapshot)?;
                    }
                } else if let Some(next_cont) = next {
                    Self::update_position_after_task(next_cont, completed_task_id, snapshot)?;
                }
            }
            WorkflowContinuation::Fork { branches, join, .. } => {
                // First let recursion advance position WITHIN a branch (for
                // multi-task branch chains) or within the join continuation.
                for branch in branches {
                    Self::update_position_after_task(branch, completed_task_id, snapshot)?;
                }
                if let Some(join_cont) = join {
                    Self::update_position_after_task(join_cont, completed_task_id, snapshot)?;
                }

                // If recursion didn't move the position off the just-completed
                // task, we finished a branch's terminal node — the inner Task
                // arm has no `next` to advance to. Drive the fork forward at
                // this level: pick the next branch that hasn't started, or
                // step into the join when every branch is done. Without this
                // step, single-Task branches (the common fan-out shape) leave
                // the snapshot pinned to `AtTask(branch_n)` forever and the
                // next dispatch just re-discovers the same already-completed
                // task — workflow stalls indefinitely.
                let still_at_completed = snapshot
                    .current_task_id()
                    .is_some_and(|c| c == *completed_task_id);
                if !still_at_completed {
                    return Ok(());
                }

                // Sequential branch execution: the first branch whose entry
                // task has no result yet is where we resume. Matches
                // `collect_cached_branches`' lookup so the two views of "branch
                // is done" stay in sync.
                for branch in branches {
                    let first_tid = glirulus_core::TaskId::from(branch.first_task_id());
                    if snapshot.get_task_result(&first_tid).is_none() {
                        Self::set_position_at(branch, snapshot)?;
                        return Ok(());
                    }
                }

                // Every branch finished — advance to the join. If the fork has
                // no join, leave the position at the last completed task and
                // let `is_workflow_complete` mark the workflow done.
                if let Some(join_cont) = join {
                    Self::set_position_at(join_cont, snapshot)?;
                }
            }
            WorkflowContinuation::Branch {
                id,
                branches,
                default,
                next,
                ..
            } => {
                let branch_id = glirulus_core::TaskId::from(id.as_str());
                if snapshot.get_task_result(&branch_id).is_some() {
                    let Some(chosen) =
                        Self::chosen_branch(id, branches, default.as_deref(), snapshot)?
                    else {
                        if let Some(next_cont) = next {
                            if branch_id == *completed_task_id {
                                Self::set_position_at(next_cont, snapshot)?;
                            } else {
                                Self::update_position_after_task(
                                    next_cont,
                                    completed_task_id,
                                    snapshot,
                                )?;
                            }
                        }
                        return Ok(());
                    };
                    if branch_id == *completed_task_id {
                        Self::set_position_at(chosen, snapshot)?;
                    } else if Self::find_task_id_in_continuation(chosen, completed_task_id) {
                        Self::update_position_after_task(chosen, completed_task_id, snapshot)?;
                        if Self::is_workflow_complete(chosen, snapshot) {
                            if let Some(next_cont) = next {
                                Self::set_position_at(next_cont, snapshot)?;
                            }
                        }
                    } else if let Some(next_cont) = next {
                        Self::update_position_after_task(next_cont, completed_task_id, snapshot)?;
                    }
                }
            }
            WorkflowContinuation::Loop { body, next, .. } => {
                Self::update_position_after_task(body, completed_task_id, snapshot)?;
                if let Some(next_cont) = next {
                    Self::update_position_after_task(next_cont, completed_task_id, snapshot)?;
                }
            }
            WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                Self::update_position_after_task(child, completed_task_id, snapshot)?;
                if let Some(next_cont) = next {
                    Self::update_position_after_task(next_cont, completed_task_id, snapshot)?;
                }
            }
        }
        Ok(())
    }

    /// Walk the continuation tree and resolve any Loop nodes whose body has
    /// completed. When the terminal body task's output decodes to
    /// `LoopDecision::Done`, the loop node is marked as completed in the
    /// snapshot. For `Again`, body task results are cleared and the iteration
    /// counter is advanced so the body becomes available for re-execution.
    pub(super) async fn resolve_loop_completions(
        continuation: &WorkflowContinuation,
        snapshot: &mut WorkflowSnapshot,
        backend: &B,
    ) -> Result<(), crate::error::RuntimeError> {
        Self::resolve_loops_recursive(continuation, snapshot, backend).await
    }

    #[allow(clippy::too_many_lines)]
    fn resolve_loops_recursive<'a>(
        continuation: &'a WorkflowContinuation,
        snapshot: &'a mut WorkflowSnapshot,
        backend: &'a B,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<(), crate::error::RuntimeError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match continuation {
                WorkflowContinuation::Loop {
                    id,
                    body,
                    max_iterations,
                    on_max,
                    next,
                } => {
                    // Only resolve if the loop isn't already marked complete.
                    if snapshot
                        .get_task_result(&glirulus_core::TaskId::from(id))
                        .is_none()
                    {
                        let terminal_id = body.terminal_task_id();
                        if let Some(result) =
                            snapshot.get_task_result(&glirulus_core::TaskId::from(terminal_id))
                        {
                            let output = result.output.clone();
                            match crate::execution::decode_loop_envelope(&output) {
                                Ok((LoopDecision::Done, inner)) => {
                                    snapshot.clear_loop_iteration(&glirulus_core::TaskId::from(id));
                                    snapshot.mark_task_completed(
                                        glirulus_core::TaskId::from(id),
                                        inner,
                                    );
                                    backend.save_snapshot(snapshot).await?;
                                }
                                Ok((LoopDecision::Again, again_value)) => {
                                    let current_iter =
                                        snapshot.loop_iteration(&glirulus_core::TaskId::from(id));
                                    let next_iter = current_iter + 1;
                                    if next_iter >= *max_iterations {
                                        match on_max {
                      glirulus_core::workflow::MaxIterationsPolicy::Fail => {
                        return Err(
                          WorkflowError::MaxIterationsExceeded {
                            loop_id: glirulus_core::TaskId::from(id),
                            max_iterations: *max_iterations,
                          }
                          .into(),
                        );
                      }
                      glirulus_core::workflow::MaxIterationsPolicy::ExitWithLast => {
                        snapshot.clear_loop_iteration(&glirulus_core::TaskId::from(id));
                        snapshot.mark_task_completed(glirulus_core::TaskId::from(id.as_str()), again_value);
                        backend.save_snapshot(snapshot).await?;
                      }
                    }
                                    } else {
                                        // Clear body task results so the body becomes
                                        // available for re-execution on the next poll.
                                        let body_ser = body.to_serializable();
                                        for tid in &body_ser.task_ids() {
                                            snapshot.remove_task_result(
                                                &glirulus_core::TaskId::from(*tid),
                                            );
                                        }
                                        snapshot.set_loop_iteration(
                                            glirulus_core::TaskId::from(id),
                                            next_iter,
                                        );
                                        backend.save_snapshot(snapshot).await?;
                                    }
                                }
                                Err(e) => {
                                    return Err(CodecError::DecodeFailed {
                                        task_id: glirulus_core::TaskId::from(id),
                                        expected_type: "LoopEnvelope",
                                        source: e,
                                    }
                                    .into());
                                }
                            }
                        }
                    }
                    // Recurse into body and next.
                    Self::resolve_loops_recursive(body, snapshot, backend).await?;
                    if let Some(next) = next {
                        Self::resolve_loops_recursive(next, snapshot, backend).await?;
                    }
                }
                WorkflowContinuation::Task { next, .. }
                | WorkflowContinuation::Delay { next, .. }
                | WorkflowContinuation::AwaitSignal { next, .. }
                | WorkflowContinuation::Branch { next, .. } => {
                    if let Some(next) = next {
                        Self::resolve_loops_recursive(next, snapshot, backend).await?;
                    }
                }
                WorkflowContinuation::Fork { branches, join, .. } => {
                    for branch in branches {
                        Self::resolve_loops_recursive(branch, snapshot, backend).await?;
                    }
                    if let Some(join) = join {
                        Self::resolve_loops_recursive(join, snapshot, backend).await?;
                    }
                }
                WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                    Self::resolve_loops_recursive(child, snapshot, backend).await?;
                    if let Some(next) = next {
                        Self::resolve_loops_recursive(next, snapshot, backend).await?;
                    }
                }
            }
            Ok(())
        })
    }

    /// A selector checkpoint retains the full versioned input for its chosen
    /// child. A routing key selects its child; an explicit empty projection
    /// completes a source-authored empty arm without inventing a task.
    fn chosen_branch<'a>(
        id: &str,
        branches: &'a std::collections::HashMap<String, Box<WorkflowContinuation>>,
        default: Option<&'a WorkflowContinuation>,
        snapshot: &WorkflowSnapshot,
    ) -> Result<Option<&'a WorkflowContinuation>, crate::error::RuntimeError> {
        let payload = snapshot
            .get_task_result_bytes(&glirulus_core::TaskId::from(id))
            .ok_or_else(|| {
                WorkflowError::ResumeError("branch selector checkpoint unavailable".into())
            })?;
        #[cfg(feature = "json")]
        {
            let value: serde_json::Value = serde_json::from_slice(&payload).map_err(|_| {
                WorkflowError::ResumeError("branch selector input unavailable".into())
            })?;
            let key = value
                .get("routingKey")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    WorkflowError::ResumeError("branch routing key unavailable".into())
                })?;
            // A trusted external selector may explicitly select an authored empty
            // arm. It has no child task; absent/unknown keys still fail closed.
            let empty = value
                .get("routingEmpty")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if empty {
                if branches.contains_key(key) || default.is_some() {
                    return Err(WorkflowError::ResumeError(
                        "empty branch conflicts with a declared child".into(),
                    )
                    .into());
                }
                return Ok(None);
            }
            branches
                .get(key)
                .map(Box::as_ref)
                .or(default)
                .map(Some)
                .ok_or_else(|| {
                    WorkflowError::ResumeError("branch routing key is undeclared".into()).into()
                })
        }
        #[cfg(not(feature = "json"))]
        {
            let _ = (payload, branches, default);
            Err(WorkflowError::ResumeError(
                "external branch requires a JSON routing projection".into(),
            )
            .into())
        }
    }

    /// Check if the workflow is complete based on the snapshot.
    pub(super) fn is_workflow_complete(
        continuation: &WorkflowContinuation,
        snapshot: &WorkflowSnapshot,
    ) -> bool {
        // Check if all tasks in the continuation are completed
        match continuation {
            WorkflowContinuation::Task { id, next, .. } => {
                if snapshot
                    .get_task_result(&glirulus_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                if let Some(next_cont) = next {
                    Self::is_workflow_complete(next_cont, snapshot)
                } else {
                    true // Last task completed
                }
            }
            WorkflowContinuation::Delay { id, next, .. }
            | WorkflowContinuation::AwaitSignal { id, next, .. } => {
                if snapshot
                    .get_task_result(&glirulus_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                next.as_ref()
                    .is_none_or(|n| Self::is_workflow_complete(n, snapshot))
            }
            WorkflowContinuation::Fork { branches, join, .. } => {
                // All branches must be completed (recursively check entire branch chain)
                for branch in branches {
                    if !Self::is_workflow_complete(branch, snapshot) {
                        return false;
                    }
                }
                // Join must be completed if it exists
                if let Some(join_cont) = join {
                    Self::is_workflow_complete(join_cont, snapshot)
                } else {
                    true
                }
            }
            WorkflowContinuation::Branch {
                id,
                branches,
                default,
                next,
                ..
            } => Self::chosen_branch(id, branches, default.as_deref(), snapshot).is_ok_and(
                |chosen| {
                    chosen.is_none_or(|chosen| Self::is_workflow_complete(chosen, snapshot))
                        && next
                            .as_ref()
                            .is_none_or(|n| Self::is_workflow_complete(n, snapshot))
                },
            ),
            WorkflowContinuation::Loop { id, next, .. } => {
                // Loop is complete when the loop node itself has a cached result
                if snapshot
                    .get_task_result(&glirulus_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                next.as_ref()
                    .is_none_or(|n| Self::is_workflow_complete(n, snapshot))
            }
            WorkflowContinuation::ChildWorkflow { id, next, .. } => {
                // ChildWorkflow is complete when the node itself has a cached result
                if snapshot
                    .get_task_result(&glirulus_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                next.as_ref()
                    .is_none_or(|n| Self::is_workflow_complete(n, snapshot))
            }
        }
    }
}
