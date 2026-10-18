//! Parallel branch collection and fork/join construction.

use super::{BranchCollector, ContinuationState, ForkBuilder, RegistryBehavior, WorkflowBuilder};
use crate::codec::{Codec, sealed};
use crate::error::BuildError;
use crate::priority::Priority;
use crate::registry::TaskRegistry;
use crate::task::{
    BranchOutputs, ErasedBranch, RegisterableTask, branch, to_heterogeneous_join_task_arc,
    wrap_core_task,
};
use crate::workflow::WorkflowContinuation;
use std::marker::PhantomData;
use std::sync::Arc;

/// Fork methods - unified implementation.
impl<C, Input, Output, M, Cont, R> WorkflowBuilder<C, Input, Output, M, Cont, R> {
    /// Fork the workflow into multiple parallel branches with heterogeneous outputs.
    ///
    /// Each branch receives the same input (the current workflow's output) and executes in parallel.
    /// Branches can return different types. After all branches complete, use `join()` to combine
    /// the results using `BranchOutputs` for type-safe named access.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use glirulus_core::task::TaskMetadata;
    ///
    /// workflow
    ///     .then("prepare", |input| async { Ok(input) })
    ///     .with_metadata(TaskMetadata {
    ///         display_name: Some("Prepare Input".into()),
    ///         ..Default::default()
    ///     })
    ///     .branches(|b| {
    ///         b.add("count", |i: u32| async move { Ok(i * 2) });
    ///         b.add("name", |i: u32| async move { Ok(format!("item_{}", i)) });
    ///     })
    ///     .join("combine", |outputs: BranchOutputs<_>| async move {
    ///         let count: u32 = outputs.get_by_id("count")?;
    ///         let name: String = outputs.get_by_id("name")?;
    ///         Ok(format!("{}: {}", name, count))
    ///     })
    ///     .with_metadata(TaskMetadata {
    ///         display_name: Some("Combine Results".into()),
    ///         ..Default::default()
    ///     })
    /// ```
    pub fn branches<F>(self, f: F) -> ForkBuilder<C, Input, Output, M, Cont, R>
    where
        F: FnOnce(&mut BranchCollector<C, Output>),
        C: Codec,
    {
        let codec = Arc::clone(&self.context.codec);
        let mut collector = BranchCollector {
            codec,
            branches: Vec::new(),
            _phantom: PhantomData,
        };
        f(&mut collector);

        ForkBuilder {
            context: self.context,
            continuation: self.continuation,
            branches: collector.branches,
            registry: self.registry,
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }

    /// Fork the workflow into multiple parallel branches (low-level API).
    pub fn fork(self) -> ForkBuilder<C, Input, Output, M, Cont, R> {
        ForkBuilder {
            context: self.context,
            continuation: self.continuation,
            branches: Vec::new(),
            registry: self.registry,
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

impl<C, Input> BranchCollector<C, Input> {
    /// Add a branch to the fork.
    ///
    /// Each branch receives the same input and can return a different output type.
    /// Duplicate IDs are checked at `build()` time.
    pub fn add<F, Fut, BranchOutput>(&mut self, id: &str, func: F)
    where
        F: Fn(Input) -> Fut + Send + Sync + 'static,
        Input: Send + 'static,
        BranchOutput: Send + 'static,
        Fut: std::future::Future<Output = Result<BranchOutput, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<Input> + sealed::EncodeValue<BranchOutput>,
    {
        let erased = branch(id, func).erase(Arc::clone(&self.codec));
        self.branches.push(erased);
    }

    /// Add a `#[task]` struct as a branch (no injected deps — requires `Default`).
    pub fn add_task<T>(&mut self)
    where
        T: RegisterableTask<Input = Input> + Default,
        Input: Send + 'static,
        T::Output: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<Input> + sealed::EncodeValue<T::Output> + 'static,
    {
        self.add_task_with(T::default());
    }

    /// Add a `#[task]` struct instance as a branch (for tasks with injected deps).
    pub fn add_task_with<T>(&mut self, task: T)
    where
        T: RegisterableTask<Input = Input>,
        Input: Send + 'static,
        T::Output: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<Input> + sealed::EncodeValue<T::Output> + 'static,
    {
        let id = T::task_id();
        let codec = Arc::clone(&self.codec);
        let task = Arc::new(task);
        let untyped = wrap_core_task(id, task, codec);
        self.branches.push(ErasedBranch {
            id: id.to_string(),
            task: untyped,
        });
    }
}

/// For `ForkBuilder` methods - unified implementation using `RegistryBehavior` and `ContinuationState`.
impl<C, Input, Output, M, Cont, R> ForkBuilder<C, Input, Output, M, Cont, R>
where
    R: RegistryBehavior,
    Cont: ContinuationState,
{
    /// Add a branch to the fork.
    ///
    /// # Returns
    ///
    /// Returns a new `ForkBuilder` with the branch added.
    ///
    #[must_use]
    pub fn branch<F, Fut, BranchOutput>(mut self, id: &str, func: F) -> Self
    where
        F: Fn(Output) -> Fut + Send + Sync + 'static,
        Output: Send + 'static,
        BranchOutput: Send + 'static,
        Fut: std::future::Future<Output = Result<BranchOutput, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<BranchOutput> + 'static,
    {
        let codec = Arc::clone(&self.context.codec);
        let func = Arc::new(func);

        // Register if registry is enabled (no-op for NoRegistry)
        self.registry
            .maybe_register::<Output, BranchOutput, _, _, _>(id, codec.clone(), &func);

        // Create branch using a closure that calls through the Arc
        let func_clone = Arc::clone(&func);
        let erased = branch(id, move |input| func_clone(input)).erase(codec);
        self.branches.push(erased);
        self
    }

    /// Add a `#[task]` struct as a fork branch (no injected deps — requires `Default`).
    #[must_use]
    pub fn branch_task<T>(self) -> Self
    where
        T: RegisterableTask<Input = Output> + Default,
        Output: Send + 'static,
        T::Output: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<T::Output> + 'static,
    {
        self.branch_task_with(T::default())
    }

    /// Add a `#[task]` struct instance as a fork branch (for tasks with injected deps).
    #[must_use]
    pub fn branch_task_with<T>(mut self, task: T) -> Self
    where
        T: RegisterableTask<Input = Output>,
        Output: Send + 'static,
        T::Output: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<T::Output> + 'static,
    {
        let id = T::task_id();
        let metadata = T::metadata();
        let codec = Arc::clone(&self.context.codec);
        let task = Arc::new(task);

        self.registry.maybe_register_core_task::<T, C>(
            id,
            codec.clone(),
            Arc::clone(&task),
            metadata,
        );

        let untyped = wrap_core_task(id, task, codec);
        self.branches.push(ErasedBranch {
            id: id.to_string(),
            task: untyped,
        });
        self
    }

    /// Join the results from all branches.
    pub fn join<F, Fut, JoinOutput>(
        mut self,
        id: &str,
        func: F,
    ) -> WorkflowBuilder<C, Input, JoinOutput, M, WorkflowContinuation, R>
    where
        F: Fn(BranchOutputs<C>) -> Fut + Send + Sync + 'static,
        JoinOutput: Send + 'static,
        Fut: std::future::Future<Output = Result<JoinOutput, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec
            + sealed::EncodeValue<JoinOutput>
            + sealed::DecodeValue<crate::branch_results::NamedBranchResults>
            + Send
            + Sync
            + 'static,
    {
        let codec = Arc::clone(&self.context.codec);
        let func = Arc::new(func);

        // Register if registry is enabled (no-op for NoRegistry)
        self.registry
            .maybe_register_join::<JoinOutput, _, _, _>(id, codec.clone(), &func);

        let join_task_fn = to_heterogeneous_join_task_arc(id, func, codec);

        let fork_id = WorkflowContinuation::derive_fork_id(
            &self
                .branches
                .iter()
                .map(|b| b.id.as_str())
                .collect::<Vec<_>>(),
        );

        let branch_continuations: Vec<Arc<WorkflowContinuation>> = self
            .branches
            .into_iter()
            .map(|b| {
                Arc::new(WorkflowContinuation::Task {
                    id: b.id,
                    func: Some(b.task),
                    timeout: None,
                    retry_policy: None,
                    version: None,
                    priority: None,
                    tags: Vec::new(),
                    next: None,
                })
            })
            .collect();

        //

        let join_task = WorkflowContinuation::Task {
            id: id.to_string(),
            func: Some(join_task_fn),
            timeout: None,
            retry_policy: None,
            version: None,
            priority: None,
            tags: Vec::new(),
            next: None,
        };

        let fork_continuation = WorkflowContinuation::Fork {
            id: fork_id,
            branches: branch_continuations.into_boxed_slice(),
            join: Some(Box::new(join_task)),
        };

        let continuation = self.continuation.append(fork_continuation);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(id.to_string()),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

/// For `ForkBuilder` methods for referencing pre-registered tasks (only available with `TaskRegistry`).
impl<C, Input, Output, M, Cont> ForkBuilder<C, Input, Output, M, Cont, TaskRegistry>
where
    Cont: ContinuationState,
{
    /// Add a pre-registered branch task by ID.
    ///
    /// If the task ID is not found, the error is recorded and reported at
    /// `build()` time.
    #[must_use]
    pub fn branch_registered(mut self, id: &str) -> Self
    where
        Output: Send + 'static,
    {
        match self.registry.get(id) {
            Some(task) => {
                self.branches.push(ErasedBranch {
                    id: id.to_string(),
                    task,
                });
            }
            None => {
                self.errors.push(BuildError::TaskNotFound(id.to_string()));
            }
        }
        self
    }

    /// Join using a pre-registered join task by ID.
    ///
    /// If the task ID is not found, the error is recorded and reported at
    /// `build()` time.
    pub fn join_registered<JoinOutput>(
        mut self,
        id: &str,
    ) -> WorkflowBuilder<C, Input, JoinOutput, M, WorkflowContinuation, TaskRegistry>
    where
        Output: Send + 'static,
        JoinOutput: Send + 'static,
    {
        let join_task_fn = self.registry.get(id);
        if join_task_fn.is_none() {
            self.errors.push(BuildError::TaskNotFound(id.to_string()));
        }
        let join_timeout = self.registry.get_metadata(id).and_then(|m| m.timeout);

        let fork_id = WorkflowContinuation::derive_fork_id(
            &self
                .branches
                .iter()
                .map(|b| b.id.as_str())
                .collect::<Vec<_>>(),
        );

        let branch_continuations: Vec<Arc<WorkflowContinuation>> = self
            .branches
            .into_iter()
            .map(|b| {
                let meta = self.registry.get_metadata(&b.id);
                let timeout = meta.and_then(|m| m.timeout);
                let retry_policy = self
                    .registry
                    .get_metadata(&b.id)
                    .and_then(|m| m.retries.clone());
                let version = self
                    .registry
                    .get_metadata(&b.id)
                    .and_then(|m| m.version.clone());
                let priority = self
                    .registry
                    .get_metadata(&b.id)
                    .and_then(|m| m.priority.map(Priority::as_u8));
                let tags = self
                    .registry
                    .get_metadata(&b.id)
                    .map(|m| m.tags.clone())
                    .unwrap_or_default();
                Arc::new(WorkflowContinuation::Task {
                    id: b.id,
                    func: Some(b.task),
                    timeout,
                    retry_policy,
                    version,
                    priority,
                    tags,
                    next: None,
                })
            })
            .collect();

        let join_retry_policy = self
            .registry
            .get_metadata(id)
            .and_then(|m| m.retries.clone());
        let join_version = self
            .registry
            .get_metadata(id)
            .and_then(|m| m.version.clone());
        let join_priority = self
            .registry
            .get_metadata(id)
            .and_then(|m| m.priority.map(Priority::as_u8));
        let join_tags = self
            .registry
            .get_metadata(id)
            .map(|m| m.tags.clone())
            .unwrap_or_default();
        let join_task = WorkflowContinuation::Task {
            id: id.to_string(),
            func: join_task_fn,
            timeout: join_timeout,
            retry_policy: join_retry_policy,
            version: join_version,
            priority: join_priority,
            tags: join_tags,
            next: None,
        };

        let fork_continuation = WorkflowContinuation::Fork {
            id: fork_id,
            branches: branch_continuations.into_boxed_slice(),
            join: Some(Box::new(join_task)),
        };

        let continuation = self.continuation.append(fork_continuation);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(id.to_string()),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}
