//! Sequential task, loop and registered-task construction.

use super::{ContinuationState, RegistryBehavior, WorkflowBuilder};
use crate::codec::{Codec, sealed};
use crate::error::BuildError;
use crate::loop_result::LoopResult;
use crate::priority::Priority;
use crate::registry::TaskRegistry;
use crate::task::{
    RegisterableTask, to_core_loop_task_arc, to_core_task_arc, wrap_core_loop_task, wrap_core_task,
};
use crate::workflow::{MaxIterationsPolicy, WorkflowContinuation};
use std::marker::PhantomData;
use std::sync::Arc;

/// Methods for adding tasks - unified implementation using `RegistryBehavior` and `ContinuationState`.
impl<C, Input, Output, M, Cont, R> WorkflowBuilder<C, Input, Output, M, Cont, R>
where
    R: RegistryBehavior,
    Cont: ContinuationState,
{
    /// Add a sequential task to the workflow.
    pub fn then<F, Fut, NewOutput>(
        mut self,
        id: &str,
        func: F,
    ) -> WorkflowBuilder<C, Input, NewOutput, M, WorkflowContinuation, R>
    where
        F: Fn(Output) -> Fut + Send + Sync + 'static,
        Output: Send + 'static,
        NewOutput: Send + 'static,
        Fut: std::future::Future<Output = Result<NewOutput, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<NewOutput> + 'static,
    {
        let codec = Arc::clone(&self.context.codec);
        let func = Arc::new(func);

        // Register if registry is enabled (no-op for NoRegistry)
        self.registry
            .maybe_register::<Output, NewOutput, _, _, _>(id, codec.clone(), &func);

        let task = to_core_task_arc(id, func, codec);

        let new_task = WorkflowContinuation::Task {
            id: id.to_string(),
            func: Some(task),
            timeout: None,
            retry_policy: None,
            version: None,
            priority: None,
            tags: Vec::new(),
            next: None,
        };

        let continuation = self.continuation.append(new_task);

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

/// Loop method — available for all registry/continuation combinations.
impl<C, Input, Output, M, Cont, R> WorkflowBuilder<C, Input, Output, M, Cont, R>
where
    R: RegistryBehavior,
    Cont: ContinuationState,
{
    /// Add a loop task to the workflow.
    ///
    /// The task function receives the current value and must return
    /// `LoopResult<NewOutput>`:
    /// - `LoopResult::Again(val)` feeds `val` back as the next iteration's input.
    /// - `LoopResult::Done(val)` exits the loop; `val` becomes the loop's output.
    ///
    /// The `max_iterations` parameter sets a safety limit. When reached, the
    /// default `MaxIterationsPolicy::Fail` causes the workflow to fail.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use glirulus_core::LoopResult;
    ///
    /// workflow
    ///     .loop_task("refine", |draft: String| async move {
    ///         if is_good_enough(&draft) {
    ///             Ok(LoopResult::Done(draft))
    ///         } else {
    ///             Ok(LoopResult::Again(improve(draft)))
    ///         }
    ///     }, 10)
    /// ```
    ///
    /// If `max_iterations` is 0, an error is recorded and reported at `build()` time.
    pub fn loop_task<F, Fut, NewOutput>(
        self,
        id: &str,
        func: F,
        max_iterations: u32,
    ) -> WorkflowBuilder<C, Input, NewOutput, M, WorkflowContinuation, R>
    where
        F: Fn(Output) -> Fut + Send + Sync + 'static,
        Output: Send + 'static,
        NewOutput: Send + 'static,
        Fut: std::future::Future<Output = Result<LoopResult<NewOutput>, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<NewOutput> + 'static,
    {
        self.loop_task_with_policy(id, func, max_iterations, MaxIterationsPolicy::Fail)
    }

    /// Add a loop task with a custom `MaxIterationsPolicy`.
    ///
    /// Same as [`loop_task`](Self::loop_task) but allows specifying what happens
    /// when `max_iterations` is reached.
    ///
    pub fn loop_task_with_policy<F, Fut, NewOutput>(
        mut self,
        id: &str,
        func: F,
        max_iterations: u32,
        on_max: MaxIterationsPolicy,
    ) -> WorkflowBuilder<C, Input, NewOutput, M, WorkflowContinuation, R>
    where
        F: Fn(Output) -> Fut + Send + Sync + 'static,
        Output: Send + 'static,
        NewOutput: Send + 'static,
        Fut: std::future::Future<Output = Result<LoopResult<NewOutput>, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<NewOutput> + 'static,
    {
        if max_iterations == 0 {
            self.errors
                .push(BuildError::InvalidMaxIterations(id.to_string()));
        }
        let codec = Arc::clone(&self.context.codec);
        let func = Arc::new(func);

        self.registry
            .maybe_register_loop::<Output, NewOutput, _, _, _>(id, codec.clone(), &func);

        let task = to_core_loop_task_arc(id, func, codec);
        let loop_id = crate::workflow::loop_node_id(self.loop_counter);
        self.loop_counter += 1;

        let body = WorkflowContinuation::Task {
            id: id.to_string(),
            func: Some(task),
            timeout: None,
            retry_policy: None,
            version: None,
            priority: None,
            tags: Vec::new(),
            next: None,
        };

        let loop_node = WorkflowContinuation::Loop {
            id: loop_id.clone(),
            body: Box::new(body),
            max_iterations,
            on_max,
            next: None,
        };

        let continuation = self.continuation.append(loop_node);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(loop_id),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

/// Type-safe builder methods for `RegisterableTask` structs (generated by `#[task]`).
///
/// These methods extract the task ID, output type, and metadata directly from
/// the type, eliminating stringly-typed wiring.
impl<C, Input, Output, M, Cont, R> WorkflowBuilder<C, Input, Output, M, Cont, R>
where
    R: RegistryBehavior,
    Cont: ContinuationState,
{
    /// Add a `#[task]` struct to the workflow (no injected deps — requires `Default`).
    ///
    /// The task ID, output type, timeout, and retry policy are all derived from
    /// `T`'s [`RegisterableTask`] implementation.
    ///
    /// For tasks that carry injected dependencies, use
    /// [`then_task_with`](Self::then_task_with) instead.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Given:  #[task] async fn add_ten(input: u32) -> Result<u32, BoxError> { … }
    ///
    /// WorkflowBuilder::new(ctx)
    ///     .with_registry()
    ///     .then_task::<AddTenTask>()
    ///     .then_task::<DoubleTask>()
    ///     .build()?;
    /// ```
    pub fn then_task<T>(self) -> WorkflowBuilder<C, Input, T::Output, M, WorkflowContinuation, R>
    where
        T: RegisterableTask<Input = Output> + Default,
        Output: Send + 'static,
        T::Output: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<T::Output> + 'static,
    {
        self.then_task_with(T::default())
    }

    /// Add a `#[task]` struct instance to the workflow (for tasks with injected deps).
    ///
    /// The task ID, output type, timeout, and retry policy are derived from
    /// `T`'s [`RegisterableTask`] implementation. The caller provides the
    /// constructed instance.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Given:  #[task] async fn fetch(url: String, #[inject] client: HttpClient) -> …
    ///
    /// WorkflowBuilder::new(ctx)
    ///     .with_registry()
    ///     .then_task_with(FetchTask::new(client.clone()))
    ///     .build()?;
    /// ```
    pub fn then_task_with<T>(
        mut self,
        task: T,
    ) -> WorkflowBuilder<C, Input, T::Output, M, WorkflowContinuation, R>
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
            metadata.clone(),
        );

        let untyped = wrap_core_task(id, task, codec);
        let timeout = metadata.timeout;
        let retry_policy = metadata.retries;
        let tags = metadata.tags;
        let version = metadata.version;
        let priority = metadata.priority.map(Priority::as_u8);

        let new_task = WorkflowContinuation::Task {
            id: id.to_string(),
            func: Some(untyped),
            timeout,
            retry_policy,
            version,
            priority,
            tags,
            next: None,
        };

        let continuation = self.continuation.append(new_task);

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

    /// Add a `#[task]` loop struct (no injected deps — requires `Default`).
    ///
    /// The task's output must be `LoopResult<NewOutput>`. The loop node ID
    /// is auto-derived. See [`loop_task`](Self::loop_task) for semantics.
    pub fn loop_task_struct<T, NewOutput>(
        self,
        max_iterations: u32,
    ) -> WorkflowBuilder<C, Input, NewOutput, M, WorkflowContinuation, R>
    where
        T: RegisterableTask<Input = Output, Output = LoopResult<NewOutput>> + Default,
        Output: Send + 'static,
        NewOutput: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<NewOutput> + 'static,
    {
        self.loop_task_struct_with_policy(T::default(), max_iterations, MaxIterationsPolicy::Fail)
    }

    /// Add a `#[task]` loop struct instance (for tasks with injected deps).
    ///
    /// Uses the default `MaxIterationsPolicy::Fail`. For a custom policy,
    /// use [`loop_task_struct_with_policy`](Self::loop_task_struct_with_policy).
    pub fn loop_task_struct_with<T, NewOutput>(
        self,
        task: T,
        max_iterations: u32,
    ) -> WorkflowBuilder<C, Input, NewOutput, M, WorkflowContinuation, R>
    where
        T: RegisterableTask<Input = Output, Output = LoopResult<NewOutput>>,
        Output: Send + 'static,
        NewOutput: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<NewOutput> + 'static,
    {
        self.loop_task_struct_with_policy(task, max_iterations, MaxIterationsPolicy::Fail)
    }

    /// Add a `#[task]` loop struct instance with a custom `MaxIterationsPolicy`.
    ///
    /// The task's output must be `LoopResult<NewOutput>`. The loop node ID
    /// is auto-derived. See [`loop_task`](Self::loop_task) for semantics.
    pub fn loop_task_struct_with_policy<T, NewOutput>(
        mut self,
        task: T,
        max_iterations: u32,
        on_max: MaxIterationsPolicy,
    ) -> WorkflowBuilder<C, Input, NewOutput, M, WorkflowContinuation, R>
    where
        T: RegisterableTask<Input = Output, Output = LoopResult<NewOutput>>,
        Output: Send + 'static,
        NewOutput: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<NewOutput> + 'static,
    {
        let id = T::task_id();
        if max_iterations == 0 {
            self.errors
                .push(BuildError::InvalidMaxIterations(id.to_string()));
        }

        let metadata = T::metadata();
        let codec = Arc::clone(&self.context.codec);
        let task = Arc::new(task);

        self.registry
            .maybe_register_core_loop_task::<T, NewOutput, C>(
                id,
                codec.clone(),
                Arc::clone(&task),
                metadata.clone(),
            );

        let untyped = wrap_core_loop_task(id, task, codec);
        let timeout = metadata.timeout;
        let retry_policy = metadata.retries;
        let tags = metadata.tags;
        let version = metadata.version;
        let priority = metadata.priority.map(Priority::as_u8);

        let loop_id = crate::workflow::loop_node_id(self.loop_counter);
        self.loop_counter += 1;

        let body = WorkflowContinuation::Task {
            id: id.to_string(),
            func: Some(untyped),
            timeout,
            retry_policy,
            version,
            priority,
            tags,
            next: None,
        };

        let loop_node = WorkflowContinuation::Loop {
            id: loop_id.clone(),
            body: Box::new(body),
            max_iterations,
            on_max,
            next: None,
        };

        let continuation = self.continuation.append(loop_node);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(loop_id),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

/// Methods for referencing pre-registered tasks (only available with `TaskRegistry`).
impl<C, Input, Output, M, Cont> WorkflowBuilder<C, Input, Output, M, Cont, TaskRegistry>
where
    Cont: ContinuationState,
{
    /// Reference a pre-registered task by ID.
    ///
    /// The task must have been registered in the registry before calling this method.
    /// Type safety is maintained through the `NewOutput` type parameter - ensure it
    /// matches the registered task's output type.
    ///
    /// If the task ID is not found in the registry, the error is recorded and
    /// reported at `build()` time.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use glirulus_core::prelude::*;
    /// # use glirulus_core::codec::{Encoder, Decoder, sealed};
    /// # use bytes::Bytes;
    /// # use std::sync::Arc;
    /// # struct MyCodec;
    /// # impl Encoder for MyCodec {}
    /// # impl Decoder for MyCodec {}
    /// # impl<T> sealed::EncodeValue<T> for MyCodec {
    /// #     fn encode_value(&self, _: &T) -> Result<Bytes, BoxError> { Ok(Bytes::new()) }
    /// # }
    /// # impl<T> sealed::DecodeValue<T> for MyCodec {
    /// #     fn decode_value(&self, _: Bytes) -> Result<T, BoxError> { Err("dummy".into()) }
    /// # }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let codec = Arc::new(MyCodec);
    /// # let ctx = WorkflowContext::new("my-workflow", codec.clone(), Arc::new(()));
    /// use glirulus_core::prelude::*;
    ///
    /// let mut registry = TaskRegistry::new();
    /// registry.register_fn("double", codec.clone(), |i: u32| async move { Ok(i * 2) });
    ///
    /// let workflow: SerializableWorkflow<_, u32> = WorkflowBuilder::new(ctx)
    ///     .with_existing_registry(registry)
    ///     .then_registered::<u32>("double")
    ///     .with_metadata(TaskMetadata {
    ///         display_name: Some("Double".into()),
    ///         ..Default::default()
    ///     })
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn then_registered<NewOutput>(
        mut self,
        id: &str,
    ) -> WorkflowBuilder<C, Input, NewOutput, M, WorkflowContinuation, TaskRegistry>
    where
        Output: Send + 'static,
        NewOutput: Send + 'static,
    {
        let func = self.registry.get(id);
        if func.is_none() {
            self.errors.push(BuildError::TaskNotFound(id.to_string()));
        }
        let meta = self.registry.get_metadata(id);
        let timeout = meta.and_then(|m| m.timeout);
        let retry_policy = self
            .registry
            .get_metadata(id)
            .and_then(|m| m.retries.clone());
        let version = self
            .registry
            .get_metadata(id)
            .and_then(|m| m.version.clone());
        let priority = self
            .registry
            .get_metadata(id)
            .and_then(|m| m.priority.map(Priority::as_u8));
        let tags = self
            .registry
            .get_metadata(id)
            .map(|m| m.tags.clone())
            .unwrap_or_default();

        let new_task = WorkflowContinuation::Task {
            id: id.to_string(),
            func,
            timeout,
            retry_policy,
            version,
            priority,
            tags,
            next: None,
        };

        let continuation = self.continuation.append(new_task);

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

    /// Reference a pre-registered loop body task by ID.
    ///
    /// The body task must already be registered in the registry under
    /// `body_task_id`. The loop node ID is derived as `"loop_{body_task_id}"`.
    ///
    /// If the body task is not found, or `max_iterations` is 0, the error is
    /// recorded and reported at `build()` time.
    pub fn loop_task_registered<NewOutput>(
        mut self,
        body_task_id: &str,
        max_iterations: u32,
        on_max: MaxIterationsPolicy,
    ) -> WorkflowBuilder<C, Input, NewOutput, M, WorkflowContinuation, TaskRegistry>
    where
        Output: Send + 'static,
        NewOutput: Send + 'static,
    {
        if max_iterations == 0 {
            self.errors
                .push(BuildError::InvalidMaxIterations(body_task_id.to_string()));
        }
        let func = self.registry.get(body_task_id);
        if func.is_none() {
            self.errors
                .push(BuildError::TaskNotFound(body_task_id.to_string()));
        }
        let meta = self.registry.get_metadata(body_task_id);
        let timeout = meta.and_then(|m| m.timeout);
        let retry_policy = self
            .registry
            .get_metadata(body_task_id)
            .and_then(|m| m.retries.clone());
        let version = self
            .registry
            .get_metadata(body_task_id)
            .and_then(|m| m.version.clone());
        let priority = self
            .registry
            .get_metadata(body_task_id)
            .and_then(|m| m.priority.map(Priority::as_u8));
        let tags = self
            .registry
            .get_metadata(body_task_id)
            .map(|m| m.tags.clone())
            .unwrap_or_default();

        let body = WorkflowContinuation::Task {
            id: body_task_id.to_string(),
            func,
            timeout,
            retry_policy,
            version,
            priority,
            tags,
            next: None,
        };

        let loop_id = crate::workflow::loop_node_id(self.loop_counter);
        self.loop_counter += 1;
        let loop_node = WorkflowContinuation::Loop {
            id: loop_id.clone(),
            body: Box::new(body),
            max_iterations,
            on_max,
            next: None,
        };

        let continuation = self.continuation.append(loop_node);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(loop_id),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

/// Metadata attachment — only available after a task has been added.
impl<C, Input, Output, M> WorkflowBuilder<C, Input, Output, M, WorkflowContinuation, TaskRegistry> {
    /// Attach metadata to the most recently added task.
    ///
    /// This method allows chaining metadata after `then()`, `then_registered()`,
    /// or `join()` calls.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use glirulus_core::prelude::*;
    /// # use glirulus_core::codec::{Encoder, Decoder, sealed};
    /// # use bytes::Bytes;
    /// # use std::sync::Arc;
    /// # struct MyCodec;
    /// # impl Encoder for MyCodec {}
    /// # impl Decoder for MyCodec {}
    /// # impl<T> sealed::EncodeValue<T> for MyCodec {
    /// #     fn encode_value(&self, _: &T) -> Result<Bytes, BoxError> { Ok(Bytes::new()) }
    /// # }
    /// # impl<T> sealed::DecodeValue<T> for MyCodec {
    /// #     fn decode_value(&self, _: Bytes) -> Result<T, BoxError> { Err("dummy".into()) }
    /// # }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let codec = Arc::new(MyCodec);
    /// # let ctx = WorkflowContext::new("my-workflow", codec, Arc::new(()));
    /// use glirulus_core::prelude::*;
    /// use glirulus_core::task::RetryPolicy;
    /// use std::time::Duration;
    ///
    /// let workflow = WorkflowBuilder::new(ctx)
    ///     .with_registry()
    ///     .then("double", |i: u32| async move { Ok(i * 2) })
    ///     .with_metadata(TaskMetadata {
    ///         display_name: Some("Double".into()),
    ///         timeout: Some(Duration::from_secs(30)),
    ///         ..Default::default()
    ///     })
    ///     .then("add_ten", |i: u32| async move { Ok(i + 10) })
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_metadata(mut self, metadata: crate::task::TaskMetadata) -> Self {
        if let Some(ref id) = self.last_task_id {
            let timeout = metadata.timeout;
            let retry_policy = metadata.retries.clone();
            let version = metadata.version.clone();
            self.registry.set_metadata(id, metadata);
            // Also update the timeout, retry policy, and version on the continuation node
            // so they're available for direct execution (not just the serializable roundtrip path).
            let tid = crate::TaskId::from(id);
            self.continuation.set_task_timeout(&tid, timeout);
            self.continuation.set_task_retry_policy(&tid, retry_policy);
            self.continuation.set_task_version(&tid, version);
        }
        self
    }
}
