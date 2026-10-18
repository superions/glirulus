//! Fluent workflow builder API.
//!
//! [`WorkflowBuilder`] is the primary entry point for constructing workflows
//! programmatically. It supports sequential tasks (`.then`), parallel
//! fork/join (`.branches` + `.join`), durable delays, signal waits, and
//! conditional routing (`.route`).
//!
//! For the proc-macro DSL alternative, see the `workflow!` macro in `glirulus-macros`.

use crate::codec::Codec;
use crate::codec::sealed;
use crate::context::WorkflowContext;
use crate::error::{BuildError, BuildErrors};
use crate::loop_result::LoopResult;
use crate::registry::TaskRegistry;
use crate::task::{BranchOutputs, ErasedBranch};
use crate::workflow::{SerializableWorkflow, Workflow, WorkflowContinuation};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

// Keep nominal types here so implementation splits preserve their defining paths.
mod fork;
mod route;
mod tasks;

/// Marker type for empty continuation (no tasks yet).
pub struct NoContinuation;

/// Marker type for no registry (non-serializable workflow).
pub struct NoRegistry;

/// Trait for continuation state - allows unified handling of empty vs existing continuation.
pub trait ContinuationState {
    /// Append a new task to this continuation state, returning a `WorkflowContinuation`.
    fn append(self, new_task: WorkflowContinuation) -> WorkflowContinuation;
}

impl ContinuationState for NoContinuation {
    fn append(self, new_task: WorkflowContinuation) -> WorkflowContinuation {
        new_task
    }
}

impl ContinuationState for WorkflowContinuation {
    fn append(mut self, new_task: WorkflowContinuation) -> WorkflowContinuation {
        self.append_to_chain(new_task);
        self
    }
}

/// Trait for registry behavior - allows unified implementation of builder methods.
pub trait RegistryBehavior {
    /// Register a task (no-op for `NoRegistry`, actual registration for `TaskRegistry`).
    fn maybe_register<I, O, F, Fut, C>(&mut self, _id: &str, _codec: Arc<C>, _func: &Arc<F>)
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        I: Send + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<O, crate::error::BoxError>> + Send + 'static,
        C: Codec + sealed::DecodeValue<I> + sealed::EncodeValue<O> + 'static;

    /// Register a `CoreTask` struct (no-op for `NoRegistry`, actual registration for `TaskRegistry`).
    fn maybe_register_core_task<T, C>(
        &mut self,
        _id: &str,
        _codec: Arc<C>,
        _task: Arc<T>,
        _metadata: crate::task::TaskMetadata,
    ) where
        T: crate::task::CoreTask + 'static,
        T::Input: Send + 'static,
        T::Output: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<T::Input> + sealed::EncodeValue<T::Output> + 'static;

    /// Register a `CoreTask` struct whose output is `LoopResult<O>` (no-op for `NoRegistry`).
    fn maybe_register_core_loop_task<T, O, C>(
        &mut self,
        _id: &str,
        _codec: Arc<C>,
        _task: Arc<T>,
        _metadata: crate::task::TaskMetadata,
    ) where
        T: crate::task::CoreTask<Output = LoopResult<O>> + 'static,
        T::Input: Send + 'static,
        O: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<T::Input> + sealed::EncodeValue<O> + 'static;

    /// Register a loop body task with two-step encoding (no-op for `NoRegistry`).
    fn maybe_register_loop<I, O, F, Fut, C>(&mut self, _id: &str, _codec: Arc<C>, _func: &Arc<F>)
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        I: Send + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<LoopResult<O>, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<I> + sealed::EncodeValue<O> + 'static;

    /// Register a join task (no-op for `NoRegistry`, actual registration for `TaskRegistry`).
    fn maybe_register_join<O, F, Fut, C>(&mut self, _id: &str, _codec: Arc<C>, _func: &Arc<F>)
    where
        F: Fn(BranchOutputs<C>) -> Fut + Send + Sync + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<O, crate::error::BoxError>> + Send + 'static,
        C: Codec
            + sealed::EncodeValue<O>
            + sealed::DecodeValue<crate::branch_results::NamedBranchResults>
            + Send
            + Sync
            + 'static;
}

impl RegistryBehavior for NoRegistry {
    fn maybe_register<I, O, F, Fut, C>(&mut self, _id: &str, _codec: Arc<C>, _func: &Arc<F>)
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        I: Send + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<O, crate::error::BoxError>> + Send + 'static,
        C: Codec + sealed::DecodeValue<I> + sealed::EncodeValue<O> + 'static,
    {
        // No-op for non-serializable workflows
    }

    fn maybe_register_core_task<T, C>(
        &mut self,
        _id: &str,
        _codec: Arc<C>,
        _task: Arc<T>,
        _metadata: crate::task::TaskMetadata,
    ) where
        T: crate::task::CoreTask + 'static,
        T::Input: Send + 'static,
        T::Output: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<T::Input> + sealed::EncodeValue<T::Output> + 'static,
    {
        // No-op for non-serializable workflows
    }

    fn maybe_register_core_loop_task<T, O, C>(
        &mut self,
        _id: &str,
        _codec: Arc<C>,
        _task: Arc<T>,
        _metadata: crate::task::TaskMetadata,
    ) where
        T: crate::task::CoreTask<Output = LoopResult<O>> + 'static,
        T::Input: Send + 'static,
        O: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<T::Input> + sealed::EncodeValue<O> + 'static,
    {
        // No-op for non-serializable workflows
    }

    fn maybe_register_loop<I, O, F, Fut, C>(&mut self, _id: &str, _codec: Arc<C>, _func: &Arc<F>)
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        I: Send + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<LoopResult<O>, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<I> + sealed::EncodeValue<O> + 'static,
    {
        // No-op for non-serializable workflows
    }

    fn maybe_register_join<O, F, Fut, C>(&mut self, _id: &str, _codec: Arc<C>, _func: &Arc<F>)
    where
        F: Fn(BranchOutputs<C>) -> Fut + Send + Sync + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<O, crate::error::BoxError>> + Send + 'static,
        C: Codec
            + sealed::EncodeValue<O>
            + sealed::DecodeValue<crate::branch_results::NamedBranchResults>
            + Send
            + Sync
            + 'static,
    {
        // No-op for non-serializable workflows
    }
}

impl RegistryBehavior for TaskRegistry {
    fn maybe_register<I, O, F, Fut, C>(&mut self, id: &str, codec: Arc<C>, func: &Arc<F>)
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        I: Send + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<O, crate::error::BoxError>> + Send + 'static,
        C: Codec + sealed::DecodeValue<I> + sealed::EncodeValue<O> + 'static,
    {
        use crate::task::TaskMetadata;
        self.register_fn_arc(id, codec, Arc::clone(func), TaskMetadata::default());
    }

    fn maybe_register_core_task<T, C>(
        &mut self,
        id: &str,
        codec: Arc<C>,
        task: Arc<T>,
        metadata: crate::task::TaskMetadata,
    ) where
        T: crate::task::CoreTask + 'static,
        T::Input: Send + 'static,
        T::Output: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<T::Input> + sealed::EncodeValue<T::Output> + 'static,
    {
        self.register_task_arc(id, codec, task, metadata);
    }

    fn maybe_register_core_loop_task<T, O, C>(
        &mut self,
        id: &str,
        codec: Arc<C>,
        task: Arc<T>,
        metadata: crate::task::TaskMetadata,
    ) where
        T: crate::task::CoreTask<Output = LoopResult<O>> + 'static,
        T::Input: Send + 'static,
        O: Send + 'static,
        T::Future: Send + 'static,
        C: Codec + sealed::DecodeValue<T::Input> + sealed::EncodeValue<O> + 'static,
    {
        self.register_loop_task_arc(id, codec, task, metadata);
    }

    fn maybe_register_loop<I, O, F, Fut, C>(&mut self, id: &str, codec: Arc<C>, func: &Arc<F>)
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        I: Send + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<LoopResult<O>, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<I> + sealed::EncodeValue<O> + 'static,
    {
        use crate::task::TaskMetadata;
        self.register_loop_fn_arc(id, codec, Arc::clone(func), TaskMetadata::default());
    }

    fn maybe_register_join<O, F, Fut, C>(&mut self, id: &str, codec: Arc<C>, func: &Arc<F>)
    where
        F: Fn(BranchOutputs<C>) -> Fut + Send + Sync + 'static,
        O: Send + 'static,
        Fut: std::future::Future<Output = Result<O, crate::error::BoxError>> + Send + 'static,
        C: Codec
            + sealed::EncodeValue<O>
            + sealed::DecodeValue<crate::branch_results::NamedBranchResults>
            + Send
            + Sync
            + 'static,
    {
        use crate::task::TaskMetadata;
        self.register_arc_join(id, codec, Arc::clone(func), TaskMetadata::default());
    }
}

/// Fluent builder for assembling workflow pipelines.
///
/// Start with [`WorkflowBuilder::new`], chain steps with `.then()`,
/// `.delay()`, `.wait_for_signal()`, `.branches()`, `.fork()`, or
/// `.route()`, and finish with `.build()`.
///
/// Call `.with_registry()` before adding steps to get a
/// [`SerializableWorkflow`] (required for distributed execution).
pub struct WorkflowBuilder<C, Input, Output, M = (), Cont = NoContinuation, R = NoRegistry> {
    context: WorkflowContext<C, M>,
    continuation: Cont,
    registry: R,
    last_task_id: Option<String>,
    branch_counter: usize,
    loop_counter: usize,
    child_counter: usize,
    errors: BuildErrors,
    _phantom: PhantomData<(Input, Output)>,
}

#[allow(clippy::mismatching_type_param_order)] // Input used for both Input and Output initially
impl<C, Input, M> WorkflowBuilder<C, Input, Input, M, NoContinuation, NoRegistry> {
    /// Create a new workflow builder with a context object.
    ///
    /// The context contains the workflow ID, codec and metadata that will be available
    /// at any execution point via the workflow context.
    #[must_use]
    pub fn new(ctx: WorkflowContext<C, M>) -> Self
    where
        C: Codec,
        M: Send + Sync + 'static,
    {
        Self {
            context: ctx,
            continuation: NoContinuation,
            registry: NoRegistry,
            last_task_id: None,
            branch_counter: 0,
            loop_counter: 0,
            child_counter: 0,
            errors: BuildErrors::new(),
            _phantom: PhantomData,
        }
    }

    /// Enable registry tracking for serializable workflows with a new empty registry.
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
    /// # let metadata = Arc::new(());
    /// use glirulus_core::prelude::*;
    /// use std::time::Duration;
    ///
    /// let ctx = WorkflowContext::new("my-workflow", codec, metadata);
    /// let workflow = WorkflowBuilder::new(ctx)
    ///     .with_registry()  // Enable serialization
    ///     .then("step1", |i: u32| async move { Ok(i + 1) })
    ///     .with_metadata(TaskMetadata {
    ///         display_name: Some("Increment".into()),
    ///         timeout: Some(Duration::from_secs(30)),
    ///         ..Default::default()
    ///     })
    ///     .build()?;  // Returns SerializableWorkflow
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_registry(
        self,
    ) -> WorkflowBuilder<C, Input, Input, M, NoContinuation, TaskRegistry> {
        self.with_existing_registry(TaskRegistry::new())
    }

    /// Enable registry tracking with an existing registry.
    ///
    /// Use this to reference pre-registered tasks via `then_registered` or to
    /// compose workflows from task libraries.
    ///
    /// **Note**: Takes ownership of the registry. For deserialization/hydration,
    /// rebuild the same registry from code on the deserializing side.
    /// See [`TaskRegistry`] docs for the pattern.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use glirulus_core::prelude::*;
    /// # use glirulus_core::codec::{Encoder, Decoder, sealed};
    /// # use glirulus_core::workflow::SerializableContinuation;
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
    /// # let metadata = Arc::new(());
    /// // Shared function for building registry (called on both sides)
    /// fn build_registry(codec: Arc<MyCodec>) -> TaskRegistry {
    ///     let mut registry = TaskRegistry::new();
    ///     registry.register_fn("step1", codec.clone(), |i: u32| async move { Ok(i + 1) });
    ///     registry
    /// }
    ///
    /// // Build workflow
    /// let registry = build_registry(codec.clone());
    /// let ctx = WorkflowContext::new("my-workflow", codec.clone(), metadata);
    /// let workflow: SerializableWorkflow<_, u32> = WorkflowBuilder::new(ctx)
    ///     .with_existing_registry(registry)
    ///     .then_registered::<u32>("step1")
    ///     .build()?;
    ///
    /// // Deserialize (on another side): rebuild registry, then convert to runnable
    /// let registry = build_registry(codec.clone());
    /// let serializable = workflow.continuation().to_serializable();
    /// let runnable = serializable.to_runnable(&registry)?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_existing_registry(
        self,
        registry: TaskRegistry,
    ) -> WorkflowBuilder<C, Input, Input, M, NoContinuation, TaskRegistry> {
        WorkflowBuilder {
            context: self.context,
            continuation: NoContinuation,
            registry,
            last_task_id: None,
            branch_counter: 0,
            loop_counter: 0,
            child_counter: 0,
            errors: BuildErrors::new(),
            _phantom: PhantomData,
        }
    }
}

/// Delay method — available for all registry/continuation combinations.
impl<C, Input, Output, M, Cont, R> WorkflowBuilder<C, Input, Output, M, Cont, R>
where
    Cont: ContinuationState,
{
    /// Add a durable delay to the workflow.
    ///
    /// The delay is transparent to data flow — the input passes through unchanged.
    /// In non-durable runners the delay is a simple sleep. In durable runners
    /// the workflow parks at the delay, persists `wake_at`, and returns
    /// `WorkflowStatus::Waiting`. A later `resume()` call advances past the
    /// delay once the wall clock reaches `wake_at`.
    #[must_use]
    pub fn delay(
        self,
        id: &str,
        duration: std::time::Duration,
    ) -> WorkflowBuilder<C, Input, Output, M, WorkflowContinuation, R> {
        let new_node = WorkflowContinuation::Delay {
            id: id.to_string(),
            duration,
            next: None,
        };
        let continuation = self.continuation.append(new_node);
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

    /// Wait for a named external signal before continuing.
    ///
    /// The signal payload (if any) becomes the input to the next step.
    /// If a timeout is specified and expires before a signal arrives,
    /// `None` is passed as the payload.
    #[must_use]
    pub fn wait_for_signal(
        self,
        id: &str,
        signal_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> WorkflowBuilder<C, Input, Output, M, WorkflowContinuation, R> {
        let new_node = WorkflowContinuation::AwaitSignal {
            id: id.to_string(),
            signal_name: signal_name.to_string(),
            timeout,
            next: None,
        };
        let continuation = self.continuation.append(new_node);
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

    /// Inline a child workflow into the pipeline.
    ///
    /// The child's entire continuation tree is embedded as a
    /// `WorkflowContinuation::ChildWorkflow` node. The child's output becomes
    /// the input to the next step.
    ///
    /// For serializable workflows (built with `.with_registry()`), use
    /// [`then_serializable_flow`](WorkflowBuilder::then_serializable_flow)
    /// instead so that the child's task registry is merged into the parent's.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let child = WorkflowBuilder::new(child_ctx)
    ///     .then("double", |i: u32| async move { Ok(i * 2) })
    ///     .build()?;
    ///
    /// let parent = WorkflowBuilder::new(parent_ctx)
    ///     .then("add_one", |i: u32| async move { Ok(i + 1) })
    ///     .then_flow(child)
    ///     .build()?;
    /// ```
    #[must_use]
    pub fn then_flow<ChildOutput>(
        mut self,
        child: Workflow<C, ChildOutput, M>,
    ) -> WorkflowBuilder<C, Input, ChildOutput, M, WorkflowContinuation, R> {
        let child_id = format!("child_{}", self.child_counter);
        self.child_counter += 1;

        let child_cont = child.into_continuation();
        let new_node = WorkflowContinuation::ChildWorkflow {
            id: child_id.clone(),
            child: Arc::new(child_cont),
            next: None,
        };

        let continuation = self.continuation.append(new_node);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(child_id),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

/// Child workflow composition with registry merging (only available with `TaskRegistry`).
impl<C, Input, Output, M, Cont> WorkflowBuilder<C, Input, Output, M, Cont, TaskRegistry>
where
    Cont: ContinuationState,
{
    /// Inline a serializable child workflow, merging its task registry into the parent's.
    ///
    /// This is the serializable counterpart of [`then_flow`](WorkflowBuilder::then_flow).
    /// The child's task registry entries are merged into the parent's registry so
    /// that distributed execution can look up all task implementations.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let child = WorkflowBuilder::new(child_ctx)
    ///     .with_registry()
    ///     .then("double", |i: u32| async move { Ok(i * 2) })
    ///     .build()?;
    ///
    /// let parent = WorkflowBuilder::new(parent_ctx)
    ///     .with_registry()
    ///     .then("add_one", |i: u32| async move { Ok(i + 1) })
    ///     .then_serializable_flow(child)
    ///     .build()?;
    /// ```
    #[must_use]
    pub fn then_serializable_flow<ChildOutput>(
        mut self,
        child: SerializableWorkflow<C, ChildOutput, M>,
    ) -> WorkflowBuilder<C, Input, ChildOutput, M, WorkflowContinuation, TaskRegistry> {
        let child_id = format!("child_{}", self.child_counter);
        self.child_counter += 1;

        let (child_cont, child_registry) = child.into_parts();
        self.registry.merge(child_registry);

        let new_node = WorkflowContinuation::ChildWorkflow {
            id: child_id.clone(),
            child: Arc::new(child_cont),
            next: None,
        };

        let continuation = self.continuation.append(new_node);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(child_id),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

/// Build method for `WorkflowBuilder` without registry - returns Workflow.
impl<C, Input, Output, M> WorkflowBuilder<C, Input, Output, M, WorkflowContinuation, NoRegistry> {
    /// Build the workflow into an executable workflow.
    ///
    /// # Errors
    ///
    /// Returns accumulated build errors or duplicate task IDs.
    pub fn build(mut self) -> Result<Workflow<C, Input, M>, BuildErrors>
    where
        Input: Send + 'static,
        Output: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + sealed::DecodeValue<Input>
            + sealed::DecodeValue<Output>
            + sealed::EncodeValue<Input>
            + sealed::EncodeValue<Output>,
    {
        if let Some(dup) = self.continuation.find_duplicate_id() {
            self.errors.push(BuildError::DuplicateTaskId(dup));
        }
        if !self.errors.is_empty() {
            return Err(self.errors);
        }

        let definition_hash = self
            .continuation
            .to_serializable()
            .compute_definition_hash();
        let task_index = Arc::new(crate::task_index::TaskIndex::build(&self.continuation));

        Ok(Workflow {
            definition_hash,
            continuation: self.continuation,
            context: self.context,
            task_index,
            _phantom: PhantomData,
        })
    }
}

/// Build method for `WorkflowBuilder` with registry - returns `SerializableWorkflow`.
impl<C, Input, Output, M> WorkflowBuilder<C, Input, Output, M, WorkflowContinuation, TaskRegistry> {
    /// Build the workflow into a serializable workflow.
    ///
    /// # Errors
    ///
    /// Returns accumulated build errors or duplicate task IDs.
    pub fn build(mut self) -> Result<SerializableWorkflow<C, Input, M>, BuildErrors>
    where
        Input: Send + 'static,
        Output: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + sealed::DecodeValue<Input>
            + sealed::DecodeValue<Output>
            + sealed::EncodeValue<Input>
            + sealed::EncodeValue<Output>,
    {
        if let Some(dup) = self.continuation.find_duplicate_id() {
            self.errors.push(BuildError::DuplicateTaskId(dup));
        }
        if !self.errors.is_empty() {
            return Err(self.errors);
        }

        let definition_hash = self
            .continuation
            .to_serializable()
            .compute_definition_hash();
        let task_index = Arc::new(crate::task_index::TaskIndex::build(&self.continuation));

        let inner = Workflow {
            definition_hash,
            continuation: self.continuation,
            context: self.context,
            task_index,
            _phantom: PhantomData,
        };

        Ok(SerializableWorkflow {
            inner,
            registry: self.registry,
        })
    }
}

/// Collector for adding branches in a closure.
///
/// Used by [`WorkflowBuilder::branches`] to collect multiple branches.
pub struct BranchCollector<C, Input> {
    codec: Arc<C>,
    branches: Vec<ErasedBranch>,
    _phantom: PhantomData<Input>,
}

/// Builder for constructing fork branches fluently.
///
/// Created by calling `.fork()` on a `WorkflowBuilder`. Add branches with `.branch()`,
/// then complete with `.join()`.
pub struct ForkBuilder<C, Input, Output, M, Cont = NoContinuation, R = NoRegistry> {
    context: WorkflowContext<C, M>,
    continuation: Cont,
    branches: Vec<ErasedBranch>,
    registry: R,
    branch_counter: usize,
    loop_counter: usize,
    child_counter: usize,
    errors: BuildErrors,
    _phantom: PhantomData<(Input, Output)>,
}

// ============================================================================
// Conditional Branching (route)
// ============================================================================

/// A lightweight sub-builder for constructing branch continuations.
///
/// This is used inside `RouteBuilder::branch()` closures to build
/// the continuation chain for each named branch.
pub struct SubBuilder<C, Input, Output, R = NoRegistry> {
    codec: Arc<C>,
    continuation: Option<WorkflowContinuation>,
    registry: R,
    errors: BuildErrors,
    _phantom: PhantomData<(Input, Output)>,
}

/// Builder for constructing conditional branches fluently.
///
/// Created by calling `.route()` on a `WorkflowBuilder`. Add branches with
/// `.branch()`, optionally set a default with `.default_branch()`, then close
/// with `.done()` to return to the parent builder.
///
/// The `K` type parameter constrains the routing keys to a `BranchKey` enum,
/// providing compile-time safety: `.branch()` takes a `K` variant and `.done()`
/// checks exhaustiveness.
pub struct RouteBuilder<C, Input, Output, BranchOut, K, M, Cont, R = NoRegistry> {
    context: WorkflowContext<C, M>,
    parent_continuation: Cont,
    registry: R,
    branch_id: String,
    key_fn: crate::task::UntypedCoreTask,
    named_branches: HashMap<String, Box<WorkflowContinuation>>,
    default_branch: Option<Box<WorkflowContinuation>>,
    branch_counter: usize,
    loop_counter: usize,
    child_counter: usize,
    errors: BuildErrors,
    _phantom: PhantomData<(Input, Output, BranchOut, K)>,
}

/// Builder for constructing conditional branches using pre-registered tasks.
///
/// Created by calling `.route_registered()` on a `WorkflowBuilder`.
/// Add branches with `.branch_registered()`, optionally set a default with
/// `.default_registered()`, then close with `.done()`.
pub struct RouteRegisteredBuilder<C, Input, Output, BranchOut, M, Cont> {
    context: WorkflowContext<C, M>,
    parent_continuation: Cont,
    registry: TaskRegistry,
    branch_id: String,
    key_fn: Option<crate::task::UntypedCoreTask>,
    named_branches: HashMap<String, Box<WorkflowContinuation>>,
    default_branch: Option<Box<WorkflowContinuation>>,
    branch_counter: usize,
    loop_counter: usize,
    child_counter: usize,
    errors: BuildErrors,
    _phantom: PhantomData<(Input, Output, BranchOut)>,
}
