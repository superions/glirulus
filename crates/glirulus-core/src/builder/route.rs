//! Conditional route and sub-flow construction.

use super::{
    ContinuationState, RegistryBehavior, RouteBuilder, RouteRegisteredBuilder, SubBuilder,
    WorkflowBuilder,
};
use crate::branch_key::BranchKey;
use crate::codec::{Codec, sealed};
use crate::error::{BuildError, BuildErrors};
use crate::priority::Priority;
use crate::registry::TaskRegistry;
use crate::task::{BranchEnvelope, to_core_task_arc};
use crate::workflow::WorkflowContinuation;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

impl<C, Input, Output, R> SubBuilder<C, Input, Output, R>
where
    R: RegistryBehavior,
{
    /// Add a sequential task to this sub-flow.
    pub fn then<F, Fut, NewOutput>(
        mut self,
        id: &str,
        func: F,
    ) -> SubBuilder<C, Input, NewOutput, R>
    where
        F: Fn(Output) -> Fut + Send + Sync + 'static,
        Output: Send + 'static,
        NewOutput: Send + 'static,
        Fut: std::future::Future<Output = Result<NewOutput, crate::error::BoxError>>
            + Send
            + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<NewOutput> + 'static,
    {
        let codec = Arc::clone(&self.codec);
        let func = Arc::new(func);

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

        let continuation = match self.continuation {
            None => Some(new_task),
            Some(mut cont) => {
                cont.append_to_chain(new_task);
                Some(cont)
            }
        };

        SubBuilder {
            codec: self.codec,
            continuation,
            registry: self.registry,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

/// Delay and signal methods for `SubBuilder`.
impl<C, Input, Output, R> SubBuilder<C, Input, Output, R> {
    /// Add a durable delay to this sub-flow.
    #[must_use]
    pub fn delay(self, id: &str, duration: std::time::Duration) -> Self {
        let new_node = WorkflowContinuation::Delay {
            id: id.to_string(),
            duration,
            next: None,
        };
        let continuation = match self.continuation {
            None => Some(new_node),
            Some(mut cont) => {
                cont.append_to_chain(new_node);
                Some(cont)
            }
        };
        SubBuilder {
            codec: self.codec,
            continuation,
            registry: self.registry,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }

    /// Wait for a named external signal in this sub-flow.
    #[must_use]
    pub fn wait_for_signal(
        self,
        id: &str,
        signal_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> Self {
        let new_node = WorkflowContinuation::AwaitSignal {
            id: id.to_string(),
            signal_name: signal_name.to_string(),
            timeout,
            next: None,
        };
        let continuation = match self.continuation {
            None => Some(new_node),
            Some(mut cont) => {
                cont.append_to_chain(new_node);
                Some(cont)
            }
        };
        SubBuilder {
            codec: self.codec,
            continuation,
            registry: self.registry,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }

    /// Consume the sub-builder and return the built continuation, registry, and any errors.
    fn build(mut self) -> (Option<WorkflowContinuation>, BuildErrors, R) {
        if self.continuation.is_none() {
            self.errors.push(BuildError::EmptyBranch);
        }
        (self.continuation, self.errors, self.registry)
    }
}

/// `route` method — available for all registry/continuation combinations.
impl<C, Input, Output, M, Cont, R> WorkflowBuilder<C, Input, Output, M, Cont, R>
where
    R: RegistryBehavior,
    Cont: ContinuationState,
{
    /// Add a conditional branching node to the workflow.
    ///
    /// The `key_fn` extracts a typed routing key from the previous step's output.
    /// The key type `K` must implement [`BranchKey`], constraining the set of
    /// valid branch names at compile time. The builder's `.done()` method
    /// verifies exhaustiveness: every variant in `K` must have a corresponding
    /// `.branch()` call (or a `.default_branch()` must be provided).
    ///
    /// # Type Parameters
    ///
    /// - `BranchOut`: The common return type of all branches.
    /// - `K`: A [`BranchKey`] enum whose variants map to branch names.
    pub fn route<BranchOut, K, KeyFn, Fut>(
        mut self,
        key_fn: KeyFn,
    ) -> RouteBuilder<C, Input, Output, BranchOut, K, M, Cont, R>
    where
        K: BranchKey,
        KeyFn: Fn(Output) -> Fut + Send + Sync + 'static,
        Output: Send + 'static,
        BranchOut: Send + 'static,
        Fut: std::future::Future<Output = Result<K, crate::error::BoxError>> + Send + 'static,
        C: Codec + sealed::DecodeValue<Output> + sealed::EncodeValue<String> + 'static,
    {
        let codec = Arc::clone(&self.context.codec);
        self.branch_counter += 1;
        let branch_id = format!("branch_{}", self.branch_counter);
        let key_fn_id = crate::workflow::key_fn_id(&branch_id);

        // Wrap the typed key_fn to produce String for the core layer
        let key_fn_arc = Arc::new(key_fn);
        let key_fn_string = {
            let key_fn_arc = Arc::clone(&key_fn_arc);
            Arc::new(move |input: Output| {
                let key_fn = Arc::clone(&key_fn_arc);
                async move {
                    let k = key_fn(input).await?;
                    Ok(k.as_key().to_string())
                }
            })
        };

        // Register the string-producing wrapper in the registry (if enabled)
        self.registry.maybe_register::<Output, String, _, _, _>(
            &key_fn_id,
            codec.clone(),
            &key_fn_string,
        );

        let key_fn_task = to_core_task_arc(&key_fn_id, key_fn_string, codec);

        RouteBuilder {
            context: self.context,
            parent_continuation: self.continuation,
            registry: self.registry,
            branch_id,
            key_fn: key_fn_task,
            named_branches: HashMap::new(),
            default_branch: None,
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

/// `route_registered` — only available with `TaskRegistry`.
impl<C, Input, Output, M, Cont> WorkflowBuilder<C, Input, Output, M, Cont, TaskRegistry>
where
    Cont: ContinuationState,
{
    /// Add a conditional branching node using pre-registered tasks.
    ///
    /// The key function must already be registered in the registry under
    /// [`key_fn_id(id)`](crate::workflow::key_fn_id) (the convention used by
    /// both the programmatic `route` API and the `workflow!` macro).
    ///
    /// If the key function is not found, the error is recorded and reported
    /// at `build()` time.
    pub fn route_registered<BranchOut>(
        mut self,
        id: &str,
    ) -> RouteRegisteredBuilder<C, Input, Output, BranchOut, M, Cont> {
        let key_fn_id_str = crate::workflow::key_fn_id(id);
        let key_fn = self.registry.get(&key_fn_id_str);
        if key_fn.is_none() {
            self.errors.push(BuildError::TaskNotFound(key_fn_id_str));
        }

        RouteRegisteredBuilder {
            context: self.context,
            parent_continuation: self.continuation,
            registry: self.registry,
            branch_id: id.to_string(),
            key_fn,
            named_branches: HashMap::new(),
            default_branch: None,
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

impl<C, Input, Output, BranchOut, M, Cont>
    RouteRegisteredBuilder<C, Input, Output, BranchOut, M, Cont>
where
    Cont: ContinuationState,
{
    /// Build a chain of `WorkflowContinuation::Task` nodes from registered task IDs.
    ///
    /// Returns `(Ok(chain), errors)` on success or `(Err(()), errors)` if any task
    /// was not found (errors are pushed to the returned vec).
    fn build_chain_from_registry(
        registry: &TaskRegistry,
        task_ids: &[&str],
        errors: &mut BuildErrors,
    ) -> Option<Box<WorkflowContinuation>> {
        let mut current: Option<WorkflowContinuation> = None;
        let mut ok = true;
        for id in task_ids.iter().rev() {
            let func = registry.get(id);
            if func.is_none() {
                errors.push(BuildError::TaskNotFound((*id).to_string()));
                ok = false;
            }
            let meta = registry.get_metadata(id);
            let timeout = meta.and_then(|m| m.timeout);
            let retry_policy = registry.get_metadata(id).and_then(|m| m.retries.clone());
            let version = registry.get_metadata(id).and_then(|m| m.version.clone());
            let priority = registry
                .get_metadata(id)
                .and_then(|m| m.priority.map(Priority::as_u8));
            let tags = registry
                .get_metadata(id)
                .map(|m| m.tags.clone())
                .unwrap_or_default();
            current = Some(WorkflowContinuation::Task {
                id: (*id).to_string(),
                func,
                timeout,
                retry_policy,
                version,
                priority,
                tags,
                next: current.map(Box::new),
            });
        }
        if !ok || current.is_none() {
            if current.is_none() {
                errors.push(BuildError::EmptyFork);
            }
            return None;
        }
        current.map(Box::new)
    }

    /// Add a named branch containing a chain of pre-registered tasks.
    ///
    /// If any task ID is not found, the error is recorded and reported at
    /// `build()` time.
    #[must_use]
    pub fn branch_registered(mut self, key: &str, task_ids: &[&str]) -> Self {
        if let Some(chain) =
            Self::build_chain_from_registry(&self.registry, task_ids, &mut self.errors)
        {
            self.named_branches.insert(key.to_string(), chain);
        }
        self
    }

    /// Set the default branch (used when no key matches) from pre-registered tasks.
    ///
    /// If any task ID is not found, the error is recorded and reported at
    /// `build()` time.
    #[must_use]
    pub fn default_registered(mut self, task_ids: &[&str]) -> Self {
        if let Some(chain) =
            Self::build_chain_from_registry(&self.registry, task_ids, &mut self.errors)
        {
            self.default_branch = Some(chain);
        }
        self
    }

    /// Close the branching node and return to the parent builder.
    pub fn done(
        self,
    ) -> WorkflowBuilder<C, Input, BranchEnvelope<BranchOut>, M, WorkflowContinuation, TaskRegistry>
    {
        let branch_node = WorkflowContinuation::Branch {
            id: self.branch_id.clone(),
            key_fn: self.key_fn,
            branches: self.named_branches,
            default: self.default_branch,
            next: None,
        };

        let continuation = self.parent_continuation.append(branch_node);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(self.branch_id),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}

impl<C, Input, Output, BranchOut, K, M, Cont, R>
    RouteBuilder<C, Input, Output, BranchOut, K, M, Cont, R>
where
    K: BranchKey,
    R: RegistryBehavior,
    Cont: ContinuationState,
{
    /// Add a named branch to the conditional branching node.
    ///
    /// The `key` is a variant of the `BranchKey` enum, ensuring only declared
    /// keys can be used. The closure receives a [`SubBuilder`] and must chain
    /// at least one step on it. The final output type must match `BranchOut`.
    ///
    /// If the closure doesn't add any steps, the error is recorded and
    /// reported at `build()` time.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn branch<F>(mut self, key: K, build_fn: F) -> Self
    where
        F: FnOnce(SubBuilder<C, Output, Output, R>) -> SubBuilder<C, Output, BranchOut, R>,
    {
        let sub = SubBuilder {
            codec: Arc::clone(&self.context.codec),
            continuation: None,
            registry: self.registry,
            errors: BuildErrors::new(),
            _phantom: PhantomData,
        };
        let built = build_fn(sub);
        let (continuation, errors, registry) = built.build();
        self.registry = registry;
        self.errors.extend(errors);
        if let Some(cont) = continuation {
            self.named_branches
                .insert(key.as_key().to_string(), Box::new(cont));
        }
        self
    }

    /// Set the default branch (used when no key matches).
    ///
    /// The closure receives a [`SubBuilder`] and must chain at least one step on it.
    ///
    /// If the closure doesn't add any steps, the error is recorded and
    /// reported at `build()` time.
    #[must_use]
    pub fn default_branch<F>(mut self, build_fn: F) -> Self
    where
        F: FnOnce(SubBuilder<C, Output, Output, R>) -> SubBuilder<C, Output, BranchOut, R>,
    {
        let sub = SubBuilder {
            codec: Arc::clone(&self.context.codec),
            continuation: None,
            registry: self.registry,
            errors: BuildErrors::new(),
            _phantom: PhantomData,
        };
        let built = build_fn(sub);
        let (continuation, errors, registry) = built.build();
        self.registry = registry;
        self.errors.extend(errors);
        if let Some(cont) = continuation {
            self.default_branch = Some(Box::new(cont));
        }
        self
    }

    /// Close the branching node and return to the parent builder.
    ///
    /// Performs exhaustiveness checks against `K::all_keys()`:
    /// - **Missing branches**: keys in the enum with no `.branch()` call and no
    ///   `.default_branch()` → error recorded.
    /// - **Orphan branches**: keys passed to `.branch()` that are not in the enum
    ///   → error recorded.
    ///
    /// All errors are reported at `build()` time.
    #[allow(clippy::type_complexity)]
    pub fn done(
        mut self,
    ) -> WorkflowBuilder<C, Input, BranchEnvelope<BranchOut>, M, WorkflowContinuation, R> {
        let all_keys: std::collections::HashSet<&str> = K::all_keys().iter().copied().collect();
        let declared_keys: std::collections::HashSet<&str> =
            self.named_branches.keys().map(String::as_str).collect();

        // Check for orphan branches (keys not in the enum)
        let orphans: Vec<String> = declared_keys
            .difference(&all_keys)
            .map(|k| (*k).to_string())
            .collect();
        if !orphans.is_empty() {
            self.errors.push(BuildError::OrphanBranches {
                branch_id: self.branch_id.clone(),
                orphan_keys: orphans,
            });
        }

        // Check for missing branches (keys in the enum with no branch and no default)
        if self.default_branch.is_none() {
            let missing: Vec<String> = all_keys
                .difference(&declared_keys)
                .map(|k| (*k).to_string())
                .collect();
            if !missing.is_empty() {
                self.errors.push(BuildError::MissingBranches {
                    branch_id: self.branch_id.clone(),
                    missing_keys: missing,
                });
            }
        }

        let branch_node = WorkflowContinuation::Branch {
            id: self.branch_id.clone(),
            key_fn: Some(self.key_fn),
            branches: self.named_branches,
            default: self.default_branch,
            next: None,
        };

        let continuation = self.parent_continuation.append(branch_node);

        WorkflowBuilder {
            continuation,
            context: self.context,
            registry: self.registry,
            last_task_id: Some(self.branch_id),
            branch_counter: self.branch_counter,
            loop_counter: self.loop_counter,
            child_counter: self.child_counter,
            errors: self.errors,
            _phantom: PhantomData,
        }
    }
}
