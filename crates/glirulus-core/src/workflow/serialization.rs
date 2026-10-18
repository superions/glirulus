//! Rehydrate continuation structure and preserve its canonical definition hash.

use super::{
    SerializableContinuation, SerializableWorkflow, SerializedWorkflowState, Workflow,
    WorkflowContinuation, key_fn_id,
};
use crate::context::WorkflowContext;
use crate::registry::TaskRegistry;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

impl SerializableContinuation {
    /// Convert this serializable continuation into a runnable `WorkflowContinuation`.
    ///
    /// Looks up each task ID in the registry to get the actual implementation.
    ///
    /// # Errors
    ///
    /// Returns `BuildError::TaskNotFound` if any task ID is not in the registry.
    pub fn to_runnable(
        &self,
        registry: &crate::registry::TaskRegistry,
    ) -> Result<WorkflowContinuation, crate::error::BuildError> {
        if let Some(dup) = self.find_duplicate_id() {
            return Err(crate::error::BuildError::DuplicateTaskId(dup));
        }

        self.to_runnable_unchecked(registry)
    }

    /// Convert without duplicate check (called after validation).
    #[allow(clippy::too_many_lines)]
    fn to_runnable_unchecked(
        &self,
        registry: &crate::registry::TaskRegistry,
    ) -> Result<WorkflowContinuation, crate::error::BuildError> {
        match self {
            SerializableContinuation::Task {
                id,
                timeout_ms,
                retry_policy,
                version,
                priority,
                tags,
                next,
            } => {
                let func = registry
                    .get(id)
                    .ok_or_else(|| crate::error::BuildError::TaskNotFound(id.clone()))?;
                let next = next
                    .as_ref()
                    .map(|n| n.to_runnable_unchecked(registry).map(Box::new))
                    .transpose()?;
                Ok(WorkflowContinuation::Task {
                    id: id.clone(),
                    func: Some(func),
                    timeout: timeout_ms.map(std::time::Duration::from_millis),
                    retry_policy: retry_policy.clone(),
                    version: version.clone(),
                    priority: *priority,
                    tags: tags.clone(),
                    next,
                })
            }
            SerializableContinuation::Fork { id, branches, join } => {
                let branches: Result<Vec<_>, _> = branches
                    .iter()
                    .map(|b| b.to_runnable_unchecked(registry).map(Arc::new))
                    .collect();
                let join = join
                    .as_ref()
                    .map(|j| j.to_runnable_unchecked(registry).map(Box::new))
                    .transpose()?;
                Ok(WorkflowContinuation::Fork {
                    id: id.clone(),
                    branches: branches?.into_boxed_slice(),
                    join,
                })
            }
            SerializableContinuation::Delay {
                id,
                duration_ms,
                next,
            } => {
                let next = next
                    .as_ref()
                    .map(|n| n.to_runnable_unchecked(registry).map(Box::new))
                    .transpose()?;
                Ok(WorkflowContinuation::Delay {
                    id: id.clone(),
                    duration: std::time::Duration::from_millis(*duration_ms),
                    next,
                })
            }
            SerializableContinuation::AwaitSignal {
                id,
                signal_name,
                timeout_ms,
                next,
            } => {
                let next = next
                    .as_ref()
                    .map(|n| n.to_runnable_unchecked(registry).map(Box::new))
                    .transpose()?;
                Ok(WorkflowContinuation::AwaitSignal {
                    id: id.clone(),
                    signal_name: signal_name.clone(),
                    timeout: timeout_ms.map(std::time::Duration::from_millis),
                    next,
                })
            }
            SerializableContinuation::Branch {
                id,
                branches,
                default,
                next,
            } => {
                let kf_id = key_fn_id(id);
                let key_fn = registry
                    .get(&kf_id)
                    .ok_or(crate::error::BuildError::TaskNotFound(kf_id))?;
                let branches: Result<HashMap<_, _>, _> = branches
                    .iter()
                    .map(|(k, v)| {
                        v.to_runnable_unchecked(registry)
                            .map(|c| (k.clone(), Box::new(c)))
                    })
                    .collect();
                let default = default
                    .as_ref()
                    .map(|d| d.to_runnable_unchecked(registry).map(Box::new))
                    .transpose()?;
                let next = next
                    .as_ref()
                    .map(|n| n.to_runnable_unchecked(registry).map(Box::new))
                    .transpose()?;
                Ok(WorkflowContinuation::Branch {
                    id: id.clone(),
                    key_fn: Some(key_fn),
                    branches: branches?,
                    default,
                    next,
                })
            }
            SerializableContinuation::Loop {
                id,
                body,
                max_iterations,
                on_max,
                next,
            } => {
                let body = body.to_runnable_unchecked(registry)?;
                let next = next
                    .as_ref()
                    .map(|n| n.to_runnable_unchecked(registry).map(Box::new))
                    .transpose()?;
                Ok(WorkflowContinuation::Loop {
                    id: id.clone(),
                    body: Box::new(body),
                    max_iterations: *max_iterations,
                    on_max: *on_max,
                    next,
                })
            }
            SerializableContinuation::ChildWorkflow { id, child, next } => {
                let child = child.to_runnable_unchecked(registry)?;
                let next = next
                    .as_ref()
                    .map(|n| n.to_runnable_unchecked(registry).map(Box::new))
                    .transpose()?;
                Ok(WorkflowContinuation::ChildWorkflow {
                    id: id.clone(),
                    child: Arc::new(child),
                    next,
                })
            }
        }
    }

    /// Get all task IDs referenced in this continuation.
    #[must_use]
    pub fn task_ids(&self) -> Vec<&str> {
        fn collect<'a>(cont: &'a SerializableContinuation, ids: &mut Vec<&'a str>) {
            match cont {
                SerializableContinuation::Task { id, next, .. }
                | SerializableContinuation::Delay { id, next, .. }
                | SerializableContinuation::AwaitSignal { id, next, .. } => {
                    ids.push(id.as_str());
                    if let Some(n) = next {
                        collect(n, ids);
                    }
                }
                SerializableContinuation::Fork { id, branches, join } => {
                    ids.push(id.as_str());
                    for b in branches {
                        collect(b, ids);
                    }
                    if let Some(j) = join {
                        collect(j, ids);
                    }
                }
                SerializableContinuation::Branch {
                    id,
                    branches,
                    default,
                    next,
                } => {
                    ids.push(id.as_str());
                    for b in branches.values() {
                        collect(b, ids);
                    }
                    if let Some(d) = default {
                        collect(d, ids);
                    }
                    if let Some(n) = next {
                        collect(n, ids);
                    }
                }
                SerializableContinuation::Loop { id, body, next, .. } => {
                    ids.push(id.as_str());
                    collect(body, ids);
                    if let Some(n) = next {
                        collect(n, ids);
                    }
                }
                SerializableContinuation::ChildWorkflow { id, child, next } => {
                    ids.push(id.as_str());
                    collect(child, ids);
                    if let Some(n) = next {
                        collect(n, ids);
                    }
                }
            }
        }
        let mut ids = vec![];
        collect(self, &mut ids);
        ids
    }

    /// Compute a SHA256 hash of this continuation's structure.
    ///
    /// This hash serves as a "version" identifier for the workflow definition.
    /// It can be used to detect when a serialized workflow state was created
    /// with a different workflow definition than the current one.
    ///
    /// The hash is computed from the canonical structure of task IDs and their
    /// arrangement.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn compute_definition_hash(&self) -> crate::DefinitionHash {
        #[allow(clippy::too_many_lines)]
        fn hash_continuation(cont: &SerializableContinuation, hasher: &mut Sha256) {
            match cont {
                SerializableContinuation::Task {
                    id,
                    timeout_ms,
                    retry_policy,
                    version,
                    next,
                    ..
                } => {
                    hasher.update(b"T:"); // Tag for Task
                    hasher.update(id.as_bytes());
                    if let Some(ms) = timeout_ms {
                        hasher.update(b":t:");
                        hasher.update(ms.to_string().as_bytes());
                    }
                    if let Some(rp) = retry_policy {
                        hasher.update(b":r:");
                        hasher.update(rp.max_retries.to_string().as_bytes());
                        hasher.update(b":");
                        hasher.update(rp.initial_delay.as_millis().to_string().as_bytes());
                        hasher.update(b":");
                        hasher.update(rp.backoff_multiplier.to_string().as_bytes());
                    }
                    if let Some(v) = version {
                        hasher.update(b":v:");
                        hasher.update(v.as_bytes());
                    }
                    hasher.update(b";");
                    if let Some(n) = next {
                        hash_continuation(n, hasher);
                    }
                }
                SerializableContinuation::Fork { id, branches, join } => {
                    hasher.update(b"F:");
                    hasher.update(id.as_bytes());
                    hasher.update(b"[");
                    for branch in branches {
                        hash_continuation(branch, hasher);
                        hasher.update(b",");
                    }
                    hasher.update(b"]");
                    if let Some(j) = join {
                        hasher.update(b"J:");
                        hash_continuation(j, hasher);
                    }
                }
                SerializableContinuation::Delay {
                    id,
                    duration_ms,
                    next,
                } => {
                    hasher.update(b"D:");
                    hasher.update(id.as_bytes());
                    hasher.update(b":");
                    hasher.update(duration_ms.to_string().as_bytes());
                    hasher.update(b";");
                    if let Some(n) = next {
                        hash_continuation(n, hasher);
                    }
                }
                SerializableContinuation::AwaitSignal {
                    id,
                    signal_name,
                    timeout_ms,
                    next,
                } => {
                    hasher.update(b"S:");
                    hasher.update(id.as_bytes());
                    hasher.update(b":");
                    hasher.update(signal_name.as_bytes());
                    if let Some(ms) = timeout_ms {
                        hasher.update(b":t:");
                        hasher.update(ms.to_string().as_bytes());
                    }
                    hasher.update(b";");
                    if let Some(n) = next {
                        hash_continuation(n, hasher);
                    }
                }
                SerializableContinuation::Branch {
                    id,
                    branches,
                    default,
                    next,
                } => {
                    hasher.update(b"B:");
                    hasher.update(id.as_bytes());
                    hasher.update(b"{");
                    // Sort keys for deterministic hashing
                    let mut keys: Vec<&String> = branches.keys().collect();
                    keys.sort();
                    for key in keys {
                        hasher.update(key.as_bytes());
                        hasher.update(b"=>");
                        if let Some(branch) = branches.get(key) {
                            hash_continuation(branch, hasher);
                        }
                        hasher.update(b",");
                    }
                    hasher.update(b"}");
                    if let Some(d) = default {
                        hasher.update(b"_=>");
                        hash_continuation(d, hasher);
                    }
                    hasher.update(b";");
                    if let Some(n) = next {
                        hash_continuation(n, hasher);
                    }
                }
                SerializableContinuation::Loop {
                    id,
                    body,
                    max_iterations,
                    on_max,
                    next,
                } => {
                    hasher.update(b"L:");
                    hasher.update(id.as_bytes());
                    hasher.update(b":");
                    hasher.update(max_iterations.to_string().as_bytes());
                    hasher.update(b":");
                    hasher.update(on_max.to_string().as_bytes());
                    hasher.update(b"{");
                    hash_continuation(body, hasher);
                    hasher.update(b"}");
                    hasher.update(b";");
                    if let Some(n) = next {
                        hash_continuation(n, hasher);
                    }
                }
                SerializableContinuation::ChildWorkflow { id, child, next } => {
                    hasher.update(b"CW:");
                    hasher.update(id.as_bytes());
                    hasher.update(b"{");
                    hash_continuation(child, hasher);
                    hasher.update(b"}");
                    hasher.update(b";");
                    if let Some(n) = next {
                        hash_continuation(n, hasher);
                    }
                }
            }
        }

        let mut hasher = Sha256::new();
        hash_continuation(self, &mut hasher);
        crate::DefinitionHash::from_hash(crate::Hash32::from_digest(hasher))
    }
}

impl<C, Input, M> SerializableWorkflow<C, Input, M> {
    /// Get the workflow ID.
    #[must_use]
    pub fn workflow_id(&self) -> &str {
        self.inner.workflow_id()
    }

    /// Get the definition hash.
    #[must_use]
    pub fn definition_hash(&self) -> &crate::DefinitionHash {
        self.inner.definition_hash()
    }

    /// Get a reference to the inner workflow.
    #[must_use]
    pub fn workflow(&self) -> &Workflow<C, Input, M> {
        &self.inner
    }

    /// Get a reference to the context.
    #[must_use]
    pub fn context(&self) -> &WorkflowContext<C, M> {
        self.inner.context()
    }

    /// Get a reference to the codec.
    #[must_use]
    pub fn codec(&self) -> &Arc<C> {
        self.inner.codec()
    }

    /// Get a reference to the continuation.
    #[must_use]
    pub fn continuation(&self) -> &WorkflowContinuation {
        self.inner.continuation()
    }

    /// Get a reference to the metadata.
    #[must_use]
    pub fn metadata(&self) -> &Arc<M> {
        self.inner.metadata()
    }

    /// Get a reference to the internal task registry.
    #[must_use]
    pub fn registry(&self) -> &TaskRegistry {
        &self.registry
    }

    /// Consume the workflow and return its continuation tree and task registry.
    ///
    /// Useful for inlining this workflow as a child inside another workflow
    /// while merging task registries.
    #[must_use]
    pub fn into_parts(self) -> (WorkflowContinuation, TaskRegistry) {
        (self.inner.continuation, self.registry)
    }

    /// Convert to a serializable state representation.
    ///
    /// Returns a `SerializedWorkflowState` that includes the workflow ID,
    /// definition hash, and continuation structure. This can be serialized
    /// and later deserialized to resume the workflow.
    #[must_use]
    pub fn to_serializable(&self) -> SerializedWorkflowState {
        SerializedWorkflowState {
            workflow_id: self.inner.workflow_id().to_string(),
            definition_hash: self.inner.definition_hash,
            continuation: self.inner.continuation().to_serializable(),
        }
    }

    /// Convert a serialized workflow state to runnable using the internal registry.
    ///
    /// # Errors
    ///
    /// Returns `BuildError::DefinitionMismatch` if the definition hash doesn't
    /// match this workflow's hash, indicating the serialized state was created with
    /// a different workflow definition.
    ///
    /// Returns `BuildError::TaskNotFound` if any task ID is not in the registry.
    pub fn to_runnable(
        &self,
        state: &SerializedWorkflowState,
    ) -> Result<WorkflowContinuation, crate::error::BuildError> {
        if state.definition_hash != self.inner.definition_hash {
            return Err(crate::error::BuildError::DefinitionMismatch {
                expected: self.inner.definition_hash,
                found: state.definition_hash,
            });
        }
        state.continuation.to_runnable(&self.registry)
    }
}

impl<C, Input, M> Deref for SerializableWorkflow<C, Input, M> {
    type Target = Workflow<C, Input, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
