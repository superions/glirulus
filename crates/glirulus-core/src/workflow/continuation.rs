//! Traversal and mutation of the runnable continuation tree.

use super::{NodeInfo, NodeIter, NodeKind, SerializableContinuation, WorkflowContinuation};
use crate::task::RetryPolicy;

const EMPTY_TAGS: &[String] = &[];

impl<'a> Iterator for NodeIter<'a> {
    type Item = NodeInfo<'a>;

    #[allow(clippy::too_many_lines)]
    fn next(&mut self) -> Option<Self::Item> {
        let (cont, predecessor) = self.stack.pop()?;

        let (id, kind, timeout, retry_policy, priority, tags, version) = match cont {
            WorkflowContinuation::Task {
                id,
                timeout,
                retry_policy,
                priority,
                tags,
                version,
                ..
            } => (
                id.as_str(),
                NodeKind::Task,
                *timeout,
                retry_policy.as_ref(),
                *priority,
                tags.as_slice(),
                version.as_deref(),
            ),
            WorkflowContinuation::Fork { id, .. } => (
                id.as_str(),
                NodeKind::Fork,
                None,
                None,
                None,
                EMPTY_TAGS,
                None,
            ),
            WorkflowContinuation::Delay { id, duration, .. } => (
                id.as_str(),
                NodeKind::Delay,
                Some(*duration),
                None,
                None,
                EMPTY_TAGS,
                None,
            ),
            WorkflowContinuation::AwaitSignal { id, timeout, .. } => (
                id.as_str(),
                NodeKind::AwaitSignal,
                *timeout,
                None,
                None,
                EMPTY_TAGS,
                None,
            ),
            WorkflowContinuation::Branch { id, .. } => (
                id.as_str(),
                NodeKind::Branch,
                None,
                None,
                None,
                EMPTY_TAGS,
                None,
            ),
            WorkflowContinuation::Loop { id, .. } => (
                id.as_str(),
                NodeKind::Loop,
                None,
                None,
                None,
                EMPTY_TAGS,
                None,
            ),
            WorkflowContinuation::ChildWorkflow { id, .. } => (
                id.as_str(),
                NodeKind::ChildWorkflow,
                None,
                None,
                None,
                EMPTY_TAGS,
                None,
            ),
        };

        // Push children in reverse order so the first child is popped next.
        match cont {
            WorkflowContinuation::Task { id, next, .. }
            | WorkflowContinuation::Delay { id, next, .. }
            | WorkflowContinuation::AwaitSignal { id, next, .. } => {
                if let Some(n) = next {
                    self.stack.push((n, Some(id)));
                }
            }
            WorkflowContinuation::Fork { id, branches, join } => {
                if let Some(j) = join {
                    self.stack.push((j, Some(id)));
                }
                for b in branches.iter().rev() {
                    self.stack.push((b, Some(id)));
                }
            }
            WorkflowContinuation::Branch {
                id,
                branches,
                default,
                next,
                ..
            } => {
                if let Some(n) = next {
                    self.stack.push((n, Some(id)));
                }
                if let Some(d) = default {
                    self.stack.push((d, Some(id)));
                }
                // stable sort for deterministic iteration
                let mut keys: Vec<&String> = branches.keys().collect();
                keys.sort();
                for k in keys.into_iter().rev() {
                    self.stack.push((&branches[k], Some(id)));
                }
            }
            WorkflowContinuation::Loop { id, body, next, .. } => {
                if let Some(n) = next {
                    self.stack.push((n, Some(id)));
                }
                self.stack.push((body, Some(id)));
            }
            WorkflowContinuation::ChildWorkflow {
                id, child, next, ..
            } => {
                if let Some(n) = next {
                    self.stack.push((n, Some(id)));
                }
                self.stack.push((child, Some(id)));
            }
        }

        Some(NodeInfo {
            id,
            kind,
            predecessor_id: predecessor,
            timeout,
            retry_policy,
            priority,
            tags,
            version,
        })
    }
}

impl WorkflowContinuation {
    /// Derive a fork ID from a list of branch IDs.
    ///
    /// The fork ID is a concatenation of branch IDs separated by `||`.
    #[must_use]
    pub fn derive_fork_id(branch_ids: &[&str]) -> String {
        branch_ids.join("||")
    }

    /// Get the ID of this continuation node.
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            WorkflowContinuation::Task { id, .. }
            | WorkflowContinuation::Fork { id, .. }
            | WorkflowContinuation::Delay { id, .. }
            | WorkflowContinuation::AwaitSignal { id, .. }
            | WorkflowContinuation::Branch { id, .. }
            | WorkflowContinuation::Loop { id, .. }
            | WorkflowContinuation::ChildWorkflow { id, .. } => id,
        }
    }

    /// Get the next continuation in the chain, if any.
    ///
    #[must_use]
    pub fn get_next(&self) -> Option<&WorkflowContinuation> {
        match self {
            Self::Task { next, .. }
            | Self::Delay { next, .. }
            | Self::AwaitSignal { next, .. }
            | Self::Branch { next, .. }
            | Self::Loop { next, .. }
            | Self::ChildWorkflow { next, .. } => next.as_deref(),
            Self::Fork { join, .. } => join.as_deref(),
        }
    }

    /// Get the first task ID from this continuation.
    ///
    /// For a `Task`, returns its ID. For a `Fork`, returns the first task ID
    /// from the first branch.
    #[must_use]
    pub fn first_task_id(&self) -> &str {
        match self {
            WorkflowContinuation::Task { id, .. }
            | WorkflowContinuation::Delay { id, .. }
            | WorkflowContinuation::AwaitSignal { id, .. }
            | WorkflowContinuation::Branch { id, .. } => id,
            WorkflowContinuation::Fork { branches, .. } => {
                if let Some(first_branch) = branches.first() {
                    first_branch.first_task_id()
                } else {
                    "unknown"
                }
            }
            WorkflowContinuation::Loop { body, .. } => body.first_task_id(),
            WorkflowContinuation::ChildWorkflow { child, .. } => child.first_task_id(),
        }
    }

    /// Get the execution priority of the first task in this continuation.
    ///
    /// Returns `Some(priority)` for `Task` nodes, `None` for non-task nodes
    /// (Delay, Signal, Branch). Recurses through Fork, Loop, and `ChildWorkflow`.
    #[must_use]
    pub fn first_task_priority(&self) -> Option<u8> {
        match self {
            WorkflowContinuation::Task { priority, .. } => *priority,
            WorkflowContinuation::Delay { .. }
            | WorkflowContinuation::AwaitSignal { .. }
            | WorkflowContinuation::Branch { .. } => None,
            WorkflowContinuation::Fork { branches, .. } => {
                branches.first().and_then(|b| b.first_task_priority())
            }
            WorkflowContinuation::Loop { body, .. } => body.first_task_priority(),
            WorkflowContinuation::ChildWorkflow { child, .. } => child.first_task_priority(),
        }
    }

    /// Get the affinity tags of the first task in this continuation.
    ///
    /// Returns the tags for `Task` nodes, empty for non-task nodes
    /// (Delay, Signal, Branch). Recurses through Fork, Loop, and `ChildWorkflow`.
    #[must_use]
    pub fn first_task_tags(&self) -> Vec<String> {
        match self {
            WorkflowContinuation::Task { tags, .. } => tags.clone(),
            WorkflowContinuation::Delay { .. }
            | WorkflowContinuation::AwaitSignal { .. }
            | WorkflowContinuation::Branch { .. } => vec![],
            WorkflowContinuation::Fork { branches, .. } => branches
                .first()
                .map(|b| b.first_task_tags())
                .unwrap_or_default(),
            WorkflowContinuation::Loop { body, .. } => body.first_task_tags(),
            WorkflowContinuation::ChildWorkflow { child, .. } => child.first_task_tags(),
        }
    }

    /// Build a [`crate::snapshot::TaskHint`] from the first task in this continuation.
    ///
    /// Combines [`Self::first_task_id`], [`Self::first_task_priority`], and
    /// [`Self::first_task_tags`] into a single struct for passing through
    /// `prepare_run` and `ParkReason`.
    #[must_use]
    pub fn first_task_hint(&self) -> crate::snapshot::TaskHint {
        crate::snapshot::TaskHint::new(
            self.first_task_id(),
            self.first_task_priority(),
            &self.first_task_tags(),
        )
    }

    /// Get the terminal task ID of this continuation chain.
    ///
    /// Follows `get_next()` pointers to the end and returns the ID of the
    /// last node. This is the task whose output is the "final" output of the
    /// chain (e.g. the `LoopResult` envelope for a loop body).
    #[must_use]
    pub fn terminal_task_id(&self) -> &str {
        let mut current = self;
        while let Some(next) = current.get_next() {
            current = next;
        }
        current.first_task_id()
    }

    /// Find a task node by ID (immutable).
    ///
    /// Recursively walks the full continuation tree, including through `Arc`
    /// fork branches, and returns a reference to the matching `Task` node.
    /// Resolve a [`TaskId`](crate::TaskId) back to the human-readable task
    /// name stored on the matching `Task` node, if any. O(N) tree walk.
    #[must_use]
    pub fn find_task_name(&self, target_id: &crate::TaskId) -> Option<&str> {
        self.find_task(target_id).and_then(|n| match n {
            WorkflowContinuation::Task { id, .. } => Some(id.as_str()),
            _ => None,
        })
    }

    fn find_task(&self, target_id: &crate::TaskId) -> Option<&Self> {
        match self {
            WorkflowContinuation::Task { id, next, .. } => {
                if crate::TaskId::from(id.as_str()) == *target_id {
                    return Some(self);
                }
                next.as_ref().and_then(|n| n.find_task(target_id))
            }
            WorkflowContinuation::Delay { next, .. }
            | WorkflowContinuation::AwaitSignal { next, .. } => {
                next.as_ref().and_then(|n| n.find_task(target_id))
            }
            WorkflowContinuation::Fork { branches, join, .. } => {
                for branch in branches {
                    if let Some(found) = branch.find_task(target_id) {
                        return Some(found);
                    }
                }
                join.as_ref().and_then(|j| j.find_task(target_id))
            }
            WorkflowContinuation::Branch {
                branches,
                default,
                next,
                ..
            } => {
                for branch in branches.values() {
                    if let Some(found) = branch.find_task(target_id) {
                        return Some(found);
                    }
                }
                if let Some(d) = default
                    && let Some(found) = d.find_task(target_id)
                {
                    return Some(found);
                }
                next.as_ref().and_then(|n| n.find_task(target_id))
            }
            WorkflowContinuation::Loop { body, next, .. } => body
                .find_task(target_id)
                .or_else(|| next.as_ref().and_then(|n| n.find_task(target_id))),
            WorkflowContinuation::ChildWorkflow { child, next, .. } => child
                .find_task(target_id)
                .or_else(|| next.as_ref().and_then(|n| n.find_task(target_id))),
        }
    }

    /// Find a task node by ID (mutable).
    ///
    /// Same traversal as [`find_task`](Self::find_task) but returns a mutable
    /// reference. Fork branches behind `Arc` are skipped since they cannot be
    /// mutated; only the join continuation is searched.
    fn find_task_mut(&mut self, target_id: &crate::TaskId) -> Option<&mut Self> {
        match self {
            WorkflowContinuation::Task { id, .. }
                if crate::TaskId::from(id.as_str()) == *target_id =>
            {
                Some(self)
            }
            WorkflowContinuation::Task { next, .. } => {
                next.as_mut().and_then(|n| n.find_task_mut(target_id))
            }
            WorkflowContinuation::Delay { next, .. }
            | WorkflowContinuation::AwaitSignal { next, .. } => {
                next.as_mut().and_then(|n| n.find_task_mut(target_id))
            }
            WorkflowContinuation::Fork { join, .. } => {
                join.as_mut().and_then(|j| j.find_task_mut(target_id))
            }
            WorkflowContinuation::Branch {
                branches,
                default,
                next,
                ..
            } => {
                for branch in branches.values_mut() {
                    if let Some(found) = branch.find_task_mut(target_id) {
                        return Some(found);
                    }
                }
                if let Some(d) = default
                    && let Some(found) = d.find_task_mut(target_id)
                {
                    return Some(found);
                }
                next.as_mut().and_then(|n| n.find_task_mut(target_id))
            }
            WorkflowContinuation::Loop { body, next, .. } => {
                if let Some(found) = body.find_task_mut(target_id) {
                    return Some(found);
                }
                next.as_mut().and_then(|n| n.find_task_mut(target_id))
            }
            WorkflowContinuation::ChildWorkflow { next, .. } => {
                // Arc child branches cannot be mutated; only search next.
                next.as_mut().and_then(|n| n.find_task_mut(target_id))
            }
        }
    }

    /// Set the timeout on a specific task node found by ID.
    pub fn set_task_timeout(
        &mut self,
        target_id: &crate::TaskId,
        timeout: Option<std::time::Duration>,
    ) {
        if let Some(WorkflowContinuation::Task { timeout: t, .. }) = self.find_task_mut(target_id) {
            *t = timeout;
        }
    }

    /// Set the retry policy on a specific task node found by ID.
    pub fn set_task_retry_policy(
        &mut self,
        target_id: &crate::TaskId,
        policy: Option<RetryPolicy>,
    ) {
        if let Some(WorkflowContinuation::Task { retry_policy, .. }) = self.find_task_mut(target_id)
        {
            *retry_policy = policy;
        }
    }

    /// Set the schema version on a specific task node found by ID.
    pub fn set_task_version(&mut self, target_id: &crate::TaskId, ver: Option<String>) {
        if let Some(WorkflowContinuation::Task { version, .. }) = self.find_task_mut(target_id) {
            *version = ver;
        }
    }

    /// Look up the retry policy configured on a specific task by ID.
    #[must_use]
    pub fn get_task_retry_policy(&self, task_id: &crate::TaskId) -> Option<&RetryPolicy> {
        match self.find_task(task_id)? {
            WorkflowContinuation::Task { retry_policy, .. } => retry_policy.as_ref(),
            _ => None,
        }
    }

    /// Look up the timeout configured on a specific task by ID.
    #[must_use]
    pub fn get_task_timeout(&self, task_id: &crate::TaskId) -> Option<std::time::Duration> {
        match self.find_task(task_id)? {
            WorkflowContinuation::Task { timeout, .. } => *timeout,
            _ => None,
        }
    }

    /// Look up the priority configured on a specific task by ID.
    #[must_use]
    pub fn get_task_priority(&self, task_id: &crate::TaskId) -> Option<u8> {
        match self.find_task(task_id)? {
            WorkflowContinuation::Task { priority, .. } => *priority,
            _ => None,
        }
    }

    /// Look up the affinity tags configured on a specific task by ID.
    #[must_use]
    pub fn get_task_tags(&self, task_id: &crate::TaskId) -> Vec<String> {
        match self.find_task(task_id) {
            Some(WorkflowContinuation::Task { tags, .. }) => tags.clone(),
            _ => vec![],
        }
    }

    /// Set the affinity tags on a specific task node found by ID.
    pub fn set_task_tags(&mut self, target_id: &crate::TaskId, new_tags: Vec<String>) {
        if let Some(WorkflowContinuation::Task { tags, .. }) = self.find_task_mut(target_id) {
            *tags = new_tags;
        }
    }

    /// Build a [`TaskMetadata`](crate::task::TaskMetadata) from the fields
    /// available on the continuation node for the given task.
    ///
    /// Only `timeout`, `retries`, `version`, and `tags` are populated — display
    /// name and description are left as defaults since they are not stored in
    /// the continuation tree.
    #[must_use]
    pub fn build_task_metadata(&self, task_id: &crate::TaskId) -> crate::task::TaskMetadata {
        match self.find_task(task_id) {
            Some(WorkflowContinuation::Task {
                timeout,
                retry_policy,
                version,
                priority,
                tags,
                ..
            }) => crate::task::TaskMetadata::from_node_fields(
                *timeout,
                retry_policy.clone(),
                version.clone(),
                *priority,
                tags.clone(),
            ),
            _ => crate::task::TaskMetadata::default(),
        }
    }

    /// Returns a lazy iterator over all nodes in topological (execution) order.
    ///
    /// The traversal mirrors the order that the workflow engine would visit
    /// each node during execution, making the result useful for introspection,
    /// UI visualisation, and documentation generation.
    ///
    /// Each [`NodeInfo`] includes a `predecessor_id` linking back to the node
    /// whose completion triggers this one. The root node has `None`.
    #[must_use]
    pub fn iter_nodes(&self) -> NodeIter<'_> {
        NodeIter {
            stack: vec![(self, None)],
        }
    }

    /// Convert to a serializable representation (strips out task implementations).
    #[must_use]
    pub fn to_serializable(&self) -> SerializableContinuation {
        match self {
            #[allow(clippy::cast_possible_truncation)] // Durations > u64::MAX ms are not realistic
            WorkflowContinuation::Task {
                id,
                timeout,
                retry_policy,
                version,
                priority,
                tags,
                next,
                ..
            } => SerializableContinuation::Task {
                id: id.clone(),
                timeout_ms: timeout.map(|d| d.as_millis() as u64),
                retry_policy: retry_policy.clone(),
                version: version.clone(),
                priority: *priority,
                tags: tags.clone(),
                next: next.as_ref().map(|n| Box::new(n.to_serializable())),
            },
            WorkflowContinuation::Fork { id, branches, join } => SerializableContinuation::Fork {
                id: id.clone(),
                branches: branches.iter().map(|b| b.to_serializable()).collect(),
                join: join.as_ref().map(|j| Box::new(j.to_serializable())),
            },
            #[allow(clippy::cast_possible_truncation)] // Durations > u64::MAX ms are not realistic
            WorkflowContinuation::Delay { id, duration, next } => SerializableContinuation::Delay {
                id: id.clone(),
                duration_ms: duration.as_millis() as u64,
                next: next.as_ref().map(|n| Box::new(n.to_serializable())),
            },
            #[allow(clippy::cast_possible_truncation)]
            WorkflowContinuation::AwaitSignal {
                id,
                signal_name,
                timeout,
                next,
            } => SerializableContinuation::AwaitSignal {
                id: id.clone(),
                signal_name: signal_name.clone(),
                timeout_ms: timeout.map(|d| d.as_millis() as u64),
                next: next.as_ref().map(|n| Box::new(n.to_serializable())),
            },
            WorkflowContinuation::Branch {
                id,
                branches,
                default,
                next,
                ..
            } => SerializableContinuation::Branch {
                id: id.clone(),
                branches: branches
                    .iter()
                    .map(|(k, v)| (k.clone(), Box::new(v.to_serializable())))
                    .collect(),
                default: default.as_ref().map(|d| Box::new(d.to_serializable())),
                next: next.as_ref().map(|n| Box::new(n.to_serializable())),
            },
            WorkflowContinuation::ChildWorkflow { id, child, next } => {
                SerializableContinuation::ChildWorkflow {
                    id: id.clone(),
                    child: Box::new(child.to_serializable()),
                    next: next.as_ref().map(|n| Box::new(n.to_serializable())),
                }
            }
            WorkflowContinuation::Loop {
                id,
                body,
                max_iterations,
                on_max,
                next,
            } => SerializableContinuation::Loop {
                id: id.clone(),
                body: Box::new(body.to_serializable()),
                max_iterations: *max_iterations,
                on_max: *on_max,
                next: next.as_ref().map(|n| Box::new(n.to_serializable())),
            },
        }
    }

    /// Append a new node to the end of this continuation chain.
    ///
    /// Recursively walks the chain to find the tail and attaches `new_node` there.
    pub fn append_to_chain(&mut self, new_node: WorkflowContinuation) {
        match self {
            WorkflowContinuation::Task { next, .. }
            | WorkflowContinuation::Delay { next, .. }
            | WorkflowContinuation::AwaitSignal { next, .. }
            | WorkflowContinuation::Branch { next, .. }
            | WorkflowContinuation::Loop { next, .. }
            | WorkflowContinuation::ChildWorkflow { next, .. } => match next {
                Some(next_box) => next_box.append_to_chain(new_node),
                None => *next = Some(Box::new(new_node)),
            },
            WorkflowContinuation::Fork { join, .. } => match join {
                Some(join_box) => join_box.append_to_chain(new_node),
                None => *join = Some(Box::new(new_node)),
            },
        }
    }
}
