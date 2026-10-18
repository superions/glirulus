//! [`SignalStore`] implementation for Postgres.
//!
//! Overrides the 3 default composite methods with single-transaction
//! implementations for true ACID atomicity.

use glirulus_core::codec;
use glirulus_core::snapshot::{PauseRequest, SignalKind, SignalRequest, WorkflowSnapshot};
use glirulus_persistence::validation::validate_signal_allowed;
use glirulus_persistence::{BackendError, SignalStore};
use sqlx::Row;

use crate::backend::PostgresBackend;
use crate::error::PgError;
use crate::history::append_history;
use crate::wakeup::{TASK_READY_CHANNEL, build_task_ready_payload};

/// Native owners arbitrate committed acceptance before the engine takes its
/// snapshot lock. The same connection retains that decision through commit.
pub trait WaitArbitrator: Send + Sync {
    /// Resolve under the native acceptance lock before an engine row is locked.
    /// `observed_deadline` is a stale observation, never acceptance authority.
    fn resolve<'a>(
        &'a self,
        connection: &'a mut sqlx::PgConnection,
        instance_id: &'a str,
        signal_name: &'a str,
        observed_deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> WaitFuture<'a>;
}
/// A resolution borrowing the caller's still-open PostgreSQL transaction.
pub type WaitFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<WaitResolution, BackendError>> + Send + 'a>,
>;
/// Technical advancement authorized by the native acceptance owner.
pub enum WaitResolution {
    /// No accepted outcome; retain the native absolute deadline while parked.
    Pending {
        /// None represents an explicitly unbounded wait.
        deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// A committed native outcome wins over delayed delivery or execution.
    Accepted {
        /// Stable UUID text shared with the native outbox intent.
        event_id: String,
        /// Native acceptance time, rather than engine delivery time.
        accepted_at: chrono::DateTime<chrono::Utc>,
        /// Versioned technical process input for the continuation.
        payload: bytes::Bytes,
    },
    /// Native arbitration found no timely accepted outcome.
    Expired,
    /// Native Stop forbids further continuation.
    Stopped,
}

fn parked_signal(
    snapshot: &WorkflowSnapshot,
) -> Option<(String, Option<chrono::DateTime<chrono::Utc>>)> {
    match &snapshot.state {
        glirulus_core::snapshot::WorkflowSnapshotState::InProgress {
            position:
                glirulus_core::snapshot::ExecutionPosition::AtSignal {
                    signal_name,
                    wake_at,
                    ..
                },
            ..
        } => Some((signal_name.clone(), *wake_at)),
        _ => None,
    }
}

impl<C: codec::SnapshotCodec> PostgresBackend<C> {
    async fn observed_wait_deadline_in(
        &self,
        connection: &mut sqlx::PgConnection,
        instance_id: &str,
        signal_name: &str,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, BackendError> {
        let raw: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT h.data FROM sayiir_workflow_snapshots s JOIN sayiir_workflow_snapshot_history h
       ON h.instance_id=s.instance_id AND h.version=s.history_version WHERE s.instance_id=$1",
        )
        .bind(instance_id)
        .fetch_optional(connection)
        .await
        .map_err(PgError)?;
        match raw {
            Some(raw) => Ok(parked_signal(&self.decode(&raw)?)
                .filter(|(name, _)| name == signal_name)
                .and_then(|(_, deadline)| deadline)),
            None => Ok(None),
        }
    }

    async fn arbitrate_wait(
        &self,
        connection: &mut sqlx::PgConnection,
        instance_id: &str,
        signal_name: &str,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<WaitResolution, BackendError> {
        let arbitrator = self
            .wait_arbitrator
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        match arbitrator {
            Some(arbitrator) => {
                arbitrator
                    .resolve(connection, instance_id, signal_name, deadline)
                    .await
            }
            None if deadline.is_some_and(|deadline| deadline <= chrono::Utc::now()) => {
                Ok(WaitResolution::Expired)
            }
            None => Ok(WaitResolution::Pending {
                deadline_at: deadline,
            }),
        }
    }

    async fn insert_correlated_event_in(
        &self,
        connection: &mut sqlx::PgConnection,
        instance_id: &str,
        signal_name: &str,
        event_id: &str,
        accepted_at: chrono::DateTime<chrono::Utc>,
        payload: &bytes::Bytes,
    ) -> Result<bool, BackendError> {
        let inserted = sqlx::query(
      "INSERT INTO sayiir_workflow_events(instance_id,signal_name,event_id,accepted_at,payload)
       VALUES($1,$2,$3::text::uuid,$4,$5)
       ON CONFLICT(instance_id,event_id) WHERE event_id IS NOT NULL DO NOTHING RETURNING id",
    )
    .bind(instance_id)
    .bind(signal_name)
    .bind(event_id)
    .bind(accepted_at)
    .bind(payload.as_ref())
    .fetch_optional(&mut *connection)
    .await
    .map_err(PgError)?;
        if inserted.is_some() {
            return Ok(true);
        }
        let row = sqlx::query(
            "SELECT signal_name,accepted_at,payload FROM sayiir_workflow_events
       WHERE instance_id=$1 AND event_id=$2::text::uuid",
        )
        .bind(instance_id)
        .bind(event_id)
        .fetch_one(&mut *connection)
        .await
        .map_err(PgError)?;
        if row.get::<String, _>("signal_name") != signal_name
            || row.get::<chrono::DateTime<chrono::Utc>, _>("accepted_at") != accepted_at
            || row.get::<Vec<u8>, _>("payload").as_slice() != payload.as_ref()
        {
            return Err(BackendError::Backend(
                "correlated signal identity conflicts".into(),
            ));
        }
        Ok(false)
    }

    /// Caller holds native acceptance/Stop locks in this transaction. Consumed
    /// events retain their identity so a lost delivery acknowledgement is safe.
    pub async fn send_correlated_event_in(
        &self,
        connection: &mut sqlx::PgConnection,
        instance_id: &str,
        signal_name: &str,
        event_id: &str,
        accepted_at: chrono::DateTime<chrono::Utc>,
        payload: bytes::Bytes,
    ) -> Result<bool, BackendError> {
        // Resolve native authority before locking the snapshot, matching parking
        // and deadline processing rather than reversing their lock order.
        let deadline = self
            .observed_wait_deadline_in(connection, instance_id, signal_name)
            .await?;
        let resolution = self
            .arbitrate_wait(connection, instance_id, signal_name, deadline)
            .await?;
        let locked = self
            .lock_snapshot_for_mutation(connection, instance_id)
            .await?;
        let inserted = self
            .insert_correlated_event_in(
                connection,
                instance_id,
                signal_name,
                event_id,
                accepted_at,
                &payload,
            )
            .await?;
        if let Some((mut snapshot, _)) = locked {
            self.advance_signal_in(connection, &mut snapshot, signal_name, resolution)
                .await?;
        }
        Ok(inserted)
    }

    /// Persist entering a wait and drain its early event in one transaction.
    /// Never overwrite a snapshot already advanced by a concurrent delivery.
    pub(crate) async fn save_snapshot_with_wait(
        &self,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(), BackendError> {
        let Some((signal_name, deadline)) = parked_signal(snapshot) else {
            return self.save_snapshot_mode(snapshot, false, false, None).await;
        };
        let instance_id = snapshot.instance_id.to_string();
        let mut tx = self.pool.begin().await.map_err(PgError)?;
        let resolution = self
            .arbitrate_wait(&mut tx, &instance_id, &signal_name, deadline)
            .await?;
        if let Some((current, _)) = self
            .lock_snapshot_for_mutation(&mut tx, &instance_id)
            .await?
        {
            let (signal_id, _) = signal_resume_target(snapshot, &signal_name)
                .ok_or_else(|| BackendError::Backend("incoming signal wait unavailable".into()))?;
            // Retry hydrates previously committed results while beginning at an
            // earlier position. A retained signal alone does not mean this header
            // already advanced past the incoming checkpoint.
            let already_advanced = current.get_task_result(&signal_id).is_some()
                && signal_resume_target(&current, &signal_name).is_none()
                && current.current_task_id() != snapshot.last_completed_task_id();
            if current.state.is_terminal() || already_advanced {
                *snapshot = current;
                tx.commit().await.map_err(PgError)?;
                return Ok(());
            }
        }
        if let WaitResolution::Pending { deadline_at } = &resolution {
            if let glirulus_core::snapshot::WorkflowSnapshotState::InProgress {
                position: glirulus_core::snapshot::ExecutionPosition::AtSignal { wake_at, .. },
                ..
            } = &mut snapshot.state
            {
                *wake_at = *deadline_at;
            }
        }
        // This flushes the preceding task's output before marking the signal task
        // complete; a snapshot carries only one unflushed output at a time.
        self.save_snapshot_mode(snapshot, false, false, Some(&mut tx))
            .await?;
        self.advance_signal_in(&mut tx, snapshot, &signal_name, resolution)
            .await?;
        tx.commit().await.map_err(PgError)?;
        Ok(())
    }

    async fn complete_signal_in(
        &self,
        connection: &mut sqlx::PgConnection,
        snapshot: &mut WorkflowSnapshot,
        signal_id: glirulus_core::TaskId,
        next_task_id: Option<glirulus_core::TaskId>,
        payload: &bytes::Bytes,
    ) -> Result<(), BackendError> {
        snapshot.mark_task_completed(signal_id, payload.clone());
        if let Some(task_id) = next_task_id {
            snapshot.update_position(glirulus_core::snapshot::ExecutionPosition::AtTask { task_id });
        } else {
            // Terminal state drops the task map. Flush this output first on the
            // same connection so output, snapshot and event consumption commit together.
            sqlx::query(
                "INSERT INTO sayiir_workflow_tasks
                    (instance_id, task_id, status, completed_at, output)
                 VALUES ($1, $2, 'completed', now(), $3)
                 ON CONFLICT (instance_id, task_id) DO UPDATE SET
                    status = 'completed',
                    completed_at = COALESCE(sayiir_workflow_tasks.completed_at, EXCLUDED.completed_at),
                    error = NULL,
                    output = EXCLUDED.output
                 WHERE sayiir_workflow_tasks.output IS NULL
                    OR sayiir_workflow_tasks.output IS DISTINCT FROM EXCLUDED.output",
            )
            .bind(snapshot.instance_id.as_ref())
            .bind(signal_id.as_bytes().as_slice())
            .bind(payload.as_ref())
            .execute(&mut *connection)
            .await
            .map_err(PgError)?;
            snapshot.mark_completed(payload.clone());
        }
        self.save_snapshot_mode(snapshot, false, false, Some(connection))
            .await
    }

    async fn advance_signal_in(
        &self,
        connection: &mut sqlx::PgConnection,
        snapshot: &mut WorkflowSnapshot,
        signal_name: &str,
        resolution: WaitResolution,
    ) -> Result<Option<bytes::Bytes>, BackendError> {
        let Some((signal_id, next_task_id)) = signal_resume_target(snapshot, signal_name) else {
            return Ok(None);
        };
        let instance_id = snapshot.instance_id.to_string();
        match resolution {
            WaitResolution::Accepted {
                event_id,
                accepted_at,
                payload,
            } => {
                self.insert_correlated_event_in(
                    connection,
                    &instance_id,
                    signal_name,
                    &event_id,
                    accepted_at,
                    &payload,
                )
                .await?;
            }
            WaitResolution::Expired => {
                snapshot.mark_failed("signal wait expired".into());
                self.save_snapshot_mode(snapshot, false, false, Some(connection))
                    .await?;
                return Ok(None);
            }
            WaitResolution::Stopped => {
                snapshot.mark_cancelled(Some("native run stopped".into()), None, Some(signal_id));
                self.save_snapshot_mode(snapshot, false, false, Some(connection))
                    .await?;
                return Ok(None);
            }
            WaitResolution::Pending { deadline_at } => {
                if let glirulus_core::snapshot::WorkflowSnapshotState::InProgress {
                    position: glirulus_core::snapshot::ExecutionPosition::AtSignal { wake_at, .. },
                    ..
                } = &mut snapshot.state
                {
                    *wake_at = deadline_at;
                }
            }
        }
        // A technical retry may re-enter this wait after its event was consumed.
        // The retained signal task output is the same durable checkpoint, not a
        // new delivery and not permission to reconstruct a different outcome.
        if let Some(payload) = snapshot.get_task_result_bytes(&signal_id) {
            self.complete_signal_in(connection, snapshot, signal_id, next_task_id, &payload)
                .await?;
            return Ok(Some(payload));
        }
        let row = sqlx::query(
            "SELECT id,payload FROM sayiir_workflow_events WHERE instance_id=$1 AND signal_name=$2
       AND consumed_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE",
        )
        .bind(&instance_id)
        .bind(signal_name)
        .fetch_optional(&mut *connection)
        .await
        .map_err(PgError)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let id: i64 = row.get("id");
        let payload = bytes::Bytes::from(row.get::<Vec<u8>, _>("payload"));
        self.complete_signal_in(connection, snapshot, signal_id, next_task_id, &payload)
            .await?;
        sqlx::query("UPDATE sayiir_workflow_events SET consumed_at=now() WHERE id=$1 AND consumed_at IS NULL")
      .bind(id)
      .execute(connection)
      .await
      .map_err(PgError)?;
        Ok(Some(payload))
    }

    /// Existing worker polling advances due or buffered waits; no per-instance
    /// task, lease, timer store or additional scheduler owns a parked wait.
    pub(crate) async fn advance_ready_signal_waits(
        &self,
        limit: usize,
    ) -> Result<(), BackendError> {
        let eligible = self
            .eligible_definitions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let rows = sqlx::query(
      "SELECT s.instance_id,h.data FROM sayiir_workflow_snapshots s
       JOIN sayiir_workflow_snapshot_history h ON h.instance_id=s.instance_id AND h.version=s.history_version
       WHERE s.status='InProgress' AND s.position_kind='AtSignal'
       AND ($2::bytea[] IS NULL OR s.definition_hash=ANY($2))
       AND (s.delay_wake_at <= now() OR EXISTS(SELECT 1 FROM sayiir_workflow_events e
         WHERE e.instance_id=s.instance_id AND e.signal_name=s.signal_name AND e.consumed_at IS NULL))
       ORDER BY s.updated_at,s.instance_id LIMIT $1",
    )
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .bind(eligible)
    .fetch_all(&self.pool)
    .await
    .map_err(PgError)?;
        for row in rows {
            let instance_id: String = row.get("instance_id");
            let observed = self.decode(&row.get::<Vec<u8>, _>("data"))?;
            let Some((signal_name, deadline)) = parked_signal(&observed) else {
                continue;
            };
            let mut tx = self.pool.begin().await.map_err(PgError)?;
            let resolution = self
                .arbitrate_wait(&mut tx, &instance_id, &signal_name, deadline)
                .await?;
            if let Some((mut snapshot, _)) = self
                .lock_snapshot_for_mutation(&mut tx, &instance_id)
                .await?
            {
                self.advance_signal_in(&mut tx, &mut snapshot, &signal_name, resolution)
                    .await?;
            }
            tx.commit().await.map_err(PgError)?;
        }
        Ok(())
    }
}

impl<C> SignalStore for PostgresBackend<C>
where
    C: codec::SnapshotCodec,
{
    #[tracing::instrument(
        name = "db.store_signal",
        skip(self, request),
        fields(db.system = "postgresql", kind = %kind.as_ref()),
        err(level = tracing::Level::ERROR),
    )]
    async fn store_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
        request: SignalRequest,
    ) -> Result<(), BackendError> {
        tracing::debug!("storing signal");
        // Lock the snapshot row for the duration of validate-then-insert so a
        // concurrent `save_snapshot` can't transition the workflow to a state
        // that would have failed `validate_signal_allowed` between our read
        // and our write. `save_snapshot` upserts into the same row and will
        // block on the lock until this transaction commits.
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        self.store_signal_in(instance_id, kind, request, &mut tx)
            .await?;
        tx.commit().await.map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.get_signal",
        skip(self),
        fields(db.system = "postgresql", kind = %kind.as_ref()),
        err(level = tracing::Level::ERROR),
    )]
    async fn get_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
    ) -> Result<Option<SignalRequest>, BackendError> {
        tracing::debug!("getting signal");
        let row = sqlx::query(
            "SELECT reason, requested_by, created_at
             FROM sayiir_workflow_signals
             WHERE instance_id = $1 AND kind = $2",
        )
        .bind(instance_id)
        .bind(kind.as_ref())
        .fetch_optional(&self.pool)
        .await
        .map_err(PgError)?;

        Ok(row.map(|r| SignalRequest {
            reason: r.get("reason"),
            requested_by: r.get("requested_by"),
            requested_at: r.get("created_at"),
        }))
    }

    #[tracing::instrument(
        name = "db.clear_signal",
        skip(self),
        fields(db.system = "postgresql", kind = %kind.as_ref()),
        err(level = tracing::Level::ERROR),
    )]
    async fn clear_signal(&self, instance_id: &str, kind: SignalKind) -> Result<(), BackendError> {
        tracing::debug!("clearing signal");
        sqlx::query("DELETE FROM sayiir_workflow_signals WHERE instance_id = $1 AND kind = $2")
            .bind(instance_id)
            .bind(kind.as_ref())
            .execute(&self.pool)
            .await
            .map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.send_event",
        skip(self, payload),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    #[allow(clippy::too_many_lines)]
    async fn send_event(
        &self,
        instance_id: &str,
        signal_name: &str,
        payload: bytes::Bytes,
    ) -> Result<(), BackendError> {
        let mut tx = self.pool.begin().await.map_err(PgError)?;
        let deadline = self
            .observed_wait_deadline_in(&mut tx, instance_id, signal_name)
            .await?;
        let resolution = self
            .arbitrate_wait(&mut tx, instance_id, signal_name, deadline)
            .await?;
        let locked = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?;
        sqlx::query(
            "INSERT INTO sayiir_workflow_events(instance_id,signal_name,payload) VALUES($1,$2,$3)",
        )
        .bind(instance_id)
        .bind(signal_name)
        .bind(payload.as_ref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;
        if let Some((mut snapshot, _)) = locked {
            self.advance_signal_in(&mut tx, &mut snapshot, signal_name, resolution)
                .await?;
        }
        tx.commit().await.map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.consume_event",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn consume_event(
        &self,
        instance_id: &str,
        signal_name: &str,
    ) -> Result<Option<bytes::Bytes>, BackendError> {
        let mut tx = self.pool.begin().await.map_err(PgError)?;
        let deadline = self
            .observed_wait_deadline_in(&mut tx, instance_id, signal_name)
            .await?;
        let resolution = self
            .arbitrate_wait(&mut tx, instance_id, signal_name, deadline)
            .await?;
        let payload = match self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
        {
            Some((mut snapshot, _)) if signal_resume_target(&snapshot, signal_name).is_some() => {
                self.advance_signal_in(&mut tx, &mut snapshot, signal_name, resolution)
                    .await?
            }
            _ => {
                let unarbitrated = self
                    .wait_arbitrator
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .is_none();
                if unarbitrated {
                    // Plain FIFO consumption must not advance a different wait or
                    // bypass native arbitration and retained correlated identities.
                    sqlx::query_scalar::<_, Vec<u8>>(
                        "UPDATE sayiir_workflow_events SET consumed_at = now()
                         WHERE id = (
                            SELECT id FROM sayiir_workflow_events
                            WHERE instance_id = $1 AND signal_name = $2
                              AND event_id IS NULL AND consumed_at IS NULL
                            ORDER BY id LIMIT 1 FOR UPDATE
                         ) AND event_id IS NULL AND consumed_at IS NULL
                         RETURNING payload",
                    )
                    .bind(instance_id)
                    .bind(signal_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(PgError)?
                    .map(bytes::Bytes::from)
                } else {
                    None
                }
            }
        };
        tx.commit().await.map_err(PgError)?;
        Ok(payload)
    }

    // --- Overridden composites: single ACID transactions ---

    #[tracing::instrument(
        name = "db.check_and_cancel",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn check_and_cancel(
        &self,
        instance_id: &str,
        interrupted_at_task: Option<glirulus_core::TaskId>,
    ) -> Result<bool, BackendError> {
        tracing::debug!("checking for cancel signal");
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        // Check for cancel signal (lock the row)
        let signal_row = sqlx::query(
            "SELECT reason, requested_by
             FROM sayiir_workflow_signals
             WHERE instance_id = $1 AND kind = $2
             FOR UPDATE",
        )
        .bind(instance_id)
        .bind(SignalKind::Cancel.as_ref())
        .fetch_optional(&mut *tx)
        .await
        .map_err(PgError)?;

        let Some(signal_row) = signal_row else {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        };

        // Lock the snapshot row and load the latest blob from history.
        let Some((mut snapshot, prev_history_version)) = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
        else {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        };

        if !snapshot.state.is_in_progress() {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        }

        let reason: Option<String> = signal_row.get("reason");
        let requested_by: Option<String> = signal_row.get("requested_by");
        snapshot.mark_cancelled(reason, requested_by, interrupted_at_task);

        let (data, data_hash) = self.encode_blob(&mut snapshot)?;
        let status = snapshot.state.as_ref();
        let error = snapshot.error_message().map(ToString::to_string);
        let pos_kind = snapshot.position_kind();
        let wake_at = snapshot.delay_wake_at();
        let next_history_version = prev_history_version + 1;
        // History row's `current_task_id` records the task the workflow
        // was interrupted at. `snapshot.current_task_id()` returns None
        // here because `mark_cancelled` already transitioned to the
        // Cancelled variant (current_task_id only matches InProgress
        // AtTask), so binding it would lose the interrupted-task
        // pointer in the indexed history column. Use the caller's
        // `interrupted_at_task` directly.
        let task_id_bytes: Option<[u8; 32]> = interrupted_at_task.map(|t| *t.as_bytes());
        let task_id: Option<&[u8]> = task_id_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let notify_payload = build_task_ready_payload(&snapshot);

        // Pipeline the snapshot UPDATE and pg_notify into one statement.
        // pg_notify lands in the outer SELECT (not a sibling CTE) so it
        // can't be pruned by the planner — `FROM upd` forces upd to
        // execute, and the WHERE gates the notify when there's no
        // payload (cancel typically lands on a terminal state with no
        // current_task_id, so payload is NULL).
        sqlx::query(
            "WITH upd AS (
                 UPDATE sayiir_workflow_snapshots
                 SET status = $1, error = $2,
                     position_kind = $3, delay_wake_at = $4,
                     history_version = $5, data_hash = $6,
                     completed_at = now(), updated_at = now()
                 WHERE instance_id = $7
                 RETURNING 1
             )
             SELECT pg_notify($8, $9) FROM upd WHERE $9 IS NOT NULL",
        )
        .bind(status)
        .bind(&error)
        .bind(pos_kind)
        .bind(wake_at)
        .bind(next_history_version)
        .bind(data_hash.as_slice())
        .bind(instance_id)
        .bind(TASK_READY_CHANNEL)
        .bind(notify_payload.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        append_history(
            &mut tx,
            instance_id,
            next_history_version,
            status,
            task_id,
            &data,
            &data_hash,
        )
        .await?;

        // Mark any still-active tasks as cancelled
        sqlx::query(
            "UPDATE sayiir_workflow_tasks SET status = 'cancelled', completed_at = now()
             WHERE instance_id = $1 AND status = 'active'",
        )
        .bind(instance_id)
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        // Clear the signal
        sqlx::query("DELETE FROM sayiir_workflow_signals WHERE instance_id = $1 AND kind = $2")
            .bind(instance_id)
            .bind(SignalKind::Cancel.as_ref())
            .execute(&mut *tx)
            .await
            .map_err(PgError)?;

        tx.commit().await.map_err(PgError)?;
        tracing::info!(instance_id, "workflow cancelled");
        Ok(true)
    }

    #[tracing::instrument(
        name = "db.check_and_pause",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn check_and_pause(&self, instance_id: &str) -> Result<bool, BackendError> {
        tracing::debug!("checking for pause signal");
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        // Check for pause signal (lock the row)
        let signal_row = sqlx::query(
            "SELECT reason, requested_by
             FROM sayiir_workflow_signals
             WHERE instance_id = $1 AND kind = $2
             FOR UPDATE",
        )
        .bind(instance_id)
        .bind(SignalKind::Pause.as_ref())
        .fetch_optional(&mut *tx)
        .await
        .map_err(PgError)?;

        let Some(signal_row) = signal_row else {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        };

        // Lock the snapshot row and load the latest blob from history.
        let Some((mut snapshot, prev_history_version)) = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
        else {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        };

        if !snapshot.state.is_in_progress() {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        }

        let reason: Option<String> = signal_row.get("reason");
        let requested_by: Option<String> = signal_row.get("requested_by");
        let pause_request = PauseRequest::new(reason, requested_by);
        snapshot.mark_paused(&pause_request);

        let (data, data_hash) = self.encode_blob(&mut snapshot)?;
        let status = snapshot.state.as_ref();
        let task_id_bytes: Option<[u8; 32]> = snapshot.current_task_id().map(|t| *t.as_bytes());
        let task_id: Option<&[u8]> = task_id_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let task_count = snapshot.completed_task_count();
        let pos_kind = snapshot.position_kind();
        let wake_at = snapshot.delay_wake_at();
        let next_history_version = prev_history_version + 1;
        let notify_payload = build_task_ready_payload(&snapshot);

        // Pipeline UPDATE + pg_notify in one statement. Paused snapshots
        // typically don't carry a wakeup payload, but mirroring the
        // pattern keeps the signal-store write paths uniform with
        // save_snapshot.
        sqlx::query(
            "WITH upd AS (
                 UPDATE sayiir_workflow_snapshots
                 SET status = $1, current_task_id = $2,
                     completed_task_count = $3, position_kind = $4,
                     delay_wake_at = $5, history_version = $6,
                     data_hash = $7, updated_at = now()
                 WHERE instance_id = $8
                 RETURNING 1
             )
             SELECT pg_notify($9, $10) FROM upd WHERE $10 IS NOT NULL",
        )
        .bind(status)
        .bind(task_id)
        .bind(task_count)
        .bind(pos_kind)
        .bind(wake_at)
        .bind(next_history_version)
        .bind(data_hash.as_slice())
        .bind(instance_id)
        .bind(TASK_READY_CHANNEL)
        .bind(notify_payload.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        append_history(
            &mut tx,
            instance_id,
            next_history_version,
            status,
            task_id,
            &data,
            &data_hash,
        )
        .await?;

        // Clear the signal
        sqlx::query("DELETE FROM sayiir_workflow_signals WHERE instance_id = $1 AND kind = $2")
            .bind(instance_id)
            .bind(SignalKind::Pause.as_ref())
            .execute(&mut *tx)
            .await
            .map_err(PgError)?;

        tx.commit().await.map_err(PgError)?;
        tracing::info!(instance_id, "workflow paused");
        Ok(true)
    }

    #[tracing::instrument(
        name = "db.unpause",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn unpause(&self, instance_id: &str) -> Result<WorkflowSnapshot, BackendError> {
        tracing::debug!("unpausing workflow");
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        let (mut snapshot, prev_history_version) = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
            .ok_or_else(|| BackendError::NotFound(instance_id.to_string()))?;

        if !snapshot.state.is_paused() {
            let state_name = snapshot.state.as_ref();
            // Explicit rollback releases the FOR UPDATE row lock
            // immediately rather than waiting for sqlx's async
            // Transaction Drop to fire — matches the pattern in
            // check_and_cancel/check_and_pause and matters under
            // bursty admin scripts that hammer unpause across many
            // already-running workflows.
            tx.rollback().await.map_err(PgError)?;
            return Err(BackendError::CannotPause(format!(
                "Workflow is not paused (current state: {state_name:?})"
            )));
        }

        snapshot.mark_unpaused();

        let (data, data_hash) = self.encode_blob(&mut snapshot)?;
        let status = snapshot.state.as_ref();
        let task_id_bytes: Option<[u8; 32]> = snapshot.current_task_id().map(|t| *t.as_bytes());
        let task_id: Option<&[u8]> = task_id_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let task_count = snapshot.completed_task_count();
        let pos_kind = snapshot.position_kind();
        let wake_at = snapshot.delay_wake_at();
        let next_history_version = prev_history_version + 1;
        let notify_payload = build_task_ready_payload(&snapshot);

        // Unpause restores the snapshot to InProgress AtTask, so this is
        // the one signal-store path that actually emits a NOTIFY.
        // Folding it into the UPDATE eliminates the separate pg_notify
        // round-trip.
        sqlx::query(
            "WITH upd AS (
                 UPDATE sayiir_workflow_snapshots
                 SET status = $1, current_task_id = $2,
                     completed_task_count = $3, position_kind = $4,
                     delay_wake_at = $5, history_version = $6,
                     data_hash = $7, updated_at = now()
                 WHERE instance_id = $8
                 RETURNING 1
             )
             SELECT pg_notify($9, $10) FROM upd WHERE $10 IS NOT NULL",
        )
        .bind(status)
        .bind(task_id)
        .bind(task_count)
        .bind(pos_kind)
        .bind(wake_at)
        .bind(next_history_version)
        .bind(data_hash.as_slice())
        .bind(instance_id)
        .bind(TASK_READY_CHANNEL)
        .bind(notify_payload.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        append_history(
            &mut tx,
            instance_id,
            next_history_version,
            status,
            task_id,
            &data,
            &data_hash,
        )
        .await?;

        tx.commit().await.map_err(PgError)?;
        tracing::info!(instance_id, "workflow unpaused");
        Ok(snapshot)
    }
}

/// If `snapshot` is parked at `AtSignal` waiting for `signal_name`,
/// return the (signal_id, next_task_id) pair so `send_event` can
/// advance the workflow inline. Returns `None` for any other position
/// or signal-name mismatch.
fn signal_resume_target(
    snapshot: &WorkflowSnapshot,
    signal_name: &str,
) -> Option<(glirulus_core::TaskId, Option<glirulus_core::TaskId>)> {
    use glirulus_core::snapshot::{ExecutionPosition, WorkflowSnapshotState};
    match &snapshot.state {
        WorkflowSnapshotState::InProgress {
            position:
                ExecutionPosition::AtSignal {
                    signal_id,
                    signal_name: parked_name,
                    next_task_id,
                    ..
                },
            ..
        } if parked_name == signal_name => Some((*signal_id, *next_task_id)),
        _ => None,
    }
}

impl<C: codec::SnapshotCodec> PostgresBackend<C> {
    /// Admit a signal in the caller's transaction, retaining native validation.
    pub async fn store_signal_in(
        &self,
        instance_id: &str,
        kind: SignalKind,
        request: SignalRequest,
        connection: &mut sqlx::PgConnection,
    ) -> Result<(), BackendError> {
        let row = sqlx::query(
            "SELECT status FROM sayiir_workflow_snapshots
             WHERE instance_id = $1
             FOR UPDATE",
        )
        .bind(instance_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(PgError)?
        .ok_or_else(|| BackendError::NotFound(instance_id.to_string()))?;

        let status: String = row.get("status");
        validate_signal_allowed(&status, kind)?;

        sqlx::query(
            "INSERT INTO sayiir_workflow_signals (instance_id, kind, reason, requested_by)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (instance_id, kind) DO UPDATE SET
                reason = $3, requested_by = $4, created_at = now()",
        )
        .bind(instance_id)
        .bind(kind.as_ref())
        .bind(&request.reason)
        .bind(&request.requested_by)
        .execute(&mut *connection)
        .await
        .map_err(PgError)?;

        Ok(())
    }
}
