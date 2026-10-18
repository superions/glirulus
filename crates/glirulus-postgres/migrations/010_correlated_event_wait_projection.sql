-- SPDX-License-Identifier: MIT
-- Copyright (c) 2026 Daniel Scharf
-- Complete the standalone schema for correlated signal admission and wait polling.
-- Uncorrelated events retain NULL identities; consumption keeps event rows for replay.
ALTER TABLE sayiir_workflow_events
    ADD COLUMN event_id UUID,
    ADD COLUMN accepted_at TIMESTAMPTZ,
    ADD COLUMN consumed_at TIMESTAMPTZ;

-- The correlated-event INSERT uses this partial key for idempotent admission.
CREATE UNIQUE INDEX sayiir_workflow_event_identity
    ON sayiir_workflow_events (instance_id, event_id)
    WHERE event_id IS NOT NULL;

-- Snapshot saves project the active signal name for bounded ready-wait polling.
ALTER TABLE sayiir_workflow_snapshots ADD COLUMN signal_name TEXT;
