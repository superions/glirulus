use glirulus_core::builder::WorkflowBuilder;
use glirulus_core::codec::Decoder;
use glirulus_core::context::WorkflowContext;
use glirulus_core::error::BoxError;
use glirulus_core::workflow::WorkflowStatus;
use glirulus_runtime::CheckpointingRunner;
use glirulus_runtime::persistence::{InMemoryBackend, SnapshotStore};
use glirulus_runtime::serialization::JsonCodec;
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let context = WorkflowContext::new("basic", Arc::new(JsonCodec), Arc::new(()));
    let workflow = WorkflowBuilder::new(context)
        .then("increment", |input: u32| async move { Ok(input + 1) })
        .build()?;

    // This backend is process-local. Use glirulus-postgres for durable storage.
    let runner = CheckpointingRunner::new(InMemoryBackend::new());
    let status = runner.run(&workflow, "basic", 41u32).await?;
    if !matches!(status, WorkflowStatus::Completed) {
        return Err(format!("expected a completed workflow, got {status:?}").into());
    }

    let snapshot = runner.backend().load_snapshot("basic").await?;
    let output = snapshot
        .final_output_bytes()
        .ok_or("completed workflow has no final output")?;
    let result: u32 = JsonCodec.decode(output)?;
    println!("{result}");
    Ok(())
}
