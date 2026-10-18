use bytes::Bytes;
use glirulus_core::error::BoxError;
use glirulus_runtime::serialization::JsonCodec;
use glirulus_runtime::{BranchKey, execute_continuation_async, task, workflow};

#[derive(BranchKey)]
enum Route {
    Accepted,
}

#[task(id = "macro_consumer.decorate")]
async fn decorate(input: u32) -> String {
    format!("{input}:{}", Route::Accepted.as_key())
}

#[tokio::test]
async fn public_macros_execute_with_renamed_crate_paths() -> Result<(), BoxError> {
    // Omitting codec also exercises workflow!'s generated Runtime crate path.
    let workflow: glirulus_core::workflow::SerializableWorkflow<JsonCodec, u32> = workflow! {
        name: "macro-consumer",
        steps: [decorate],
    }?;
    let output = execute_continuation_async(
        workflow.continuation(),
        Bytes::from_static(b"41"),
        &JsonCodec,
    )
    .await?;
    assert_eq!(serde_json::from_slice::<String>(&output)?, "41:accepted");
    Ok(())
}
