//! Batch-3 constructor contracts: the adapter is created inside the
//! kernel closure and one opaque handle binds shared kernel surfaces.

use bytes::Bytes;
use yeetz_s3_kernel::state_kernel::{CanonicalRecord, KernelLineage, SuccessorPolicy};
use yeetz_s3_kernel::{KernelHandle, S3Config};

#[tokio::test]
async fn h1_in_memory_handle_binds_shared_lineages_and_keyspaces() {
    let handle = KernelHandle::with_in_memory_store("handle-contract");
    let lineage = KernelLineage::new("handle/shared", SuccessorPolicy::SuccessorCapable).unwrap();
    let writer = handle.state_kernel(lineage.clone());
    let record = CanonicalRecord::new(
        &lineage,
        0,
        None,
        "handle.create",
        "yeetz.handle.v1",
        b"shared".to_vec(),
        "handle-contract",
        "yeetz-s3-kernel",
        "batch-3",
    )
    .unwrap();
    writer.append_genesis(&record).await.unwrap();

    let reader = handle.state_kernel(lineage);
    assert_eq!(
        reader.read_terminal_record().await.unwrap().payload(),
        b"shared"
    );

    let left = handle.atomic_keyspace("handle-left").unwrap();
    let right = handle.atomic_keyspace("handle-left").unwrap();
    left.create("key", Bytes::from_static(b"value"))
        .await
        .unwrap();
    assert_eq!(
        right.get("key").await.unwrap().unwrap(),
        Bytes::from_static(b"value")
    );
}

#[test]
fn h2_config_constructor_keeps_adapter_failure_inside_kernel() {
    let config = S3Config::custom(
        "handle-contract",
        "us-east-1",
        "http://127.0.0.1:1",
        "public-key",
        "never-print-this-secret",
    );
    let error = KernelHandle::from_s3_config(&config).unwrap_err();
    let rendered = error.to_string();
    assert!(rendered.contains("kernel store initialization failed"));
    assert!(!rendered.contains("never-print-this-secret"));
}
