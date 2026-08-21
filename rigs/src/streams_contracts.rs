//! Rig body: streams crate core contracts (ADR 0017). The rig proves
//! the durable promises a teardown would re-fire — concurrent
//! allocation, dense replay, damage loudness, idempotence, cursor
//! monotonicity — over the in-memory store.

use std::sync::Arc;

use yeetz_s3_kernel::KernelHandle;
use yeetz_s3_streams::{Replay, SchemaId, StableEventId, Streams};

pub async fn run() -> Result<Vec<String>, String> {
    let mut verdicts = Vec::new();
    let kernel = KernelHandle::with_in_memory_store("streams-rig");
    let streams = Arc::new(Streams::new(&kernel).expect("streams"));
    let schema = SchemaId::new("rig.event.v1").unwrap();
    let stream = streams.create_stream(&[]).await.expect("create stream");

    // S1: 8 concurrent appends — one winner per seq, dense log.
    let mut tasks = Vec::new();
    for index in 1..=8u64 {
        let streams = Arc::clone(&streams);
        let stream = stream.clone();
        let schema = schema.clone();
        tasks.push(tokio::spawn(async move {
            streams
                .append(
                    &stream,
                    &schema,
                    &StableEventId::new(&format!("e{index}")).unwrap(),
                    format!("p{index}").as_bytes(),
                )
                .await
                .expect("append")
                .seq
        }));
    }
    let mut seqs: Vec<u64> = futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|joined| joined.expect("task"))
        .collect();
    seqs.sort_unstable();
    if seqs == (1..=8).collect::<Vec<_>>() {
        verdicts.push("S1: 8 concurrent appends -> distinct seqs 1..=8".to_string());
    } else {
        return Err(format!("S1 diverged: {seqs:?}"));
    }

    // S2: dense replay, LIST-qualified complete.
    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            events,
            complete: true,
            ..
        } if events.len() == 8 => {
            verdicts.push("S2: dense replay of 8 events, complete".to_string());
        }
        other => return Err(format!("S2 diverged: {other:?}")),
    }

    // S4: damage is loud and named — delete a mid-log object through
    // the kernel keyspace and read.
    let keyspace = kernel.atomic_keyspace("streams/v1").unwrap();
    keyspace
        .delete(&format!("{}/log/00000000000000000004", stream.as_str()))
        .await
        .unwrap();
    match streams.read(&stream, 0, 100).await {
        Replay::Corrupt {
            missing_or_mismatched,
        } if missing_or_mismatched == vec![4] => {
            verdicts.push("S4: mid-log deletion -> Corrupt naming seq 4".to_string());
        }
        other => return Err(format!("S4 diverged: {other:?}")),
    }

    // Fresh stream for the post-damage legs (the one above is
    // deliberately corrupt now).
    let stream = streams.create_stream(&[]).await.expect("second stream");

    // S3: idempotent re-append converges to the original receipt.
    let first = streams
        .append(&stream, &schema, &StableEventId::new("idem").unwrap(), b"x")
        .await
        .expect("append");
    let retry = streams
        .append(&stream, &schema, &StableEventId::new("idem").unwrap(), b"x")
        .await
        .expect("re-append");
    if first.seq == retry.seq {
        verdicts.push(format!(
            "S3: idempotent re-append converged at seq {}",
            first.seq
        ));
    } else {
        return Err(format!("S3 diverged: {} vs {}", first.seq, retry.seq));
    }

    // Cursor: monotonic advance + idempotent convergence.
    streams
        .advance_cursor(&stream, "rig-worker", first.seq)
        .await
        .expect("cursor");
    let again = streams
        .advance_cursor(&stream, "rig-worker", first.seq)
        .await
        .expect("idempotent cursor");
    if again.seq == first.seq {
        verdicts.push(format!(
            "cursor: monotonic + idempotent at seq {}",
            again.seq
        ));
    } else {
        return Err("cursor diverged".to_string());
    }

    Ok(verdicts)
}
