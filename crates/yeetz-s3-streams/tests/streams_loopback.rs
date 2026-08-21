//! Loopback-backed S-suite contracts (ADR 0017): lost-response cuts
//! (S3), damage over the real wire (S4), accelerator loss/recovery
//! (S6), stale-LIST fail-closed (S9), and the deterministic crash
//! matrix (S10). The counterpart lives in `support/loopback.rs`.

mod support;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use support::loopback::{FaultPhase, Loopback, StorageOp};
use support::streams_on_store;
use yeetz_s3_streams::{Replay, SchemaId, StableEventId, Streams};

fn schema() -> SchemaId {
    SchemaId::new("loopback.v1").unwrap()
}

fn event(value: &str) -> StableEventId {
    StableEventId::new(value).unwrap()
}

fn log_key(stream: &yeetz_s3_streams::StreamId, seq: u64) -> String {
    // The counterpart sees the PHYSICAL key (kernel keyspace root
    // included).
    format!("keyspace/streams/v1/{}/log/{seq:020}", stream.as_str())
}

fn seeded_event(stream: &yeetz_s3_streams::StreamId, seq: u64, event_id: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "format_version": 1,
            "stream_id": stream.as_str(),
            "seq": seq,
            "stable_event_id": event_id,
            "schema_id": "loopback.v1",
            "payload_len": 0,
            "payload_sha256": hex::encode(Sha256::digest([])),
            "payload": "",
        }))
        .unwrap(),
    )
}

async fn counterpart_streams() -> (Loopback, Streams) {
    let loopback = Loopback::start().await;
    let streams = streams_on_store(&loopback.kernel());
    (loopback, streams)
}

/// S3 (lost-response leg): an append whose create PUT lands but loses
/// its response errors to the caller; the byte-identical retry
/// converges to the original receipt — one event, no duplicate.
#[tokio::test]
async fn s3_lost_response_converges_on_retry() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    // Cut the seq-1 create PUT: applied server-side, response lost.
    loopback
        .arm_fault(
            StorageOp::Put,
            Some(&log_key(&stream, 1)),
            FaultPhase::After,
        )
        .await;
    let first = streams
        .append(&stream, &schema(), &event("lost-then-found"), b"payload")
        .await;
    assert!(first.is_err(), "the lost response surfaces as an error");
    // The object IS stored server-side: read it back raw.
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    assert!(
        keyspace
            .get(&format!("{}/log/00000000000000000001", stream.as_str()))
            .await
            .unwrap()
            .is_some(),
        "the cut append applied server-side"
    );
    // The retry converges to the original receipt.
    let receipt = streams
        .append(&stream, &schema(), &event("lost-then-found"), b"payload")
        .await
        .unwrap();
    assert_eq!(receipt.seq, 1, "retry returns the original seq");
    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert!(complete);
            assert_eq!(events.len(), 1, "no duplicate from the retry");
            assert_eq!(events[0].stable_event_id.as_str(), "lost-then-found");
        }
        other => panic!("expected page, got {other:?}"),
    }
    loopback.shutdown();
}

/// A verified hint is an allocation floor even when LIST lags by more
/// than the append collision budget. The hinted event remains inside
/// the bounded retry scan, and a fresh event starts after it.
#[tokio::test]
async fn verified_hint_floors_retry_and_fresh_allocation() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    loopback.freeze_list().await;

    let mut terminal = Bytes::new();
    for seq in 1..=70u64 {
        terminal = seeded_event(&stream, seq, &format!("seed-{seq}"));
        keyspace
            .create(
                &format!("{}/log/{seq:020}", stream.as_str()),
                terminal.clone(),
            )
            .await
            .unwrap();
    }
    keyspace
        .create(
            &format!("{}/tail", stream.as_str()),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "format_version": 1,
                    "highest_validated_dense_seq": 70,
                    "terminal_record_digest": hex::encode(Sha256::digest(&terminal)),
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();

    let retry = streams
        .append(&stream, &schema(), &event("seed-70"), &[])
        .await
        .unwrap();
    assert_eq!(retry.seq, 70);
    let fresh = streams
        .append(&stream, &schema(), &event("fresh-71"), &[])
        .await
        .unwrap();
    assert_eq!(fresh.seq, 71);
    loopback.shutdown();
}

/// S4 (loopback leg): damage over the real wire is loud and named.
#[tokio::test]
async fn s4_loopback_damage_is_named() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=5u64 {
        streams
            .append(&stream, &schema(), &event(&format!("wire-{index}")), &[])
            .await
            .unwrap();
    }
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    keyspace
        .delete(&format!("{}/log/00000000000000000004", stream.as_str()))
        .await
        .unwrap();
    match streams.read(&stream, 0, 100).await {
        Replay::Corrupt {
            missing_or_mismatched,
        } => {
            assert_eq!(missing_or_mismatched, vec![4]);
        }
        other => panic!("expected Corrupt over the wire, got {other:?}"),
    }
    loopback.shutdown();
}

/// S6: accelerator loss and recovery. Deleting the tail hint costs
/// performance only — and, per the ruled completeness contract,
/// WITHHOLDS `complete=true` until a witness is recovered: the
/// first full read heals the hint (complete=false, no witness), the
/// next certifies; a partial read rebuilds it by exponential probe
/// + binary search.
#[tokio::test]
async fn s6_accelerator_loss_and_recovery() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=5u64 {
        streams
            .append(&stream, &schema(), &event(&format!("h{index}")), &[])
            .await
            .unwrap();
    }
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    let tail_key = format!("{}/tail", stream.as_str());
    // Full function with the accelerator deleted: every event is
    // served, but completeness is withheld — no verified witness
    // (ruled contract).
    keyspace.delete(&tail_key).await.unwrap();
    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            complete, events, ..
        } => {
            assert!(!complete, "no verified witness -> complete withheld");
            assert_eq!(events.len(), 5);
        }
        other => panic!("expected page without accelerator, got {other:?}"),
    }
    // The read healed the hint: seq 5, digest of envelope 5.
    let healed = keyspace
        .get(&tail_key)
        .await
        .unwrap()
        .expect("hint rewritten");
    let hint: serde_json::Value = serde_json::from_slice(&healed).unwrap();
    assert_eq!(hint["highest_validated_dense_seq"], serde_json::json!(5));
    // With the recovered witness, the same read certifies.
    match streams.read(&stream, 0, 100).await {
        Replay::Page { complete, .. } => assert!(complete, "recovered witness certifies"),
        other => panic!("expected certified page after heal, got {other:?}"),
    }

    // Partial-read rebuild: delete again, read a prefix page — the
    // binary-search probe rebuilds to the TRUE tail (5), not the page
    // end (2).
    keyspace.delete(&tail_key).await.unwrap();
    match streams.read(&stream, 0, 2).await {
        Replay::Page { complete, .. } => assert!(!complete),
        other => panic!("expected partial page, got {other:?}"),
    }
    let recovered = keyspace
        .get(&tail_key)
        .await
        .unwrap()
        .expect("hint recovered by probe");
    let hint: serde_json::Value = serde_json::from_slice(&recovered).unwrap();
    assert_eq!(
        hint["highest_validated_dense_seq"],
        serde_json::json!(5),
        "probe rebuilds to the true tail"
    );
    loopback.shutdown();
}

/// S9: stale-LIST degradation — under-report only, fail-closed on
/// contradicted witnesses. (a) A digest-verified hint above the
/// LIST-qualified end refuses a false complete. (b) A GET-absent key
/// the LIST still reports fails closed.
#[tokio::test]
async fn s9_stale_list_fails_closed() {
    // (a) Frozen LIST under-reports past the page end while a
    // verified hint witnesses an event beyond it.
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema(), &event("s1"), &[])
        .await
        .unwrap();
    streams
        .append(&stream, &schema(), &event("s2"), &[])
        .await
        .unwrap();
    loopback.freeze_list().await; // listing now shows only seq 0..=2
    streams
        .append(&stream, &schema(), &event("s3"), &[])
        .await
        .unwrap();
    streams
        .append(&stream, &schema(), &event("s4"), &[])
        .await
        .unwrap();
    // A partial page ending at 2 with a verified hint at 4: the stale
    // LIST would grant a false complete — refused.
    match streams.read(&stream, 1, 1).await {
        Replay::BackendUnqualified { witness } => {
            assert!(witness.contains("stale LIST"), "witness: {witness}");
        }
        other => panic!("expected BackendUnqualified on contradicted hint, got {other:?}"),
    }
    // Under-report-only degradation: the same frozen LIST still
    // serves a read whose page end the hint agrees with (hint ≤ end).
    match streams.read(&stream, 0, 4).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert_eq!(events.len(), 4, "GETs are not degraded by the frozen LIST");
            assert!(complete, "nothing truly exists past 4");
        }
        other => panic!("expected qualified complete page, got {other:?}"),
    }
    loopback.shutdown();

    // (b) A hidden key: GET says absent at seq 2, LIST still reports
    // it — contradictory witnesses fail closed.
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema(), &event("c1"), &[])
        .await
        .unwrap();
    streams
        .append(&stream, &schema(), &event("c2"), &[])
        .await
        .unwrap();
    streams
        .append(&stream, &schema(), &event("c3"), &[])
        .await
        .unwrap();
    loopback.hide_key(&log_key(&stream, 2)).await;
    match streams.read(&stream, 0, 100).await {
        Replay::BackendUnqualified { witness } => {
            assert!(witness.contains("contradicts"), "witness: {witness}");
        }
        other => panic!("expected BackendUnqualified on GET/LIST contradiction, got {other:?}"),
    }
    loopback.shutdown();
}

/// G130 (ruled completeness contract): a frozen LIST plus a deleted
/// tail hint must never yield a false complete.
/// (a) The false-completeness hole itself: a limit-cut read (page
///     ends before the LIST-hidden suffix) with no witness serves
///     `complete: false`.
/// (b) The withheld read recovers a GET-verified witness at the
///     TRUE tail; with the LIST still frozen, the follow-up
///     limit-cut read fails CLOSED (BackendUnqualified) instead of
///     false-completing.
/// (c) A full read with the witness gone also withholds
///     completeness — always, even when the page happens to reach
///     the true tail; the recovered witness lets the next read
///     certify.
#[tokio::test]
async fn g130_frozen_list_without_witness_withholds_completeness() {
    // (a) + (b): the limit-cut shape — today's false-complete hole.
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema(), &event("g1"), &[])
        .await
        .unwrap();
    streams
        .append(&stream, &schema(), &event("g2"), &[])
        .await
        .unwrap();
    loopback.freeze_list().await; // listing frozen at 0..=2
    streams
        .append(&stream, &schema(), &event("g3"), &[])
        .await
        .unwrap();
    streams
        .append(&stream, &schema(), &event("g4"), &[])
        .await
        .unwrap();
    // Delete the only witness.
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    keyspace
        .delete(&format!("{}/tail", stream.as_str()))
        .await
        .unwrap();
    match streams.read(&stream, 0, 2).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert_eq!(events.len(), 2);
            assert!(
                !complete,
                "G130: no verified witness -> completeness withheld"
            );
        }
        other => panic!("expected page, got {other:?}"),
    }
    // The withheld read recovered a GET-verified witness at the true
    // tail (4); with the LIST still frozen, the follow-up limit-cut
    // read fails closed on the contradiction.
    match streams.read(&stream, 0, 2).await {
        Replay::BackendUnqualified { witness } => {
            assert!(witness.contains("stale LIST"), "witness: {witness}");
        }
        other => panic!("expected BackendUnqualified on recovered witness, got {other:?}"),
    }
    loopback.shutdown();

    // (c) full-read shape: the page reaches the true tail via GETs,
    // but with the witness gone completeness stays withheld.
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    streams
        .append(&stream, &schema(), &event("f1"), &[])
        .await
        .unwrap();
    streams
        .append(&stream, &schema(), &event("f2"), &[])
        .await
        .unwrap();
    loopback.freeze_list().await;
    streams
        .append(&stream, &schema(), &event("f3"), &[])
        .await
        .unwrap();
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    keyspace
        .delete(&format!("{}/tail", stream.as_str()))
        .await
        .unwrap();
    match streams.read(&stream, 0, 100).await {
        Replay::Page {
            events, complete, ..
        } => {
            assert_eq!(events.len(), 3, "GETs are not degraded by the frozen LIST");
            assert!(!complete, "no verified witness -> complete=false always");
        }
        other => panic!("expected page, got {other:?}"),
    }
    match streams.read(&stream, 0, 100).await {
        Replay::Page { complete, .. } => assert!(complete, "recovered witness certifies"),
        other => panic!("expected certified page, got {other:?}"),
    }
    loopback.shutdown();
}

#[tokio::test]
async fn tail_recovery_get_failure_is_not_treated_as_absence() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    for index in 1..=2u64 {
        streams
            .append(&stream, &schema(), &event(&format!("r{index}")), &[])
            .await
            .unwrap();
    }
    loopback.freeze_list().await;
    for index in 3..=4u64 {
        streams
            .append(&stream, &schema(), &event(&format!("r{index}")), &[])
            .await
            .unwrap();
    }
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    let tail_key = format!("{}/tail", stream.as_str());
    keyspace.delete(&tail_key).await.unwrap();
    loopback
        .arm_fault(
            StorageOp::Get,
            Some(&log_key(&stream, 3)),
            FaultPhase::Before,
        )
        .await;

    match streams.read(&stream, 0, 2).await {
        Replay::Page { complete, .. } => assert!(!complete),
        other => panic!("expected uncertified page, got {other:?}"),
    }
    assert!(
        keyspace.get(&tail_key).await.unwrap().is_none(),
        "failed recovery must not publish a low witness"
    );
    match streams.read(&stream, 0, 2).await {
        Replay::Page { complete, .. } => assert!(!complete),
        other => panic!("expected recovery retry to remain uncertified, got {other:?}"),
    }
    loopback.shutdown();
}

/// A verified but lagging hint proves only its own prefix. It cannot
/// certify a page cut there when a frozen LIST hides a later suffix.
#[tokio::test]
async fn g130_lagging_witness_cannot_certify_hidden_suffix() {
    let (loopback, streams) = counterpart_streams().await;
    let stream = streams.create_stream(&[]).await.unwrap();
    for id in ["l1", "l2"] {
        streams
            .append(&stream, &schema(), &event(id), &[])
            .await
            .unwrap();
    }
    let keyspace = loopback.kernel().atomic_keyspace("streams/v1").unwrap();
    let tail_key = format!("{}/tail", stream.as_str());
    let stale_hint = keyspace.get(&tail_key).await.unwrap().unwrap();
    loopback.freeze_list().await;
    for id in ["l3", "l4"] {
        streams
            .append(&stream, &schema(), &event(id), &[])
            .await
            .unwrap();
    }
    let (_, etag) = keyspace.get_with_etag(&tail_key).await.unwrap().unwrap();
    keyspace
        .compare_exchange(&tail_key, &etag, stale_hint)
        .await
        .unwrap();

    match streams.read(&stream, 0, 2).await {
        Replay::BackendUnqualified { witness } => {
            assert!(
                witness.contains("GET-visible successor"),
                "witness: {witness}"
            );
        }
        other => panic!("expected lagging witness to fail closed, got {other:?}"),
    }
    loopback.shutdown();
}

/// S10: deterministic multi-operation histories — crash cuts at every
/// storage op (before/after/lost-response), each followed by the
/// client retry a real caller performs, converge to the sequential
/// spec exactly.
#[tokio::test]
async fn s10_crash_matrix_converges_to_sequential_spec() {
    // Dry run: how many storage requests does the history make?
    let total = {
        let (loopback, streams) = counterpart_streams().await;
        run_history(&streams, None).await;
        let total = loopback.request_count();
        loopback.shutdown();
        total
    };
    assert!(
        total >= 8,
        "history exercises a meaningful op set (got {total})"
    );

    for cut_index in 0..total {
        for phase in [FaultPhase::Before, FaultPhase::After] {
            let (loopback, streams) = counterpart_streams().await;
            loopback.arm_fault_at_index(cut_index, phase).await;
            let verdict = run_history(&streams, Some(phase)).await;
            assert!(
                verdict.ok,
                "cut at storage op {cut_index} ({phase:?}) diverged: {:?}",
                verdict.detail
            );
            assert!(
                loopback.fault_fired(),
                "cut at index {cut_index} ({phase:?}) never fired — dry-run op drift"
            );
            loopback.shutdown();
        }
    }
}

/// One driver-level history op.
enum Step {
    Create,
    Append(&'static str, &'static [u8]),
    Cursor,
    Read,
}

struct Verdict {
    ok: bool,
    detail: String,
}

async fn run_history(streams: &Streams, cut_phase: Option<FaultPhase>) -> Verdict {
    let steps = [
        Step::Create,
        Step::Append("alpha", b"a-payload"),
        Step::Append("beta", b"b-payload"),
        Step::Cursor,
        Step::Read,
    ];
    // Build the concrete sequence with receipts threaded through.
    let mut stream: Option<yeetz_s3_streams::StreamId> = None;
    let mut alpha_seq: Option<u64> = None;
    let mut events: Vec<String> = Vec::new();
    let mut cursor_seq: Option<u64> = None;
    let mut cursor_target = 0u64;
    for (index, step) in steps.iter().enumerate() {
        match step {
            Step::Create => {
                // create_stream is the non-idempotent boundary: a cut
                // genesis may or may not persist; a fresh mint on
                // retry is a NEW stream — the spec tracks the final
                // acknowledged one.
                for _ in 0..3 {
                    match streams.create_stream(&[]).await {
                        Ok(id) => {
                            stream = Some(id);
                            break;
                        }
                        Err(_) => continue,
                    }
                }
            }
            Step::Append(id, payload) => {
                let Some(stream) = stream.clone() else {
                    break;
                };
                // A crashed append is retried with identical inputs —
                // idempotent convergence is the contract under test.
                for _ in 0..3 {
                    if let Ok(receipt) = streams
                        .append(&stream, &schema().clone(), &event(id), payload)
                        .await
                    {
                        if index == 1 {
                            alpha_seq = Some(receipt.seq);
                            cursor_target = receipt.seq;
                        }
                        break;
                    }
                }
            }
            Step::Cursor => {
                let Some(stream) = stream.clone() else {
                    break;
                };
                for _ in 0..3 {
                    if let Ok(cursor) = streams
                        .advance_cursor(&stream, "s10-worker", cursor_target)
                        .await
                    {
                        cursor_seq = Some(cursor.seq);
                        break;
                    }
                    // The target may not exist yet if an append never
                    // landed (Before cuts retried clean, so it did).
                }
            }
            Step::Read => {
                let Some(stream) = stream.clone() else {
                    break;
                };
                for _ in 0..3 {
                    match streams.read(&stream, 0, 100).await {
                        Replay::Page {
                            events: fetched,
                            complete,
                            ..
                        } => {
                            events = fetched
                                .iter()
                                .map(|e| e.stable_event_id.as_str().to_string())
                                .collect();
                            let _ = complete;
                            break;
                        }
                        Replay::Empty => break,
                        _ => continue, // retry after a cut
                    }
                }
            }
        }
    }
    let _ = cut_phase;
    // Sequential spec: [alpha, beta], cursor at alpha, dense read.
    let ok_stream = stream.is_some();
    let ok_events = events == vec!["alpha".to_string(), "beta".to_string()];
    let ok_cursor = cursor_seq.is_some_and(|seq| Some(seq) == alpha_seq);
    Verdict {
        ok: ok_stream && ok_events && ok_cursor,
        detail: format!(
            "events={events:?} cursor={cursor_seq:?} alpha={alpha_seq:?} stream={ok_stream}"
        ),
    }
}

#[tokio::test]
async fn loopback_list_zero_max_keys_is_empty_and_not_truncated() {
    let loopback = Loopback::start().await;
    let streams = streams_on_store(&loopback.kernel());
    let _stream = streams.create_stream(&[]).await.unwrap();
    let body = reqwest::Client::new()
        .get(format!(
            "{}/{}?list-type=2&prefix=&max-keys=0",
            loopback.endpoint,
            support::loopback::BUCKET
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("<KeyCount>0</KeyCount>"));
    assert!(body.contains("<IsTruncated>false</IsTruncated>"));
    assert!(!body.contains("<NextContinuationToken>"));
    loopback.shutdown();
}
