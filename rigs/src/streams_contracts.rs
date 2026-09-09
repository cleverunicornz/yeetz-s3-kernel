//! Rig body: streams crate core contracts (ADR 0017). The rig proves
//! the durable promises a teardown would re-fire — concurrent
//! allocation, dense replay, damage loudness, idempotence, cursor
//! monotonicity, caller-owned creation, predecessor-expected append,
//! and fixed strict windows — over the in-memory store. Fault and
//! retention races are loopback-test territory, not rig verdicts.

use std::sync::Arc;

use yeetz_s3_kernel::KernelHandle;
use yeetz_s3_streams::{
    AppendExpectedFailure, CreateStreamError, CreateStreamOutcome, Replay, SchemaId, StableEventId,
    StreamId, Streams, StreamsError,
};

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

    // C1: caller-owned identity — Created on the first create,
    // Existing when the identical canonical genesis (same id + same
    // config bytes) already landed: a lost-response retry converges.
    let owned = StreamId::new("rig-owned-events-v1").unwrap();
    match streams.create_stream_with_id(&owned, b"rig-config").await {
        Ok(CreateStreamOutcome::Created) => {}
        _ => return Err("C1 diverged: fresh caller-ID create not Created".to_string()),
    }
    match streams.create_stream_with_id(&owned, b"rig-config").await {
        Ok(CreateStreamOutcome::Existing) => {}
        _ => return Err("C1 diverged: identical retry not Existing".to_string()),
    }
    verdicts.push("C1: caller-ID create -> Created, identical retry -> Existing".to_string());

    // C2: a valid different config under the same id is a typed
    // conflict; the incumbent genesis is never overwritten or adopted.
    match streams
        .create_stream_with_id(&owned, b"rig-config-other")
        .await
    {
        Err(CreateStreamError::ConfigurationConflict { .. }) => {}
        _ => return Err("C2 diverged: different config did not conflict".to_string()),
    }
    let incumbent = streams.read_config(&owned).await.expect("incumbent config");
    if incumbent.as_deref() == Some(b"rig-config") {
        verdicts.push(
            "C2: different config -> ConfigurationConflict, incumbent config intact".to_string(),
        );
    } else {
        return Err(format!("C2 diverged: incumbent config was {incumbent:?}"));
    }

    // E1: predecessor-expected append lands at the exact successor —
    // the genesis predecessor (seq 0) targets seq 1.
    let predecessor = streams
        .read_event(&owned, 0)
        .await
        .expect("genesis read")
        .event_ref()
        .clone();
    let first_expected = streams
        .append_expected(
            &predecessor,
            &schema,
            &StableEventId::new("expected-one").unwrap(),
            b"expected-payload-1",
        )
        .await
        .expect("append_expected exact successor");
    if first_expected.seq == 1 {
        verdicts.push(
            "E1: append_expected from genesis predecessor -> exact successor seq 1".to_string(),
        );
    } else {
        return Err(format!("E1 diverged: landed at seq {}", first_expected.seq));
    }

    // E2: the verified envelope at the receipt's seq carries exactly
    // the identity the receipt names.
    let verified = streams
        .read_event(&owned, first_expected.seq)
        .await
        .expect("verified read");
    if verified.event_ref() == &first_expected.event_ref()
        && verified.payload().as_ref() == "expected-payload-1".as_bytes()
    {
        verdicts.push("E2: verified envelope identity agrees with receipt".to_string());
    } else {
        return Err("E2 diverged: verified identity disagreed with receipt".to_string());
    }

    // E3: exact retry converges to the original receipt — before and
    // after the suffix advanced past the target.
    let retry = streams
        .append_expected(
            &predecessor,
            &schema,
            &StableEventId::new("expected-one").unwrap(),
            b"expected-payload-1",
        )
        .await
        .expect("exact retry");
    let second = streams
        .append_expected(
            &first_expected.event_ref(),
            &schema,
            &StableEventId::new("expected-two").unwrap(),
            b"expected-payload-2",
        )
        .await
        .expect("second expected append");
    let retry_late = streams
        .append_expected(
            &predecessor,
            &schema,
            &StableEventId::new("expected-one").unwrap(),
            b"expected-payload-1",
        )
        .await
        .expect("exact retry after suffix advance");
    if second.seq == 2 && retry == first_expected && retry_late == first_expected {
        verdicts.push(format!(
            "E3: exact retry converged at seq {} before and after suffix advance",
            first_expected.seq
        ));
    } else {
        return Err(format!(
            "E3 diverged: successor seq {}, retries {} / {}",
            second.seq, retry.seq, retry_late.seq
        ));
    }

    // E4: same stable id, different payload at the occupied successor
    // is the typed idempotency conflict naming the incumbent seq.
    let same_id = streams
        .append_expected(
            &predecessor,
            &schema,
            &StableEventId::new("expected-one").unwrap(),
            b"mutated-payload",
        )
        .await
        .expect_err("same id with different payload must conflict");
    match same_id.kind.as_ref() {
        AppendExpectedFailure::Storage(StreamsError::IdempotencyConflict {
            conflicting_seq,
            ..
        }) if *conflicting_seq == first_expected.seq => {}
        kind => return Err(format!("E4 diverged: {kind:?}")),
    }
    verdicts.push(format!(
        "E4: same-id payload change -> IdempotencyConflict naming seq {}",
        first_expected.seq
    ));

    // E5: a different operation at the genesis predecessor's occupied
    // successor (seq 1) is a PositionConflict naming that incumbent —
    // it cannot move to another slot: the free seq 3 stays absent and
    // the honest successor still takes it.
    let other = streams
        .append_expected(
            &predecessor,
            &schema,
            &StableEventId::new("expected-other").unwrap(),
            b"other-payload",
        )
        .await
        .expect_err("occupied successor must be a position conflict");
    match other.kind.as_ref() {
        AppendExpectedFailure::PositionConflict {
            target_seq,
            occupant,
            ..
        } if *target_seq == first_expected.seq && *occupant == first_expected.event_ref() => {}
        kind => return Err(format!("E5 diverged: {kind:?}")),
    }
    match streams.read_event(&owned, 3).await {
        Err(StreamsError::EventMissing { seq: 3, .. }) => {}
        outcome => {
            return Err(format!(
                "E5 diverged: conflicting operation moved to another slot: {outcome:?}"
            ));
        }
    }
    let third = streams
        .append_expected(
            &second.event_ref(),
            &schema,
            &StableEventId::new("expected-three").unwrap(),
            b"expected-payload-3",
        )
        .await
        .expect("third expected append");
    if third.seq == 3 {
        verdicts.push(
            "E5: occupied successor -> PositionConflict naming incumbent; seq 3 stayed free"
                .to_string(),
        );
    } else {
        return Err(format!(
            "E5 diverged: honest successor landed at seq {}",
            third.seq
        ));
    }

    // R: a fixed strict window served by pagination is complete with
    // no skipped event and stays byte-identical after the log grows
    // past it.
    let ranged = streams.create_stream(&[]).await.expect("range stream");
    for index in 1..=9u64 {
        streams
            .append(
                &ranged,
                &schema,
                &StableEventId::new(&format!("r{index}")).unwrap(),
                format!("r{index}").as_bytes(),
            )
            .await
            .expect("range append");
    }
    let fixed = walk_window(&streams, &ranged, 9, 4).await?;
    let demanded: Vec<(u64, Vec<u8>)> = (1..=9u64)
        .map(|index| (index, format!("r{index}").into_bytes()))
        .collect();
    if fixed != demanded {
        return Err(format!("R diverged: first walk returned {fixed:?}"));
    }
    for index in 10..=12u64 {
        streams
            .append(
                &ranged,
                &schema,
                &StableEventId::new(&format!("r{index}")).unwrap(),
                format!("r{index}").as_bytes(),
            )
            .await
            .expect("suffix append");
    }
    let regrown = walk_window(&streams, &ranged, 9, 4).await?;
    if regrown == fixed {
        verdicts.push(
            "R: paginated window (0,9] complete, byte-identical after suffix growth to seq 12"
                .to_string(),
        );
    } else {
        return Err("R diverged: fixed window changed after suffix growth".to_string());
    }

    Ok(verdicts)
}

/// Walk the strict window `(0, through]` in `limit`-sized pages,
/// resuming with `after_seq` set to the last returned seq. Returns
/// one `(seq, payload)` pair per served event; fails on an empty
/// page, a non-terminating walk, or a typed read error.
async fn walk_window(
    streams: &Streams,
    stream: &StreamId,
    through: u64,
    limit: usize,
) -> Result<Vec<(u64, Vec<u8>)>, String> {
    let mut walked = Vec::new();
    let mut after = 0;
    let mut pages = 0;
    loop {
        let page = streams
            .read_range(stream, after, through, limit)
            .await
            .map_err(|error| format!("read_range at after {after} failed: {error:?}"))?;
        let Some(last) = page.events.last().map(|event| event.seq()) else {
            return Err(format!("R diverged: empty page at after {after}"));
        };
        for event in &page.events {
            walked.push((event.seq(), event.payload().to_vec()));
        }
        pages += 1;
        if page.reached_end {
            return Ok(walked);
        }
        if pages > 16 {
            return Err("R diverged: pagination did not terminate".to_string());
        }
        after = last;
    }
}
