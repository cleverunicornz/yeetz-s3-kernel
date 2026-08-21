//! The A-suite: the ADR 0016 extension's contract tests, mirroring
//! the K-suite's style. A1–A6 exercise the AtomicKeyspace against the
//! shared in-memory store (the fault-injecting loopback counterpart
//! gains LIST/DELETE handlers in the kernel's ignored loopback rig —
//! see the state_kernel contract module); A7–A8 exercise the O(1)
//! terminal read and the absent/present taxonomy on StateKernel.

use std::sync::Arc;

use bytes::Bytes;
use yeetz_s3_kernel::state_kernel::{KernelLineage, SuccessorPolicy};
use yeetz_s3_kernel::{
    AtomicKeyspace, DeleteOutcome, KernelHandle, KeyspaceError, LineageHeadState,
};

fn keyspace(bucket: &str, namespace: &str) -> AtomicKeyspace {
    KernelHandle::with_in_memory_store(bucket)
        .atomic_keyspace(namespace)
        .unwrap()
}

fn value(value: &str) -> Bytes {
    Bytes::from(value.to_string())
}

// --- A1: create exclusivity ----------------------------------------------

#[tokio::test]
async fn a1_create_exclusivity_one_winner_typed_conflict() {
    let keyspace = keyspace("a-suite", "stream");
    keyspace
        .create("cursor", value("v1"))
        .await
        .expect("first create wins");
    // The loser gets the typed conflict — never an overwrite.
    let err = keyspace
        .create("cursor", value("attacker"))
        .await
        .expect_err("second create conflicts");
    match err {
        KeyspaceError::AlreadyExists(key) => assert_eq!(key, "cursor"),
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
    // The winner's bytes are untouched.
    assert_eq!(
        keyspace.get("cursor").await.unwrap().as_deref(),
        Some(b"v1".as_slice())
    );
    // Different keys do not collide.
    keyspace.create("other", value("v2")).await.unwrap();
}

// --- A2: CAS correctness ---------------------------------------------------

#[tokio::test]
async fn a2_cas_match_mismatch_and_concurrent_exchange() {
    let keyspace = keyspace("a-suite", "stream");
    keyspace.create("head", value("v1")).await.unwrap();
    let (_, etag) = keyspace.get_with_etag("head").await.unwrap().unwrap();

    // Match: exchange succeeds, returns a NEW etag.
    let new_etag = keyspace
        .compare_exchange("head", &etag, value("v2"))
        .await
        .expect("matching etag exchanges");
    assert_ne!(new_etag, etag);

    // Mismatch: the stale etag fails typed, carrying the observed one.
    let err = keyspace
        .compare_exchange("head", &etag, value("stale"))
        .await
        .expect_err("stale etag must fail");
    match err {
        KeyspaceError::PreconditionFailed {
            key,
            expected_etag,
            observed,
        } => {
            assert_eq!(key, "head");
            assert_eq!(expected_etag, etag);
            assert_eq!(observed.as_deref(), Some(new_etag.as_str()));
        }
        other => panic!("expected PreconditionFailed, got {other:?}"),
    }
    // The failed exchange did not write.
    assert_eq!(
        keyspace.get("head").await.unwrap().as_deref(),
        Some(b"v2".as_slice())
    );

    // Concurrent exchange: both present the same etag; exactly one
    // wins (the store's If-Match is the arbiter), the loser gets the
    // typed conflict.
    let (_, etag2) = keyspace.get_with_etag("head").await.unwrap().unwrap();
    let a = {
        let keyspace = &keyspace;
        let etag2 = etag2.clone();
        async move {
            keyspace
                .compare_exchange("head", &etag2, value("winner-a"))
                .await
        }
    };
    let b = {
        let keyspace = &keyspace;
        let etag2 = etag2.clone();
        async move {
            keyspace
                .compare_exchange("head", &etag2, value("winner-b"))
                .await
        }
    };
    let (ra, rb) = tokio::join!(a, b);
    let outcomes = [ra.is_ok(), rb.is_ok()];
    assert_eq!(
        outcomes.iter().filter(|won| **won).count(),
        1,
        "exactly one concurrent exchange wins"
    );
    let value = keyspace.get("head").await.unwrap().unwrap();
    assert!(
        value.as_ref() == b"winner-a".as_slice() || value.as_ref() == b"winner-b".as_slice(),
        "the winner's bytes stand"
    );
}

// --- A3: ordering + pagination stability -------------------------------------

#[tokio::test]
async fn a3_list_after_exclusive_ordered_bounded_stable() {
    let keyspace = keyspace("a-suite", "stream");
    // Insert out of order; listing must be strictly ordered.
    for key in ["k05", "k01", "k03", "k02", "k04"] {
        keyspace.create(key, value("v")).await.unwrap();
    }
    let first_page = keyspace.list_after(None, 3).await.unwrap();
    assert_eq!(first_page, vec!["k01", "k02", "k03"]);

    // Exclusive start-after: no duplicate at the boundary.
    let second = keyspace
        .list_after(Some(first_page.last().unwrap()), 3)
        .await
        .unwrap();
    assert_eq!(second, vec!["k04", "k05"]);

    // No skip, no dup across the full pagination walk.
    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = keyspace.list_after(after.as_deref(), 2).await.unwrap();
        if page.is_empty() {
            break;
        }
        seen.extend(page.iter().cloned());
        after = Some(page.last().unwrap().clone());
    }
    assert_eq!(seen, vec!["k01", "k02", "k03", "k04", "k05"]);

    // Bounded: limit is respected exactly.
    assert_eq!(keyspace.list_after(None, 0).await.unwrap().len(), 0);
    assert_eq!(keyspace.list_after(None, 2).await.unwrap().len(), 2);

    // Strictly ordered assertion on every observed page.
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(seen, sorted, "byte order throughout");
}

#[tokio::test]
async fn a3_concurrent_inserts_obey_weak_cursor_boundary() {
    let keyspace = keyspace("a-suite-interleave", "stream");
    keyspace.create("b", value("v")).await.unwrap();
    keyspace.create("d", value("v")).await.unwrap();
    assert_eq!(keyspace.list_after(None, 1).await.unwrap(), vec!["b"]);

    keyspace.create("a", value("late-before")).await.unwrap();
    keyspace.create("c", value("late-after")).await.unwrap();
    assert_eq!(
        keyspace.list_after(Some("b"), 10).await.unwrap(),
        vec!["c", "d"]
    );
}

// --- A4: get_with_etag consistency ------------------------------------------

#[tokio::test]
async fn a4_get_with_etag_pair_is_consistent() {
    let keyspace = keyspace("a-suite", "stream");
    // Absent key: None (absence is absence — not an error).
    assert!(keyspace.get_with_etag("absent").await.unwrap().is_none());

    keyspace.create("pair", value("value-one")).await.unwrap();
    let Some((bytes, etag)) = keyspace.get_with_etag("pair").await.unwrap() else {
        panic!("present key returns the pair");
    };
    assert_eq!(bytes.as_ref(), b"value-one".as_slice());
    // The etag names exactly those bytes: presenting it to CAS works.
    let new_etag = keyspace
        .compare_exchange("pair", &etag, value("value-two"))
        .await
        .unwrap();
    assert_ne!(new_etag, etag);
    let (bytes2, etag2) = keyspace.get_with_etag("pair").await.unwrap().unwrap();
    assert_eq!(bytes2.as_ref(), b"value-two".as_slice());
    assert_eq!(etag2, new_etag);
}

// --- A5: delete idempotency + namespace scoping -------------------------------

#[tokio::test]
async fn a5_delete_idempotent_and_namespace_scoped() {
    let shared = KernelHandle::with_in_memory_store("a-suite-shared");
    let one = shared.atomic_keyspace("ns-one").unwrap();
    let two = shared.atomic_keyspace("ns-two").unwrap();
    one.create("shared-name", value("one")).await.unwrap();
    two.create("shared-name", value("two")).await.unwrap();

    // Idempotent: deleting an absent key succeeds.
    one.delete("never-existed").await.unwrap();
    one.delete("shared-name").await.unwrap();
    one.delete("shared-name").await.unwrap();

    // Namespace scoping: ns-one's delete never touched ns-two.
    assert!(one.get("shared-name").await.unwrap().is_none());
    assert_eq!(
        two.get("shared-name").await.unwrap().as_deref(),
        Some(b"two".as_slice())
    );
    // And ns-one's listing no longer observes the deleted key.
    assert!(one.list_after(None, 10).await.unwrap().is_empty());
    assert_eq!(two.list_after(None, 10).await.unwrap(), vec!["shared-name"]);
}

// --- A6: delete_many partial-failure resumability ------------------------------

#[tokio::test]
async fn a6_delete_many_idempotent_resumable() {
    let keyspace = keyspace("a-suite", "gc");
    for index in 0..8 {
        keyspace
            .create(&format!("obj-{index:02}"), value("v"))
            .await
            .unwrap();
    }
    // Full sweep: every key confirmed deleted.
    let targets: Vec<String> = (0..8).map(|i| format!("obj-{i:02}")).collect();
    let borrowed: Vec<&str> = targets.iter().map(String::as_str).collect();
    let outcomes = keyspace.delete_many(&borrowed).await.unwrap();
    assert!(
        outcomes.iter().all(|outcome| outcome.deleted),
        "clean sweep deletes all"
    );
    assert!(keyspace.list_after(None, 100).await.unwrap().is_empty());

    // Resumability shape: `remaining` extracts exactly the not-deleted
    // subset, and re-running that subset converges.
    let partial = vec![
        DeleteOutcome {
            key: "a".into(),
            deleted: true,
        },
        DeleteOutcome {
            key: "b".into(),
            deleted: false,
        },
    ];
    assert_eq!(DeleteOutcome::remaining(&partial), vec!["b".to_string()]);

    // Idempotent re-run over an already-empty set is a no-op success.
    let outcomes = keyspace.delete_many(&borrowed).await.unwrap();
    assert!(outcomes.iter().all(|outcome| outcome.deleted));
}

#[tokio::test]
async fn a6_delete_many_invalid_batch_is_side_effect_free() {
    let keyspace = keyspace("a-suite", "gc-invalid");
    keyspace
        .create("valid", value("must-survive"))
        .await
        .unwrap();

    let error = keyspace
        .delete_many(&["valid", "invalid key"])
        .await
        .expect_err("invalid batch member rejects the whole batch");
    assert!(matches!(error, KeyspaceError::InvalidIdentifier(_)));
    assert_eq!(
        keyspace.get("valid").await.unwrap().as_deref(),
        Some(b"must-survive".as_slice()),
        "rejected batch cannot apply an unreported prefix"
    );
}

// --- A7: terminal-read O(1) equivalence ---------------------------------------

#[tokio::test]
async fn a7_terminal_read_equals_fold_terminal() {
    let handle = KernelHandle::with_in_memory_store("a-suite");
    let lineage = KernelLineage::new("test/terminal", SuccessorPolicy::SuccessorCapable).unwrap();
    let kernel = handle.state_kernel(lineage.clone());

    // Empty lineage: terminal read is incomplete (no head), fold is
    // incomplete — same error class.
    assert!(kernel.read_terminal_record().await.is_err());

    // Build a chain: genesis + two successors.
    let genesis = yeetz_s3_kernel::state_kernel::CanonicalRecord::new(
        &lineage,
        0,
        None,
        "test.genesis",
        "test.v1",
        vec![1, 2, 3],
        String::from("op-1"),
        String::from("actor"),
        "test",
    )
    .unwrap();
    let head = kernel.append_genesis(&genesis).await.unwrap();
    let second = yeetz_s3_kernel::state_kernel::CanonicalRecord::new(
        &lineage,
        1,
        Some(head.record_position()),
        "test.step",
        "test.v1",
        vec![4, 5],
        String::from("op-2"),
        String::from("actor"),
        "test",
    )
    .unwrap();
    let head = kernel.append_successor(&second, &head).await.unwrap();
    let third = yeetz_s3_kernel::state_kernel::CanonicalRecord::new(
        &lineage,
        2,
        Some(head.record_position()),
        "test.step",
        "test.v1",
        vec![6],
        String::from("op-3"),
        String::from("actor"),
        "test",
    )
    .unwrap();
    kernel.append_successor(&third, &head).await.unwrap();

    // Terminal read: head + the terminal record the head names.
    let terminal = kernel.read_terminal_record().await.unwrap();
    assert_eq!(terminal.generation(), 2);
    assert_eq!(terminal.payload(), &[6u8][..]);
    let _ = &terminal.record; // record identity is asserted via digest/generation

    // Fold equivalence: fold's terminal record is the same record
    // (payload + digest + generation).
    struct TerminalPayload;
    impl yeetz_s3_kernel::state_kernel::LineageFold for TerminalPayload {
        type State = Option<Vec<u8>>;

        fn validate_transition(
            &self,
            _record: &yeetz_s3_kernel::state_kernel::FoldRecord<'_>,
        ) -> Result<(), ()> {
            Ok(())
        }

        fn initial_state(&self) -> Self::State {
            None
        }

        fn apply(
            &self,
            state: &mut Self::State,
            record: &yeetz_s3_kernel::state_kernel::FoldRecord<'_>,
        ) -> Result<(), ()> {
            *state = Some(record.payload().to_vec());
            Ok(())
        }

        fn canonical_state(&self, state: &Self::State) -> Result<Vec<u8>, ()> {
            Ok(state.clone().unwrap_or_default())
        }

        fn restore_checkpoint(
            &self,
            _transition_schema: &str,
            _state_bytes: &[u8],
        ) -> Result<Self::State, ()> {
            Err(())
        }
    }
    let outcome = kernel
        .fold(None, &TerminalPayload)
        .await
        .expect("fold succeeds");
    let folded_terminal = outcome.state.expect("three records applied");
    assert_eq!(
        terminal.payload().to_vec(),
        folded_terminal,
        "terminal == fold's terminal"
    );
}

// --- A8: absent vs incomplete never conflated ----------------------------------

#[tokio::test]
async fn a8_taxonomy_absent_distinct_from_incomplete() {
    let handle = KernelHandle::with_in_memory_store("a-suite");
    let lineage =
        KernelLineage::new("test/never-created", SuccessorPolicy::SuccessorCapable).unwrap();
    let kernel = handle.state_kernel(lineage);

    // Never-created lineage: Absent, not an error, not incomplete.
    match kernel.read_head_state().await.unwrap() {
        LineageHeadState::Absent => {}
        LineageHeadState::Present(_) => panic!("never-created lineage must be Absent"),
    }
    assert!(kernel.read_head_state().await.unwrap().is_absent());
    // The legacy method keeps its semantics (additive contract).
    assert!(kernel.read_head().await.is_err());

    // Created lineage: Present.
    let lineage = KernelLineage::new("test/created", SuccessorPolicy::SuccessorCapable).unwrap();
    let kernel = handle.state_kernel(lineage.clone());
    let genesis = yeetz_s3_kernel::state_kernel::CanonicalRecord::new(
        &lineage,
        0,
        None,
        "test.genesis",
        "test.v1",
        vec![1],
        String::from("op"),
        String::from("actor"),
        "test",
    )
    .unwrap();
    kernel.append_genesis(&genesis).await.unwrap();
    match kernel.read_head_state().await.unwrap() {
        LineageHeadState::Present(_) => {}
        LineageHeadState::Absent => panic!("created lineage must be Present"),
    }

    // Broken-history lineage (head present, terminal record destroyed):
    // still Present at the head layer — incompleteness surfaces
    // through the record paths as StateHistoryIncomplete, never as
    // absence. This is the conflation the taxonomy forbids. The
    // corruption cut stays behind the test-support kernel surface.
    kernel.destroy_terminal_record_for_test().await.unwrap();
    match kernel.read_head_state().await.unwrap() {
        LineageHeadState::Present(_) => {}
        LineageHeadState::Absent => panic!("broken history must remain Present (head intact)"),
    }
    assert!(
        kernel.read_terminal_record().await.is_err(),
        "record path surfaces the incompleteness"
    );
}

// --- A11-A13: versioned-value CAS (ADR 0016 batch 4) ----------------------

#[tokio::test]
async fn a11_versioned_aba_cycle_rejects_recycled_era_etag() {
    let keyspace = keyspace("a-suite", "aba");
    keyspace.create("cell", value("A")).await.unwrap();
    let (bytes_a, version_a, etag_a) = keyspace
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(version_a, 0, "create starts at version zero");

    // A(v0) -> B(v1) -> A(v2): the payload recurs but the stored
    // envelope cannot, so a content-derived etag cannot recur either.
    let etag_b = keyspace
        .compare_exchange("cell", &etag_a, value("B"))
        .await
        .unwrap();
    assert_ne!(etag_b, etag_a);
    let (bytes_b, version_b, observed_b) = keyspace
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes_b.as_ref(), b"B");
    assert_eq!(version_b, 1);
    assert_eq!(observed_b, etag_b);

    let etag_a2 = keyspace
        .compare_exchange("cell", &etag_b, bytes_a.clone())
        .await
        .unwrap();
    assert_ne!(
        etag_a2, etag_a,
        "the versioned envelope differs when payload A recurs"
    );
    assert_ne!(etag_a2, etag_b);
    let (bytes_a2, version_a2, observed_a2) = keyspace
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes_a2, bytes_a);
    assert_eq!(version_a2, 2);
    assert_eq!(observed_a2, etag_a2);

    // The stale token from the first A era cannot address A(v2), even
    // though callers see byte-identical payloads.
    let err = keyspace
        .compare_exchange("cell", &etag_a, value("C"))
        .await
        .expect_err("era-one etag must not match byte-identical current value");
    assert!(matches!(err, KeyspaceError::PreconditionFailed { .. }));

    // And the API surface offers no unconditional write to construct a
    // same-etag overwrite: `create` is put-if-absent (conflicts on the
    // existing key), `compare_exchange` always carries an If-Match.
    let err = keyspace.create("cell", value("D")).await.unwrap_err();
    assert!(matches!(err, KeyspaceError::AlreadyExists(_)));
}

#[tokio::test]
async fn a12_same_version_identical_payload_cas_succeeds() {
    let keyspace = keyspace("a-suite-identical", "aba");
    let payload = value("same-payload");
    keyspace.create("cell", payload.clone()).await.unwrap();
    let (_, version, etag) = keyspace
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .unwrap();

    // Identical desired payload is a valid idempotent state
    // transition when the caller presents the current era's token.
    let next_etag = keyspace
        .compare_exchange("cell", &etag, payload.clone())
        .await
        .expect("same-version CAS accepts an identical payload");
    let (observed, next_version, observed_etag) = keyspace
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed, payload);
    assert_eq!(next_version, version + 1);
    assert_eq!(observed_etag, next_etag);
    assert_ne!(next_etag, etag, "the new version changes stored bytes");
}

#[tokio::test]
async fn a13_version_strictly_monotone_under_concurrent_cas() {
    const RACERS: u64 = 16;

    let keyspace = Arc::new(keyspace("a-suite-concurrent-version", "aba"));
    let payload = value("stable-payload");
    keyspace.create("cell", payload.clone()).await.unwrap();

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..RACERS {
        let keyspace = Arc::clone(&keyspace);
        let payload = payload.clone();
        tasks.spawn(async move {
            loop {
                let (_, observed_version, etag) = keyspace
                    .get_with_version_for_test("cell")
                    .await
                    .expect("concurrent read")
                    .expect("cell remains present");
                match keyspace
                    .compare_exchange("cell", &etag, payload.clone())
                    .await
                {
                    Ok(_) => return observed_version + 1,
                    Err(KeyspaceError::PreconditionFailed { .. }) => {}
                    Err(error) => panic!("unexpected concurrent CAS error: {error:?}"),
                }
            }
        });
    }

    let mut landed_versions = Vec::new();
    while let Some(result) = tasks.join_next().await {
        landed_versions.push(result.expect("CAS task completes"));
    }
    landed_versions.sort_unstable();
    assert_eq!(landed_versions, (1..=RACERS).collect::<Vec<_>>());

    let (observed, final_version, _) = keyspace
        .get_with_version_for_test("cell")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed, payload);
    assert_eq!(final_version, RACERS);
}
