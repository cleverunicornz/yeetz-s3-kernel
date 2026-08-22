//! Kernel-owned real-backend ABA probe — human rulings #3 and #4 (ADR
//! 0016/0017 addenda, teardown finding G155). This bounded helper
//! measures the backend's raw etag behavior, then proves that the
//! AtomicKeyspace value envelope closes content-etag recurrence across
//! module-level CAS eras:
//!
//! - etag recurrence for identical bytes (delete + rewrite, and the
//!   A→B→A content cycle),
//! - conditional-PUT semantics (If-None-Match create race; If-Match
//!   CAS with correct/stale etags; CAS against absent; the ABA case:
//!   CAS with the era-1 etag against a recreated era-2 object of
//!   identical bytes),
//! - LIST-after-write visibility (the strong-LIST qualification,
//!   sampled),
//! - a conditional-multipart capability battery: CompleteMultipartUpload
//!   with If-Match, abort/incomplete-upload visibility, and
//!   GetObject-by-part,
//! - the same A→B→A cycle through AtomicKeyspace, including versions,
//!   stale-token rejection, and an identical-payload transition, plus
//!   the batch-7 cross-deletion leg: destroy → re-create identical
//!   bytes → era-1 token rejection and the era-2 incarnation/version.
//!
//! Writes only under `aba-probe/<run-id>/…` and
//! `keyspace/aba-probe/<run-id>/…`, then deletes everything it created
//! before exiting (cleanup is asserted, not assumed).

use std::sync::Arc;

use bytes::Bytes;
use yeetz_sdk_s3::{ObjectStoreClient, ObjectStoreError, S3Config};

use crate::state_kernel::{
    CanonicalRecord, KernelError, KernelLineage, StateKernel, SuccessorPolicy,
};
use crate::{AtomicKeyspace, KEYSPACE_ROOT, KeyspaceError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Parallel racers for the If-None-Match create probe.
const CREATE_RACERS: usize = 8;

/// S3 requires every non-final multipart part to be at least 5 MiB.
const MIN_MULTIPART_PART_BYTES: usize = 5 * 1024 * 1024;

pub async fn run_real_s3_aba_probe(config: &S3Config) -> Result<Vec<String>, String> {
    let client = Arc::new(ObjectStoreClient::new(config).map_err(|e| format!("client: {e}"))?);

    // Run-scoped prefix so concurrent/past runs never collide.
    let run_id = mint_run_id();
    let prefix = format!("aba-probe/{run_id}/");
    let module_namespace = format!("aba-probe/{run_id}");
    let module_prefix = format!("{KEYSPACE_ROOT}/{module_namespace}/");
    let streaming_namespace = format!("aba-probe/{run_id}-streaming");
    let streaming_prefix = format!("{KEYSPACE_ROOT}/{streaming_namespace}/");
    let streaming_chunk_prefix = format!("keyspace-chunks/v1/{streaming_namespace}/");
    let lineage_name = format!("aba-probe/{run_id}-lineage");
    let lineage_prefix = format!("{lineage_name}/");
    let mut created: Vec<String> = Vec::new();
    let mut multipart_uploads: Vec<(String, String)> = Vec::new();
    let mut verdicts: Vec<String> = Vec::new();

    let result = async {
        battery(&client, &prefix, &mut created, &mut verdicts).await?;
        multipart_battery(
            &client,
            &prefix,
            &mut created,
            &mut multipart_uploads,
            &mut verdicts,
        )
        .await?;
        module_battery(
            Arc::clone(&client),
            &module_namespace,
            &mut created,
            &mut verdicts,
        )
        .await?;
        streaming_battery(Arc::clone(&client), &streaming_namespace, &mut verdicts).await?;
        lineage_battery(
            Arc::clone(&client),
            &lineage_name,
            &mut created,
            &mut verdicts,
        )
        .await
    }
    .await;

    // Cleanup regardless of verdict — abort every upload id we initiated,
    // delete exactly the objects we created, then assert every run-scoped
    // prefix is gone. Completed/already-aborted upload ids reject abort;
    // those errors are expected and the list witness below decides whether
    // any in-progress upload remains.
    for (key, upload_id) in &multipart_uploads {
        let _ = client.abort_multipart_upload(key, upload_id).await;
    }
    for key in &created {
        let _ = client.delete(key).await;
    }
    match client.list_multipart_uploads_for_test(&prefix).await {
        Ok(leftover) if !leftover.is_empty() => {
            return Err(format!(
                "multipart cleanup failed, uploads remain under {prefix}: {leftover:?}"
            ));
        }
        Ok(_) => note(
            &mut verdicts,
            format!("multipart cleanup: no in-progress uploads under {prefix}"),
        ),
        Err(error) => note(
            &mut verdicts,
            format!(
                "multipart cleanup visibility: UNWITNESSED (ListMultipartUploads failed: {error})"
            ),
        ),
    }
    for cleanup_prefix in [
        &prefix,
        &module_prefix,
        &streaming_prefix,
        &streaming_chunk_prefix,
        &lineage_prefix,
    ] {
        let leftover = client
            .list_prefix(cleanup_prefix)
            .await
            .map_err(|e| format!("cleanup list {cleanup_prefix}: {e}"))?;
        if !leftover.is_empty() {
            return Err(format!(
                "cleanup failed, keys remain under {cleanup_prefix}: {leftover:?}"
            ));
        }
    }
    note(
        &mut verdicts,
        format!("cleanup: {prefix}, {module_prefix} and {lineage_prefix} empty after run"),
    );

    result?;
    Ok(verdicts)
}

/// Record a verdict row: streamed to stdout as measured (a hazard
/// failure mid-battery still leaves the full table in the run log)
/// and collected for the return value.
fn note(verdicts: &mut Vec<String>, row: String) {
    println!("{row}");
    verdicts.push(row);
}

async fn battery(
    client: &ObjectStoreClient,
    prefix: &str,
    created: &mut Vec<String>,
    verdicts: &mut Vec<String>,
) -> Result<(), String> {
    // Distinct, non-trivial payloads (64B — single PUT, the exact
    // write path the current keyspace uses). Multipart behavior is
    // measured separately below.
    let payload_a: Bytes = Bytes::from(vec![0xA5; 64]);
    let payload_b: Bytes = Bytes::from(vec![0x5A; 64]);
    let payload_c: Bytes = Bytes::from((0u8..64).collect::<Vec<u8>>());

    // --- 1. Etag recurrence: delete + rewrite identical bytes --------
    let k1 = format!("{prefix}etag-recur");
    created.push(k1.clone());
    let e1 = create_etag(client, &k1, &payload_a).await?;
    client
        .delete(&k1)
        .await
        .map_err(|e| format!("delete k1: {e}"))?;
    let e2 = create_etag(client, &k1, &payload_a).await?;
    note(
        verdicts,
        if e1 == e2 {
            format!("etag recurrence (delete + rewrite identical bytes): YES e1==e2=={e1}")
        } else {
            format!("etag recurrence (delete + rewrite identical bytes): NO (e1={e1}, e2={e2})")
        },
    );

    // --- 2. Content cycle A→B→A at one key (sequential overwrites) ----
    let k2 = format!("{prefix}cycle-aba");
    created.push(k2.clone());
    let ea1 = create_etag(client, &k2, &payload_a).await?;
    let eb = cas_etag(client, &k2, &payload_b, &ea1).await?;
    let ea2 = cas_etag(client, &k2, &payload_a, &eb).await?;
    // Distinct content MUST carry a distinct etag, else CAS is
    // meaningless on this backend outright (surprise, fail loud).
    if eb == ea1 {
        return Err(format!(
            "cycle: distinct content shares an etag ({ea1}) — CAS unusable"
        ));
    }
    note(
        verdicts,
        if ea1 == ea2 {
            format!(
                "etag cycle A→B→A: A recurs ({ea1} == {ea2}); B differs ({eb}) — content-hash etag scheme"
            )
        } else {
            format!("etag cycle A→B→A: fresh etag per write (A: {ea1} != {ea2}; B: {eb})")
        },
    );
    if ea1 == ea2 {
        match client
            .upload_conditional(&k2, payload_c.clone(), Some(&ea1))
            .await
        {
            Ok(_) => note(
                verdicts,
                format!(
                    "raw A→B→A ABA hazard: era-1 etag {ea1} recurred and its stale If-Match was ACCEPTED"
                ),
            ),
            Err(error) => note(
                verdicts,
                format!(
                    "raw A→B→A defense: era-1 etag {ea1} recurred but its If-Match was rejected ({error})"
                ),
            ),
        }
    } else {
        match client
            .upload_conditional(&k2, payload_c.clone(), Some(&ea1))
            .await
        {
            Err(error) if error.to_string().contains("CAS failed") => note(
                verdicts,
                "raw A→B→A control: non-recurring era-1 etag rejected".to_string(),
            ),
            Err(error) => {
                return Err(format!(
                    "raw A→B→A control: unexpected stale-CAS error: {error}"
                ));
            }
            Ok(_) => {
                return Err("raw A→B→A control: non-recurring stale etag was ACCEPTED".to_string());
            }
        }
    }

    // --- 3. If-None-Match create race: one winner ----------------------
    let k3 = format!("{prefix}create-race");
    created.push(k3.clone());
    let racers = (0..CREATE_RACERS).map(|index| {
        let payload = payload_c.clone();
        let key = k3.clone();
        async move { (index, client.upload_conditional(&key, payload, None).await) }
    });
    let results = futures::future::join_all(racers).await;
    let winners = results.iter().filter(|(_, r)| r.is_ok()).count();
    let rejected = results
        .iter()
        .filter(|(_, r)| matches!(r, Err(e) if e.to_string().contains("CAS failed")))
        .count();
    if winners != 1 || winners + rejected != CREATE_RACERS {
        let summary: Vec<String> = results
            .iter()
            .map(|(i, r)| {
                format!(
                    "racer {i}: {:?}",
                    r.as_ref().map(|_| "ok").map_err(|e| e.to_string())
                )
            })
            .collect();
        return Err(format!(
            "create race: {winners} winners / {rejected} rejected of {CREATE_RACERS}: {summary:?}"
        ));
    }
    note(
        verdicts,
        format!(
            "If-None-Match create race ({CREATE_RACERS} parallel): exactly one winner, {rejected} PreconditionFailed"
        ),
    );

    // --- 4. CAS with the correct etag is accepted -----------------------
    let k4 = format!("{prefix}cas-correct");
    created.push(k4.clone());
    let e_before = create_etag(client, &k4, &payload_b).await?;
    client
        .upload_conditional(&k4, payload_c.clone(), Some(&e_before))
        .await
        .map_err(|e| format!("cas-correct rejected: {e}"))?;
    let after = client
        .download(&k4)
        .await
        .map_err(|e| format!("read k4: {e}"))?;
    if after != payload_c {
        return Err("cas-correct: object did not take the new bytes".to_string());
    }
    note(
        verdicts,
        "If-Match CAS with correct etag: accepted, bytes replaced".to_string(),
    );

    // --- 5. CAS with a stale etag is rejected ----------------------------
    let k5 = format!("{prefix}cas-stale");
    created.push(k5.clone());
    let e_old = create_etag(client, &k5, &payload_b).await?;
    let _e_new = cas_etag(client, &k5, &payload_c, &e_old).await?;
    match client
        .upload_conditional(&k5, Bytes::from_static(b"stale"), Some(&e_old))
        .await
    {
        Err(e) if e.to_string().contains("CAS failed") => {
            note(
                verdicts,
                "If-Match CAS with stale etag (distinct content): PreconditionFailed".to_string(),
            );
        }
        Err(e) => return Err(format!("cas-stale: unexpected error flavor: {e}")),
        Ok(_) => {
            return Err(
                "cas-stale: stale-etag CAS ACCEPTED (distinct content) — CAS broken".to_string(),
            );
        }
    }
    let body = client
        .download(&k5)
        .await
        .map_err(|e| format!("read k5: {e}"))?;
    if body != payload_c {
        return Err("cas-stale: object changed despite rejection".to_string());
    }

    // --- 6. THE ABA case: CAS the era-1 etag against a recreated -------
    //         era-2 object of identical bytes.
    let k6 = format!("{prefix}aba-cas");
    created.push(k6.clone());
    let era1 = create_etag(client, &k6, &payload_a).await?;
    client
        .delete(&k6)
        .await
        .map_err(|e| format!("delete k6: {e}"))?;
    let era2 = create_etag(client, &k6, &payload_a).await?;
    if era1 == era2 {
        // Recurring etags: an If-Match on the era-1 etag now addresses
        // era-2. Measure whether the store accepts it.
        match client
            .upload_conditional(&k6, Bytes::from_static(b"era-3"), Some(&era1))
            .await
        {
            Ok(_) => {
                note(
                    verdicts,
                    format!(
                        "raw delete/recreate ABA hazard: etag recurs for identical content ({era1}) and era-1 If-Match was ACCEPTED against era-2"
                    ),
                );
            }
            Err(e) => {
                note(
                    verdicts,
                    format!(
                        "ABA defense: etag recurs ({era1}) but era-1 If-Match is REJECTED against era-2 ({e})"
                    ),
                );
            }
        }
    } else {
        // Fresh etags: the era-1 etag is stale by construction; its
        // CAS must fail (this is the control for the hazard leg).
        match client
            .upload_conditional(&k6, Bytes::from_static(b"era-3"), Some(&era1))
            .await
        {
            Err(e) if e.to_string().contains("CAS failed") => {
                note(
                    verdicts,
                    format!(
                        "ABA unconstructible: fresh etag per write (era1={era1} != era2={era2}); stale CAS PreconditionFailed"
                    ),
                );
            }
            Err(e) => return Err(format!("aba-cas control: unexpected error flavor: {e}")),
            Ok(_) => {
                return Err(format!(
                    "ABA HAZARD CONFIRMED: etags differ per era ({era1} != {era2}) \
                     yet the era-1 If-Match was ACCEPTED — the store is matching \
                     something other than the addressed incarnation"
                ));
            }
        }
    }

    // --- 7. CAS with the etag of a deleted object ------------------------
    let k7 = format!("{prefix}cas-absent");
    created.push(k7.clone());
    let e_dead = create_etag(client, &k7, &payload_a).await?;
    client
        .delete(&k7)
        .await
        .map_err(|e| format!("delete k7: {e}"))?;
    match client
        .upload_conditional(&k7, Bytes::from_static(b"zombie"), Some(&e_dead))
        .await
    {
        Ok(_) => {
            return Err(
                "cas-absent: If-Match against a deleted key ACCEPTED — must not succeed"
                    .to_string(),
            );
        }
        Err(e) => note(
            verdicts,
            format!("If-Match CAS against deleted key: rejected ({e})"),
        ),
    }
    // And the key must not have been resurrected by that attempt.
    if client
        .exists(&k7)
        .await
        .map_err(|e| format!("exists k7: {e}"))?
    {
        return Err("cas-absent: rejected CAS resurrected the key".to_string());
    }

    // --- 8. Create-after-delete: the ABA window is reachable -------------
    client
        .upload_conditional(&k7, payload_b.clone(), None)
        .await
        .map_err(|e| format!("create-after-delete rejected: {e}"))?;
    verdicts
        .push("If-None-Match create after delete: accepted (recreate window exists)".to_string());

    // --- 9. LIST-after-write visibility (strong-LIST sample) -------------
    let k9 = format!("{prefix}list-visibility");
    created.push(k9.clone());
    client
        .upload_conditional(&k9, payload_a.clone(), None)
        .await
        .map_err(|e| format!("create k9: {e}"))?;
    let listed = client
        .list_prefix(prefix)
        .await
        .map_err(|e| format!("list k9: {e}"))?;
    if !listed.contains(&k9) {
        return Err("list-after-write: freshly PUT key not visible in immediate LIST".to_string());
    }
    client
        .delete(&k9)
        .await
        .map_err(|e| format!("delete k9: {e}"))?;
    let listed = client
        .list_prefix(prefix)
        .await
        .map_err(|e| format!("list k9 again: {e}"))?;
    if listed.contains(&k9) {
        return Err("list-after-delete: deleted key still visible in LIST".to_string());
    }
    verdicts
        .push("LIST-after-write/delete visibility: immediate, consistent (sampled)".to_string());

    Ok(())
}

/// Real-backend capability measurement for the serious single-object
/// alternative to chunk manifests. Unsupported legs are recorded, not
/// treated as kernel regressions: this battery gathers a ruling witness.
/// Successful operations still verify bytes and cleanup loudly.
async fn multipart_battery(
    client: &ObjectStoreClient,
    prefix: &str,
    created: &mut Vec<String>,
    multipart_uploads: &mut Vec<(String, String)>,
    verdicts: &mut Vec<String>,
) -> Result<(), String> {
    // --- MPU-1. Incomplete-upload visibility and abort -----------------
    let abort_key = format!("{prefix}multipart-abort");
    let abort_id = client
        .initiate_multipart_upload(&abort_key, "application/octet-stream")
        .await
        .map_err(|error| format!("multipart abort leg initiate: {error}"))?;
    multipart_uploads.push((abort_key.clone(), abort_id.clone()));
    match client.list_multipart_uploads_for_test(prefix).await {
        Ok(uploads) if uploads.contains(&(abort_key.clone(), abort_id.clone())) => note(
            verdicts,
            "ListMultipartUploads: initiated upload visible".to_string(),
        ),
        Ok(uploads) => note(
            verdicts,
            format!(
                "ListMultipartUploads: PARTIAL — initiated upload hidden (observed {uploads:?})"
            ),
        ),
        Err(error) => note(
            verdicts,
            format!("ListMultipartUploads: UNSUPPORTED/UNWITNESSED ({error})"),
        ),
    }
    match client.abort_multipart_upload(&abort_key, &abort_id).await {
        Ok(()) => note(verdicts, "AbortMultipartUpload: accepted".to_string()),
        Err(error) => note(
            verdicts,
            format!("AbortMultipartUpload: UNSUPPORTED/FAILED ({error})"),
        ),
    }
    match client.list_multipart_uploads_for_test(prefix).await {
        Ok(uploads) if !uploads.contains(&(abort_key.clone(), abort_id.clone())) => note(
            verdicts,
            "AbortMultipartUpload visibility: upload absent after abort".to_string(),
        ),
        Ok(uploads) => note(
            verdicts,
            format!("AbortMultipartUpload visibility: PARTIAL — upload still listed ({uploads:?})"),
        ),
        Err(error) => note(
            verdicts,
            format!("AbortMultipartUpload visibility: UNWITNESSED ({error})"),
        ),
    }

    // --- MPU-2. CompleteMultipartUpload + If-Match ---------------------
    let conditional_key = format!("{prefix}multipart-conditional");
    created.push(conditional_key.clone());
    let base = Bytes::from_static(b"conditional-multipart-base");
    let base_etag = create_etag(client, &conditional_key, &base).await?;
    let first_part = Bytes::from(vec![0x31; MIN_MULTIPART_PART_BYTES]);
    let second_part = Bytes::from(vec![0x32; 64 * 1024]);
    let upload_id = client
        .initiate_multipart_upload(&conditional_key, "application/octet-stream")
        .await
        .map_err(|error| format!("conditional multipart initiate: {error}"))?;
    multipart_uploads.push((conditional_key.clone(), upload_id.clone()));
    let part_1 = client
        .upload_multipart_part_for_test(&conditional_key, &upload_id, 1, first_part.clone())
        .await
        .map_err(|error| format!("conditional multipart part 1: {error}"))?;
    let part_2 = client
        .upload_multipart_part_for_test(&conditional_key, &upload_id, 2, second_part.clone())
        .await
        .map_err(|error| format!("conditional multipart part 2: {error}"))?;
    let parts = vec![part_1, part_2];

    let conditional_supported = match client
        .complete_multipart_upload_if_match_for_test(
            &conditional_key,
            &upload_id,
            parts.clone(),
            &base_etag,
        )
        .await
    {
        Ok(()) => {
            note(
                verdicts,
                "CompleteMultipartUpload If-Match current etag: SUPPORTED".to_string(),
            );
            true
        }
        Err(error) => {
            note(
                verdicts,
                format!(
                    "CompleteMultipartUpload If-Match current etag: UNSUPPORTED/PARTIAL ({error})"
                ),
            );
            match client
                .complete_multipart_upload(&conditional_key, &upload_id, parts)
                .await
            {
                Ok(_) => note(
                    verdicts,
                    "CompleteMultipartUpload unconditional fallback: accepted for capability measurement"
                        .to_string(),
                ),
                Err(fallback_error) => {
                    note(
                        verdicts,
                        format!(
                            "CompleteMultipartUpload unconditional fallback: FAILED ({fallback_error})"
                        ),
                    );
                    return Ok(());
                }
            }
            false
        }
    };

    let completed = client
        .download(&conditional_key)
        .await
        .map_err(|error| format!("multipart completed object read: {error}"))?;
    let expected_len = first_part.len() + second_part.len();
    if completed.len() != expected_len
        || completed[..first_part.len()] != first_part[..]
        || completed[first_part.len()..] != second_part[..]
    {
        return Err(format!(
            "multipart completion bytes disagree: expected {expected_len}, observed {}",
            completed.len()
        ));
    }
    note(
        verdicts,
        format!("multipart completion bytes: exact concatenation ({expected_len} bytes)"),
    );

    match (
        client
            .download_multipart_part_for_test(&conditional_key, 1)
            .await,
        client
            .download_multipart_part_for_test(&conditional_key, 2)
            .await,
    ) {
        (Ok(observed_1), Ok(observed_2))
            if observed_1 == first_part && observed_2 == second_part =>
        {
            note(
                verdicts,
                "GetObject partNumber: SUPPORTED with original part boundaries".to_string(),
            );
        }
        (Ok(observed_1), Ok(observed_2)) => note(
            verdicts,
            format!(
                "GetObject partNumber: PARTIAL/INCOMPATIBLE (part lengths {} and {})",
                observed_1.len(),
                observed_2.len()
            ),
        ),
        (observed_1, observed_2) => note(
            verdicts,
            format!(
                "GetObject partNumber: UNSUPPORTED/PARTIAL (part1={:?}, part2={:?})",
                observed_1
                    .as_ref()
                    .map(Bytes::len)
                    .map_err(ToString::to_string),
                observed_2
                    .as_ref()
                    .map(Bytes::len)
                    .map_err(ToString::to_string)
            ),
        ),
    }

    // A matching conditional completion is useful only if the same upload
    // is rejected once its destination etag becomes stale.
    if conditional_supported {
        let completed_etag = read_etag(client, &conditional_key).await?;
        let stale_upload_id = client
            .initiate_multipart_upload(&conditional_key, "application/octet-stream")
            .await
            .map_err(|error| format!("stale conditional multipart initiate: {error}"))?;
        multipart_uploads.push((conditional_key.clone(), stale_upload_id.clone()));
        let stale_part = client
            .upload_multipart_part_for_test(
                &conditional_key,
                &stale_upload_id,
                1,
                Bytes::from_static(b"stale-multipart-candidate"),
            )
            .await
            .map_err(|error| format!("stale conditional multipart part: {error}"))?;
        let successor = Bytes::from_static(b"conditional-multipart-successor");
        let _successor_etag =
            cas_etag(client, &conditional_key, &successor, &completed_etag).await?;
        let stale_result = client
            .complete_multipart_upload_if_match_for_test(
                &conditional_key,
                &stale_upload_id,
                vec![stale_part],
                &completed_etag,
            )
            .await;
        match &stale_result {
            Err(ObjectStoreError::PreconditionFailed(_)) => note(
                verdicts,
                "CompleteMultipartUpload stale If-Match: PreconditionFailed".to_string(),
            ),
            Err(error) => note(
                verdicts,
                format!("CompleteMultipartUpload stale If-Match: PARTIAL error flavor ({error})"),
            ),
            Ok(()) => note(
                verdicts,
                "CompleteMultipartUpload stale If-Match: ACCEPTED — conditional completion unsafe"
                    .to_string(),
            ),
        }
        let after_stale = client
            .download(&conditional_key)
            .await
            .map_err(|error| format!("stale conditional multipart readback: {error}"))?;
        if stale_result.is_err() && after_stale != successor {
            return Err(
                "stale conditional multipart errored but changed destination bytes".to_string(),
            );
        }
    }

    Ok(())
}

/// The ADR 0004 streaming legs on the real backend: a chunked v3
/// round trip (create, whole collect, verified reader, boundary
/// range), v3 CAS with stale-token rejection naming the manifest era,
/// conditional control-only delete, the maintenance fence, the
/// delete-free meter, and the quiesced sweep doubling as chunk-root
/// cleanup. The lost-response oracle's fault cuts are loopback-only
/// (real backends cannot drop responses deterministically); the
/// oracle's prerequisites — conditional manifest PUT exclusivity and
/// exact rereads — are what this battery witnesses.
async fn streaming_battery(
    client: Arc<ObjectStoreClient>,
    namespace: &str,
    verdicts: &mut Vec<String>,
) -> Result<(), String> {
    let keyspace = AtomicKeyspace::new(client, namespace)
        .map_err(|error| format!("streaming bind: {error}"))?;
    let key = "streamed";
    let chunk_bytes = 16 * 1024 * 1024;
    let len = 2 * chunk_bytes + 4096;
    let payload: Bytes = {
        let mut bytes = Vec::with_capacity(len);
        let mut state = 0x5EED_0001u32;
        while bytes.len() < len {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            bytes.push((state >> 24) as u8);
        }
        Bytes::from(bytes)
    };

    // 1. Streamed chunked create: the manifest PUT is the commit.
    let mut writer = keyspace
        .begin_stream_create(key)
        .await
        .map_err(|error| format!("streamed begin: {error}"))?;
    writer
        .write_all(&payload)
        .await
        .map_err(|error| format!("streamed write: {error}"))?;
    let pending = writer
        .seal()
        .await
        .map_err(|error| format!("streamed seal: {error}"))?;
    let receipt = pending
        .commit()
        .await
        .map_err(|error| format!("streamed commit: {error}"))?;
    note(
        verdicts,
        format!(
            "streamed v3 create: committed {} chunks ({} bytes), representation chunked",
            receipt.chunk_count, receipt.logical_len
        ),
    );

    // Whole collect and the verified reader agree, byte exact.
    let whole = keyspace
        .get(key)
        .await
        .map_err(|error| format!("streamed whole get: {error}"))?
        .ok_or_else(|| "streamed whole get: absent".to_string())?;
    if whole != payload {
        return Err("streamed v3 whole-collect bytes mismatch".to_string());
    }
    let mut reader = keyspace
        .open_stream(key)
        .await
        .map_err(|error| format!("streamed open: {error}"))?
        .ok_or_else(|| "streamed open: absent".to_string())?;
    let mut streamed = Vec::new();
    reader
        .read_to_end(&mut streamed)
        .await
        .map_err(|error| format!("streamed read: {error}"))?;
    if streamed != payload {
        return Err("streamed v3 verified-reader bytes mismatch".to_string());
    }
    note(
        verdicts,
        "streamed v3 read: whole collect and verified reader byte-exact".to_string(),
    );

    // 2. Boundary range: exact slice across the 16 MiB boundary.
    let start = chunk_bytes as u64 - 1;
    let end = chunk_bytes as u64 + 1;
    let mut range_reader = keyspace
        .open_stream_range(key, start..end)
        .await
        .map_err(|error| format!("streamed range open: {error}"))?
        .ok_or_else(|| "streamed range: absent".to_string())?;
    let mut range_bytes = Vec::new();
    range_reader
        .read_to_end(&mut range_bytes)
        .await
        .map_err(|error| format!("streamed range read: {error}"))?;
    if range_bytes.as_slice() != &payload[start as usize..end as usize] {
        return Err("streamed v3 boundary range mismatch".to_string());
    }
    note(
        verdicts,
        "streamed v3 range: boundary slice exact (16 MiB−1 .. 16 MiB+1)".to_string(),
    );

    // 3. v3→v3 streamed CAS; the stale token rejects naming the
    //    manifest era (the §2.3 v3-aware enrichment on real S3).
    let successor: Bytes = {
        let mut bytes = Vec::with_capacity(len);
        let mut state = 0x0DD_BA11u32;
        while bytes.len() < len {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            bytes.push((state >> 24) as u8);
        }
        Bytes::from(bytes)
    };
    let mut cas_writer = keyspace
        .begin_stream_compare_exchange(key, &receipt.etag)
        .await
        .map_err(|error| format!("streamed CAS begin: {error}"))?;
    cas_writer
        .write_all(&successor)
        .await
        .map_err(|error| format!("streamed CAS write: {error}"))?;
    let cas_receipt = cas_writer
        .seal()
        .await
        .map_err(|error| format!("streamed CAS seal: {error}"))?
        .commit()
        .await
        .map_err(|error| format!("streamed CAS commit: {error}"))?;
    match keyspace.delete_if_match(key, &receipt.etag).await {
        Err(KeyspaceError::PreconditionFailed {
            observed_incarnation: Some(0),
            observed_version: Some(1),
            ..
        }) => note(
            verdicts,
            "v3 stale-token conditional delete: PreconditionFailed naming manifest era (0,1)"
                .to_string(),
        ),
        Err(error) => {
            return Err(format!("v3 stale-token delete: unexpected error {error}"));
        }
        Ok(()) => return Err("v3 stale-token delete was ACCEPTED".to_string()),
    }
    note(
        verdicts,
        "streamed v3 CAS: successor committed; manifest If-Match consumable exactly once"
            .to_string(),
    );

    // 4. Meter: the superseded generation is candidate garbage; the
    //    current generation is referenced. Delete-free.
    let inventory = keyspace
        .chunk_inventory()
        .await
        .map_err(|error| format!("chunk inventory: {error}"))?;
    if inventory.listed_chunks != 6
        || inventory.referenced_chunks != 3
        || inventory.candidate_orphan_chunks != 3
        || inventory.unresolved_chunks != 0
    {
        return Err(format!("chunk inventory mismatch: {inventory:?}"));
    }
    note(
        verdicts,
        format!(
            "chunk meter: {} listed, {} referenced, {} candidates, 0 unresolved, delete-free",
            inventory.listed_chunks, inventory.referenced_chunks, inventory.candidate_orphan_chunks
        ),
    );

    // 5. Conditional control-only delete of the v3 manifest.
    keyspace
        .delete_if_match(key, &cas_receipt.etag)
        .await
        .map_err(|error| format!("v3 conditional delete: {error}"))?;
    if keyspace
        .get(key)
        .await
        .map_err(|error| format!("post-delete get: {error}"))?
        .is_some()
    {
        return Err("v3 conditional delete left the control present".to_string());
    }
    note(
        verdicts,
        "v3 conditional delete: control removed; chunks remain garbage until sweep".to_string(),
    );

    // 6. The fence: begins refuse while fenced; the quiesced sweep
    //    reclaims every orphan (this doubles as the probe's chunk-root
    //    cleanup); the release restores begins.
    keyspace
        .set_maintenance_fence()
        .await
        .map_err(|error| format!("set fence: {error}"))?;
    match keyspace.begin_stream_create(key).await {
        Err(KeyspaceError::MaintenanceFenced(_)) => note(
            verdicts,
            "maintenance fence: streamed begin refused while fenced".to_string(),
        ),
        other => return Err(format!("fenced begin did not refuse: {other:?}")),
    }
    let report = keyspace
        .sweep_chunks()
        .await
        .map_err(|error| format!("quiesced sweep: {error}"))?;
    if report.deleted != 6 || report.remaining != 0 || report.retained != 0 {
        return Err(format!("quiesced sweep mismatch: {report:?}"));
    }
    let re_run = keyspace
        .sweep_chunks()
        .await
        .map_err(|error| format!("idempotent re-sweep: {error}"))?;
    if re_run.examined != 0 || re_run.deleted != 0 {
        return Err(format!("idempotent re-sweep mismatch: {re_run:?}"));
    }
    note(
        verdicts,
        format!(
            "quiesced sweep: {} chunks reclaimed, idempotent re-run clean",
            report.deleted
        ),
    );
    keyspace
        .release_maintenance_fence()
        .await
        .map_err(|error| format!("release fence: {error}"))?;
    keyspace
        .begin_stream_create(key)
        .await
        .map_err(|error| format!("unfenced begin: {error}"))?;
    note(
        verdicts,
        "maintenance fence: release (conditional-delete CAS) restores streamed begins".to_string(),
    );
    Ok(())
}

/// Module-level companion to the raw backend battery. The raw probe is
/// allowed to observe content-etag ABA; this leg must close it through
/// AtomicKeyspace's versioned envelope.
async fn module_battery(
    client: Arc<ObjectStoreClient>,
    namespace: &str,
    created: &mut Vec<String>,
    verdicts: &mut Vec<String>,
) -> Result<(), String> {
    let keyspace =
        AtomicKeyspace::new(client, namespace).map_err(|error| format!("module bind: {error}"))?;
    let key = "wrapped-cycle";
    created.push(format!("{KEYSPACE_ROOT}/{namespace}/{key}"));

    let payload_a = Bytes::from(vec![0xA5; 64]);
    let payload_b = Bytes::from(vec![0x5A; 64]);
    keyspace
        .create(key, payload_a.clone())
        .await
        .map_err(|error| format!("module create: {error}"))?;
    let (observed_a1, incarnation_a1, version_a1, etag_a1) = keyspace
        .get_with_version(key)
        .await
        .map_err(|error| format!("module get A(v0): {error}"))?
        .ok_or_else(|| "module get A(v0): key absent".to_string())?;
    if observed_a1 != payload_a || version_a1 != 0 {
        return Err(format!(
            "module create envelope mismatch: payload_matches={}, version={version_a1}",
            observed_a1 == payload_a
        ));
    }

    let etag_b = keyspace
        .compare_exchange(key, &etag_a1, payload_b.clone())
        .await
        .map_err(|error| format!("module CAS A(v0)→B(v1): {error}"))?;
    let etag_a2 = keyspace
        .compare_exchange(key, &etag_b, payload_a.clone())
        .await
        .map_err(|error| format!("module CAS B(v1)→A(v2): {error}"))?;
    let (observed_a2, incarnation_a2, version_a2, observed_etag_a2) = keyspace
        .get_with_version(key)
        .await
        .map_err(|error| format!("module get A(v2): {error}"))?
        .ok_or_else(|| "module get A(v2): key absent".to_string())?;
    if observed_a2 != payload_a || version_a2 != 2 || observed_etag_a2 != etag_a2 {
        return Err(format!(
            "module A(v2) mismatch: payload_matches={}, version={version_a2}, etag_matches={}",
            observed_a2 == payload_a,
            observed_etag_a2 == etag_a2
        ));
    }
    if etag_a2 == etag_a1 {
        return Err(format!(
            "module ABA closure failed: A(v0) and A(v2) share etag {etag_a1}"
        ));
    }
    note(
        verdicts,
        format!(
            "AtomicKeyspace A(v0)→B(v1)→A(v2): payload recurred, envelope etag did not ({etag_a1} != {etag_a2})"
        ),
    );

    match keyspace
        .compare_exchange(key, &etag_a1, Bytes::from_static(b"stale-writer"))
        .await
    {
        Err(KeyspaceError::PreconditionFailed { .. }) => note(
            verdicts,
            "AtomicKeyspace stale A(v0) token against A(v2): PreconditionFailed".to_string(),
        ),
        Err(error) => {
            return Err(format!(
                "module stale A(v0) token returned unexpected error: {error}"
            ));
        }
        Ok(_) => return Err("module stale A(v0) token was ACCEPTED against A(v2)".to_string()),
    }

    let etag_a3 = keyspace
        .compare_exchange(key, &etag_a2, payload_a.clone())
        .await
        .map_err(|error| format!("module identical-payload CAS A(v2)→A(v3): {error}"))?;
    let (observed_a3, incarnation_a3, version_a3, observed_etag_a3) = keyspace
        .get_with_version(key)
        .await
        .map_err(|error| format!("module get A(v3): {error}"))?
        .ok_or_else(|| "module get A(v3): key absent".to_string())?;
    if observed_a3 != payload_a
        || version_a3 != 3
        || observed_etag_a3 != etag_a3
        || incarnation_a1 != 0
        || incarnation_a2 != 0
        || incarnation_a3 != 0
    {
        return Err(format!(
            "module identical-payload transition mismatch: payload_matches={}, version={version_a3}, etag_matches={}, incarnations={incarnation_a1}/{incarnation_a2}/{incarnation_a3}",
            observed_a3 == payload_a,
            observed_etag_a3 == etag_a3
        ));
    }
    note(
        verdicts,
        "AtomicKeyspace current-token identical-payload CAS: accepted at version 3 \
         of one incarnation (batch 7: the incarnation is constant within a lifetime)"
            .to_string(),
    );

    // Batch-7 leg (teardown finding T4, 2026-08-22): the CROSS-DELETION
    // closure — the headline claim the earlier battery never measured
    // on a real backend. Destroy the lifetime, re-create with IDENTICAL
    // payload bytes (on a content-etag backend the raw era-1 bytes would
    // recur an identical etag), then prove the era-1 token is rejected
    // and the era-2 value is version 0 of incarnation 1.
    keyspace
        .destroy(key, "probe-incarnation-leg", "real-s3-probe")
        .await
        .map_err(|error| format!("module destroy: {error}"))?;
    created.push(format!("{KEYSPACE_ROOT}/{namespace}/tombstones/{key}"));
    created.push(format!("{KEYSPACE_ROOT}/{namespace}/incarnations/{key}"));
    keyspace
        .create(key, payload_a.clone())
        .await
        .map_err(|error| format!("module era-2 re-create: {error}"))?;
    let (recreated, incarnation_2, version_2, etag_era2) = keyspace
        .get_with_version(key)
        .await
        .map_err(|error| format!("module get era-2: {error}"))?
        .ok_or_else(|| "module get era-2: key absent".to_string())?;
    if recreated != payload_a || incarnation_2 != 1 || version_2 != 0 {
        return Err(format!(
            "module era-2 mismatch: payload_matches={}, incarnation={incarnation_2}, version={version_2}",
            recreated == payload_a
        ));
    }
    if etag_era2 == etag_a3 {
        return Err(format!(
            "module cross-deletion closure failed: era-2 recreated identical bytes shares era-1's etag {etag_a3}"
        ));
    }
    note(
        verdicts,
        format!(
            "AtomicKeyspace destroy→recreate(identical bytes): era-2 is incarnation 1 version 0; envelope etag moved ({etag_a3} != {etag_era2})"
        ),
    );
    match keyspace
        .compare_exchange(key, &etag_a3, Bytes::from_static(b"cross-era-stale"))
        .await
    {
        Err(KeyspaceError::PreconditionFailed {
            observed_incarnation: Some(1),
            ..
        }) => note(
            verdicts,
            "AtomicKeyspace era-1 token across the destroy: PreconditionFailed naming incarnation 1".to_string(),
        ),
        Err(error) => {
            return Err(format!(
                "module era-1 token across destroy returned unexpected error: {error}"
            ));
        }
        Ok(_) => {
            return Err(
                "module era-1 token was ACCEPTED across the destroy (cross-deletion ABA)".to_string(),
            );
        }
    }
    keyspace
        .compare_exchange(key, &etag_era2, Bytes::from_static(b"era-2-next"))
        .await
        .map_err(|error| format!("module era-2 current-token CAS: {error}"))?;
    note(
        verdicts,
        "AtomicKeyspace era-2 current-token CAS: accepted (versions advance within the new incarnation)"
            .to_string(),
    );

    Ok(())
}

/// Lineage-head leg (batch 9; Fugu teardown finding G6, 2026-08-22):
/// the same cross-deletion closure the keyspace leg proves for
/// values, measured on the lineage HEAD path — the object that
/// predated the era model and never received the envelope
/// treatment. Destroy the lineage, recreate with a byte-identical
/// genesis (on a content-etag backend the raw era-1 head bytes would
/// recur the identical etag — exactly the finding-E reproduction),
/// then prove the era-1 `HeadRead` is refused while a fresh era-2
/// token advances the reborn lineage.
async fn lineage_battery(
    client: Arc<ObjectStoreClient>,
    lineage_name: &str,
    created: &mut Vec<String>,
    verdicts: &mut Vec<String>,
) -> Result<(), String> {
    let lineage = KernelLineage::new(lineage_name, SuccessorPolicy::SuccessorCapable)
        .map_err(|error| format!("lineage bind: {error:?}"))?;
    let kernel = StateKernel::new(client, lineage.clone());

    let genesis = CanonicalRecord::new(
        &lineage,
        0,
        None,
        "probe.lineage-genesis",
        "probe.v1",
        vec![0xC3; 64],
        "probe-operation",
        "real-s3-probe",
        "probe-cause",
    )
    .map_err(|error| format!("lineage genesis record: {error:?}"))?;

    // Era 1: genesis head at incarnation 0.
    let era1 = kernel
        .append_genesis(&genesis)
        .await
        .map_err(|error| format!("lineage era-1 genesis: {error:?}"))?;
    created.push(format!(
        "{lineage_name}/objects/{}",
        era1.record_digest().as_str()
    ));

    kernel
        .destroy("probe-lineage-leg", "real-s3-probe")
        .await
        .map_err(|error| format!("lineage destroy: {error:?}"))?;
    created.push(format!("{lineage_name}/tombstone"));
    created.push(format!("{lineage_name}/incarnation"));

    // Era 2: byte-identical genesis — the exact ABA shape. The head
    // must come back stamped incarnation 1 with MOVED bytes (and
    // therefore a moved content etag).
    let era2 = kernel
        .append_genesis(&genesis)
        .await
        .map_err(|error| format!("lineage era-2 genesis: {error:?}"))?;
    if era2.incarnation() != 1 {
        return Err(format!(
            "lineage era-2 genesis is incarnation {} — expected 1",
            era2.incarnation()
        ));
    }
    if era2.etag == era1.etag {
        return Err(format!(
            "lineage cross-deletion closure failed: byte-identical genesis recurred the head etag {}",
            era1.etag
        ));
    }
    note(
        verdicts,
        format!(
            "lineage head destroy→recreate(identical genesis): era-2 stamped incarnation 1; head etag moved ({} != {})",
            era1.etag, era2.etag
        ),
    );

    // The stale era-1 HeadRead must be refused — the finding-E
    // hazard, closed.
    let stale_successor = CanonicalRecord::new(
        &lineage,
        1,
        Some(era1.record_position()),
        "probe.lineage-stale",
        "probe.v1",
        vec![0x3C; 64],
        "probe-stale-operation",
        "real-s3-probe",
        "probe-cause",
    )
    .map_err(|error| format!("lineage stale successor record: {error:?}"))?;
    match kernel.append_successor(&stale_successor, &era1).await {
        Err(KernelError::LineageHeadConflict { .. }) => note(
            verdicts,
            "lineage era-1 HeadRead across the destroy: LineageHeadConflict".to_string(),
        ),
        Err(error) => {
            return Err(format!(
                "lineage era-1 token across destroy returned unexpected error: {error:?}"
            ));
        }
        Ok(_) => {
            return Err(
                "lineage era-1 HeadRead was ACCEPTED across the destroy (cross-deletion ABA)"
                    .to_string(),
            );
        }
    }

    // The refused CAS still published its immutable record (the
    // documented unreachable-orphan shape) — track it for cleanup.
    created.push(format!(
        "{lineage_name}/objects/{}",
        stale_successor
            .digest()
            .map_err(|error| format!("lineage stale successor digest: {error:?}"))?
            .as_str()
    ));

    // A fresh era-2 token advances the reborn lineage normally.
    let era2_successor = CanonicalRecord::new(
        &lineage,
        1,
        Some(era2.record_position()),
        "probe.lineage-fresh",
        "probe.v1",
        vec![0x55; 64],
        "probe-fresh-operation",
        "real-s3-probe",
        "probe-cause",
    )
    .map_err(|error| format!("lineage fresh successor record: {error:?}"))?;
    let advanced = kernel
        .append_successor(&era2_successor, &era2)
        .await
        .map_err(|error| format!("lineage era-2 successor: {error:?}"))?;
    created.push(format!(
        "{lineage_name}/objects/{}",
        advanced.record_digest().as_str()
    ));
    created.push(format!("{lineage_name}/head"));
    if advanced.generation() != 1 || advanced.incarnation() != 1 {
        return Err(format!(
            "lineage era-2 successor landed at generation {} incarnation {} — expected 1/1",
            advanced.generation(),
            advanced.incarnation()
        ));
    }
    note(
        verdicts,
        "lineage era-2 fresh-token successor: accepted at generation 1 of incarnation 1"
            .to_string(),
    );

    Ok(())
}

/// Create if absent, then read the etag back. An absent etag is a loud
/// failure because the whole CAS design depends on it.
async fn create_etag(
    client: &ObjectStoreClient,
    key: &str,
    data: &Bytes,
) -> Result<String, String> {
    client
        .upload_conditional(key, data.clone(), None)
        .await
        .map_err(|e| format!("create {key}: {e}"))?;
    read_etag(client, key).await
}

/// Replace through If-Match, then read the newly current etag.
async fn cas_etag(
    client: &ObjectStoreClient,
    key: &str,
    data: &Bytes,
    expected_etag: &str,
) -> Result<String, String> {
    client
        .upload_conditional(key, data.clone(), Some(expected_etag))
        .await
        .map_err(|e| format!("cas {key}: {e}"))?;
    read_etag(client, key).await
}

async fn read_etag(client: &ObjectStoreClient, key: &str) -> Result<String, String> {
    let meta = client
        .download_with_etag(key)
        .await
        .map_err(|e| format!("get {key}: {e}"))?;
    meta.etag
        .ok_or_else(|| format!("get {key}: backend returned no etag — CAS unusable"))
}

/// Dependency-free run id (nanos + pid + counter) — unique per run;
/// cleanup deletes exactly what this run created.
fn mint_run_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{count}-{}", std::process::id())
}
