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
//! - the same A→B→A cycle through AtomicKeyspace, including versions,
//!   stale-token rejection, and an identical-payload transition.
//!
//! Writes only under `aba-probe/<run-id>/…` and
//! `keyspace/aba-probe/<run-id>/…`, then deletes everything it created
//! before exiting (cleanup is asserted, not assumed).

use std::sync::Arc;

use bytes::Bytes;
use yeetz_sdk_s3::{ObjectStoreClient, S3Config};

use crate::{AtomicKeyspace, KEYSPACE_ROOT, KeyspaceError};

/// Parallel racers for the If-None-Match create probe.
const CREATE_RACERS: usize = 8;

pub async fn run_real_s3_aba_probe(config: &S3Config) -> Result<Vec<String>, String> {
    let client = Arc::new(ObjectStoreClient::new(config).map_err(|e| format!("client: {e}"))?);

    // Run-scoped prefix so concurrent/past runs never collide.
    let run_id = mint_run_id();
    let prefix = format!("aba-probe/{run_id}/");
    let module_namespace = format!("aba-probe/{run_id}");
    let module_prefix = format!("{KEYSPACE_ROOT}/{module_namespace}/");
    let mut created: Vec<String> = Vec::new();
    let mut verdicts: Vec<String> = Vec::new();

    let result = match battery(&client, &prefix, &mut created, &mut verdicts).await {
        Ok(()) => {
            module_battery(
                Arc::clone(&client),
                &module_namespace,
                &mut created,
                &mut verdicts,
            )
            .await
        }
        Err(error) => Err(error),
    };

    // Cleanup regardless of verdict — delete exactly what we created,
    // then assert both run-scoped prefixes are gone (loud if the store
    // leaks).
    for key in &created {
        let _ = client.delete(key).await;
    }
    for cleanup_prefix in [&prefix, &module_prefix] {
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
        format!("cleanup: {prefix} and {module_prefix} empty after run"),
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
    // write path the kernel's keyspace uses; multipart etags are a
    // different scheme and out of scope).
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
    let (observed_a1, version_a1, etag_a1) = keyspace
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
    let (observed_a2, version_a2, observed_etag_a2) = keyspace
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
    let (observed_a3, version_a3, observed_etag_a3) = keyspace
        .get_with_version(key)
        .await
        .map_err(|error| format!("module get A(v3): {error}"))?
        .ok_or_else(|| "module get A(v3): key absent".to_string())?;
    if observed_a3 != payload_a || version_a3 != 3 || observed_etag_a3 != etag_a3 {
        return Err(format!(
            "module identical-payload transition mismatch: payload_matches={}, version={version_a3}, etag_matches={}",
            observed_a3 == payload_a,
            observed_etag_a3 == etag_a3
        ));
    }
    note(
        verdicts,
        "AtomicKeyspace current-token identical-payload CAS: accepted at version 3".to_string(),
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
