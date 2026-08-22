//! yeetz-s3-streams — append-only event logs on the kernel's AtomicKeyspace
//! (ADR 0017; the streams consensus design).
//!
//! One immutable object per event at `streams/v1/<id>/log/<seq:020>`;
//! the object's conditional create IS the allocation. Replay is
//! computed-key GETs with explicit contiguity and witness-bounded
//! completeness. Cursors are CAS'd monotone pointers. The log is
//! append-only in the write path — no unconditional overwrite, no
//! in-place mutation — and retention is certified trim (batch 5):
//! an immutable create-once certificate bounds the retained prefix,
//! reads below the floor are a typed `OffsetExpired`, and the GC
//! sweeper deletes only below the certificate (the genesis is
//! immortal). Fugu's no-fencing blocker is resolved by the kernel's
//! versioned keyspace values, which the certificate relies on.
//!
//! Forge-agnostic by law: opaque stream IDs, opaque payloads, no
//! yeetz types anywhere. The application maps names→IDs at its
//! boundary.
//!
//! # Idempotency is bounded to the pre-scan window (human-ruled)
//!
//! A stable event id makes an append retry idempotent ONLY inside
//! the pre-scan window: the seqs from
//! `max(1, tail-hint floor, LIST-max − 16)` through the LIST-derived
//! max, plus the exact-seq readback of the attempted conditional
//! create. Inside the window, the same id with identical schema and
//! payload returns the original receipt; the same id with DIFFERENT
//! content is a typed [`StreamsError::IdempotencyConflict`] — never
//! a silent second landing. Beyond the window, a re-append of the
//! same logical event (same id, identical bytes) lands as a NEW
//! event: duplicates of a logical event are possible by contract,
//! delivery is at-least-once, and consumers dedupe by stable event
//! id.
//!
//! # A fully trimmed stream is logically empty WITH a floor (finding D)
//!
//! `trim(stream, end+1)` — a certified floor above every event — is
//! the retention endpoint and is accepted. The state is meaningful:
//! reads that would start below the floor are
//! [`Replay::OffsetExpired`], reads from the boundary walk from the
//! floor itself, and appends land AT the floor (`first_retained` is
//! the first free retained slot; reads require density from there).
//! The idempotency pre-scan's window is bounded by event evidence
//! (LIST-derived max, verified hint) — the trim certificate names a
//! retention boundary, not an event — so the scan never GETs a seq
//! where no event can exist and the empty floor slot is not false
//! corruption.
//!
//! # Completeness requires a verified witness (human-ruled)
//!
//! `Replay::Page::complete == true` requires BOTH a verified tail
//! hint naming the last fetched/verified seq — the hint object present
//! and its named record confirmed with a matching digest, a witness a
//! stale LIST cannot hide — AND an empty ordered LIST probe beyond
//! that seq. A limit-cut page also confirms the immediate successor is
//! absent by computed-key GET because a valid hint may lag. With no
//! end-matching verified hint the page is served
//! `complete: false`: completeness is withheld, never guessed, and
//! the read recovers the witness with computed-key GET probes (which
//! a frozen LIST cannot degrade) so the next read can certify. The
//! honest claim behind `complete=true` is "no suffix visible under
//! the qualified backend contract, bounded by a witness". Standing
//! erasure floor: a suffix deleted together with every witness
//! (tail hint, migration seal) is undetectable at this layer —
//! explicitly not covered.

mod envelope;
mod error;
pub mod migration;

pub use envelope::{
    ENVELOPE_FORMAT_VERSION, Envelope, GENESIS_SCHEMA_ID, MAX_ENCODED_ENVELOPE_BYTES,
};
pub use error::{Replay, StreamsError};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use yeetz_s3_kernel::KernelHandle;
use yeetz_s3_kernel::atomic_keyspace::{
    AtomicKeyspace, DeleteBelowReport, KeyspaceError, TrimState,
};

/// Keyspace namespace: physical objects live at
/// `keyspace/streams/v1/<stream-id>/...`.
const NAMESPACE: &str = "streams/v1";

/// Conflicts tolerated per append before the typed budget error.
const APPEND_BUDGET: u32 = 64;

/// The bounded idempotency window: how many seqs below the
/// LIST-derived max every append pre-scans for a same-id envelope.
/// In-window retries converge (or conflict, typed); beyond it,
/// duplicate logical events are possible by contract — consumers
/// dedupe by stable event id (ADR 0017's ruled addendum).
const IDEMPOTENT_SCAN_WINDOW: u64 = 16;

/// Bound on parallel computed-key GETs per chunk.
const FETCH_PARALLELISM: usize = 8;

/// Backoff between conflicting append attempts (exponential, capped).
const APPEND_BACKOFF_START: Duration = Duration::from_millis(1);
const APPEND_BACKOFF_CAP: Duration = Duration::from_millis(25);

/// Opaque, immutable stream identifier. Minted at creation; the
/// application maps names→IDs at its boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId(String);

impl StreamId {
    /// Adopt a foreign-minted identifier (validated against the
    /// conservative keyspace key rule).
    pub fn new(opaque: &str) -> Result<Self, StreamsError> {
        validate_segment("stream id", opaque)?;
        Ok(Self(opaque.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque schema identifier carried in every envelope. Unknown schema
/// ids replay opaquely (S7); malformed envelopes error, never skip.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaId(String);

impl SchemaId {
    pub fn new(value: &str) -> Result<Self, StreamsError> {
        validate_segment("schema id", value)?;
        Ok(Self(value.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Event sequence number. Zero-padded to 20 decimal digits in the key;
/// every `u64` is representable, and there is no successor past
/// `u64::MAX` (S8: the boundary is typed, `SeqExhausted`).
pub type Seq = u64;

/// The 20-digit key encoding of a seq.
fn seq_key_component(seq: Seq) -> String {
    format!("{seq:020}")
}

/// Stable event identifier (caller-chosen; deduplicates retries).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StableEventId(String);

impl StableEventId {
    pub fn new(value: &str) -> Result<Self, StreamsError> {
        validate_segment("stable event id", value)?;
        Ok(Self(value.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Acknowledged append: the linearization point (the successful
/// conditional create) and the receipt an idempotent retry returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendReceipt {
    pub stream_id: StreamId,
    pub seq: Seq,
    pub stable_event_id: StableEventId,
    pub payload_sha256: String,
}

/// The tail-hint accelerator object: written only after validated
/// contiguous truth, allowed to lag, never declaring EOF.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TailHint {
    format_version: u32,
    highest_validated_dense_seq: Seq,
    terminal_record_digest: String,
}

/// A consumer cursor: CAS'd monotone pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub format_version: u32,
    pub stream_id: StreamId,
    pub seq: Seq,
    pub event_id: String,
}

/// Monotonic minting sequence for opaque stream IDs (time + counter +
/// pid, hashed — dependency-free).
static MINT_COUNTER: AtomicU64 = AtomicU64::new(0);

fn mint_stream_id() -> StreamId {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let count = MINT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Sha256::new();
    hasher.update(nanos.to_le_bytes());
    hasher.update(count.to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    let digest = hasher.finalize();
    StreamId(format!("s{}", hex::encode(&digest[..16])))
}

fn validate_segment(kind: &str, value: &str) -> Result<(), StreamsError> {
    let valid = !value.is_empty()
        && value.len() <= 255
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value.split('/').all(|segment| {
            let mut bytes = segment.bytes();
            bytes
                .next()
                .is_some_and(|first| first.is_ascii_alphanumeric())
                && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        });
    if valid {
        Ok(())
    } else {
        Err(StreamsError::InvalidArgument(format!("{kind} {value:?}")))
    }
}

/// The streams API over one store. Clone-cheap (Arc-bound keyspace).
#[derive(Debug, Clone)]
pub struct Streams {
    keyspace: std::sync::Arc<AtomicKeyspace>,
}

impl Streams {
    /// Bind to the kernel-owned store without exposing its adapter.
    pub fn new(kernel: &KernelHandle) -> Result<Self, StreamsError> {
        Ok(Self {
            keyspace: std::sync::Arc::new(
                kernel
                    .atomic_keyspace(NAMESPACE)
                    .map_err(|_| StreamsError::InvalidArgument(NAMESPACE.into()))?,
            ),
        })
    }

    fn log_key(stream: &StreamId, seq: Seq) -> String {
        Self::log_key_of(stream, seq)
    }

    pub(crate) fn log_key_of(stream: &StreamId, seq: Seq) -> String {
        format!("{}/log/{}", stream.as_str(), seq_key_component(seq))
    }

    fn tail_key(stream: &StreamId) -> String {
        format!("{}/tail", stream.as_str())
    }

    fn cursor_key(stream: &StreamId, consumer: &str) -> Result<String, StreamsError> {
        validate_segment("consumer", consumer)?;
        Ok(format!("{}/cursors/{}", stream.as_str(), consumer))
    }

    /// Mint a stream id and write the immutable genesis/config record
    /// (seq 0). Its conditional create defines the stream's existence.
    pub async fn create_stream(&self, config: &[u8]) -> Result<StreamId, StreamsError> {
        let stream = mint_stream_id();
        let genesis = Envelope::genesis(&stream, config)?;
        self.keyspace
            .create(&Self::log_key(&stream, 0), genesis.encoded.clone())
            .await
            .map_err(map_keyspace("create_stream"))?;
        Ok(stream)
    }

    /// Read a stream's config: the genesis (seq-0) payload that
    /// `create_stream` wrote. `None` when the stream does not exist.
    /// The genesis envelope is verified exactly as on every other
    /// read path — key agreement, format version, digest.
    pub async fn read_config(&self, stream: &StreamId) -> Result<Option<Vec<u8>>, StreamsError> {
        let bytes = self
            .keyspace
            .get(&Self::log_key(stream, 0))
            .await
            .map_err(map_keyspace("read_config"))?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let envelope =
            Envelope::decode_and_verify(stream, 0, &bytes).map_err(|_| StreamsError::Corrupt {
                stream: stream.clone(),
                missing_or_mismatched: vec![0],
            })?;
        Ok(Some(envelope.payload.to_vec()))
    }

    /// The stream's certified trim floor: the maximum trim
    /// certificate on this stream's scope (`None` = never trimmed).
    /// The certificate, not object presence, is the boundary.
    pub async fn trim_floor(&self, stream: &StreamId) -> Result<Option<Seq>, StreamsError> {
        self.keyspace
            .trim_floor(stream.as_str())
            .await
            .map_err(map_keyspace("trim_floor"))
    }

    /// Certify a trim floor for the stream: events with
    /// `seq < first_retained` become logically gone (physically gone
    /// once [`Streams::gc`] sweeps). Create-once, monotone (a lower
    /// floor cannot supersede a higher one), idempotent at the same
    /// floor. The genesis record (seq 0, config) is immortal —
    /// `first_retained >= 1` — and the floor may not exceed the
    /// current log end + 1. `floor == end + 1` (the retention
    /// endpoint) is accepted and meaningful: the stream is logically
    /// empty WITH a floor, and later appends land at the floor itself
    /// (see [`Streams::append`]). The tail hint self-invalidates below
    /// the floor (its terminal record is gone, so it stops verifying);
    /// cursors below the floor read as [`StreamsError::OffsetExpired`],
    /// never corruption.
    pub async fn trim(
        &self,
        stream: &StreamId,
        first_retained: Seq,
    ) -> Result<TrimState, StreamsError> {
        if first_retained == 0 {
            return Err(StreamsError::InvalidArgument(
                "trim floor must retain the genesis (first_retained >= 1)".into(),
            ));
        }
        // The stream must exist, and the floor must not name a seq
        // past the observable end (a floor beyond the log would
        // certify retention of events that never landed — accidental
        // data loss).
        let genesis = self
            .keyspace
            .get(&Self::log_key(stream, 0))
            .await
            .map_err(map_keyspace("trim: genesis"))?;
        if genesis.is_none() {
            return Err(StreamsError::StreamNotFound(stream.clone()));
        }
        let end = self.list_log_max(stream).await?.max(
            self.verified_tail_hint(stream)
                .await
                .map_or(0, |hint| hint.highest_validated_dense_seq),
        );
        if first_retained > end + 1 {
            return Err(StreamsError::InvalidArgument(format!(
                "trim floor {first_retained} exceeds the observable log end {end}"
            )));
        }
        self.keyspace
            .propose_trim(stream.as_str(), first_retained)
            .await
            .map_err(map_keyspace("trim: propose"))
    }

    /// Certified GC for one stream: delete the log objects below the
    /// stream's trim floor (never the genesis, never at/above the
    /// floor). Idempotent and resumable — a crash leaves extra
    /// objects, which are safe; a re-run converges. A resurrected
    /// object written below the floor (a stale writer racing the
    /// sweeper) is re-collected on the next sweep: the certificate,
    /// not object absence, is the boundary. No certificate → no-op
    /// report. Library call for the reconciler's pattern — no daemon.
    pub async fn gc(&self, stream: &StreamId) -> Result<DeleteBelowReport, StreamsError> {
        let Some(floor) = self.trim_floor(stream).await? else {
            return Ok(DeleteBelowReport::default());
        };
        let data_prefix = format!("{}/log/", stream.as_str());
        self.keyspace
            .delete_below(stream.as_str(), &data_prefix, floor)
            .await
            .map_err(map_keyspace("gc: delete_below"))
    }

    /// Append one event. The event object's conditional create IS the
    /// allocation; the successful create is the linearization point.
    /// A retry whose byte-identical envelope already landed (lost
    /// response) converges to the original receipt — bounded to the
    /// pre-scan window (see the crate docs): in window, the same
    /// stable id with different content is a typed
    /// [`StreamsError::IdempotencyConflict`]; beyond it, the same
    /// logical event re-appends as a NEW event (at-least-once;
    /// consumers dedupe by stable event id). On a fully trimmed
    /// stream (certified floor above every event) the append lands
    /// AT the floor — `first_retained` is the first free retained
    /// slot — and the pre-scan never GETs a seq where no event can
    /// exist, so the empty floor slot is not false corruption.
    pub async fn append(
        &self,
        stream: &StreamId,
        schema_id: &SchemaId,
        stable_event_id: &StableEventId,
        payload: &[u8],
    ) -> Result<AppendReceipt, StreamsError> {
        // Stream existence is the genesis object, and its integrity is
        // load-bearing for every later write.
        let genesis = self
            .keyspace
            .get(&Self::log_key(stream, 0))
            .await
            .map_err(map_keyspace("append: genesis"))?;
        let Some(genesis) = genesis else {
            return Err(StreamsError::StreamNotFound(stream.clone()));
        };
        Envelope::decode_and_verify(stream, 0, &genesis).map_err(|_| StreamsError::Corrupt {
            stream: stream.clone(),
            missing_or_mismatched: vec![0],
        })?;

        // Monotone floors: a verified hint, the LIST-derived max, and
        // the certified trim floor. LIST may lag; the hint keeps
        // allocation and lost-response convergence from walking the
        // known dense prefix again; the trim floor keeps allocation
        // above the certified boundary — a stale writer cannot land
        // below it (resurrection is rejected by the certificate, not
        // by object absence).
        let list_max = self.list_log_max(stream).await?;
        let hint = self.verified_tail_hint(stream).await;
        let hint_seq = hint
            .as_ref()
            .map(|hint| hint.highest_validated_dense_seq)
            .unwrap_or(0);
        let trim = self.trim_floor(stream).await?.unwrap_or(0);
        // Event evidence tops out at the LIST-derived max or the
        // verified hint (whichever sees further past a stale LIST).
        // The trim certificate is NOT event evidence: it names a
        // retention boundary, and on a fully trimmed stream
        // (floor = end+1) no event exists at — let alone above — the
        // floor slot. A scan whose inclusive end is the allocation
        // floor GETs that empty slot there and misreads absence as
        // corruption, wedging every future append (Fugu finding D).
        let event_max = list_max.max(hint_seq);
        if event_max == Seq::MAX {
            return Err(StreamsError::SeqExhausted(stream.clone()));
        }
        // Inclusive of the hint seq itself: an append that landed and
        // advanced the hint before its client saw the response can
        // have its event AT the hint seq. The scan never dips below
        // the trim floor: those events are logically gone, and a GET
        // against a swept seq must not read as corruption. Nor does
        // it rise above the event evidence: a seq only the floor
        // names has no envelope to find, swept or never-landed.
        let scan_from = hint_seq
            .max(list_max.saturating_sub(IDEMPOTENT_SCAN_WINDOW))
            .max(1)
            .max(trim);

        // Idempotent convergence inside the bounded window: an
        // envelope with the same stable event id, schema, and payload
        // that already landed between the scan floor and the max (the
        // lost-response window) returns the original receipt. The
        // same stable id with DIFFERENT content is a typed conflict —
        // never a silent second landing at another seq. (Byte-identity
        // across seqs is impossible — the seq is in the envelope — so
        // the predicate is everything except seq.)
        if event_max >= scan_from {
            for seq in scan_from..=event_max {
                let existing = self
                    .keyspace
                    .get(&Self::log_key(stream, seq))
                    .await
                    .map_err(map_keyspace("append: idempotent scan"))?
                    .ok_or_else(|| StreamsError::Corrupt {
                        stream: stream.clone(),
                        missing_or_mismatched: vec![seq],
                    })?;
                let envelope =
                    Envelope::decode_and_verify(stream, seq, &existing).map_err(|_| {
                        StreamsError::Corrupt {
                            stream: stream.clone(),
                            missing_or_mismatched: vec![seq],
                        }
                    })?;
                if envelope.stable_event_id != *stable_event_id {
                    continue;
                }
                if envelope.schema_id == *schema_id && envelope.payload.as_ref() == payload {
                    // Best-effort accelerator advance (monotone
                    // CAS; lost races and failures are fine).
                    self.advance_tail_hint(stream, seq, &envelope).await;
                    return Ok(AppendReceipt {
                        stream_id: stream.clone(),
                        seq,
                        stable_event_id: stable_event_id.clone(),
                        payload_sha256: sha256_hex(payload),
                    });
                }
                return Err(StreamsError::IdempotencyConflict {
                    stream: stream.clone(),
                    stable_event_id: stable_event_id.clone(),
                    conflicting_seq: seq,
                });
            }
        }

        // The next event lands one past the last known event, clamped
        // up to the certified floor. On a fully trimmed stream the
        // clamp binds: the event lands AT `first_retained` — the
        // first free retained slot — because reads walk from the floor
        // and require density (a landing at floor+1 would read back
        // as Corrupt naming the floor).
        let mut guess = (event_max + 1).max(trim);
        let mut conflicts: u32 = 0;
        let mut backoff = APPEND_BACKOFF_START;
        loop {
            let envelope = Envelope::encode(stream, guess, schema_id, stable_event_id, payload)?;
            match self
                .keyspace
                .create(&Self::log_key(stream, guess), envelope.encoded.clone())
                .await
            {
                Ok(()) => {
                    let receipt = AppendReceipt {
                        stream_id: stream.clone(),
                        seq: guess,
                        stable_event_id: stable_event_id.clone(),
                        payload_sha256: sha256_hex(payload),
                    };
                    // Best-effort accelerator advance (monotone CAS;
                    // lost races and failures are fine).
                    self.advance_tail_hint(stream, guess, &envelope).await;
                    return Ok(receipt);
                }
                Err(KeyspaceError::AlreadyExists(_)) => {
                    let existing = self
                        .keyspace
                        .get(&Self::log_key(stream, guess))
                        .await
                        .map_err(map_keyspace("append: readback"))?;
                    match existing {
                        // Byte-identical with the same stable event id:
                        // idempotent success (maintain the accelerator,
                        // as a fresh landing would).
                        Some(bytes) if bytes == envelope.encoded => {
                            self.advance_tail_hint(stream, guess, &envelope).await;
                            return Ok(AppendReceipt {
                                stream_id: stream.clone(),
                                seq: guess,
                                stable_event_id: stable_event_id.clone(),
                                payload_sha256: sha256_hex(payload),
                            });
                        }
                        Some(bytes) => {
                            // Same stable id, different content at the
                            // attempted seq: an in-window idempotency
                            // conflict, not a silent landing at +1.
                            let landed = Envelope::decode_and_verify(stream, guess, &bytes)
                                .map_err(|_| StreamsError::Corrupt {
                                    stream: stream.clone(),
                                    missing_or_mismatched: vec![guess],
                                })?;
                            if landed.stable_event_id == *stable_event_id {
                                return Err(StreamsError::IdempotencyConflict {
                                    stream: stream.clone(),
                                    stable_event_id: stable_event_id.clone(),
                                    conflicting_seq: guess,
                                });
                            }
                            // A different event holds the seq: advance +1.
                            conflicts += 1;
                            if conflicts > APPEND_BUDGET {
                                return Err(StreamsError::AppendBudgetExhausted {
                                    stream: stream.clone(),
                                    attempted: guess,
                                    budget: APPEND_BUDGET,
                                });
                            }
                            if guess == Seq::MAX {
                                return Err(StreamsError::SeqExhausted(stream.clone()));
                            }
                            guess += 1;
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(APPEND_BACKOFF_CAP);
                        }
                        None => {
                            // Create said the key exists, the readback
                            // says it does not: contradictory witnesses
                            // fail closed.
                            return Err(StreamsError::BackendUnqualified {
                                witness: format!(
                                    "append readback absent at seq {guess} after create conflict"
                                ),
                            });
                        }
                    }
                }
                Err(err) => return Err(map_keyspace("append: create")(err)),
            }
        }
    }

    /// Replay events after `after_seq` (first event is `after_seq+1`),
    /// at most `limit` (≥ 1). Typed outcome; see [`Replay`].
    pub async fn read(&self, stream: &StreamId, after_seq: Seq, limit: usize) -> Replay {
        if limit == 0 {
            return Replay::BackendUnqualified {
                witness: "read called with limit 0".into(),
            };
        }
        // Genesis presence is stream existence; its integrity is
        // load-bearing (decode failure is an error, never a skip).
        let genesis = match self.keyspace.get(&Self::log_key(stream, 0)).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Replay::NotFound {
                    stream: stream.clone(),
                };
            }
            Err(_) => {
                return Replay::Unavailable {
                    operation: "read: genesis",
                };
            }
        };
        match Envelope::decode_and_verify(stream, 0, &genesis) {
            Ok(_) => {}
            Err(_) => {
                return Replay::Corrupt {
                    missing_or_mismatched: vec![0],
                };
            }
        }
        // Nothing can exist past u64::MAX (S8).
        if after_seq == Seq::MAX {
            return Replay::Empty;
        }
        // A certified trim floor bounds the walk: starting below
        // `first_retained` is OffsetExpired — a typed boundary, never
        // an empty result, never corruption (the swept-away events
        // are logically gone, not missing).
        match self.keyspace.trim_floor(stream.as_str()).await {
            Ok(Some(first_retained)) if after_seq + 1 < first_retained => {
                return Replay::OffsetExpired { first_retained };
            }
            Ok(_) => {}
            Err(_) => {
                return Replay::Unavailable {
                    operation: "read: trim floor",
                };
            }
        }
        // Clamp + bounded-parallel computed-key GETs; stop at the
        // first absent seq (contiguity: the first fetched seq must be
        // after_seq+1, and the fetched set must be dense). The walk is
        // an explicit cursor, not a RangeFrom: iterating past MAX
        // would overflow, and the ceiling is a typed boundary.
        let mut events: Vec<Envelope> = Vec::with_capacity(limit.min(64));
        let mut last_fetched: Option<Seq> = None;
        // The seq whose GET said absent (the contiguity stop), if any.
        let mut stopped_at_absent: Option<Seq> = None;
        let mut chunk_start = after_seq + 1;
        'fetch: loop {
            // A bounded chunk that INCLUDES u64::MAX when reached
            // (saturating range ends would exclude it).
            let want = FETCH_PARALLELISM.min(limit - events.len());
            let mut chunk: Vec<Seq> = Vec::with_capacity(want);
            let mut seq = chunk_start;
            for _ in 0..want {
                chunk.push(seq);
                seq = match seq.checked_add(1) {
                    Some(next) => next,
                    None => break,
                };
            }
            if chunk.is_empty() {
                break;
            }
            let gets = chunk.iter().map(|seq| {
                let key = Self::log_key(stream, *seq);
                async move { self.keyspace.get(&key).await }
            });
            let results = futures::future::join_all(gets).await;
            for (seq, result) in chunk.iter().zip(results) {
                match result {
                    Ok(Some(bytes)) => match Envelope::decode_and_verify(stream, *seq, &bytes) {
                        Ok(envelope) => {
                            events.push(envelope);
                            last_fetched = Some(*seq);
                        }
                        Err(_) => {
                            return Replay::Corrupt {
                                missing_or_mismatched: vec![*seq],
                            };
                        }
                    },
                    Ok(None) => {
                        stopped_at_absent = Some(*seq);
                        break 'fetch;
                    }
                    Err(_) => {
                        return Replay::Unavailable {
                            operation: "read: fetch",
                        };
                    }
                }
            }
            if events.len() >= limit {
                break;
            }
            chunk_start = match chunk.last().and_then(|seq| seq.checked_add(1)) {
                Some(next) => next,
                // The chunk included u64::MAX; no next GET exists.
                None => break,
            };
        }

        // Completeness is qualified ONLY by a bounded ordered LIST
        // probe past the last fetched key (the strong-LIST backend
        // contract is a hard qualification; stale LIST under-reports —
        // staleness never loss — and a contradiction fails closed).
        let probe_from = last_fetched.unwrap_or(after_seq);
        let next_listed = match self.next_log_key_after(stream, probe_from).await {
            Ok(next) => next,
            Err(_) => {
                return Replay::BackendUnqualified {
                    witness: "completeness LIST probe failed".into(),
                };
            }
        };
        match next_listed {
            Some(next) => {
                if let Some(absent) = stopped_at_absent {
                    if next == absent {
                        // The GET said absent; the LIST says present —
                        // contradictory witnesses fail closed.
                        Replay::BackendUnqualified {
                            witness: format!("LIST contradicts GET at seq {absent}"),
                        }
                    } else {
                        // A later log key exists past the hole: the
                        // damage is named, never skipped.
                        Replay::Corrupt {
                            missing_or_mismatched: (absent..next).collect(),
                        }
                    }
                } else {
                    let last = last_fetched.expect("events nonempty");
                    if next > last {
                        // The page was cut by the limit. With no hint
                        // covering this prefix, rebuild the accelerator
                        // by binary search over computed keys (S6) — a
                        if self.verified_tail_hint(stream).await.is_none() {
                            self.recover_tail_hint(stream, last).await;
                        }
                        Replay::Page {
                            events,
                            complete: false,
                        }
                    } else {
                        // next <= last cannot happen: the probe starts
                        // after `last`. Defensive fail-closed.
                        Replay::BackendUnqualified {
                            witness: "LIST probe contradicted the fetched page".into(),
                        }
                    }
                }
            }
            None => {
                // The ordered probe beyond the last fetched seq came
                // back empty. Fail-closed on contradicted witnesses
                // (S9): a digest-verified hint above the
                // LIST-qualified end proves the LIST is stale
                // (under-reporting); a stale LIST can never grant a
                // false complete.
                let end = last_fetched.unwrap_or(after_seq);
                let verified = self.verified_tail_hint(stream).await;
                if let Some(hint) = &verified
                    && hint.highest_validated_dense_seq > end
                {
                    return Replay::BackendUnqualified {
                        witness: format!(
                            "stale LIST: verified hint at seq {} exceeds the qualified end {}",
                            hint.highest_validated_dense_seq, end
                        ),
                    };
                }
                // Witness-bounded completeness (ruled addendum, G130):
                // `complete=true` requires a VERIFIED tail hint naming
                // this qualified end. A lagging valid hint proves only
                // its own prefix and cannot bound a LIST-hidden suffix.
                // Without an end-matching witness, serve
                // `complete: false` and recover the true tail through
                // computed-key GET probes so the next read can certify.
                let end_witnessed = verified
                    .as_ref()
                    .is_some_and(|hint| hint.highest_validated_dense_seq == end);
                // A hint is allowed to lag. When the page stopped only
                // because it reached `limit`, even an end-matching hint
                // might predate a later append. Confirm the immediate
                // dense successor by computed-key GET before allowing
                // that hint to certify the end.
                if end_witnessed && stopped_at_absent.is_none() && end < Seq::MAX {
                    let successor = end + 1;
                    match self.keyspace.get(&Self::log_key(stream, successor)).await {
                        Ok(Some(bytes)) => {
                            return match Envelope::decode_and_verify(stream, successor, &bytes) {
                                Ok(_) => Replay::BackendUnqualified {
                                    witness: format!(
                                        "LIST omitted GET-visible successor at seq {successor}"
                                    ),
                                },
                                Err(_) => Replay::Corrupt {
                                    missing_or_mismatched: vec![successor],
                                },
                            };
                        }
                        Ok(None) => {}
                        Err(_) => {
                            return Replay::Unavailable {
                                operation: "read: terminal successor",
                            };
                        }
                    }
                }
                if !end_witnessed {
                    let from = last_fetched.unwrap_or(after_seq);
                    self.recover_tail_hint(stream, from).await;
                } else {
                    // Read-path self-heal: after a qualified end,
                    // advance a lagging accelerator (bounded probe;
                    // density makes existence monotone).
                    self.heal_tail_hint(stream, after_seq, events.last()).await;
                }
                if events.is_empty() {
                    Replay::Empty
                } else {
                    Replay::Page {
                        events,
                        complete: end_witnessed,
                    }
                }
            }
        }
    }

    /// Read a consumer's cursor (`None` = replay from start).
    pub async fn read_cursor(
        &self,
        stream: &StreamId,
        consumer: &str,
    ) -> Result<Option<Cursor>, StreamsError> {
        let key = Self::cursor_key(stream, consumer)?;
        match self
            .keyspace
            .get(&key)
            .await
            .map_err(map_keyspace("read_cursor"))?
        {
            Some(bytes) => self
                .decode_and_verify_cursor(stream, consumer, &bytes, "read_cursor: event")
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    /// Advance a consumer's cursor to `target_seq`. The target event
    /// must exist; the advance is monotonic-only and moves by CAS.
    pub async fn advance_cursor(
        &self,
        stream: &StreamId,
        consumer: &str,
        target_seq: Seq,
    ) -> Result<Cursor, StreamsError> {
        if target_seq == 0 {
            return Err(StreamsError::InvalidArgument(
                "cursor target must be an event seq (>= 1)".into(),
            ));
        }
        // Validate the target event exists.
        let event = self
            .keyspace
            .get(&Self::log_key(stream, target_seq))
            .await
            .map_err(map_keyspace("advance_cursor: target"))?;
        let event_id = match event {
            Some(bytes) => match Envelope::decode_and_verify(stream, target_seq, &bytes) {
                Ok(envelope) => envelope.stable_event_id.as_str().to_string(),
                Err(_) => {
                    return Err(StreamsError::Corrupt {
                        stream: stream.clone(),
                        missing_or_mismatched: vec![target_seq],
                    });
                }
            },
            None => {
                // A trimmed target is a typed boundary, not damage:
                // below the certified floor the event is logically
                // gone (absence above the floor stays EventMissing).
                if let Some(first_retained) = self.trim_floor(stream).await.ok().flatten()
                    && target_seq < first_retained
                {
                    return Err(StreamsError::OffsetExpired {
                        stream: stream.clone(),
                        first_retained,
                    });
                }
                return Err(StreamsError::EventMissing {
                    stream: stream.clone(),
                    seq: target_seq,
                });
            }
        };
        let desired = Cursor {
            format_version: ENVELOPE_FORMAT_VERSION,
            stream_id: stream.clone(),
            seq: target_seq,
            event_id,
        };
        let key = Self::cursor_key(stream, consumer)?;
        loop {
            match self.keyspace.get_with_etag(&key).await {
                Ok(None) => {
                    match self.keyspace.create(&key, cursor_bytes(&desired)).await {
                        Ok(()) => return Ok(desired),
                        // Lost the create race: re-read, re-derive.
                        Err(KeyspaceError::AlreadyExists(_)) => continue,
                        Err(err) => return Err(map_keyspace("advance_cursor: create")(err)),
                    }
                }
                Ok(Some((bytes, etag))) => {
                    let current = match self
                        .decode_and_verify_cursor(
                            stream,
                            consumer,
                            &bytes,
                            "advance_cursor: current event",
                        )
                        .await
                    {
                        Ok(cursor) => cursor,
                        // A current cursor below the certified floor
                        // references a swept event: its stored seq
                        // remains the position for monotonicity (the
                        // floor witnesses the event once existed —
                        // cursors are written only after event
                        // verification); re-verification is impossible
                        // and unnecessary. Advancing out from under a
                        // trim is the supported recovery.
                        Err(StreamsError::OffsetExpired { .. }) => {
                            let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| {
                                StreamsError::CursorCorrupt {
                                    stream: stream.clone(),
                                    consumer: consumer.to_string(),
                                    reason: "payload is malformed",
                                }
                            })?;
                            if cursor.format_version != ENVELOPE_FORMAT_VERSION
                                || cursor.stream_id != *stream
                                || cursor.seq == 0
                            {
                                return Err(StreamsError::CursorCorrupt {
                                    stream: stream.clone(),
                                    consumer: consumer.to_string(),
                                    reason: "payload is malformed",
                                });
                            }
                            cursor
                        }
                        Err(err) => return Err(err),
                    };
                    if current.seq > target_seq {
                        return Err(StreamsError::CursorNotMonotonic {
                            stream: stream.clone(),
                            current: current.seq,
                            target: target_seq,
                        });
                    }
                    if current.seq == target_seq {
                        // Idempotent: a prior advance that landed but
                        // lost its response converges here.
                        return Ok(current);
                    }
                    match self
                        .keyspace
                        .compare_exchange(&key, &etag, cursor_bytes(&desired))
                        .await
                    {
                        Ok(_) => return Ok(desired),
                        // Contention: re-read, re-derive, retry (law 4).
                        Err(KeyspaceError::PreconditionFailed { .. }) => continue,
                        Err(err) => return Err(map_keyspace("advance_cursor: cas")(err)),
                    }
                }
                Err(_) => {
                    return Err(StreamsError::Unavailable {
                        operation: "advance_cursor: read",
                    });
                }
            }
        }
    }

    async fn decode_and_verify_cursor(
        &self,
        stream: &StreamId,
        consumer: &str,
        bytes: &[u8],
        event_operation: &'static str,
    ) -> Result<Cursor, StreamsError> {
        let corrupt = |reason| StreamsError::CursorCorrupt {
            stream: stream.clone(),
            consumer: consumer.to_string(),
            reason,
        };
        let cursor: Cursor =
            serde_json::from_slice(bytes).map_err(|_| corrupt("payload is malformed"))?;
        if cursor.format_version != ENVELOPE_FORMAT_VERSION {
            return Err(corrupt("format version is unsupported"));
        }
        if cursor.stream_id != *stream {
            return Err(corrupt("stream id does not match its key"));
        }
        if cursor.seq == 0 {
            return Err(corrupt("seq does not name an event"));
        }

        let event = match self
            .keyspace
            .get(&Self::log_key(stream, cursor.seq))
            .await
            .map_err(map_keyspace(event_operation))?
        {
            Some(bytes) => bytes,
            None => {
                // A swept cursor target below the certified floor is
                // OffsetExpired — trim-induced absence is never
                // corruption (law 7's other edge).
                if let Some(first_retained) = self.trim_floor(stream).await.ok().flatten()
                    && cursor.seq < first_retained
                {
                    return Err(StreamsError::OffsetExpired {
                        stream: stream.clone(),
                        first_retained,
                    });
                }
                return Err(corrupt("referenced event is absent"));
            }
        };
        let envelope = Envelope::decode_and_verify(stream, cursor.seq, &event)
            .map_err(|_| corrupt("referenced event is corrupt"))?;
        if cursor.event_id != envelope.stable_event_id.as_str() {
            return Err(corrupt("event id does not match the referenced event"));
        }
        Ok(cursor)
    }

    /// LIST-derived max log seq (existence-witnessed floor). Seeded
    /// from the verified tail hint minus the idempotency scan window:
    /// keys at or below a verified-dense hint exist and cannot raise
    /// the max above it, so re-listing them costs O(N/1000) LISTs per
    /// append for nothing. The seeded walk still sees the hint seq's
    /// own key, so the derived max is exact; with no verified hint the
    /// walk starts at genesis. The idempotency-window formula in
    /// `append` is unchanged — only the walk's start moves.
    async fn list_log_max(&self, stream: &StreamId) -> Result<Seq, StreamsError> {
        let seed = self
            .verified_tail_hint(stream)
            .await
            .map(|hint| {
                hint.highest_validated_dense_seq
                    .saturating_sub(IDEMPOTENT_SCAN_WINDOW)
            })
            .unwrap_or(0);
        let prefix = format!("{}/log/", stream.as_str());
        let mut after = Some(Self::log_key(stream, seed)); // walk from the seed
        let mut max = seed;
        loop {
            let keys = self
                .keyspace
                .list_after(after.as_deref(), 1000)
                .await
                .map_err(map_keyspace("append: list floor"))?;
            if keys.is_empty() {
                return Ok(max);
            }
            let mut advanced = false;
            for key in &keys {
                if let Some(seq) = key.strip_prefix(&prefix).and_then(parse_seq) {
                    max = max.max(seq);
                    after = Some(key.clone());
                    advanced = true;
                }
            }
            if !advanced {
                return Ok(max);
            }
        }
    }

    /// The next log seq strictly after `after` (bounded ordered LIST
    /// probe; non-log keys — tail, cursors — are filtered out).
    async fn next_log_key_after(
        &self,
        stream: &StreamId,
        after: Seq,
    ) -> Result<Option<Seq>, KeyspaceError> {
        let prefix = format!("{}/log/", stream.as_str());
        let start = format!("{}/log/{}", stream.as_str(), seq_key_component(after));
        let keys = self.keyspace.list_after(Some(&start), 2).await?;
        Ok(keys
            .into_iter()
            .filter_map(|key| key.strip_prefix(&prefix).and_then(parse_seq))
            .filter(|seq| *seq > after)
            .min())
    }

    async fn read_tail_hint(&self, stream: &StreamId) -> Result<Option<TailHint>, StreamsError> {
        match self
            .keyspace
            .get(&Self::tail_key(stream))
            .await
            .map_err(map_keyspace("read tail hint"))?
        {
            Some(bytes) => {
                let hint: TailHint =
                    serde_json::from_slice(&bytes).map_err(|_| StreamsError::Corrupt {
                        stream: stream.clone(),
                        missing_or_mismatched: vec![],
                    })?;
                if hint.format_version != ENVELOPE_FORMAT_VERSION {
                    return Err(StreamsError::Corrupt {
                        stream: stream.clone(),
                        missing_or_mismatched: vec![],
                    });
                }
                Ok(Some(hint))
            }
            None => Ok(None),
        }
    }

    /// A present tail hint whose named terminal record is confirmed
    /// present with a matching digest — the completeness witness
    /// (ruled addendum). A hint that is absent, malformed, or whose
    /// record cannot be verified is NOT a witness.
    async fn verified_tail_hint(&self, stream: &StreamId) -> Option<TailHint> {
        let hint = self.read_tail_hint(stream).await.ok()??;
        let seq = hint.highest_validated_dense_seq;
        let bytes = self
            .keyspace
            .get(&Self::log_key(stream, seq))
            .await
            .ok()??;
        let envelope = Envelope::decode_and_verify(stream, seq, &bytes).ok()?;
        (envelope.digest_hex() == hint.terminal_record_digest).then_some(hint)
    }

    /// Best-effort monotone CAS advance of the tail hint after a
    /// landed append. Failures are fine — the hint is an accelerator.
    async fn advance_tail_hint(&self, stream: &StreamId, landed: Seq, envelope: &Envelope) {
        let desired = TailHint {
            format_version: ENVELOPE_FORMAT_VERSION,
            highest_validated_dense_seq: landed,
            terminal_record_digest: envelope.digest_hex(),
        };
        let desired_bytes =
            bytes::Bytes::from(serde_json::to_vec(&desired).expect("tail hint JSON"));
        let key = Self::tail_key(stream);
        match self.keyspace.get_with_etag(&key).await {
            Ok(None) => {
                let _ = self.keyspace.create(&key, desired_bytes).await;
            }
            Ok(Some((bytes, etag))) => {
                let replace = match serde_json::from_slice::<TailHint>(&bytes) {
                    Err(_) => true,
                    Ok(current) if current.format_version != ENVELOPE_FORMAT_VERSION => true,
                    Ok(current) if current.highest_validated_dense_seq < landed => true,
                    Ok(current) if current.highest_validated_dense_seq == landed => {
                        current.terminal_record_digest != desired.terminal_record_digest
                    }
                    Ok(current) => {
                        let seq = current.highest_validated_dense_seq;
                        match self.keyspace.get(&Self::log_key(stream, seq)).await {
                            Ok(Some(bytes)) => Envelope::decode_and_verify(stream, seq, &bytes)
                                .map(|envelope| {
                                    envelope.digest_hex() != current.terminal_record_digest
                                })
                                .unwrap_or(true),
                            Ok(None) => true,
                            Err(_) => false,
                        }
                    }
                };
                if replace {
                    let _ = self
                        .keyspace
                        .compare_exchange(&key, &etag, desired_bytes)
                        .await;
                }
            }
            Err(_) => {}
        }
    }

    /// Read-path accelerator self-heal after a LIST-qualified end:
    /// write the hint when this read validated the dense prefix it
    /// extends. A partial page additionally binary-searches the true
    /// tail (exponential probe over computed keys; density makes
    /// existence monotone).
    async fn heal_tail_hint(&self, stream: &StreamId, after_seq: Seq, events: Option<&Envelope>) {
        let landed = match events {
            Some(envelope) => envelope.seq,
            None => return,
        };
        // Validated dense prefix condition: either this read started
        // at the beginning (after_seq == 0), or an existing hint
        // already validated a prefix covering [0..=after_seq].
        let hint = self.verified_tail_hint(stream).await;
        let prefix_trusted = after_seq == 0
            || hint
                .as_ref()
                .is_some_and(|hint| hint.highest_validated_dense_seq >= after_seq);
        if !prefix_trusted {
            return;
        }
        if let Some(hint) = &hint
            && hint.highest_validated_dense_seq >= landed
        {
            return; // already at or past what we validated
        }
        self.advance_tail_hint(stream, landed, events.expect("checked"))
            .await;
    }

    /// Binary-search tail rebuild over computed keys: exponential
    /// probe (existence is monotone in seq on a dense log — a probe
    /// result below a gap is a safe LOW hint, never high), then
    /// binary search between the last hit and first miss, then verify
    /// the terminal envelope's digest before the monotone CAS.
    async fn recover_tail_hint(&self, stream: &StreamId, from: Seq) {
        let get = |seq: Seq| {
            let key = Self::log_key(stream, seq);
            async move { self.keyspace.get(&key).await }
        };
        match get(from).await {
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => return,
        }
        // Exponential probe upward. A storage failure aborts recovery;
        // it is never evidence that a sequence is absent.
        let mut low = from;
        let mut span: u64 = 1;
        loop {
            let probe = from.saturating_add(span);
            if probe == from {
                break; // saturating: no room left
            }
            match get(probe).await {
                Ok(Some(_)) => {
                    low = probe;
                    span = span.saturating_mul(2);
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
        // Binary search in (low, from+span).
        let high_bound = from.saturating_add(span);
        let mut high = high_bound;
        while high - low > 1 {
            let mid = low + (high - low) / 2;
            match get(mid).await {
                Ok(Some(_)) => low = mid,
                Ok(None) => high = mid,
                Err(_) => return,
            }
        }
        // Verify the terminal envelope, then monotone CAS.
        let bytes = match get(low).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) | Err(_) => return,
        };
        let Ok(envelope) = Envelope::decode_and_verify(stream, low, &bytes) else {
            return;
        };
        self.advance_tail_hint(stream, low, &envelope).await;
    }
}

fn cursor_bytes(cursor: &Cursor) -> bytes::Bytes {
    bytes::Bytes::from(serde_json::to_vec(cursor).expect("cursor JSON"))
}

fn parse_seq(component: &str) -> Option<Seq> {
    if component.len() == 20 && component.bytes().all(|b| b.is_ascii_digit()) {
        component.parse().ok()
    } else {
        None
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn map_keyspace(operation: &'static str) -> impl Fn(KeyspaceError) -> StreamsError {
    move |err| match err {
        KeyspaceError::Unavailable { .. } => StreamsError::Unavailable { operation },
        other => StreamsError::InvalidArgument(other.to_string()),
    }
}
