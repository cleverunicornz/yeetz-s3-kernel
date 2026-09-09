//! Typed errors and the six-state read outcome (ADR 0017).

use crate::Seq;
use crate::StableEventId;
use crate::StreamId;
use yeetz_s3_kernel::atomic_keyspace::KeyspaceError;

#[derive(Debug, thiserror::Error)]
pub enum StreamsError {
    /// A caller-supplied identifier or payload failed validation.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// The canonical encoded streams envelope exceeds the structural
    /// 16 MiB bound (ADR 0004 §3.4/S11): enforcement occurs after
    /// canonical JSON/base64 encoding and before the first keyspace
    /// effect, so every streams write stays a single inline v2
    /// object regardless of the kernel's `INLINE_MAX` ruling.
    #[error(
        "streams envelope too large: encoded_len {encoded_len} > max_encoded_len {max_encoded_len}"
    )]
    EnvelopeTooLarge {
        encoded_len: u64,
        max_encoded_len: u64,
    },
    /// The stream does not exist (no genesis object at seq 0).
    #[error("stream not found: {0:?}")]
    StreamNotFound(StreamId),
    /// Every candidate in the retry window was occupied by a
    /// different event; the operation is retryable by the caller.
    #[error("append budget exhausted for {stream:?} at seq {attempted} (budget {budget})")]
    AppendBudgetExhausted {
        stream: StreamId,
        attempted: Seq,
        budget: u32,
    },
    /// The stable event id was reused with different content inside
    /// the idempotency window (a retry that changed its bytes, or two
    /// writers minting one id for different events). Idempotency is
    /// bounded to the window; inside it this is a conflict, never a
    /// silent second landing.
    #[error(
        "idempotency conflict in {stream:?}: stable event id {stable_event_id:?} already landed at seq {conflicting_seq} with different content"
    )]
    IdempotencyConflict {
        stream: StreamId,
        stable_event_id: StableEventId,
        conflicting_seq: Seq,
    },
    /// No successor seq exists (u64::MAX occupied).
    #[error("seq space exhausted for {0:?}")]
    SeqExhausted(StreamId),
    /// The backing store failed in a way the caller should retry.
    #[error("streams store unavailable: {operation}")]
    Unavailable { operation: &'static str },
    /// The backend cannot be trusted for a contract this operation
    /// relies on (e.g. a LIST that contradicts a fetched witness).
    #[error("backend unqualified: {witness}")]
    BackendUnqualified { witness: String },
    /// A log object is missing or fails verification where the
    /// operation requires its truth (cursor targets, hints).
    #[error("corrupt stream {stream:?}: missing or mismatched seqs {missing_or_mismatched:?}")]
    Corrupt {
        stream: StreamId,
        missing_or_mismatched: Vec<Seq>,
    },
    /// A persisted migration seal is malformed or uses a format this
    /// reader cannot validate. Stored evidence corruption is distinct
    /// from a caller-supplied invalid seal.
    #[error("corrupt migration seal for {stream:?}: {reason}")]
    MigrationSealCorrupt {
        stream: StreamId,
        reason: &'static str,
    },
    /// A persisted cursor is malformed or disagrees with the event it
    /// claims to acknowledge. Stored corruption is distinct from a
    /// caller-supplied invalid argument.
    #[error("corrupt cursor {consumer:?} for {stream:?}: {reason}")]
    CursorCorrupt {
        stream: StreamId,
        consumer: String,
        reason: &'static str,
    },
    /// The cursor target event does not exist.
    #[error("event missing in {stream:?} at seq {seq}")]
    EventMissing { stream: StreamId, seq: Seq },
    /// Cursor advance target is not ahead of the current position.
    #[error("cursor not monotonic for {stream:?}: current {current}, target {target}")]
    CursorNotMonotonic {
        stream: StreamId,
        current: Seq,
        target: Seq,
    },
    /// The requested offset is below the stream's certified trim
    /// floor: the events before `first_retained` are logically gone
    /// (physically gone after the sweeper ran). A typed boundary —
    /// never an empty result, never corruption.
    #[error("offset expired for {stream:?}: first retained seq is {first_retained}")]
    OffsetExpired {
        stream: StreamId,
        first_retained: Seq,
    },
}

/// The typed read outcome (ADR 0017; seven states since the
/// batch-5 trim addendum): `NotFound | Empty | Page { events,
/// complete } | Corrupt { missing_or_mismatched_seqs } |
/// Unavailable | BackendUnqualified | OffsetExpired`.
///
/// `complete=true` is witness-bounded (human-ruled contract): it
/// requires BOTH a verified tail hint naming the last fetched/verified
/// seq — the hint object present and its named record confirmed with a
/// matching digest, a witness a stale LIST cannot hide — AND an empty
/// ordered LIST probe beyond that seq. A limit-cut page also confirms
/// the immediate successor is absent by computed-key GET because a
/// valid hint may lag. With no end-matching verified hint the page is
/// served `complete: false`: the honest claim behind
/// `complete=true` is "no suffix visible under the qualified backend
/// contract, bounded by a witness". A backend whose LIST fails or
/// contradicts a fetched witness yields `BackendUnqualified` —
/// fail-closed, never a false complete.
#[derive(Debug)]
pub enum Replay {
    /// No genesis object: the stream never existed (distinct from
    /// empty and from corruption).
    NotFound { stream: StreamId },
    /// The stream exists; there are no events after `after_seq` (the
    /// LIST-qualified end).
    Empty,
    /// A dense, verified page. Resume the walk with
    /// `after_seq = events.last().seq()` — the read is after-exclusive,
    /// so any cursor beyond the last fetched seq would skip events
    /// (the D2 defect; there is deliberately no `next_seq` field).
    /// `complete` is witness-bounded — see the enum docs.
    Page {
        events: Vec<crate::Envelope>,
        complete: bool,
    },
    /// The log is damaged: seqs missing inside a range the LIST
    /// witness says must be dense, or envelopes failing verification.
    /// Named, never skipped.
    Corrupt { missing_or_mismatched: Vec<Seq> },
    /// The walk would start below the certified trim floor
    /// (`after_seq + 1 < first_retained`): those events are logically
    /// gone — a typed boundary, not an empty page and not damage.
    /// Resume instead at `after_seq >= first_retained - 1`.
    OffsetExpired { first_retained: Seq },
    /// Store failure mid-read; retry.
    Unavailable { operation: &'static str },
    /// The backend violated a hard qualification; fail-closed.
    BackendUnqualified { witness: String },
}

impl Replay {
    /// The events of a `Page` (empty otherwise) — test/ergonomics.
    #[must_use]
    pub fn events(&self) -> &[crate::Envelope] {
        match self {
            Replay::Page { events, .. } => events,
            _ => &[],
        }
    }
}

/// Classify a kernel keyspace error from an event-object GET
/// (`AtomicKeyspace::get`) for the strict event reads
/// (`Streams::read_event` / `Streams::read_range`).
///
/// Three honest classes, kept distinct from each other and from every
/// boundary outcome:
///
/// - **Stored integrity** — the kernel could not reassemble or verify
///   the stored object for this event: the versioned value envelope,
///   a v3 manifest (malformed, oversized, non-canonical chunk count,
///   impossible logical length, root disagreement), or a referenced
///   chunk (absent, wrong length, digest mismatch). Damage at or
///   below the stream envelope still names the event:
///   [`StreamsError::Corrupt`] carries `seq`. Stored corruption is
///   never a caller error and never absence.
/// - **Store unavailability** — the object GET or a chunk fetch
///   failed: [`StreamsError::Unavailable`] under the caller's
///   operation name.
/// - **Identifier rejection** — the keyspace refused the key itself:
///   [`StreamsError::InvalidArgument`], the only caller-side failure
///   a GET can surface.
///
/// Every other keyspace outcome (write/CAS/trim/maintenance arms) is
/// unreachable from a GET; fail-closed as [`StreamsError::Unavailable`]
/// rather than accusing the caller or naming corruption.
pub(crate) fn map_event_read_error(
    stream: &StreamId,
    seq: Seq,
    operation: &'static str,
    err: KeyspaceError,
) -> StreamsError {
    match err {
        // Stored read-integrity failures reachable from
        // `AtomicKeyspace::get`: `ControlEnvelope::decode` (inline
        // value envelope, v3 manifest) and `fetch_all_chunks`
        // (manifest-referenced chunks).
        KeyspaceError::ValueEnvelopeMalformed(_)
        | KeyspaceError::ManifestMalformed(_)
        | KeyspaceError::ManifestRootMismatch(_)
        | KeyspaceError::ManifestTooLarge { .. }
        | KeyspaceError::ChunkCountInvalid { .. }
        | KeyspaceError::ValueTooLarge { .. }
        | KeyspaceError::ChunkMissing { .. }
        | KeyspaceError::ChunkIntegrity { .. } => StreamsError::Corrupt {
            stream: stream.clone(),
            missing_or_mismatched: vec![seq],
        },
        KeyspaceError::InvalidIdentifier(detail) => StreamsError::InvalidArgument(detail),
        KeyspaceError::Unavailable { .. } => StreamsError::Unavailable { operation },
        // Unreachable from a GET; fail closed as a store failure.
        _ => StreamsError::Unavailable { operation },
    }
}
