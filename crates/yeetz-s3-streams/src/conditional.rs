//! Kernel-only conditional writes: caller-named stream creation and
//! predecessor-expected append.
//!
//! Both operations reconcile one demand against an append-only,
//! certifiably trimmed log, and both are bounded by qualified
//! retention: the certified trim floor — an immutable certificate,
//! never object presence — is the boundary of what the log still
//! retains. Every answer here is stated under that qualification.
//!
//! # Effect certainty
//!
//! [`Streams::append_expected`] performs at most one consumed
//! operation: a single conditional create at the exact successor
//! position named by the predecessor. Every failure carries an
//! [`AppendExpectedEffect`] stating what the call can prove:
//!
//! - [`NotAttempted`](AppendExpectedEffect::NotAttempted): no
//!   consumed operation ran; nothing was written.
//! - [`PossiblyCommitted`](AppendExpectedEffect::PossiblyCommitted):
//!   the conditional create was invoked and its aggregate outcome
//!   cannot exclude a PUT that landed — the kernel's create reports
//!   `AlreadyExists` after cleaning up its own stale-era write, so no
//!   aggregate error is a rejection. Uncertainty, never denial.
//! - [`Committed`](AppendExpectedEffect::Committed): the canonical
//!   desired bytes are confirmed at the exact position — by the
//!   create itself or by exact readback — and the receipt names them.
//!
//! Certainty never downgrades: once the create is invoked, every
//! error keeps at least `PossiblyCommitted`, and an error observed
//! after a confirmed landing keeps `Committed`. Corruption and
//! conflict observed after an attempt carry their typed kind plus the
//! surviving effect in the one shared error wrapper.
//!
//! # Qualified retention
//!
//! `Ok` from [`Streams::append_expected`] means exactly: the
//! requested canonical envelope was confirmed at the exact successor
//! and a later successful qualified floor observation did not retire
//! the target. It is not a retention pin, a future-retention
//! guarantee, an ownership grant, or an append-plus-trim fence. The
//! floor may pass the target moments later; the bytes can be
//! collected by a later GC sweep; a target already collected cannot
//! reconstruct an earlier caller's lost receipt or uncertainty — the
//! caller retains its prior result.
//!
//! Two edges are explicit:
//!
//! - **Genesis immortality.** Seq 0 is never retired by any certified
//!   floor, so it is exempt from predecessor expiry: a fully trimmed
//!   stream (floor == 1) still accepts a fresh append from genesis at
//!   seq 1.
//! - **Below-floor zombies.** Objects a floor has retired but the
//!   sweeper has not yet collected still read back. Exact
//!   reconciliation against such a zombie proves the target bytes —
//!   and returns them as [`Expired`](AppendExpectedFailure::Expired)
//!   with a [`Committed`](AppendExpectedEffect::Committed) effect,
//!   not a success.
//!
//! A silently stale certificate LIST can miss a newer floor; that is
//! the standing qualified-backend limitation, stated rather than
//! repaired. An [`EventRef`] identifies a record for revalidation; it
//! is forgeable, carries no capability, and certifies neither a
//! record's bytes nor any prefix of the log — only a verified
//! [`Envelope`] vouches for the event it names. Predecessor identity
//! cannot be validated after the predecessor's collection, but
//! exact-target reconciliation remains allowed and proves exactly the
//! target's bytes — never that the supplied predecessor digest or id
//! was examined, and never any authorization.

use crate::error::map_event_read_error;
use crate::{
    AppendReceipt, Envelope, EventRef, SchemaId, Seq, StableEventId, StreamId, Streams,
    StreamsError, map_keyspace, validate_segment,
};
use yeetz_s3_kernel::atomic_keyspace::KeyspaceError;

/// The outcome of [`Streams::create_stream_with_id`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateStreamOutcome {
    /// The conditional create at seq 0 won: this call created the
    /// stream's genesis.
    Created,
    /// The genesis already existed with exactly the canonical bytes
    /// this call proposed: the stream is the one this configuration
    /// names. An observation, not a writer grant — it reserves the
    /// stream for no one, fences no other writer, and owns no
    /// identity.
    Existing,
}

/// The failure of [`Streams::create_stream_with_id`].
#[derive(Debug, thiserror::Error)]
pub enum CreateStreamError {
    /// Storage-layer failure. Includes validation of the caller's
    /// id, an oversize encoded genesis, a corrupt incumbent,
    /// backend disqualification, and permanent kernel admission
    /// refusals (a syntactically valid id whose genesis key is
    /// kernel-reserved or otherwise invalid). A storage-unavailable
    /// failure is safely retried with the SAME id and configuration
    /// bytes: the create is put-if-absent, so a retry converges on
    /// [`Existing`](CreateStreamOutcome::Existing) or
    /// [`Created`](CreateStreamOutcome::Created).
    #[error("create_stream_with_id storage failure: {0}")]
    Storage(#[from] StreamsError),
    /// The stream already exists with a valid genesis that is not
    /// byte-identical to the canonical genesis this configuration
    /// encodes: the id names a different configuration. The
    /// incumbent is never overwritten and its identity is never
    /// adopted.
    #[error("stream {stream:?} already exists with a different genesis")]
    ConfigurationConflict { stream: StreamId },
}

/// What an [`AppendExpectedError`] can prove about storage effects.
///
/// The ladder is one-way: once the conditional create is invoked the
/// effect is at least [`PossiblyCommitted`], and no later failure —
/// readback absence, an unavailable response, a conflicting or
/// corrupt occupant, an expired position — downgrades it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendExpectedEffect {
    /// No consumed operation ran; the call wrote nothing.
    NotAttempted,
    /// The single conditional create at the exact target was invoked.
    /// Its aggregate outcome cannot exclude a PUT that landed — the
    /// kernel's create may report `AlreadyExists` only after cleaning
    /// up its own stale-era write — so this is uncertainty, never a
    /// rejection. A caller that must know reconciles by retrying
    /// this same operation.
    PossiblyCommitted,
    /// The canonical desired envelope is confirmed at the exact
    /// target — by the create itself or by exact readback — and the
    /// receipt names it.
    Committed(AppendReceipt),
}

/// Which named position a retention expiry retired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiredSubject {
    /// The predecessor the caller expected no longer verifies: a
    /// certified trim floor retired it. The genesis (seq 0) is
    /// immortal and is never this subject.
    Predecessor,
    /// The demanded target position itself sits below the certified
    /// trim floor: retained history no longer includes it, whether
    /// or not the sweeper has collected the object.
    Target,
}

/// The failure taxonomy of [`Streams::append_expected`]. Every
/// failure is carried by [`AppendExpectedError`] together with the
/// effect the call can prove; no variant states a rejection of a
/// possibly-committed write.
#[derive(Debug, thiserror::Error)]
pub enum AppendExpectedFailure {
    /// Storage-layer failure wrapping the existing typed streams
    /// errors unchanged — including corruption and idempotency
    /// conflict observed at the target after an attempt, whose
    /// effect survives in the shared error wrapper.
    #[error("append_expected storage failure: {0}")]
    Storage(#[from] StreamsError),
    /// The event at the predecessor's seq verifies but is not the
    /// event the caller named: all four [`EventRef`] fields — stream,
    /// seq, stable event id, payload digest — must match.
    #[error("predecessor mismatch: expected {expected:?}, observed {observed:?}")]
    PredecessorMismatch {
        expected: EventRef,
        observed: EventRef,
    },
    /// A different verified event occupies the exact target position.
    /// The slot is never advanced past; nothing is written at any
    /// other position.
    #[error("position conflict in {stream:?} at seq {target_seq}: occupied by {occupant:?}")]
    PositionConflict {
        stream: StreamId,
        target_seq: Seq,
        occupant: EventRef,
    },
    /// A verified later event exists past the absent target: the
    /// demanded position is a hole in retained history. The hole is
    /// never repaired by this API.
    #[error("hole witnessed in {stream:?} at seq {target_seq}: later verified event {later:?}")]
    HoleWitnessed {
        stream: StreamId,
        target_seq: Seq,
        later: EventRef,
    },
    /// A certified trim floor retired the named subject: events
    /// before `first_retained` are logically gone whether or not the
    /// sweeper has collected the objects.
    #[error(
        "expired subject {subject:?} in {stream:?} at seq {target_seq}: first retained seq is {first_retained}"
    )]
    Expired {
        stream: StreamId,
        target_seq: Seq,
        first_retained: Seq,
        subject: ExpiredSubject,
    },
}

/// The error of [`Streams::append_expected`]: one typed failure kind
/// plus the effect certainty that survives it.
///
/// Every failure carries an effect — including corruption and
/// conflict observed after an attempt — because a landed PUT cannot
/// be un-landed by the observation that discovered it. The kind is
/// boxed so the common success path pays no allocation for the cold
/// failure taxonomy.
#[derive(Debug, thiserror::Error)]
#[error("append_expected failed: {kind} (effect: {effect:?})")]
pub struct AppendExpectedError {
    /// The typed failure; also this error's source.
    #[source]
    pub kind: Box<AppendExpectedFailure>,
    /// What the call can prove about storage effects.
    pub effect: AppendExpectedEffect,
}

/// Wrap a typed failure kind with the effect certainty that survives
/// it; the only place the cold failure kind is boxed.
fn failed(kind: AppendExpectedFailure, effect: AppendExpectedEffect) -> AppendExpectedError {
    AppendExpectedError {
        kind: Box::new(kind),
        effect,
    }
}

/// Wrap a storage failure with its surviving effect.
fn storage(err: StreamsError, effect: AppendExpectedEffect) -> AppendExpectedError {
    failed(AppendExpectedFailure::Storage(err), effect)
}

/// The retention-expiry failure naming `subject` at `target_seq`.
fn expired(
    stream: &StreamId,
    target_seq: Seq,
    first_retained: Seq,
    subject: ExpiredSubject,
) -> AppendExpectedFailure {
    AppendExpectedFailure::Expired {
        stream: stream.clone(),
        target_seq,
        first_retained,
        subject,
    }
}

/// Canonical SHA-256 digest text: exactly 64 lowercase hex
/// characters. Predecessor payload digests are validated against
/// this shape before any storage request.
fn is_canonical_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Classify an occupant of the exact target whose bytes differ from
/// the desired envelope: a malformed or key-mismatched object is
/// [`StreamsError::Corrupt`] naming the target; a verified envelope
/// carrying the caller's stable event id is
/// [`StreamsError::IdempotencyConflict`] (same id, different
/// canonical bytes or schema); any other verified event yields its
/// identifying reference for
/// [`AppendExpectedFailure::PositionConflict`].
fn classify_occupant(
    stream: &StreamId,
    stable_event_id: &StableEventId,
    target_seq: Seq,
    bytes: &[u8],
) -> Result<EventRef, StreamsError> {
    let occupant = Envelope::decode_and_verify(stream, target_seq, bytes).map_err(|_| {
        StreamsError::Corrupt {
            stream: stream.clone(),
            missing_or_mismatched: vec![target_seq],
        }
    })?;
    if occupant.stable_event_id() == stable_event_id {
        return Err(StreamsError::IdempotencyConflict {
            stream: stream.clone(),
            stable_event_id: stable_event_id.clone(),
            conflicting_seq: target_seq,
        });
    }
    Ok(occupant.event_ref().clone())
}

impl Streams {
    /// Create a stream under a caller-supplied id with a
    /// caller-supplied configuration, reconciling exactly.
    ///
    /// The caller owns the id: it is validated even if the
    /// [`StreamId`] reached this call by deserializing untrusted
    /// input past the constructor, and the canonical genesis is
    /// encoded (with the structural encoded-size bound enforced)
    /// before the first storage effect. The stream's existence is
    /// the conditional create of the genesis object at seq 0 — the
    /// same allocation semantics as [`Streams::create_stream`],
    /// which continues to mint its own fresh ids.
    ///
    /// # Outcomes
    ///
    /// - [`CreateStreamOutcome::Created`]: this call's conditional
    ///   create won; the genesis bytes are exactly this call's.
    /// - [`CreateStreamOutcome::Existing`]: a genesis already existed
    ///   byte-identical to this call's canonical genesis — the same
    ///   id and the same configuration retry converged. An
    ///   observation, not a writer grant.
    /// - [`CreateStreamError::ConfigurationConflict`]: a valid
    ///   genesis already existed with different bytes — the id names
    ///   a different configuration. Never overwritten, never
    ///   adopted.
    /// - [`CreateStreamError::Storage`] carrying
    ///   [`StreamsError::Corrupt`] naming seq 0 when the
    ///   incumbent is malformed;
    ///   [`StreamsError::BackendUnqualified`] when a conflict's
    ///   incumbent is absent on readback;
    ///   [`StreamsError::InvalidArgument`] for a deserialized-invalid
    ///   id, an oversize encoded genesis, or a syntactically valid id
    ///   whose genesis key the kernel permanently refuses (a
    ///   reserved key such as one carrying a `trims` path segment,
    ///   or a key that breaches the kernel identifier bound) — all
    ///   decided before or without any storage effect on the
    ///   caller's part; and [`StreamsError::Unavailable`] for store
    ///   failures, which the caller retries with the SAME id and
    ///   bytes — put-if-absent makes the retry converge on
    ///   `Created` or `Existing`.
    ///
    /// # Request shape
    ///
    /// Exactly one conditional create at the genesis key, plus — only
    /// on a lost race — one readback GET of that key. No tail-hint
    /// access, no log LIST, no write at any other position, and zero
    /// storage requests when admission itself fails.
    pub async fn create_stream_with_id(
        &self,
        stream: &StreamId,
        config: &[u8],
    ) -> Result<CreateStreamOutcome, CreateStreamError> {
        // Admission before any storage request: a StreamId that
        // reached this call by deserialization is re-validated here.
        validate_segment("stream id", stream.as_str()).map_err(CreateStreamError::Storage)?;
        // The canonical genesis is encoded before the first effect:
        // byte-identity between an incumbent and this caller's retry
        // is well-defined, and the structural encoded-size bound is
        // enforced by encode itself.
        let genesis = Envelope::genesis(stream, config).map_err(CreateStreamError::Storage)?;
        match self
            .keyspace
            .create(&Self::log_key(stream, 0), genesis.encoded().clone())
            .await
        {
            Ok(()) => Ok(CreateStreamOutcome::Created),
            Err(KeyspaceError::AlreadyExists(_)) => {
                // Lost the create race: inspect the incumbent. Never
                // overwrite it, never adopt an unrelated identity.
                let incumbent =
                    self.keyspace
                        .get(&Self::log_key(stream, 0))
                        .await
                        .map_err(|err| {
                            CreateStreamError::Storage(map_event_read_error(
                                stream,
                                0,
                                "create_stream_with_id: incumbent readback",
                                err,
                            ))
                        })?;
                match incumbent {
                    // Create said the key exists; the readback says it
                    // does not: contradictory witnesses fail closed.
                    None => Err(CreateStreamError::Storage(
                        StreamsError::BackendUnqualified {
                            witness: "create_stream_with_id: genesis absent after AlreadyExists"
                                .into(),
                        },
                    )),
                    // Byte-identical to this call's canonical genesis:
                    // a same-id same-config retry converged. The bytes
                    // are this caller's own verified encoding, so the
                    // comparison is the verification.
                    Some(bytes) if bytes.as_ref() == genesis.encoded().as_ref() => {
                        Ok(CreateStreamOutcome::Existing)
                    }
                    // Different bytes: a valid different genesis is a
                    // configuration conflict; a malformed one is
                    // damage naming seq 0.
                    Some(bytes) => {
                        Envelope::decode_and_verify(stream, 0, &bytes).map_err(|_| {
                            CreateStreamError::Storage(StreamsError::Corrupt {
                                stream: stream.clone(),
                                missing_or_mismatched: vec![0],
                            })
                        })?;
                        Err(CreateStreamError::ConfigurationConflict {
                            stream: stream.clone(),
                        })
                    }
                }
            }
            // Permanent kernel admission refusals: a syntactically
            // valid id can still name a genesis key the kernel
            // reserves (a `trims` path segment, the `tombstones`,
            // `incarnations`, or `fences` roots) or a logical key
            // outside the kernel identifier bound. Matched by the
            // kernel's typed errors — never a copied reserved-name
            // list — and surfaced as InvalidArgument, not a
            // retryable unavailability. The kernel rejects these
            // before any storage request.
            Err(
                err @ (KeyspaceError::InvalidIdentifier(_)
                | KeyspaceError::TombstoneImmutable(_)
                | KeyspaceError::IncarnationCounterImmutable(_)
                | KeyspaceError::MaintenanceFenceImmutable(_)
                | KeyspaceError::TrimCertificateImmutable(_)),
            ) => Err(CreateStreamError::Storage(StreamsError::InvalidArgument(
                format!("stream id {stream:?} names a refused genesis key: {err}"),
            ))),
            // Any other create failure is unresolved storage. The
            // kernel's create can surface incarnation-era errors only
            // after its PUT landed, so nothing past the permanent
            // admission matches is a caller-invalid argument: every
            // remaining failure is retryable unavailability, and
            // retrying the SAME id and bytes is safe (the create is
            // put-if-absent).
            Err(_) => Err(CreateStreamError::Storage(StreamsError::Unavailable {
                operation: "create_stream_with_id",
            })),
        }
    }

    /// Append one event at exactly the successor of a named
    /// predecessor.
    ///
    /// The caller names the position by identity: the predecessor's
    /// [`EventRef`], all four fields of which — stream, seq, stable
    /// event id, payload digest — must match the verified event at
    /// that seq (the verified genesis stands in for the read when the
    /// predecessor is seq 0). The event lands at
    /// `predecessor.seq + 1` or not at all: the target is never
    /// advanced past an occupant, a hole is never repaired, and no
    /// tail hint is written. This is not the ordinary
    /// [`append`](Streams::append) allocation loop; it shares only
    /// the underlying conditional create, called once, at one exact
    /// position.
    ///
    /// # Exact reconciliation
    ///
    /// Before the predecessor is even read, the exact target is
    /// fetched: a byte-identical canonical envelope there is a
    /// committed retry — the receipt is returned (or, if a floor has
    /// since retired the target, returned as
    /// [`Expired`](AppendExpectedFailure::Expired) with a
    /// [`Committed`](AppendExpectedEffect::Committed) effect) without
    /// reading the predecessor or any suffix and without writing
    /// anything. This works after later appends and after the
    /// predecessor was collected while the target remains retained.
    /// It proves the target's bytes — not that the supplied
    /// predecessor digest or id was examined — and authorizes
    /// nothing.
    ///
    /// # Outcomes
    ///
    /// Every error carries its [`AppendExpectedEffect`]:
    ///
    /// - Before the create
    ///   ([`NotAttempted`](AppendExpectedEffect::NotAttempted)):
    ///   admission failures —
    ///   [`Storage`](AppendExpectedFailure::Storage) of
    ///   [`StreamsError::InvalidArgument`] (including a malformed
    ///   predecessor digest), [`StreamsError::EnvelopeTooLarge`], and
    ///   [`StreamsError::SeqExhausted`], all rejected before any
    ///   storage request — then an absent genesis
    ///   ([`StreamsError::StreamNotFound`]), a corrupt genesis or
    ///   occupant ([`StreamsError::Corrupt`]), a same-stable-id
    ///   different-content occupant
    ///   ([`StreamsError::IdempotencyConflict`]), a different
    ///   verified occupant
    ///   ([`PositionConflict`](AppendExpectedFailure::PositionConflict)),
    ///   an unverified predecessor
    ///   ([`PredecessorMismatch`](AppendExpectedFailure::PredecessorMismatch),
    ///   or [`StreamsError::EventMissing`] naming it), a verified
    ///   later event past the absent target
    ///   ([`HoleWitnessed`](AppendExpectedFailure::HoleWitnessed)),
    ///   and retention expiry of the target or the non-genesis
    ///   predecessor ([`Expired`](AppendExpectedFailure::Expired)).
    /// - After the create is invoked, at least
    ///   [`PossiblyCommitted`](AppendExpectedEffect::PossiblyCommitted):
    ///   a successful create or an exact-bytes readback upgrades to
    ///   [`Committed`](AppendExpectedEffect::Committed); the
    ///   mandatory post-attempt floor observation then returns the
    ///   receipt, reports [`Expired`](AppendExpectedFailure::Expired)
    ///   of the target, or preserves the effect under
    ///   [`Storage`](AppendExpectedFailure::Storage) of
    ///   [`StreamsError::Unavailable`]. A conflicting or corrupt
    ///   readback occupant preserves its typed kind with
    ///   [`PossiblyCommitted`](AppendExpectedEffect::PossiblyCommitted);
    ///   an absent or unavailable readback stays unresolved the same
    ///   way. No aggregate create error — `AlreadyExists` included —
    ///   is ever reported as a definite rejection.
    ///
    /// # Qualified retention
    ///
    /// `Ok` means the canonical envelope was confirmed at the exact
    /// successor and a later successful floor observation did not
    /// retire the target — not a retention pin, ownership grant, or
    /// fence. The floor may reach the target after predecessor
    /// validation while the target remains retained (`Ok` stands);
    /// `floor > target` makes an observed success expired. The
    /// genesis (seq 0) is immortal and exempt from predecessor
    /// expiry, so `floor == target == 1` still appends from genesis.
    /// A silently stale certificate LIST can miss a newer floor — the
    /// standing qualified-backend limitation.
    ///
    /// # Request shape
    ///
    /// Admission is pure. Then: the genesis GET, the exact target GET
    /// (always first, before predecessor, suffix, and floor reads),
    /// the floor lookup, and — on an absent target — the predecessor
    /// GET (skipped for a seq-0 predecessor, whose verified genesis
    /// is reused; possibly one floor reread to distinguish a missing
    /// predecessor from concurrent expiry), one ordered suffix probe
    /// with at most one witness GET, a floor reobservation, the single
    /// conditional create at the exact target, and — only on a create
    /// error — one exact-target readback plus the mandatory floor
    /// observation. No tail-hint access, no write at any other
    /// position, no budget or backoff loop.
    pub async fn append_expected(
        &self,
        predecessor: &EventRef,
        schema_id: &SchemaId,
        stable_event_id: &StableEventId,
        payload: &[u8],
    ) -> Result<AppendReceipt, AppendExpectedError> {
        // --- A: pure admission. Nothing below touches storage. ---
        let stream = &predecessor.stream_id;
        validate_segment("stream id", stream.as_str())
            .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
        validate_segment("schema id", schema_id.as_str())
            .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
        validate_segment("stable event id", stable_event_id.as_str())
            .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
        validate_segment(
            "predecessor stable event id",
            predecessor.stable_event_id.as_str(),
        )
        .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
        if !is_canonical_sha256_hex(&predecessor.payload_sha256) {
            return Err(storage(
                StreamsError::InvalidArgument(format!(
                    "predecessor payload digest {:?} is not canonical lowercase 64-hex",
                    predecessor.payload_sha256
                )),
                AppendExpectedEffect::NotAttempted,
            ));
        }
        // The successor is checked, not wrapped: seq has no
        // representable successor at u64::MAX.
        let Some(target_seq) = predecessor.seq.checked_add(1) else {
            return Err(storage(
                StreamsError::SeqExhausted(stream.clone()),
                AppendExpectedEffect::NotAttempted,
            ));
        };
        // Encoded once, before any effect: byte-identity for the
        // retry comparison, the size bound preflighted, and the
        // payload digest computed exactly once here.
        let envelope = Envelope::encode(stream, target_seq, schema_id, stable_event_id, payload)
            .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
        let receipt = AppendReceipt {
            stream_id: stream.clone(),
            seq: target_seq,
            stable_event_id: stable_event_id.clone(),
            payload_sha256: envelope.payload_sha256().to_string(),
        };

        // The exact target key, computed once after admission and
        // reused by the target GET, the create, and the readback.
        let target_key = Self::log_key(stream, target_seq);

        // --- B: genesis, then the exact target FIRST — before
        // predecessor, suffix, or floor-driven reads. ---
        let genesis = self.verified_genesis(stream).await?;
        let mut max_floor: Option<Seq> = None;
        // The target read itself honors zombie precedence: an
        // outer-corrupt occupant (stored kernel value the reader
        // cannot reassemble) below a floor that has retired the
        // target is expired, not judged — the floor is observed
        // before that corruption is reported, exactly as the
        // inner-malformed occupant below is. Ordinary unavailability
        // and identifier rejection propagate without a floor lookup.
        let target_bytes = match self.keyspace.get(&target_key).await {
            Ok(bytes) => bytes,
            Err(err) => {
                let mapped =
                    map_event_read_error(stream, target_seq, "append_expected: target", err);
                let StreamsError::Corrupt { .. } = mapped else {
                    return Err(storage(mapped, AppendExpectedEffect::NotAttempted));
                };
                let floor = self
                    .observe_floor(stream, "append_expected: trim floor", &mut max_floor)
                    .await
                    .map_err(|floor_err| storage(floor_err, AppendExpectedEffect::NotAttempted))?;
                if target_seq < floor {
                    return Err(failed(
                        expired(stream, target_seq, floor, ExpiredSubject::Target),
                        AppendExpectedEffect::NotAttempted,
                    ));
                }
                return Err(storage(mapped, AppendExpectedEffect::NotAttempted));
            }
        };

        if let Some(bytes) = target_bytes.as_ref() {
            if bytes.as_ref() == envelope.encoded().as_ref() {
                // --- C: exact reconciliation. The target bytes are
                // the canonical desired envelope: committed, whether
                // or not this call wrote them. Observe the floor and
                // adjudicate; the predecessor, the suffix, and every
                // write path stay untouched. ---
                return self
                    .adjudicate_committed(stream, target_seq, receipt, &mut max_floor)
                    .await;
            }
            // --- D: occupied by different bytes. Retention is
            // observed before the occupant is judged: an expired
            // target overrides a below-floor zombie; the slot is
            // never advanced past. ---
            let floor = self
                .observe_floor(stream, "append_expected: trim floor", &mut max_floor)
                .await
                .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
            if target_seq < floor {
                return Err(failed(
                    expired(stream, target_seq, floor, ExpiredSubject::Target),
                    AppendExpectedEffect::NotAttempted,
                ));
            }
            return match classify_occupant(stream, stable_event_id, target_seq, bytes) {
                Ok(occupant) => Err(failed(
                    AppendExpectedFailure::PositionConflict {
                        stream: stream.clone(),
                        target_seq,
                        occupant,
                    },
                    AppendExpectedEffect::NotAttempted,
                )),
                Err(err) => Err(storage(err, AppendExpectedEffect::NotAttempted)),
            };
        }

        // --- E: absent target. Observe the floor first; expiry of
        // the target or of a non-genesis predecessor ends the call
        // before the predecessor is read. Seq 0 is immortal: a
        // floor of 1 never expires a genesis predecessor. ---
        let floor = self
            .observe_floor(stream, "append_expected: trim floor", &mut max_floor)
            .await
            .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
        if target_seq < floor {
            return Err(failed(
                expired(stream, target_seq, floor, ExpiredSubject::Target),
                AppendExpectedEffect::NotAttempted,
            ));
        }
        if predecessor.seq > 0 && predecessor.seq < floor {
            return Err(failed(
                expired(stream, target_seq, floor, ExpiredSubject::Predecessor),
                AppendExpectedEffect::NotAttempted,
            ));
        }
        self.verify_predecessor(stream, predecessor, &genesis, target_seq, &mut max_floor)
            .await?;

        // --- F: before any create, an ordered probe past the target:
        // a verified later event witnesses a hole, which is never
        // repaired; a LIST/GET contradiction fails closed. ---
        self.probe_later_witness(stream, target_seq).await?;

        // --- G: reobserve the floor immediately before the create.
        // The successor is never clamped to the floor; the maximum
        // observed floor is never forgotten. ---
        let floor = self
            .observe_floor(
                stream,
                "append_expected: floor before create",
                &mut max_floor,
            )
            .await
            .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
        if target_seq < floor {
            return Err(failed(
                expired(stream, target_seq, floor, ExpiredSubject::Target),
                AppendExpectedEffect::NotAttempted,
            ));
        }
        if predecessor.seq > 0 && predecessor.seq < floor {
            return Err(failed(
                expired(stream, target_seq, floor, ExpiredSubject::Predecessor),
                AppendExpectedEffect::NotAttempted,
            ));
        }

        // --- H: one conditional create at the exact target. From the
        // moment this consumed operation is invoked, the effect is at
        // least PossiblyCommitted — whatever the aggregate outcome
        // reports, including AlreadyExists. ---
        let create = self
            .keyspace
            .create(&target_key, envelope.encoded().clone())
            .await;
        match create {
            // --- I/J: a successful create upgrades to Committed;
            // the mandatory post-attempt floor observation adjudicates
            // before success is returned. ---
            Ok(()) => {
                self.adjudicate_committed(stream, target_seq, receipt, &mut max_floor)
                    .await
            }
            // --- I: any create error triggers the exact readback. An
            // exact-bytes occupant upgrades to Committed; anything
            // else preserves PossiblyCommitted, with retention
            // observed before a possibly expired occupant is
            // interpreted. ---
            Err(_) => {
                let readback = self.keyspace.get(&target_key).await;
                match readback {
                    Ok(Some(bytes)) if bytes.as_ref() == envelope.encoded().as_ref() => {
                        self.adjudicate_committed(stream, target_seq, receipt, &mut max_floor)
                            .await
                    }
                    Ok(Some(bytes)) => {
                        // Retention first: an expired target overrides
                        // the occupant, and a monotone contradiction
                        // is a disqualification.
                        match self
                            .observe_floor(stream, "append_expected: final floor", &mut max_floor)
                            .await
                        {
                            Ok(floor) if target_seq < floor => {
                                return Err(failed(
                                    expired(stream, target_seq, floor, ExpiredSubject::Target),
                                    AppendExpectedEffect::PossiblyCommitted,
                                ));
                            }
                            Ok(_) => {}
                            Err(err) => {
                                return Err(storage(err, AppendExpectedEffect::PossiblyCommitted));
                            }
                        }
                        match classify_occupant(stream, stable_event_id, target_seq, &bytes) {
                            Ok(occupant) => Err(failed(
                                AppendExpectedFailure::PositionConflict {
                                    stream: stream.clone(),
                                    target_seq,
                                    occupant,
                                },
                                AppendExpectedEffect::PossiblyCommitted,
                            )),
                            Err(err) => Err(storage(err, AppendExpectedEffect::PossiblyCommitted)),
                        }
                    }
                    // Absent readback: unresolved. The mandatory floor
                    // observation decides only whether the position is
                    // expired; otherwise the uncertainty stands —
                    // absence proves no non-effect.
                    Ok(None) => Err(self
                        .adjudicate_unresolved(stream, target_seq, &mut max_floor)
                        .await),
                    // A readback that itself failed: classify its
                    // stored-integrity taxonomy, but retention first —
                    // an expired target wins, a monotone floor
                    // contradiction disqualifies, and otherwise the
                    // typed witness (Corrupt naming the target on
                    // stored-integrity failure) is preserved, never
                    // lost to a generic unavailability.
                    Err(readback_err) => {
                        let witness = map_event_read_error(
                            stream,
                            target_seq,
                            "append_expected: create readback",
                            readback_err,
                        );
                        match self
                            .observe_floor(stream, "append_expected: final floor", &mut max_floor)
                            .await
                        {
                            Ok(floor) if target_seq < floor => Err(failed(
                                expired(stream, target_seq, floor, ExpiredSubject::Target),
                                AppendExpectedEffect::PossiblyCommitted,
                            )),
                            Err(floor_err @ StreamsError::BackendUnqualified { .. }) => {
                                Err(storage(floor_err, AppendExpectedEffect::PossiblyCommitted))
                            }
                            _ => Err(storage(witness, AppendExpectedEffect::PossiblyCommitted)),
                        }
                    }
                }
            }
        }
    }

    /// The verified genesis envelope, or
    /// [`StreamsError::StreamNotFound`] when no genesis object
    /// exists. A genesis that fails verification is
    /// [`StreamsError::Corrupt`] naming seq 0.
    async fn verified_genesis(&self, stream: &StreamId) -> Result<Envelope, AppendExpectedError> {
        let bytes = self
            .keyspace
            .get(&Self::log_key(stream, 0))
            .await
            .map_err(|err| {
                storage(
                    map_event_read_error(stream, 0, "append_expected: genesis", err),
                    AppendExpectedEffect::NotAttempted,
                )
            })?;
        let Some(bytes) = bytes else {
            return Err(storage(
                StreamsError::StreamNotFound(stream.clone()),
                AppendExpectedEffect::NotAttempted,
            ));
        };
        Envelope::decode_and_verify(stream, 0, &bytes).map_err(|_| {
            storage(
                StreamsError::Corrupt {
                    stream: stream.clone(),
                    missing_or_mismatched: vec![0],
                },
                AppendExpectedEffect::NotAttempted,
            )
        })
    }

    /// Observe the certified trim floor, monotonically: the maximum
    /// floor observed within one call is never forgotten. A later
    /// observation below the maximum — including a vanished
    /// certificate — contradicts the immutability the certificate
    /// namespace carries and fails closed as
    /// [`StreamsError::BackendUnqualified`]. Returns the effective
    /// floor (0 when the scope was never trimmed).
    async fn observe_floor(
        &self,
        stream: &StreamId,
        operation: &'static str,
        max_observed: &mut Option<Seq>,
    ) -> Result<Seq, StreamsError> {
        let observed = self
            .keyspace
            .trim_floor(stream.as_str())
            .await
            .map_err(map_keyspace(operation))?;
        match (observed, *max_observed) {
            (Some(floor), Some(max)) if floor < max => Err(StreamsError::BackendUnqualified {
                witness: format!(
                    "certified trim floor regressed from {max} to {floor} within one append_expected"
                ),
            }),
            (None, Some(max)) => Err(StreamsError::BackendUnqualified {
                witness: format!(
                    "certified trim floor {max} vanished from a later observation within one append_expected"
                ),
            }),
            (Some(floor), _) => {
                *max_observed = Some(floor);
                Ok(floor)
            }
            (None, None) => Ok(0),
        }
    }

    /// GET and verify the predecessor (the already-verified genesis
    /// stands in for seq 0), requiring all four [`EventRef`] fields
    /// to match. A missing predecessor rereads the floor once to
    /// distinguish concurrent expiry from absence. All failures are
    /// [`AppendExpectedEffect::NotAttempted`].
    async fn verify_predecessor(
        &self,
        stream: &StreamId,
        predecessor: &EventRef,
        genesis: &Envelope,
        target_seq: Seq,
        max_observed: &mut Option<Seq>,
    ) -> Result<(), AppendExpectedError> {
        if predecessor.seq == 0 {
            // The genesis was verified in step B; the immortal seq-0
            // predecessor is compared against it, not re-read.
            return if genesis.event_ref() == predecessor {
                Ok(())
            } else {
                Err(failed(
                    AppendExpectedFailure::PredecessorMismatch {
                        expected: predecessor.clone(),
                        observed: genesis.event_ref().clone(),
                    },
                    AppendExpectedEffect::NotAttempted,
                ))
            };
        }
        let bytes = self
            .keyspace
            .get(&Self::log_key(stream, predecessor.seq))
            .await
            .map_err(|err| {
                storage(
                    map_event_read_error(
                        stream,
                        predecessor.seq,
                        "append_expected: predecessor",
                        err,
                    ),
                    AppendExpectedEffect::NotAttempted,
                )
            })?;
        let Some(bytes) = bytes else {
            // Absent: reread the floor once — the certificate, not
            // object absence, is the boundary — before naming the
            // event missing. Target expiry takes priority, then a
            // nonzero predecessor's; the seq-0 genesis is exempt.
            let floor = self
                .observe_floor(stream, "append_expected: floor reread", max_observed)
                .await
                .map_err(|err| storage(err, AppendExpectedEffect::NotAttempted))?;
            if target_seq < floor {
                return Err(failed(
                    expired(stream, target_seq, floor, ExpiredSubject::Target),
                    AppendExpectedEffect::NotAttempted,
                ));
            }
            if predecessor.seq > 0 && predecessor.seq < floor {
                return Err(failed(
                    expired(stream, target_seq, floor, ExpiredSubject::Predecessor),
                    AppendExpectedEffect::NotAttempted,
                ));
            }
            return Err(storage(
                StreamsError::EventMissing {
                    stream: stream.clone(),
                    seq: predecessor.seq,
                },
                AppendExpectedEffect::NotAttempted,
            ));
        };
        let envelope =
            Envelope::decode_and_verify(stream, predecessor.seq, &bytes).map_err(|_| {
                storage(
                    StreamsError::Corrupt {
                        stream: stream.clone(),
                        missing_or_mismatched: vec![predecessor.seq],
                    },
                    AppendExpectedEffect::NotAttempted,
                )
            })?;
        if envelope.event_ref() != predecessor {
            return Err(failed(
                AppendExpectedFailure::PredecessorMismatch {
                    expected: predecessor.clone(),
                    observed: envelope.event_ref().clone(),
                },
                AppendExpectedEffect::NotAttempted,
            ));
        }
        Ok(())
    }

    /// Probe the ordered next log key strictly after the target. A
    /// listed witness is GET-verified: a verified later event is
    /// [`AppendExpectedFailure::HoleWitnessed`] (never repaired); a
    /// LIST-present/GET-absent contradiction fails closed. Tail
    /// hints are not consulted — an unverified hint would grant
    /// nothing — and no tail write ever occurs. All failures are
    /// [`AppendExpectedEffect::NotAttempted`].
    async fn probe_later_witness(
        &self,
        stream: &StreamId,
        target_seq: Seq,
    ) -> Result<(), AppendExpectedError> {
        let Some(later) = self
            .next_log_key_after(stream, target_seq)
            .await
            .map_err(|err| {
                storage(
                    map_keyspace("append_expected: suffix probe")(err),
                    AppendExpectedEffect::NotAttempted,
                )
            })?
        else {
            return Ok(());
        };
        let bytes = self
            .keyspace
            .get(&Self::log_key(stream, later))
            .await
            .map_err(|err| {
                storage(
                    map_event_read_error(stream, later, "append_expected: suffix witness", err),
                    AppendExpectedEffect::NotAttempted,
                )
            })?;
        let Some(bytes) = bytes else {
            return Err(storage(
                StreamsError::BackendUnqualified {
                    witness: format!(
                        "append_expected suffix probe: LIST named seq {later} past seq {target_seq} but GET found it absent"
                    ),
                },
                AppendExpectedEffect::NotAttempted,
            ));
        };
        let envelope = Envelope::decode_and_verify(stream, later, &bytes).map_err(|_| {
            storage(
                StreamsError::Corrupt {
                    stream: stream.clone(),
                    missing_or_mismatched: vec![later],
                },
                AppendExpectedEffect::NotAttempted,
            )
        })?;
        Err(failed(
            AppendExpectedFailure::HoleWitnessed {
                stream: stream.clone(),
                target_seq,
                later: envelope.event_ref().clone(),
            },
            AppendExpectedEffect::NotAttempted,
        ))
    }

    /// Post-attempt adjudication of a committed effect (contract J):
    /// the mandatory floor observation precedes any success, and the
    /// receipt is moved into whichever outcome the observation
    /// selects — the success path clones nothing. An expired target
    /// returns [`AppendExpectedFailure::Expired`] carrying the
    /// [`AppendExpectedEffect::Committed`] receipt; a failed
    /// observation preserves the committed effect and receipt under
    /// [`StreamsError::Unavailable`]; a nonexpired observed floor
    /// returns the receipt.
    async fn adjudicate_committed(
        &self,
        stream: &StreamId,
        target_seq: Seq,
        receipt: AppendReceipt,
        max_observed: &mut Option<Seq>,
    ) -> Result<AppendReceipt, AppendExpectedError> {
        match self
            .observe_floor(stream, "append_expected: final floor", max_observed)
            .await
        {
            Err(err) => Err(failed(
                AppendExpectedFailure::Storage(err),
                AppendExpectedEffect::Committed(receipt),
            )),
            Ok(floor) if target_seq < floor => Err(failed(
                expired(stream, target_seq, floor, ExpiredSubject::Target),
                AppendExpectedEffect::Committed(receipt),
            )),
            // Nonexpired observed floor: success, with no receipt or
            // effect clone on this path.
            Ok(_) => Ok(receipt),
        }
    }

    /// Post-attempt adjudication of an unresolved possibly-committed
    /// effect with no typed witness: an observed expiry returns
    /// [`AppendExpectedFailure::Expired`] with
    /// [`AppendExpectedEffect::PossiblyCommitted`]; anything else
    /// preserves the uncertainty under [`StreamsError::Unavailable`].
    async fn adjudicate_unresolved(
        &self,
        stream: &StreamId,
        target_seq: Seq,
        max_observed: &mut Option<Seq>,
    ) -> AppendExpectedError {
        let effect = AppendExpectedEffect::PossiblyCommitted;
        match self
            .observe_floor(stream, "append_expected: final floor", max_observed)
            .await
        {
            Ok(floor) if target_seq < floor => failed(
                expired(stream, target_seq, floor, ExpiredSubject::Target),
                effect,
            ),
            Ok(_) => storage(
                StreamsError::Unavailable {
                    operation: "append_expected: create outcome unresolved",
                },
                effect,
            ),
            Err(err) => storage(err, effect),
        }
    }
}
