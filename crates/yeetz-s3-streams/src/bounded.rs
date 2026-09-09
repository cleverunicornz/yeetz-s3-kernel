//! Strict historical reads: one exact event, or one exact contiguous
//! window, served GET-only alongside live replay.
//!
//! [`Streams::read`] answers a live question — which events follow a
//! cursor up to the current end — and pays for liveness with an
//! ordered LIST probe, tail-hint witnesses, and read-path accelerator
//! maintenance that writes. The APIs in this module answer a
//! historical demand: the caller names the target seq, or the window
//! `(after_seq, through_seq]`, and receives the verified bytes or a
//! typed reason the demand cannot be met. A strict read's answer is
//! fully determined by append-only allocation — the demanded seq
//! either has verified bytes or has a typed reason not to — so there
//! is no EOF to bound, no suffix to certify, and none of the live
//! machinery runs.
//!
//! # Request shape
//!
//! Both APIs, on every call:
//!
//! - validate every caller argument before any storage request;
//! - GET and verify the genesis (seq 0): stream existence and the
//!   integrity of its first record are load-bearing;
//! - consult the certified trim floor — a retention-control lookup
//!   whose failure is a typed error (fail closed), never a silent
//!   boundary;
//! - GET exactly the demanded target, or exactly the current demanded
//!   page window — never an event object outside it.
//!
//! No log LIST beyond the trim-certificate lookup the floor itself
//! requires, no tail-hint read, no read-path healing, no write of any
//! kind. Strict reads are pure observations: they leave no trace and
//! touch no accelerator.
//!
//! # Strictness
//!
//! A window is a demand, not a query. [`Streams::read_range`] returns
//! the complete contiguous demanded subwindow — cut only by `limit`
//! and `through_seq` — or a typed error; it never returns a partial
//! successful page with missing history. A seq absent inside the
//! demanded window is [`StreamsError::EventMissing`] naming the
//! lowest absent seq: strict reads do not distinguish a hole in the
//! log from a window that extends past the current tail, because
//! telling them apart would require exactly the requests (log LIST,
//! successor probes) this surface forbids.
//!
//! # Retention
//!
//! The trim certificate, not object presence, is the boundary: a
//! target below the certified floor is [`StreamsError::OffsetExpired`]
//! even if the object still physically exists (the sweeper has not
//! run), and the genesis is immortal — [`Streams::read_event`] serves
//! seq 0 without a trim-floor lookup. Retention is re-consulted on
//! every call and never pinned across the pages of a walk: a trim
//! landing between pages surfaces as `OffsetExpired` on the next
//! page, and the caller owns that recovery.

use crate::{
    Envelope, FETCH_PARALLELISM, Seq, StreamId, Streams, StreamsError, map_keyspace,
    validate_segment,
};

/// One strict page: the complete contiguous demanded subwindow of a
/// `(after_seq, through_seq]` read.
///
/// `events` is never empty on success and is ordered by ascending
/// seq. `reached_end` is true exactly when the last returned seq
/// equals `through_seq` — a claim about the caller's window boundary,
/// never about the log's tail: events beyond `through_seq` may exist
/// and the page says nothing about them.
///
/// Resume a walk with `after_seq` set to the last returned seq — the
/// read is after-exclusive, so a cursor beyond the last returned seq
/// would skip events.
#[derive(Debug, Clone)]
pub struct RangePage {
    /// The verified envelopes of the demanded subwindow, ascending by
    /// seq: `(after_seq, page_end]` where `page_end` is the window end
    /// `through_seq` cut by `limit` (`min(after_seq + limit,
    /// through_seq)`, saturating).
    pub events: Vec<Envelope>,
    /// True exactly when the last event's seq equals `through_seq`.
    /// False means the page was cut by `limit` and the window has
    /// more seqs to demand; true claims nothing about events beyond
    /// `through_seq`.
    pub reached_end: bool,
}

impl Streams {
    /// Read one exact event: the verified [`Envelope`] at `seq`.
    ///
    /// `seq == 0` reads the genesis/config record itself, served
    /// immediately after verification without a trim-floor lookup —
    /// the genesis is immortal (every certified floor retains it).
    ///
    /// # Outcomes
    ///
    /// - An invalid stream id — including one that reached a
    ///   [`StreamId`] by deserializing untrusted JSON past the
    ///   constructor — is [`StreamsError::InvalidArgument`] before
    ///   any storage request.
    /// - An absent genesis is [`StreamsError::StreamNotFound`]; a
    ///   genesis that fails verification is
    ///   [`StreamsError::Corrupt`] naming seq 0.
    /// - A nonzero `seq` below the certified trim floor is
    ///   [`StreamsError::OffsetExpired`] — the certificate, not
    ///   object presence, is the boundary, so this holds even before
    ///   the sweeper deletes the object.
    /// - An absent target at or above the floor is
    ///   [`StreamsError::EventMissing`].
    /// - An object at the target that is malformed or disagrees with
    ///   its key (stream id, seq) is [`StreamsError::Corrupt`] naming
    ///   that seq — an error, never a skip.
    /// - A store failure is a distinct [`StreamsError::Unavailable`]
    ///   under the failing operation's name, never a boundary
    ///   outcome.
    ///
    /// # Request shape
    ///
    /// Exactly: the genesis GET, the certified-floor lookup (skipped
    /// for `seq == 0`), and the target GET. No log LIST, no tail-hint
    /// read, no write.
    pub async fn read_event(&self, stream: &StreamId, seq: Seq) -> Result<Envelope, StreamsError> {
        validate_segment("stream id", stream.as_str())?;
        let genesis = self.strict_genesis(stream, "read_event: genesis").await?;
        if seq == 0 {
            return Ok(genesis);
        }
        // Floor before target: a swept-or-swept-pending seq below the
        // certificate is a retention boundary, not a missing event.
        if let Some(first_retained) = self.strict_floor(stream, "read_event: trim floor").await?
            && seq < first_retained
        {
            return Err(StreamsError::OffsetExpired {
                stream: stream.clone(),
                first_retained,
            });
        }
        match self.strict_fetch(stream, seq, "read_event: fetch").await? {
            Some(envelope) => Ok(envelope),
            None => Err(StreamsError::EventMissing {
                stream: stream.clone(),
                seq,
            }),
        }
    }

    /// Read one exact contiguous window: the strict subwindow
    /// `(after_seq, through_seq]` (after-exclusive, through-inclusive),
    /// at most `limit` events.
    ///
    /// A successful page carries the **complete contiguous demanded
    /// subwindow** — every seq from `after_seq + 1` up to
    /// `min(after_seq + limit, through_seq)` — or the call returns a
    /// typed error. Never a shortened success with missing history:
    /// the lowest absent seq inside the demanded subwindow is
    /// [`StreamsError::EventMissing`] naming it (a hole and a window
    /// past the current tail are deliberately indistinguishable —
    /// see the module docs), and an envelope at a demanded seq that
    /// fails verification is [`StreamsError::Corrupt`] naming that
    /// seq.
    ///
    /// # Arguments
    ///
    /// `limit == 0` or `after_seq >= through_seq` is
    /// [`StreamsError::InvalidArgument`], as is an invalid stream id —
    /// all refused before any storage request. `through_seq ==
    /// Seq::MAX` is served without overflow: `Seq::MAX` is a
    /// representable, demandable seq with no successor.
    ///
    /// # Paging
    ///
    /// [`RangePage::reached_end`] is true exactly when the last
    /// returned seq equals `through_seq` — a window boundary, never a
    /// live-EOF claim; events beyond `through_seq` may exist. Resume
    /// with `after_seq` set to the last returned seq; the resumed
    /// walk covers the window without gap or duplication.
    ///
    /// # Outcomes
    ///
    /// - [`StreamsError::StreamNotFound`] when the genesis is absent;
    ///   [`StreamsError::Corrupt`] naming seq 0 when it fails
    ///   verification.
    /// - [`StreamsError::OffsetExpired`] when the window would start
    ///   below the certified trim floor (`after_seq + 1 <
    ///   first_retained`): resume instead at
    ///   `after_seq >= first_retained - 1`. A floor lookup that fails
    ///   is an error (fail closed), never a silent boundary or
    ///   success.
    /// - [`StreamsError::EventMissing`] naming the lowest absent seq
    ///   inside the demanded subwindow.
    /// - [`StreamsError::Corrupt`] naming any demanded seq whose
    ///   object fails verification.
    /// - [`StreamsError::Unavailable`] under the failing operation's
    ///   name for store failures — distinct from every boundary
    ///   outcome.
    ///
    /// # Request shape
    ///
    /// Exactly: the genesis GET, the certified-floor lookup, and the
    /// current demanded page's event GETs — never an event GET beyond
    /// the page (no successor probes), no log LIST, no tail-hint
    /// read, no write. Page GETs run in bounded chunks
    /// (`FETCH_PARALLELISM`), and a caller's `limit` never translates
    /// into an up-front allocation larger than one chunk. Retention
    /// is not pinned across pages: each call re-verifies the genesis
    /// and re-reads the floor, so a trim landing between pages
    /// surfaces as `OffsetExpired` on the next page.
    pub async fn read_range(
        &self,
        stream: &StreamId,
        after_seq: Seq,
        through_seq: Seq,
        limit: usize,
    ) -> Result<RangePage, StreamsError> {
        // Argument preflight: nothing below touches storage.
        validate_segment("stream id", stream.as_str())?;
        if limit == 0 {
            return Err(StreamsError::InvalidArgument(
                "read_range limit must be at least 1".into(),
            ));
        }
        if after_seq >= through_seq {
            return Err(StreamsError::InvalidArgument(format!(
                "read_range window is empty: after_seq {after_seq} must be strictly below through_seq {through_seq}"
            )));
        }
        // Genesis: stream existence and the integrity of its first
        // record are load-bearing for every strict read.
        self.strict_genesis(stream, "read_range: genesis").await?;
        // The walk starts at after_seq + 1; starting below the
        // certified floor is a typed retention boundary (the same
        // edge as live replay). after_seq < through_seq <= Seq::MAX,
        // so after_seq + 1 cannot overflow.
        if let Some(first_retained) = self.strict_floor(stream, "read_range: trim floor").await?
            && after_seq + 1 < first_retained
        {
            return Err(StreamsError::OffsetExpired {
                stream: stream.clone(),
                first_retained,
            });
        }
        // The demanded subwindow: (after_seq, page_end]. saturating:
        // a limit larger than the remaining window serves the whole
        // window rather than overflowing.
        let page_end = through_seq.min(after_seq.saturating_add(limit as u64));
        let mut events: Vec<Envelope> = Vec::with_capacity(FETCH_PARALLELISM.min(limit));
        let mut next = after_seq + 1;
        while next <= page_end {
            // A bounded chunk that INCLUDES Seq::MAX when reached
            // (saturating range ends would exclude it).
            let remaining = (page_end - next + 1) as usize;
            let want = FETCH_PARALLELISM.min(remaining);
            let mut chunk: Vec<Seq> = Vec::with_capacity(want);
            let mut seq = next;
            for _ in 0..want {
                chunk.push(seq);
                seq = match seq.checked_add(1) {
                    Some(later) => later,
                    None => break, // the chunk reached Seq::MAX
                };
            }
            let fetches = chunk
                .iter()
                .map(|&seq| self.strict_fetch(stream, seq, "read_range: fetch"));
            let results = futures::future::join_all(fetches).await;
            // Ascending order: the lowest boundary or damage in the
            // chunk is the one named.
            for (&seq, result) in chunk.iter().zip(results) {
                match result {
                    Ok(Some(envelope)) => events.push(envelope),
                    Ok(None) => {
                        return Err(StreamsError::EventMissing {
                            stream: stream.clone(),
                            seq,
                        });
                    }
                    Err(err) => return Err(err),
                }
            }
            next = match chunk.last().and_then(|&last| last.checked_add(1)) {
                Some(later) => later,
                // The chunk included Seq::MAX; no successor GET
                // exists and page_end == Seq::MAX.
                None => break,
            };
        }
        // The window boundary, computed from what was actually
        // served: events is non-empty (after_seq < through_seq and
        // limit >= 1) and ends at page_end when control reaches here.
        let reached_end = events
            .last()
            .is_some_and(|event| event.seq() == through_seq);
        Ok(RangePage {
            events,
            reached_end,
        })
    }

    /// GET and verify one log object: `Ok(None)` is a confirmed
    /// absent key; a present object is decoded and fully verified
    /// (format version, key↔envelope agreement, payload length and
    /// digest) — a failure is [`StreamsError::Corrupt`] naming the
    /// seq, never a skip. Storage failures map to the caller's
    /// operation name.
    async fn strict_fetch(
        &self,
        stream: &StreamId,
        seq: Seq,
        operation: &'static str,
    ) -> Result<Option<Envelope>, StreamsError> {
        let bytes = self
            .keyspace
            .get(&Self::log_key(stream, seq))
            .await
            .map_err(map_keyspace(operation))?;
        match bytes {
            Some(bytes) => Envelope::decode_and_verify(stream, seq, &bytes)
                .map(Some)
                .map_err(|_| StreamsError::Corrupt {
                    stream: stream.clone(),
                    missing_or_mismatched: vec![seq],
                }),
            None => Ok(None),
        }
    }

    /// The verified genesis envelope, or [`StreamsError::StreamNotFound`]
    /// when no genesis object exists. A genesis that fails verification
    /// is [`StreamsError::Corrupt`] naming seq 0.
    async fn strict_genesis(
        &self,
        stream: &StreamId,
        operation: &'static str,
    ) -> Result<Envelope, StreamsError> {
        match self.strict_fetch(stream, 0, operation).await? {
            Some(envelope) => Ok(envelope),
            None => Err(StreamsError::StreamNotFound(stream.clone())),
        }
    }

    /// The stream's certified trim floor for a strict read — the
    /// retention-control lookup these reads are permitted (a
    /// trim-certificate read inside the kernel keyspace). A lookup
    /// failure is an error under the caller's operation name: an
    /// unavailable retention lookup fails closed, never a silent
    /// boundary or success.
    async fn strict_floor(
        &self,
        stream: &StreamId,
        operation: &'static str,
    ) -> Result<Option<Seq>, StreamsError> {
        self.keyspace
            .trim_floor(stream.as_str())
            .await
            .map_err(map_keyspace(operation))
    }
}
