//! Streaming-value I/O (ADR 0004): the async reader/writer surface,
//! the manifest-commit machinery shared by the whole-value APIs, the
//! three-row lost-response oracle, the maintenance fence, and the
//! chunk inventory/sweep.
//!
//! # Commit point and ordering (ADR 0004 §2.1, §4)
//!
//! Chunked writes consume input into canonical 16 MiB chunks, hash
//! each, and put-if-absent them at generation-scoped content
//! addresses under the kernel-private `keyspace-chunks` root. Before
//! the conditional manifest PUT only unreachable immutable chunks
//! exist and the old logical state stays authoritative; the one
//! conditional control PUT — `If-None-Match` for create, `If-Match`
//! for CAS — is when the value lands. Partial uploads never alter
//! existence, tombstones, versions, or incarnation counters.
//!
//! # GC contract (ADR 0004 §5) — honest boundary
//!
//! Quiescence is a deployment-scope operational assertion (no
//! streamed writer, no manifest-changing mutation, no open streamed
//! reader for the namespace); the kernel cannot prove it. The
//! maintenance fence is a cheap barrier at the kernel-reserved key
//! `keyspace/{namespace}/fences/gc`: every streamed begin performs
//! one exact GET and refuses while fenced, and the quiesced sweep
//! requires the fence. It does NOT prove that a handle opened before
//! the fence has drained — the operational assertion remains
//! load-bearing, and [`AtomicKeyspace::sweep_chunks`] run against a
//! violated assertion deletes a live writer's candidate chunks and
//! publishes a manifest over absent chunks (the A34 broken-quiescence
//! demonstration: `ChunkMissing`, the forbidden state). Online
//! operation gets the delete-free
//! [`AtomicKeyspace::chunk_inventory`] meter instead.

use std::collections::{BTreeMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use futures::future::BoxFuture;
use futures::stream::{self, FuturesUnordered, StreamExt};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use yeetz_sdk_s3::{ObjectStoreClient, ObjectStoreError};

use crate::atomic_keyspace::{AtomicKeyspace, KEYSPACE_ROOT, KeyspaceError, ValueEnvelope};
use crate::tombstone::Tombstone;
use crate::value_manifest::{
    CHUNK_BYTES, CHUNK_ROOT, ControlEnvelope, MAX_CHUNKS, MAX_IN_FLIGHT_CHUNKS, MAX_LOGICAL_BYTES,
    MIN_CHUNKS, ManifestEntry, ValueManifest, chunk_object_key, parse_chunk_object_key,
};

/// The representation a logical value currently uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueRepresentation {
    /// One inline v2 envelope object at the logical key.
    Inline,
    /// A v3 manifest at the logical key plus immutable chunks under
    /// the private chunk root.
    Chunked,
}

/// Read-side metadata for a streamed value (ADR 0004 §3.3): logical
/// length, the opaque control etag, the optional v3 root, and the
/// representation kind. It exposes neither versions/incarnations nor
/// physical paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueMetadata {
    pub logical_len: u64,
    pub etag: String,
    pub value_root_sha256: Option<[u8; 32]>,
    pub representation: ValueRepresentation,
}

/// A receipt for a committed streamed value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitReceipt {
    /// The committed control's etag — the token a subsequent CAS or
    /// conditional delete must present.
    pub etag: String,
    pub logical_len: u64,
    pub representation: ValueRepresentation,
    pub chunk_count: u32,
}

/// The streamed existence read (ADR 0004 §3.3): the same four states
/// as [`crate::KeyState`], with `Present` carrying a reader plus
/// metadata instead of a collected value. No fifth state exists for
/// partial uploads — physical chunks alone are invisible. Destroyed,
/// expired, and absent states fetch no chunks.
#[derive(Debug)]
pub enum StreamKeyState {
    Present {
        reader: ValueReader,
        metadata: ValueMetadata,
    },
    Destroyed {
        tombstone: Tombstone,
    },
    OffsetExpired {
        first_retained: u64,
    },
    Absent,
}

/// A verified-chunk snapshot reader (ADR 0004 §3.2). The reader is a
/// snapshot of the immutable references in the observed control: a
/// later CAS/destroy does not retarget it. Chunked values are fetched
/// ordered with at most [`MAX_IN_FLIGHT_CHUNKS`] in flight, each
/// fully verified against the manifest before a byte is yielded; a
/// streaming consumer may therefore receive a verified prefix before
/// a later error and must commit side effects only after EOF.
pub struct ValueReader {
    metadata: ValueMetadata,
    chunk_digests: Vec<[u8; 32]>,
    source: ReaderSource,
    /// Absolute logical offset of the next byte to yield.
    position: u64,
    /// Exclusive end of the readable window.
    end: u64,
}

enum ReaderSource {
    Inline(Bytes),
    Chunked(Box<ChunkReader>),
}

struct ChunkReader {
    store: Arc<ObjectStoreClient>,
    namespace: String,
    key: String,
    incarnation: u64,
    version: u64,
    entries: Vec<ManifestEntry>,
    /// Next ordinal to start fetching.
    next_fetch: u32,
    /// Exclusive ordinal bound of the readable window.
    last_fetch: u32,
    in_flight: Pin<Box<FuturesUnordered<BoxFuture<'static, Result<(u32, Bytes), KeyspaceError>>>>>,
    /// Verified chunks that arrived out of ordinal order.
    ready: BTreeMap<u32, Bytes>,
    /// The chunk currently being drained and the offset in it.
    current: Option<(Bytes, usize)>,
    current_ordinal: u32,
}

impl std::fmt::Debug for ValueReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ValueReader")
            .field("metadata", &self.metadata)
            .field("position", &self.position)
            .field("end", &self.end)
            .finish_non_exhaustive()
    }
}

impl ValueReader {
    /// The observed control's metadata (logical length, etag, root,
    /// representation). No versions, no physical paths.
    #[must_use]
    pub fn metadata(&self) -> &ValueMetadata {
        &self.metadata
    }

    /// The ordered chunk digests of the observed manifest (empty for
    /// inline values) — the opaque cache identity of the verified
    /// chunks (ADR 0004 §3.3: a digest, not a storage capability).
    #[must_use]
    pub fn chunk_digests(&self) -> &[[u8; 32]] {
        &self.chunk_digests
    }
}

fn fetch_chunk(
    store: Arc<ObjectStoreClient>,
    namespace: String,
    key: String,
    incarnation: u64,
    version: u64,
    entry: ManifestEntry,
    ordinal: u32,
) -> BoxFuture<'static, Result<(u32, Bytes), KeyspaceError>> {
    Box::pin(async move {
        let path = chunk_object_key(&namespace, &key, incarnation, version, &entry.digest_hex());
        match store.download(&path).await {
            Ok(bytes) => {
                if bytes.len() as u64 != u64::from(entry.encoded_len)
                    || Sha256::digest(&bytes)[..] != entry.sha256[..]
                {
                    // Truncation and swap are both this: the object
                    // under the content address is not the content.
                    Err(KeyspaceError::ChunkIntegrity {
                        key,
                        chunk: ordinal,
                    })
                } else {
                    Ok((ordinal, bytes))
                }
            }
            Err(ObjectStoreError::NotFound(_)) => Err(KeyspaceError::ChunkMissing {
                key,
                chunk: ordinal,
            }),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace chunk fetch",
            }),
        }
    })
}

impl AsyncRead for ValueReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.position >= this.end || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        match &mut this.source {
            ReaderSource::Inline(payload) => {
                let from = this.position as usize;
                let to = (this.end as usize).min(payload.len());
                let slice = &payload[from..to];
                let n = slice.len().min(buf.remaining());
                buf.put_slice(&slice[..n]);
                this.position += n as u64;
                Poll::Ready(Ok(()))
            }
            ReaderSource::Chunked(reader) => {
                let position = &mut this.position;
                let end = this.end;
                poll_chunked(reader, position, end, cx, buf)
            }
        }
    }
}

fn poll_chunked(
    reader: &mut ChunkReader,
    position: &mut u64,
    end: u64,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
) -> Poll<std::io::Result<()>> {
    // Keep the prefetch window full: at most MAX_IN_FLIGHT_CHUNKS in
    // flight, bounded ordinal order.
    while reader.next_fetch < reader.last_fetch && reader.in_flight.len() < MAX_IN_FLIGHT_CHUNKS {
        let ordinal = reader.next_fetch;
        reader.in_flight.push(fetch_chunk(
            Arc::clone(&reader.store),
            reader.namespace.clone(),
            reader.key.clone(),
            reader.incarnation,
            reader.version,
            reader.entries[ordinal as usize].clone(),
            ordinal,
        ));
        reader.next_fetch += 1;
    }
    // Drain the current chunk first, clamped to the window end.
    let window_room = end.saturating_sub(*position) as usize;
    if let Some((chunk_bytes, offset)) = reader.current.take() {
        let from = offset.min(chunk_bytes.len());
        let to = (from + buf.remaining().min(window_room)).min(chunk_bytes.len());
        let n = to - from;
        if n == 0 {
            reader.current_ordinal += 1;
            return Poll::Ready(Ok(()));
        }
        buf.put_slice(&chunk_bytes[from..to]);
        *position += n as u64;
        if to < chunk_bytes.len() {
            reader.current = Some((chunk_bytes, to));
        } else {
            reader.current_ordinal += 1;
        }
        return Poll::Ready(Ok(()));
    }
    // Need the next chunk in ordinal order.
    loop {
        if let Some(chunk_bytes) = reader.ready.remove(&reader.current_ordinal) {
            let base = u64::from(reader.current_ordinal) * CHUNK_BYTES as u64;
            debug_assert!(base < end, "window fetch bounds match the readable window");
            let start_in_chunk = (*position - base) as usize;
            let n = chunk_bytes
                .len()
                .saturating_sub(start_in_chunk)
                .min(buf.remaining())
                .min(window_room);
            if n == 0 {
                reader.current_ordinal += 1;
                continue;
            }
            buf.put_slice(&chunk_bytes[start_in_chunk..start_in_chunk + n]);
            *position += n as u64;
            if start_in_chunk + n < chunk_bytes.len() {
                reader.current = Some((chunk_bytes, start_in_chunk + n));
            } else {
                reader.current_ordinal += 1;
            }
            return Poll::Ready(Ok(()));
        }
        match reader.in_flight.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok((ordinal, bytes)))) => {
                reader.ready.insert(ordinal, bytes);
            }
            Poll::Ready(Some(Err(error))) => {
                return Poll::Ready(Err(std::io::Error::other(error)));
            }
            Poll::Ready(None) => {
                // Window exhausted: EOF.
                return Poll::Ready(Ok(()));
            }
            Poll::Pending => return Poll::Pending,
        }
    }
}

/// The write-side era binding minted at begin (ADR 0004 §4 step 1).
#[derive(Clone, Debug)]
enum WriterIntent {
    Create,
    CompareExchange { expected_etag: String },
}

/// The streaming writer (ADR 0004 §3.1): `tokio::io::AsyncWrite`
/// into canonical 16 MiB content-addressed chunks, at most four
/// uploads in flight. `seal` uploads/verifies the final chunk and
/// builds the manifest WITHOUT publishing it, so a caller can
/// finalize an independent digest before `PendingValue::commit`.
pub struct ValueWriter {
    store: Arc<ObjectStoreClient>,
    namespace: String,
    key: String,
    intent: WriterIntent,
    incarnation: u64,
    version: u64,
    buffer: Vec<u8>,
    total_len: u64,
    entries: Vec<ManifestEntry>,
    in_flight: Pin<Box<FuturesUnordered<BoxFuture<'static, Result<u32, KeyspaceError>>>>>,
    error: Option<KeyspaceError>,
    sealed: bool,
}

impl std::fmt::Debug for ValueWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ValueWriter")
            .field("key", &self.key)
            .field("incarnation", &self.incarnation)
            .field("version", &self.version)
            .field("bytes_written", &self.total_len)
            .field("sealed", &self.sealed)
            .finish_non_exhaustive()
    }
}

fn hash_entry(bytes: &Bytes) -> ManifestEntry {
    ManifestEntry {
        encoded_len: bytes.len() as u32,
        sha256: Sha256::digest(bytes).into(),
    }
}

fn put_chunk(
    store: Arc<ObjectStoreClient>,
    path: String,
    key: String,
    entry: ManifestEntry,
    ordinal: u32,
    bytes: Bytes,
) -> BoxFuture<'static, Result<u32, KeyspaceError>> {
    Box::pin(async move {
        for _ in 0..4 {
            match store.upload_conditional(&path, bytes.clone(), None).await {
                Ok(_) => return Ok(ordinal),
                Err(_) => {
                    // An identical contender may share the chunk, and
                    // a lost response may still have applied it: both
                    // reconcile by exact GET + full digest
                    // (ADR 0004 §4). A chunk that is genuinely absent
                    // retries the put-if-absent.
                    match verify_chunk(&store, &path, &key, &entry, ordinal).await {
                        Ok(()) => return Ok(ordinal),
                        // Genuinely absent: retry the put-if-absent.
                        Err(KeyspaceError::ChunkMissing { .. }) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Err(KeyspaceError::Unavailable {
            operation: "keyspace chunk upload budget exhausted",
        })
    })
}

async fn verify_chunk(
    store: &ObjectStoreClient,
    path: &str,
    key: &str,
    entry: &ManifestEntry,
    ordinal: u32,
) -> Result<(), KeyspaceError> {
    match store.download(path).await {
        Ok(bytes) => {
            if bytes.len() as u64 != u64::from(entry.encoded_len)
                || Sha256::digest(&bytes)[..] != entry.sha256[..]
            {
                // A wrong object under the content address is an
                // integrity failure, never accepted.
                Err(KeyspaceError::ChunkIntegrity {
                    key: key.to_string(),
                    chunk: ordinal,
                })
            } else {
                Ok(())
            }
        }
        Err(ObjectStoreError::NotFound(_)) => Err(KeyspaceError::ChunkMissing {
            key: key.to_string(),
            chunk: ordinal,
        }),
        Err(_) => Err(KeyspaceError::Unavailable {
            operation: "keyspace chunk verification read",
        }),
    }
}

impl AsyncWrite for ValueWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if let Some(error) = this.error.clone() {
            return Poll::Ready(Err(std::io::Error::other(error)));
        }
        if this.sealed {
            return Poll::Ready(Err(std::io::Error::other("value writer sealed")));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.total_len + buf.len() as u64 > MAX_LOGICAL_BYTES {
            let error = KeyspaceError::ValueTooLarge {
                key: this.key.clone(),
                len: this.total_len + buf.len() as u64,
                max: MAX_LOGICAL_BYTES,
            };
            this.error = Some(error.clone());
            return Poll::Ready(Err(std::io::Error::other(error)));
        }
        // Bounded in-flight backpressure.
        while this.in_flight.len() >= MAX_IN_FLIGHT_CHUNKS {
            match this.in_flight.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(_))) => {}
                Poll::Ready(Some(Err(error))) => {
                    this.error = Some(error.clone());
                    return Poll::Ready(Err(std::io::Error::other(error)));
                }
                Poll::Ready(None) => break,
                Poll::Pending => return Poll::Pending,
            }
        }
        let mut consumed = 0;
        while consumed < buf.len() {
            let room = CHUNK_BYTES - this.buffer.len();
            let take = room.min(buf.len() - consumed);
            this.buffer
                .extend_from_slice(&buf[consumed..consumed + take]);
            consumed += take;
            this.total_len += take as u64;
            if this.buffer.len() == CHUNK_BYTES {
                this.start_chunk_upload();
            }
        }
        Poll::Ready(Ok(consumed))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if let Some(error) = this.error.clone() {
            return Poll::Ready(Err(std::io::Error::other(error)));
        }
        poll_drain(this, cx).map_err(std::io::Error::other)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_flush(cx)
    }
}

fn poll_drain(this: &mut ValueWriter, cx: &mut Context<'_>) -> Poll<Result<(), KeyspaceError>> {
    while !this.in_flight.is_empty() {
        match this.in_flight.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(_))) => {}
            Poll::Ready(Some(Err(error))) => {
                this.error = Some(error.clone());
                return Poll::Ready(Err(error));
            }
            Poll::Ready(None) => break,
            Poll::Pending => return Poll::Pending,
        }
    }
    Poll::Ready(Ok(()))
}

impl ValueWriter {
    /// Hash the full buffer as the next chunk and start its
    /// put-if-absent upload. The buffer is exactly CHUNK_BYTES here.
    fn start_chunk_upload(&mut self) {
        debug_assert_eq!(self.buffer.len(), CHUNK_BYTES);
        let bytes = Bytes::from(std::mem::take(&mut self.buffer));
        let entry = hash_entry(&bytes);
        let ordinal = self.entries.len() as u32;
        let path = chunk_object_key(
            &self.namespace,
            &self.key,
            self.incarnation,
            self.version,
            &entry.digest_hex(),
        );
        self.entries.push(entry.clone());
        self.in_flight.push(put_chunk(
            Arc::clone(&self.store),
            path,
            self.key.clone(),
            entry,
            ordinal,
            bytes,
        ));
    }

    /// Drive every started upload to completion.
    async fn drain_uploads(&mut self) -> Result<(), KeyspaceError> {
        while let Some(result) = self.in_flight.next().await {
            result.map(|_| ())?;
        }
        match self.error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Seal: upload/verify the final partial chunk and build the
    /// manifest WITHOUT publishing it. A value that would be a
    /// one-chunk (or zero-chunk) v3 is rejected — inline is
    /// canonical; use the whole-value APIs for those sizes
    /// (ADR 0004 §1.2).
    pub async fn seal(mut self) -> Result<PendingValue, KeyspaceError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        self.drain_uploads().await?;
        if !self.buffer.is_empty() {
            let bytes = Bytes::from(std::mem::take(&mut self.buffer));
            let entry = hash_entry(&bytes);
            let ordinal = self.entries.len() as u32;
            let path = chunk_object_key(
                &self.namespace,
                &self.key,
                self.incarnation,
                self.version,
                &entry.digest_hex(),
            );
            self.entries.push(entry.clone());
            put_chunk(
                Arc::clone(&self.store),
                path,
                self.key.clone(),
                entry,
                ordinal,
                bytes,
            )
            .await?;
        }
        self.sealed = true;
        let chunk_count = self.entries.len() as u32;
        if !(MIN_CHUNKS..=MAX_CHUNKS).contains(&chunk_count) {
            return Err(KeyspaceError::ChunkCountInvalid {
                key: self.key.clone(),
                count: chunk_count,
            });
        }
        let manifest = ValueManifest {
            incarnation: self.incarnation,
            version: self.version,
            commit_id: crate::value_manifest::mint_commit_id(),
            logical_len: self.total_len,
            value_root_sha256: ValueManifest::compute_value_root(
                self.total_len,
                CHUNK_BYTES as u32,
                &self.entries,
            ),
            chunk_bytes: CHUNK_BYTES as u32,
            entries: self.entries.clone(),
        };
        Ok(PendingValue {
            store: self.store,
            namespace: self.namespace,
            key: self.key,
            intent: self.intent,
            manifest,
        })
    }
}

/// A sealed, unpublished streamed value. The manifest is built; only
/// the conditional control PUT remains.
pub struct PendingValue {
    store: Arc<ObjectStoreClient>,
    namespace: String,
    key: String,
    intent: WriterIntent,
    manifest: ValueManifest,
}

impl std::fmt::Debug for PendingValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingValue")
            .field("key", &self.key)
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

impl PendingValue {
    fn keyspace(&self) -> AtomicKeyspace {
        AtomicKeyspace {
            store: Arc::clone(&self.store),
            namespace: self.namespace.clone(),
        }
    }

    fn object_key(&self) -> String {
        format!("{KEYSPACE_ROOT}/{}/{}", self.namespace, self.key)
    }

    fn receipt(&self, etag: String) -> CommitReceipt {
        CommitReceipt {
            etag,
            logical_len: self.manifest.logical_len,
            representation: ValueRepresentation::Chunked,
            chunk_count: self.manifest.chunk_count(),
        }
    }

    /// The logical byte length that will land on commit — the input a
    /// caller's independent digest (e.g. a Git OID) is finalized
    /// against, before any publication (ADR 0004 §3.1).
    #[must_use]
    pub fn logical_len(&self) -> u64 {
        self.manifest.logical_len
    }

    /// The chunk count of the sealed value.
    #[must_use]
    pub fn chunk_count(&self) -> u32 {
        self.manifest.chunk_count()
    }

    /// The value root the manifest commits to
    /// (SHA-256 over domain || boundaries || ordered chunk table).
    #[must_use]
    pub fn value_root_hex(&self) -> String {
        hex::encode(self.manifest.value_root_sha256)
    }

    /// Publish the control conditionally — `If-None-Match` for a
    /// create binding, `If-Match` for a CAS binding — then reconcile
    /// (ADR 0004 §2.1, §2.2, §2.4).
    pub async fn commit(self) -> Result<CommitReceipt, KeyspaceError> {
        let object_key = self.object_key();
        let manifest_bytes = self.manifest.encode();
        let intent = self.intent.clone();
        match intent {
            WriterIntent::Create => {
                match self
                    .store
                    .upload_conditional(&object_key, manifest_bytes, None)
                    .await
                {
                    Ok(Some(etag)) => self.finish_create_after_publish(&object_key, &etag).await,
                    Ok(None) => Err(KeyspaceError::Unavailable {
                        operation: "keyspace create (no etag on manifest publish)",
                    }),
                    Err(ObjectStoreError::PreconditionFailed(_)) => {
                        Err(KeyspaceError::AlreadyExists(self.key.clone()))
                    }
                    Err(_) => {
                        match self
                            .keyspace()
                            .adjudicate_manifest_put(&self.key, &object_key, &self.manifest)
                            .await?
                        {
                            Ambiguity::Landed { etag: Some(etag) } => {
                                self.finish_create_after_publish(&object_key, &etag).await
                            }
                            Ambiguity::Landed { etag: None } => Err(KeyspaceError::Unavailable {
                                operation: "keyspace create (ambiguous manifest commit: no etag)",
                            }),
                            Ambiguity::LostConflict => {
                                Err(KeyspaceError::AlreadyExists(self.key.clone()))
                            }
                            Ambiguity::Ambiguous => Err(KeyspaceError::Unavailable {
                                operation: "keyspace create (ambiguous manifest commit)",
                            }),
                        }
                    }
                }
            }
            WriterIntent::CompareExchange { expected_etag } => {
                match self
                    .store
                    .upload_conditional(&object_key, manifest_bytes, Some(&expected_etag))
                    .await
                {
                    Ok(Some(etag)) => Ok(self.receipt(etag)),
                    Ok(None) => Err(KeyspaceError::Unavailable {
                        operation: "keyspace compare_exchange (no etag on manifest publish)",
                    }),
                    Err(ObjectStoreError::PreconditionFailed(_)) => Err(self
                        .keyspace()
                        .cas_conflict(&self.key, &expected_etag)
                        .await),
                    Err(_) => {
                        match self
                            .keyspace()
                            .adjudicate_manifest_put(&self.key, &object_key, &self.manifest)
                            .await?
                        {
                            Ambiguity::Landed { etag: Some(etag) } => Ok(self.receipt(etag)),
                            Ambiguity::Landed { etag: None } => Err(KeyspaceError::Unavailable {
                                operation: "keyspace compare_exchange (ambiguous manifest commit: no etag)",
                            }),
                            Ambiguity::LostConflict => Err(self
                                .keyspace()
                                .cas_conflict(&self.key, &expected_etag)
                                .await),
                            Ambiguity::Ambiguous => Err(KeyspaceError::Unavailable {
                                operation: "keyspace compare_exchange (ambiguous manifest commit)",
                            }),
                        }
                    }
                }
            }
        }
    }

    /// Create's mandatory incarnation post-check (ADR 0004 §2.4). If
    /// the counter moved after publication, reread the control WITH
    /// etag, confirm our exact commit, and evict only with the
    /// observed etag. A streamed writer cannot re-upload its bytes at
    /// the fresh era, so the honest outcome after eviction is a typed
    /// [`KeyspaceError::StaleIncarnation`].
    async fn finish_create_after_publish(
        &self,
        object_key: &str,
        published_etag: &str,
    ) -> Result<CommitReceipt, KeyspaceError> {
        let now = self.keyspace().current_incarnation(&self.key).await?;
        if now == self.manifest.incarnation {
            return Ok(self.receipt(published_etag.to_string()));
        }
        // Stale era: confirm our own exact commit and evict
        // conditionally — never an unconditional delete.
        match self.store.download_with_etag(object_key).await {
            Ok(meta) => {
                let Some(observed_etag) = meta.etag else {
                    return Err(KeyspaceError::Unavailable {
                        operation: "keyspace create (stale-era eviction: no etag)",
                    });
                };
                if meta.data == self.manifest.encode() {
                    match self
                        .store
                        .delete_conditional(object_key, &observed_etag)
                        .await
                    {
                        Ok(()) | Err(ObjectStoreError::NotFound(_)) => {
                            Err(KeyspaceError::StaleIncarnation(self.key.clone()))
                        }
                        Err(ObjectStoreError::PreconditionFailed(_)) => {
                            Err(KeyspaceError::AlreadyExists(self.key.clone()))
                        }
                        Err(_) => Err(KeyspaceError::Unavailable {
                            operation: "keyspace create (stale-era eviction)",
                        }),
                    }
                } else {
                    // A newer lifetime holds the key.
                    Err(KeyspaceError::AlreadyExists(self.key.clone()))
                }
            }
            Err(ObjectStoreError::NotFound(_)) => {
                Err(KeyspaceError::StaleIncarnation(self.key.clone()))
            }
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace create (stale-era eviction check)",
            }),
        }
    }
}

/// The oracle's three legal adjudications (ADR 0004 §2.2).
#[derive(Debug)]
pub(crate) enum Ambiguity {
    /// Row 1: this writer landed.
    Landed { etag: Option<String> },
    /// Row 2: a foreign writer holds exactly the bound target
    /// generation — a typed create/CAS conflict.
    LostConflict,
    /// Row 3: beyond the bound target, destroyed, absent, or logically
    /// retired — `Unavailable` with an explicit ambiguous-write
    /// operation. Never a fabricated success or conflict.
    Ambiguous,
}

/// The stale-era disposition after a published create manifest.
enum StaleEra {
    /// The counter still names our incarnation: the create landed.
    Fresh,
    /// Our stale-era bytes were conditionally evicted; retry at the
    /// fresh incarnation.
    Retry,
    /// A newer lifetime holds the key: the typed conflict.
    Conflict(KeyspaceError),
}

impl AtomicKeyspace {
    // ---- shared reads -------------------------------------------------------

    /// Whether a seq-shaped key is below the namespace's certified
    /// trim floor (logically retired history).
    pub(crate) async fn seq_retired(&self, key: &str) -> Result<Option<u64>, KeyspaceError> {
        Ok(Self::seq_retired_at_floor(key, self.trim_floor("").await?))
    }

    fn seq_retired_at_floor(key: &str, first_retained: Option<u64>) -> Option<u64> {
        if let Some(seq) = key.rsplit('/').next().and_then(Self::parse_seq_component)
            && seq > 0
            && let Some(first_retained) = first_retained
            && seq < first_retained
        {
            return Some(first_retained);
        }
        None
    }

    /// The control-metadata era read (ADR 0004 §2.3): incarnation and
    /// version from a v2 OR v3 control, never a chunk fetch.
    pub(crate) async fn read_control_era(
        &self,
        key: &str,
    ) -> Result<Option<(u64, u64)>, KeyspaceError> {
        let object_key = self.object_key(key)?;
        match self.store.download(&object_key).await {
            Ok(bytes) => {
                let control = ControlEnvelope::decode(key, &bytes)?;
                Ok(Some((control.incarnation(), control.version())))
            }
            Err(ObjectStoreError::NotFound(_)) => Ok(None),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace control era read",
            }),
        }
    }

    /// Collect a control object's logical value: inline payload, or
    /// every manifest chunk fetched in order with at most four in
    /// flight, each verified, concatenated to logical length. Whole
    /// reads are explicit collection and may allocate to logical
    /// length (ADR 0004 §3.1).
    pub(crate) async fn collect_value(
        &self,
        key: &str,
        control_bytes: &Bytes,
    ) -> Result<Bytes, KeyspaceError> {
        match ControlEnvelope::decode(key, control_bytes)? {
            ControlEnvelope::Inline(envelope) => Ok(envelope.payload),
            ControlEnvelope::Chunked(manifest) => self.fetch_all_chunks(key, &manifest).await,
        }
    }

    pub(crate) async fn fetch_all_chunks(
        &self,
        key: &str,
        manifest: &ValueManifest,
    ) -> Result<Bytes, KeyspaceError> {
        let store = Arc::clone(&self.store);
        let namespace = self.namespace.clone();
        let key = key.to_string();
        let incarnation = manifest.incarnation;
        let version = manifest.version;
        let mut collected = Vec::with_capacity(manifest.logical_len as usize);
        // Owned future list (no closures capturing parameter
        // references): keeps the future's auto-trait leakage clean
        // for spawned callers.
        let mut fetches: Vec<BoxFuture<'static, Result<(u32, Bytes), KeyspaceError>>> = Vec::new();
        for (ordinal, entry) in manifest.entries.iter().enumerate() {
            fetches.push(fetch_chunk(
                Arc::clone(&store),
                namespace.clone(),
                key.clone(),
                incarnation,
                version,
                entry.clone(),
                ordinal as u32,
            ));
        }
        let mut fetches = stream::iter(fetches).buffered(MAX_IN_FLIGHT_CHUNKS);
        while let Some(result) = fetches.next().await {
            let (_, bytes) = result?;
            collected.extend_from_slice(&bytes);
        }
        Ok(Bytes::from(collected))
    }

    // ---- streamed begins ------------------------------------------------------

    /// Begin a streamed create (ADR 0004 §3.1): bind the target
    /// generation (current incarnation, version 0) and check the
    /// maintenance fence — one exact GET; streamed begins refuse
    /// while fenced.
    pub async fn begin_stream_create(&self, key: &str) -> Result<ValueWriter, KeyspaceError> {
        Self::ensure_not_reserved_key(key)?;
        self.object_key(key)?;
        let incarnation = self.current_incarnation(key).await?;
        self.ensure_unfenced(key).await?;
        Ok(ValueWriter {
            store: Arc::clone(&self.store),
            namespace: self.namespace.clone(),
            key: key.to_string(),
            intent: WriterIntent::Create,
            incarnation,
            version: 0,
            buffer: Vec::with_capacity(CHUNK_BYTES),
            total_len: 0,
            entries: Vec::new(),
            in_flight: Box::pin(FuturesUnordered::new()),
            error: None,
            sealed: false,
        })
    }

    /// Begin a streamed CAS: verify the expected etag against the
    /// current control (typed conflict on mismatch), bind the checked
    /// successor era, and check the maintenance fence.
    pub async fn begin_stream_compare_exchange(
        &self,
        key: &str,
        expected_etag: &str,
    ) -> Result<ValueWriter, KeyspaceError> {
        Self::ensure_not_reserved_key(key)?;
        let object_key = self.object_key(key)?;
        let meta = match self.store.download_with_etag(&object_key).await {
            Ok(meta) => meta,
            Err(ObjectStoreError::NotFound(_)) => {
                return Err(KeyspaceError::PreconditionFailed {
                    key: key.to_string(),
                    expected_etag: expected_etag.to_string(),
                    observed: None,
                    observed_incarnation: None,
                    observed_version: None,
                });
            }
            Err(_) => {
                return Err(KeyspaceError::Unavailable {
                    operation: "keyspace begin_stream_compare_exchange read",
                });
            }
        };
        let Some(observed_etag) = meta.etag else {
            return Err(KeyspaceError::Unavailable {
                operation: "keyspace begin_stream_compare_exchange read (no etag reported)",
            });
        };
        if observed_etag != expected_etag {
            return Err(self.cas_conflict(key, expected_etag).await);
        }
        let control = ControlEnvelope::decode(key, &meta.data)?;
        let version = control
            .version()
            .checked_add(1)
            .ok_or_else(|| KeyspaceError::VersionExhausted(key.to_string()))?;
        self.ensure_unfenced(key).await?;
        Ok(ValueWriter {
            store: Arc::clone(&self.store),
            namespace: self.namespace.clone(),
            key: key.to_string(),
            intent: WriterIntent::CompareExchange {
                expected_etag: expected_etag.to_string(),
            },
            incarnation: control.incarnation(),
            version,
            buffer: Vec::with_capacity(CHUNK_BYTES),
            total_len: 0,
            entries: Vec::new(),
            in_flight: Box::pin(FuturesUnordered::new()),
            error: None,
            sealed: false,
        })
    }

    /// One exact fence GET; refuses while fenced (ADR 0004 §5.1).
    async fn ensure_unfenced(&self, key: &str) -> Result<(), KeyspaceError> {
        if self.fence_present().await? {
            return Err(KeyspaceError::MaintenanceFenced(key.to_string()));
        }
        Ok(())
    }

    async fn fence_present(&self) -> Result<bool, KeyspaceError> {
        match self.store.download(&self.fence_object_key()).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::NotFound(_)) => Ok(false),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "maintenance fence read",
            }),
        }
    }

    fn fence_object_key(&self) -> String {
        format!(
            "{KEYSPACE_ROOT}/{}/{}/gc",
            self.namespace,
            AtomicKeyspace::FENCE_ROOT
        )
    }

    // ---- streamed reads ---------------------------------------------------------

    /// Open a verified snapshot reader (`None` when absent). The
    /// control is fetched once with etag; inline v2 yields the payload
    /// with no chunk request; chunked v3 validates the manifest/root
    /// and then fetches ordered chunks with at most four in flight
    /// (ADR 0004 §3.2).
    pub async fn open_stream(&self, key: &str) -> Result<Option<ValueReader>, KeyspaceError> {
        self.open_stream_window(key, None).await
    }

    /// Open a reader for a logical half-open range `[start, end)`:
    /// validates the whole ordered manifest table, fetches only the
    /// intersecting complete chunks, verifies each, and slices the
    /// boundary chunks (boundary overfetch stays below 32 MiB; v1
    /// uses no backend Range GET within a chunk — partial bytes
    /// cannot verify the manifest's full-chunk SHA-256).
    pub async fn open_stream_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Option<ValueReader>, KeyspaceError> {
        if range.start > range.end {
            let logical_len = self
                .open_stream(key)
                .await?
                .map_or(0, |reader| reader.metadata().logical_len);
            return Err(KeyspaceError::InvalidRange {
                key: key.to_string(),
                start: range.start,
                end: range.end,
                logical_len,
            });
        }
        self.open_stream_window(key, Some(range)).await
    }

    async fn open_stream_window(
        &self,
        key: &str,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<Option<ValueReader>, KeyspaceError> {
        let object_key = self.object_key(key)?;
        let meta = match self.store.download_with_etag(&object_key).await {
            Ok(meta) => meta,
            Err(ObjectStoreError::NotFound(_)) => return Ok(None),
            Err(_) => {
                return Err(KeyspaceError::Unavailable {
                    operation: "keyspace open_stream control read",
                });
            }
        };
        let Some(etag) = meta.etag else {
            return Err(KeyspaceError::Unavailable {
                operation: "keyspace open_stream (no etag reported)",
            });
        };
        let control = ControlEnvelope::decode(key, &meta.data)?;
        let (metadata, source, digests, logical_len) = match control {
            ControlEnvelope::Inline(envelope) => {
                let logical_len = envelope.payload.len() as u64;
                (
                    ValueMetadata {
                        logical_len,
                        etag,
                        value_root_sha256: None,
                        representation: ValueRepresentation::Inline,
                    },
                    ReaderSource::Inline(envelope.payload),
                    Vec::new(),
                    logical_len,
                )
            }
            ControlEnvelope::Chunked(manifest) => {
                let logical_len = manifest.logical_len;
                let digests: Vec<[u8; 32]> =
                    manifest.entries.iter().map(|entry| entry.sha256).collect();
                let metadata = ValueMetadata {
                    logical_len,
                    etag,
                    value_root_sha256: Some(manifest.value_root_sha256),
                    representation: ValueRepresentation::Chunked,
                };
                let chunk_count = manifest.chunk_count();
                let source = ReaderSource::Chunked(Box::new(ChunkReader {
                    store: Arc::clone(&self.store),
                    namespace: self.namespace.clone(),
                    key: key.to_string(),
                    incarnation: manifest.incarnation,
                    version: manifest.version,
                    entries: manifest.entries,
                    next_fetch: 0,
                    last_fetch: chunk_count,
                    in_flight: Box::pin(FuturesUnordered::new()),
                    ready: BTreeMap::new(),
                    current: None,
                    current_ordinal: 0,
                }));
                (metadata, source, digests, logical_len)
            }
        };
        let window = match range {
            None => 0..logical_len,
            Some(range) => {
                if range.end > logical_len {
                    return Err(KeyspaceError::InvalidRange {
                        key: key.to_string(),
                        start: range.start,
                        end: range.end,
                        logical_len,
                    });
                }
                range
            }
        };
        let mut reader = ValueReader {
            metadata,
            chunk_digests: digests,
            source,
            position: window.start,
            end: window.end,
        };
        if let ReaderSource::Chunked(chunk_reader) = &mut reader.source {
            // Fetch only the complete chunks intersecting the window.
            let first = (window.start / CHUNK_BYTES as u64).min(u32::MAX as u64) as u32;
            let last_exclusive = if window.end == 0 {
                0
            } else {
                (((window.end - 1) / CHUNK_BYTES as u64) + 1)
                    .min(u64::from(chunk_reader.last_fetch)) as u32
            };
            chunk_reader.next_fetch = first;
            chunk_reader.last_fetch = last_exclusive;
            chunk_reader.current_ordinal = first;
        }
        Ok(Some(reader))
    }

    /// The streamed existence read (ADR 0004 §3.3). Destroyed,
    /// expired, and absent states fetch no chunks.
    pub async fn read_state_stream(&self, key: &str) -> Result<StreamKeyState, KeyspaceError> {
        self.object_key(key)?;
        if let Some(first_retained) = self.seq_retired(key).await? {
            return Ok(StreamKeyState::OffsetExpired { first_retained });
        }
        if let Some(reader) = self.open_stream(key).await? {
            let metadata = reader.metadata().clone();
            return Ok(StreamKeyState::Present { reader, metadata });
        }
        let tombstone_object_key = format!(
            "{KEYSPACE_ROOT}/{}/{}",
            self.namespace,
            Self::tombstone_key(key)
        );
        match self.store.download(&tombstone_object_key).await {
            Ok(bytes) => Ok(StreamKeyState::Destroyed {
                tombstone: Tombstone::decode(bytes.as_ref())?,
            }),
            Err(ObjectStoreError::NotFound(_)) => Ok(StreamKeyState::Absent),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "read_state_stream: tombstone",
            }),
        }
    }

    // ---- whole-value chunked paths ------------------------------------------------

    /// Whole-value create above `INLINE_MAX`: chunk, manifest,
    /// conditional publish, incarnation recheck, conditional
    /// stale-era eviction, bounded retry at the fresh era (the kernel
    /// holds the bytes, unlike a streamed writer).
    pub(crate) async fn create_chunked(
        &self,
        key: &str,
        value: &Bytes,
    ) -> Result<(), KeyspaceError> {
        let object_key = self.object_key(key)?;
        for _ in 0..8 {
            let incarnation = self.current_incarnation(key).await?;
            let manifest = self.stage_chunks(key, value, incarnation, 0).await?;
            let disposition = match self
                .store
                .upload_conditional(&object_key, manifest.encode(), None)
                .await
            {
                Ok(Some(_)) => self.evict_stale_era(key, &object_key, &manifest).await?,
                Ok(None) => {
                    return Err(KeyspaceError::Unavailable {
                        operation: "keyspace create (no etag on manifest publish)",
                    });
                }
                Err(ObjectStoreError::PreconditionFailed(_)) => {
                    return Err(KeyspaceError::AlreadyExists(key.to_string()));
                }
                Err(_) => {
                    match self
                        .adjudicate_manifest_put(key, &object_key, &manifest)
                        .await?
                    {
                        Ambiguity::Landed { etag: Some(_) } => {
                            self.evict_stale_era(key, &object_key, &manifest).await?
                        }
                        Ambiguity::Landed { etag: None } => {
                            return Err(KeyspaceError::Unavailable {
                                operation: "keyspace create (ambiguous manifest commit: no etag)",
                            });
                        }
                        Ambiguity::LostConflict => {
                            return Err(KeyspaceError::AlreadyExists(key.to_string()));
                        }
                        Ambiguity::Ambiguous => {
                            return Err(KeyspaceError::Unavailable {
                                operation: "keyspace create (ambiguous manifest commit)",
                            });
                        }
                    }
                }
            };
            match disposition {
                StaleEra::Fresh => return Ok(()),
                // Evicted: the loop retries at the fresh incarnation.
                StaleEra::Retry => {}
                StaleEra::Conflict(error) => return Err(error),
            }
        }
        Err(KeyspaceError::Unavailable {
            operation: "keyspace create (incarnation contention)",
        })
    }

    /// Whole-value CAS above `INLINE_MAX`. The etag is consumable
    /// exactly once, so no incarnation recheck is needed on success.
    pub(crate) async fn compare_exchange_chunked(
        &self,
        key: &str,
        expected_etag: &str,
        incarnation: u64,
        next_version: u64,
        value: &Bytes,
    ) -> Result<String, KeyspaceError> {
        let object_key = self.object_key(key)?;
        let manifest = self
            .stage_chunks(key, value, incarnation, next_version)
            .await?;
        match self
            .store
            .upload_conditional(&object_key, manifest.encode(), Some(expected_etag))
            .await
        {
            Ok(etag) => etag.ok_or(KeyspaceError::Unavailable {
                operation: "keyspace compare_exchange (no etag)",
            }),
            Err(ObjectStoreError::PreconditionFailed(_)) => {
                Err(self.cas_conflict(key, expected_etag).await)
            }
            Err(_) => {
                match self
                    .adjudicate_manifest_put(key, &object_key, &manifest)
                    .await?
                {
                    Ambiguity::Landed { etag } => etag.ok_or(KeyspaceError::Unavailable {
                        operation: "keyspace compare_exchange (ambiguous manifest commit: no etag)",
                    }),
                    Ambiguity::LostConflict => Err(self.cas_conflict(key, expected_etag).await),
                    Ambiguity::Ambiguous => Err(KeyspaceError::Unavailable {
                        operation: "keyspace compare_exchange (ambiguous manifest commit)",
                    }),
                }
            }
        }
    }

    /// Upload every chunk of `value` at the bound generation
    /// (put-if-absent with full verify on conflict, ≤4 in flight) and
    /// build the unpublished manifest.
    async fn stage_chunks(
        &self,
        key: &str,
        value: &Bytes,
        incarnation: u64,
        version: u64,
    ) -> Result<ValueManifest, KeyspaceError> {
        let mut staged: Vec<BoxFuture<'static, Result<u32, KeyspaceError>>> = Vec::new();
        for (ordinal, chunk) in value.chunks(CHUNK_BYTES).enumerate() {
            let bytes = value.slice_ref(chunk);
            let entry = hash_entry(&bytes);
            let path = chunk_object_key(
                &self.namespace,
                key,
                incarnation,
                version,
                &entry.digest_hex(),
            );
            staged.push(put_chunk(
                Arc::clone(&self.store),
                path,
                key.to_string(),
                entry,
                ordinal as u32,
                bytes,
            ));
        }
        let mut uploads = stream::iter(staged).buffered(MAX_IN_FLIGHT_CHUNKS);
        while let Some(result) = uploads.next().await {
            result?;
        }
        let entries: Vec<ManifestEntry> = value
            .chunks(CHUNK_BYTES)
            .map(|chunk| hash_entry(&value.slice_ref(chunk)))
            .collect();
        let logical_len = value.len() as u64;
        Ok(ValueManifest {
            incarnation,
            version,
            commit_id: crate::value_manifest::mint_commit_id(),
            logical_len,
            value_root_sha256: ValueManifest::compute_value_root(
                logical_len,
                CHUNK_BYTES as u32,
                &entries,
            ),
            chunk_bytes: CHUNK_BYTES as u32,
            entries,
        })
    }

    /// Create's post-publish incarnation check for the whole-value
    /// path: confirm the counter, and on a moved counter reread with
    /// etag and evict ONLY our exact commit with the batch-8
    /// conditional delete (ADR 0004 §2.4).
    async fn evict_stale_era(
        &self,
        key: &str,
        object_key: &str,
        manifest: &ValueManifest,
    ) -> Result<StaleEra, KeyspaceError> {
        let now = self.current_incarnation(key).await?;
        if now == manifest.incarnation {
            return Ok(StaleEra::Fresh);
        }
        match self.store.download_with_etag(object_key).await {
            Ok(meta) => {
                let Some(observed_etag) = meta.etag else {
                    return Err(KeyspaceError::Unavailable {
                        operation: "keyspace create (stale-era eviction: no etag)",
                    });
                };
                if meta.data == manifest.encode() {
                    match self
                        .store
                        .delete_conditional(object_key, &observed_etag)
                        .await
                    {
                        Ok(()) | Err(ObjectStoreError::NotFound(_)) => Ok(StaleEra::Retry),
                        Err(ObjectStoreError::PreconditionFailed(_)) => Ok(StaleEra::Conflict(
                            KeyspaceError::AlreadyExists(key.to_string()),
                        )),
                        Err(_) => Err(KeyspaceError::Unavailable {
                            operation: "keyspace create (stale-era eviction)",
                        }),
                    }
                } else {
                    Ok(StaleEra::Conflict(KeyspaceError::AlreadyExists(
                        key.to_string(),
                    )))
                }
            }
            Err(ObjectStoreError::NotFound(_)) => Ok(StaleEra::Retry),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace create (stale-era eviction check)",
            }),
        }
    }

    /// The three-row oracle for a manifest PUT that returned an
    /// ambiguous transport error (ADR 0004 §2.2). Row 1: exactly the
    /// bound generation and our commit — landed. Row 2: exactly the
    /// bound generation, foreign commit — typed conflict. Row 3:
    /// anything beyond the target, destroyed, absent, or logically
    /// retired — ambiguous. Malformed current control stays an
    /// integrity failure; no outcome is inferred from candidate
    /// chunks.
    pub(crate) async fn adjudicate_manifest_put(
        &self,
        key: &str,
        object_key: &str,
        manifest: &ValueManifest,
    ) -> Result<Ambiguity, KeyspaceError> {
        if self.seq_retired(key).await?.is_some() {
            return Ok(Ambiguity::Ambiguous);
        }
        match self.store.download_with_etag(object_key).await {
            Err(ObjectStoreError::NotFound(_)) => Ok(Ambiguity::Ambiguous),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace ambiguous commit reread",
            }),
            Ok(meta) => {
                let control = ControlEnvelope::decode(key, &meta.data)?;
                if control.incarnation() == manifest.incarnation
                    && control.version() == manifest.version
                {
                    match control {
                        ControlEnvelope::Chunked(observed)
                            if observed.commit_id == manifest.commit_id =>
                        {
                            Ok(Ambiguity::Landed { etag: meta.etag })
                        }
                        _ => Ok(Ambiguity::LostConflict),
                    }
                } else {
                    Ok(Ambiguity::Ambiguous)
                }
            }
        }
    }

    // ---- maintenance fence ----------------------------------------------------------

    /// Set the maintenance fence at the kernel-reserved key
    /// `keyspace/{namespace}/fences/gc` (ADR 0004 §5.1, acceptance
    /// footnote N2). Idempotent. The fence is a cheap barrier — it
    /// does NOT prove quiescence; the operational assertion remains
    /// load-bearing.
    pub async fn set_maintenance_fence(&self) -> Result<(), KeyspaceError> {
        let fence = ValueEnvelope::new(0, 0, Bytes::from_static(b"fenced")).encode();
        match self
            .store
            .upload_conditional(&self.fence_object_key(), fence, None)
            .await
        {
            Ok(_) | Err(ObjectStoreError::PreconditionFailed(_)) => Ok(()),
            Err(_) => Err(KeyspaceError::Unavailable {
                operation: "keyspace set_maintenance_fence",
            }),
        }
    }

    /// Release the fence by conditional delete (the release CAS).
    /// Idempotent; bounded contention retry.
    pub async fn release_maintenance_fence(&self) -> Result<(), KeyspaceError> {
        let fence_object_key = self.fence_object_key();
        for _ in 0..8 {
            match self.store.download_with_etag(&fence_object_key).await {
                Ok(meta) => {
                    let Some(etag) = meta.etag else {
                        return Err(KeyspaceError::Unavailable {
                            operation: "keyspace release_maintenance_fence (no etag)",
                        });
                    };
                    match self
                        .store
                        .delete_conditional(&fence_object_key, &etag)
                        .await
                    {
                        Ok(()) | Err(ObjectStoreError::NotFound(_)) => return Ok(()),
                        Err(ObjectStoreError::PreconditionFailed(_)) => {}
                        Err(_) => {
                            return Err(KeyspaceError::Unavailable {
                                operation: "keyspace release_maintenance_fence",
                            });
                        }
                    }
                }
                Err(ObjectStoreError::NotFound(_)) => return Ok(()),
                Err(_) => {
                    return Err(KeyspaceError::Unavailable {
                        operation: "keyspace release_maintenance_fence read",
                    });
                }
            }
        }
        Err(KeyspaceError::Unavailable {
            operation: "keyspace release_maintenance_fence (contention)",
        })
    }

    /// Whether the maintenance fence currently stands (test/probe
    /// inspection).
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn maintenance_fence_present_for_test(&self) -> Result<bool, KeyspaceError> {
        self.fence_present().await
    }

    // ---- chunk inventory and quiesced sweep --------------------------------------------

    fn chunk_root_prefix(&self) -> String {
        format!("{CHUNK_ROOT}/v1/{}/", self.namespace)
    }

    /// One LIST page of the namespace's private chunk root, with
    /// sizes.
    async fn list_chunk_page(
        &self,
        after: Option<&str>,
    ) -> Result<Vec<(String, u64)>, KeyspaceError> {
        self.store
            .list_prefix_after_with_sizes(&self.chunk_root_prefix(), after, 1000)
            .await
            .map_err(|_| KeyspaceError::Unavailable {
                operation: "keyspace chunk root list",
            })
    }

    /// Delete-free online chunk metering (ADR 0004 §5.3): list the
    /// private chunks, derive their logical keys/generations,
    /// exact-read current control once per key, and classify. Chunks
    /// not referenced by a current validated manifest are
    /// **candidates** — a concurrent writer's uploads are
    /// indistinguishable from true orphans online — and keys whose
    /// control read fails (unavailable or corrupt) count as
    /// unresolved. It deletes nothing.
    pub async fn chunk_inventory(&self) -> Result<ChunkInventory, KeyspaceError> {
        let mut inventory = ChunkInventory::default();
        let mut after: Option<String> = None;
        loop {
            let page = self.list_chunk_page(after.as_deref()).await?;
            if page.is_empty() {
                break;
            }
            let last = page.last().map(|(key, _)| key.clone());
            let classification = self.classify_chunk_page(&page).await?;
            inventory.listed_chunks += page.len() as u64;
            inventory.listed_bytes += page.iter().map(|(_, size)| size).sum::<u64>();
            inventory.referenced_chunks += classification.referenced_count;
            inventory.unresolved_chunks += classification.unresolved_count;
            for orphan in &classification.orphan_candidates {
                let size = page
                    .iter()
                    .find(|(listed, _)| listed == orphan)
                    .map_or(0, |(_, size)| *size);
                inventory.candidate_orphan_chunks += 1;
                inventory.candidate_orphan_bytes += size;
            }
            after = last;
            if page.len() < 1000 {
                break;
            }
        }
        Ok(inventory)
    }

    /// Classify one LIST page by exact control reads: which chunks
    /// the current validated manifests reference, which are orphan
    /// candidates, and which cannot be classified (fail closed).
    async fn classify_chunk_page(
        &self,
        page: &[(String, u64)],
    ) -> Result<PageClassification, KeyspaceError> {
        let mut classification = PageClassification::default();
        // Group chunk paths by logical key (the namespace is fixed by
        // the list prefix).
        let mut by_key: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (object_key, _) in page {
            match parse_chunk_object_key(object_key) {
                Some(parsed) if parsed.namespace == self.namespace => {
                    by_key
                        .entry(parsed.logical_key)
                        .or_default()
                        .push(object_key.clone());
                }
                // A path under the chunk root that does not parse
                // exactly — or names another namespace — is refused by
                // the format decoder: unresolved.
                _ => classification.unresolved_count += 1,
            }
        }
        // A certified root trim is the logical commit even when a
        // pre-sweep v3 control survives below it. Resolve the floor
        // once per bounded page; an unavailable floor read aborts the
        // sweep rather than risking a live-chunk delete.
        let root_trim_floor = self.trim_floor("").await?;
        for (key, chunk_keys) in by_key {
            let control_object_key = self.object_key(&key)?;
            // Non-retired keys exact-read current control once.
            let referenced: Option<HashSet<String>> =
                if Self::seq_retired_at_floor(&key, root_trim_floor).is_some() {
                    Some(HashSet::new())
                } else {
                    match self.store.download(&control_object_key).await {
                        Ok(bytes) => match ControlEnvelope::decode(&key, &bytes) {
                            Ok(ControlEnvelope::Chunked(manifest)) => Some(
                                manifest
                                    .entries
                                    .iter()
                                    .map(|entry| {
                                        chunk_object_key(
                                            &self.namespace,
                                            &key,
                                            manifest.incarnation,
                                            manifest.version,
                                            &entry.digest_hex(),
                                        )
                                    })
                                    .collect(),
                            ),
                            // Inline v2 references no chunks.
                            Ok(ControlEnvelope::Inline(_)) => Some(HashSet::new()),
                            // Corrupt control: fail closed for this key.
                            Err(_) => None,
                        },
                        // Absent control: every chunk is an orphan candidate
                        // (online this includes live candidates — quiescence
                        // is what makes them true orphans).
                        Err(ObjectStoreError::NotFound(_)) => Some(HashSet::new()),
                        Err(_) => None,
                    }
                };
            match referenced {
                Some(referenced) => {
                    for chunk_key in chunk_keys {
                        if referenced.contains(&chunk_key) {
                            classification.referenced_count += 1;
                        } else {
                            classification.orphan_candidates.push(chunk_key);
                        }
                    }
                }
                None => {
                    // Unavailable or corrupt control: fail closed —
                    // never delete what cannot be classified.
                    classification.unresolved_count += chunk_keys.len() as u64;
                }
            }
        }
        Ok(classification)
    }

    /// The quiesced chunk sweep (ADR 0004 §5.4): REQUIRES the
    /// maintenance fence and the deployment-scope operational
    /// assertion that no streamed writer, manifest-changing mutation,
    /// or open streamed reader remains for the namespace. Lists chunk
    /// objects (never manifests), exact-reads control once per key,
    /// retains exactly the current validated manifest's references,
    /// and deletes the rest in bounded, idempotent, resumable
    /// batches. Unavailable or corrupt control fails closed for that
    /// key (its chunks stay, counted in `remaining`). A stale or
    /// frozen chunk LIST hides garbage and causes a leak only —
    /// eligibility always comes from the exact control read.
    pub async fn sweep_chunks(&self) -> Result<ChunkSweepReport, KeyspaceError> {
        if !self.fence_present().await? {
            return Err(KeyspaceError::MaintenanceFenceRequired(
                self.namespace.clone(),
            ));
        }
        let mut report = ChunkSweepReport::default();
        let mut after: Option<String> = None;
        loop {
            let page = self.list_chunk_page(after.as_deref()).await?;
            if page.is_empty() {
                break;
            }
            let last = page.last().map(|(key, _)| key.clone());
            let classification = self.classify_chunk_page(&page).await?;
            report.examined += page.len() as u64;
            report.retained += classification.referenced_count;
            report.remaining += classification.unresolved_count;
            for chunk_key in &classification.orphan_candidates {
                match self.store.delete(chunk_key).await {
                    Ok(()) => report.deleted += 1,
                    Err(_) => report.remaining += 1,
                }
            }
            after = last;
            if page.len() < 1000 {
                break;
            }
        }
        Ok(report)
    }
}

#[derive(Default)]
struct PageClassification {
    referenced_count: u64,
    orphan_candidates: Vec<String>,
    unresolved_count: u64,
}

/// Delete-free online chunk metering (ADR 0004 §5.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChunkInventory {
    pub listed_chunks: u64,
    pub referenced_chunks: u64,
    /// Not referenced by a current validated manifest — includes
    /// live candidates while writers run; only quiescence makes them
    /// orphans.
    pub candidate_orphan_chunks: u64,
    pub unresolved_chunks: u64,
    pub listed_bytes: u64,
    pub candidate_orphan_bytes: u64,
}

/// The outcome of a quiesced chunk sweep: bounded, idempotent, and
/// resumable — a crash mid-sweep leaves `remaining > 0` (extra
/// objects, safe); a re-run converges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChunkSweepReport {
    /// Chunk objects the sweep considered.
    pub examined: u64,
    /// Confirmed deletes.
    pub deleted: u64,
    /// Unresolved (fail-closed) plus failed deletes — the resumable
    /// remainder.
    pub remaining: u64,
    /// Chunks retained as current validated manifest references.
    pub retained: u64,
}
