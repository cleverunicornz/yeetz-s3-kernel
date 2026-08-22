# ADR 0004: Streaming-value I/O v2 — manifest commit, batch-8 composition, and bounded chunks

Status: **ACCEPTED — 2026-08-22, via human-delegated synthesis (see Acceptance Record)**

Supersedes: ADR 0003 (proposed). ADR 0003 remains immutable; this record is
the forward-only amended successor.

## Amendment changelog — independent adversarial review

The independent review returned **SHIP WITH AMENDMENTS**. This successor keeps
ADR 0003's manifest/chunk architecture and changes the record as follows:

1. Adds the missing third lost-response outcome: a successor CAS, destroy, or
   deletion may move the key beyond the candidate before reconciliation;
   that outcome is `Unavailable`/ambiguous, never a fabricated conflict.
2. Composes the format with accepted batch 8 `delete_if_match`, makes every
   control-envelope metadata decoder v3-aware, and requires conditional—not
   read-check-unconditional—stale-era eviction.
3. Separates `CHUNK_BYTES` from `INLINE_MAX`; 16 MiB chunks remain proposed,
   while 64 MiB inline is now the recommended but human-unruled threshold.
4. States that GC quiescence is an operational assertion the kernel cannot
   prove, names the exact broken-quiescence data-loss race, adds delete-free
   online orphan metering, and specifies only a cheap new-writer fence.
5. Adds a typed 16 MiB **encoded streams-envelope** bound so streams staying
   inline is structural rather than a workload assumption.
6. Adds a lineage reserved-root guard for both `keyspace` and
   `keyspace-chunks`.
7. Requires `chunk_count >= 2`, bounds logical-key encoding expansion to 2x,
   proves the 892-byte worst-case S3 key, and states that `INLINE_MAX` measures
   encoded kernel payload bytes.
8. Renumbers the proposed A-suite from A24: A15/A16 are batch-7 canaries and
   batch 8 owns A17-A23.
9. Re-grounds the native multipart alternative against the pinned
   `aws-sdk-s3 = 1.130.0` and a new live Exoscale capability battery, not the
   obsolete claim that the pinned SDK lacks conditional completion.
10. Names yeetz's collecting existence/header probes (`object_exists` and
    `FindHeader::try_header`) as mandatory downstream migration sites.

The requested live evidence leg ran at
[ci-dev run 32592980637](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/32592980637)
against Exoscale SOS. Verdict: **PARTIAL MULTIPART WITNESS**. Correct
`CompleteMultipartUpload + If-Match` succeeded; stale `If-Match` returned
`PreconditionFailed`; `ListMultipartUploads` saw the incomplete upload;
abort removed it; completed bytes were exact; cleanup was empty. However,
`GetObject?partNumber=N` failed with `UnsupportedArgument: PartNumber is
unsupported`. This removes the conditional-publication objection but does
not supply portable part-addressed cache/integrity reads.

## Authorization and source grounds

This remains a design artifact. Acceptance authorizes a separate kernel batch
with its contract suite and proof; this PR does not authorize implementation
or migration.

Grounds:

- Kernel main `3fe2189` after accepted ADR 0001 batch 8.
- `crates/yeetz-s3-kernel/src/atomic_keyspace.rs`: v2
  `{incarnation, version, payload}`, create's incarnation recheck and current
  unconditional stale-era eviction, whole-value `get`/`get_with_etag`,
  `delete_if_match`, `destroy`, tombstones, counters, and certified trim.
- `atomic_keyspace.rs:800-847`: `delete_if_match` conditionally deletes the
  value object and enriches conflicts by decoding v2 at the current
  `ValueEnvelope::decode` call. A v3-unaware decoder would silently degrade
  observed incarnation/version to `None`.
- `atomic_keyspace.rs:895-930`: `destroy` obtains its generation through
  `get_with_version`; that internal control read must understand v3 without
  collecting chunks.
- ADR 0001 batch 8: conditional delete is the raw delete-side dual of CAS,
  fails closed when the backend lacks the wire primitive, and explicitly
  names stale-era eviction/destroy composition as the following hardening
  site. Its wire contracts run on loopback because plain in-memory
  `object_store` cannot model `DELETE + If-Match`.
- `crates/yeetz-s3-kernel/src/state_kernel.rs:is_valid_lineage`: any valid
  identifier is currently accepted, including the physical root `keyspace`.
  This is a pre-existing collision hole; v2 must not add a second one.
- `crates/yeetz-sdk-s3/src/store.rs`: an endpoint-configured AWS client is
  already pinned and used for explicit multipart and conditional delete.
  `aws-sdk-s3 = 1.130.0` exposes `if_match` and `if_none_match` on
  `CompleteMultipartUpload`.
- `crates/yeetz-s3-streams/src/envelope.rs`: payload bytes are base64 in JSON
  and no event/config size bound exists. Base64 expansion is approximately
  4/3; a raw payload near 12 MiB can cross a 16 MiB encoded envelope.
- Yeetz main `ecb1713`, `crates/yeetz-git-store/src/lib.rs`:
  `object_exists()` calls `AtomicKeyspace::get`, and
  `FindHeader::try_header()` calls `read_object`; both collect the full value.

## Decision summary

The recommended design remains a dual representation behind one logical
`AtomicKeyspace` value:

1. **Inline v2:** new encoded payloads at or below `INLINE_MAX` retain the
   existing v2 envelope and one-object request profile. Existing v2 objects
   remain readable regardless of size.
2. **Chunked v3:** larger encoded payloads use a v3 manifest at the existing
   logical key plus immutable, fixed 16 MiB SHA-256-addressed chunks in a
   separate kernel-private root.
3. **CAS unit:** the inline envelope or v3 manifest at the logical key is the
   only CAS/delete unit. Its store etag remains the opaque mutation token.
   The successful conditional manifest PUT is the commit point.
4. **Era state:** `incarnation` and `version` live in the control envelope,
   never in chunks. Partial uploads do not alter existence, tombstones,
   versions, or incarnation counters.
5. **Read path:** fetch/validate the whole control first, then verified full
   chunks. Logical ranges select complete chunks; v1 uses no backend Range GET
   within a chunk.
6. **API:** additive async reader/writer handles; existing whole-value methods
   remain and collect chunked values when explicitly called.
7. **GC:** physical chunk deletion is quiesced-only in this proposal.
   Certified trim/destroy remain logical boundaries. Online inspection is
   delete-free.
8. **Small-value closure:** streams enforce a typed encoded-envelope bound;
   lineages cannot occupy either keyspace physical root.

## 1. Representation, canonicality, and physical isolation

### 1.1 Two independent size decisions

`CHUNK_BYTES` and `INLINE_MAX` are not the same decision:

- **`CHUNK_BYTES = 16 MiB` remains recommended.** It matches cachey's
  per-value/page ceiling, bounds per-request verification, and gives one
  stable content-addressed unit.
- **`INLINE_MAX = 64 MiB` is the recommended human ruling, not yet settled.**
  It preserves the current 16–64 MiB encoded band as one PUT/GET and has the
  same peak payload memory as four in-flight 16 MiB chunks. The alternative
  is ADR 0003's 16 MiB threshold, which introduces 2–5 object requests for
  that band without caller choice.

`INLINE_MAX` measures the opaque **encoded payload passed to
`AtomicKeyspace`**, not a consumer's decompressed or source size. A zstd-at-
rest Git object, a base64 streams envelope, and an OCI blob own different
source-to-encoded ratios. Consumer-level inflation and source-size limits
remain above the kernel.

Existing v2 envelopes stay valid even if their payload exceeds the selected
new-write threshold. No eager rewrite.

### 1.2 Canonical v3 manifest

The logical key's control object uses a canonical binary envelope:

```text
magic = "yeetz-keyspace-value/v3\0"
incarnation: u64
version: u64
kind = chunked-v1
commit_id: [u8; 16]
logical_len: u64
value_root_sha256: [u8; 32]
chunk_bytes: u32                 // exactly 16 MiB
chunk_count: u32                 // 2..=65,536
chunks[chunk_count]: {
    encoded_len: u32,
    sha256: [u8; 32],
}
```

`value_root_sha256` is
`SHA-256(domain || logical_len || chunk_bytes || chunk_count || ordered
(encoded_len, chunk_sha256) entries)`. It commits to boundaries, order, and
every chunk while permitting a range reader to validate the table without
fetching unrelated chunks.

Canonicality:

- `chunk_count >= 2`; one-chunk v3 is rejected because inline is canonical.
- Every non-final chunk is exactly 16 MiB; final is `1..=16 MiB`.
- `logical_len`, count, entry lengths, and manifest length must agree before
  count-derived allocation.
- Empty values are inline.
- Bad root, unsupported flags, non-canonical fields, oversized manifest,
  count overflow, and length disagreement are integrity failures.
- `commit_id` is writer-minted and retained only across retries of the same
  `PendingValue`; it is not logical content identity.

### 1.3 Chunk path and key-length proof

Conceptual private layout:

```text
keyspace-chunks/v1/<namespace>/<hex(logical-key)>/
    <candidate-incarnation:020>/<candidate-version:020>/<chunk-sha256>
```

The logical key is UTF-8/ASCII bytes encoded as lowercase hex: reversible,
exactly 2x expansion, never percent encoding's 3x worst case. With the
existing 255-byte namespace and 255-byte logical-key limits, the longest
v1 physical chunk key is:

```text
19 root/version + 255 namespace + 1 slash + 510 encoded key +
1 slash + 20 incarnation + 1 slash + 20 version + 1 slash + 64 digest
= 892 bytes
```

That remains below S3's 1,024-byte key limit. The format decoder refuses any
other key encoding.

Chunks are immutable put-if-absent objects scoped by namespace, logical key,
and candidate generation. A conflict is accepted only after full length and
SHA-256 verification. Identical contenders for one successor generation may
share chunks. Cross-key storage deduplication is deliberately absent; a read
cache may still share verified bytes by digest.

`AtomicKeyspace::list_after` continues to expose only `keyspace/...` logical
keys, never `keyspace-chunks/...` internals.

### 1.4 Reserved-root guard

`KernelLineage::new` must reject any lineage whose first component is
`keyspace` or `keyspace-chunks`, including the exact roots and descendants.
`keyspace-x` remains valid: segment equality, not substring matching. This
closes the latent current collision and protects the new root structurally;
application convention is insufficient.

## 2. Commit, CAS, incarnations, and batch-8 delete

### 2.1 Commit point and concurrency

For create, final control publication uses `If-None-Match: *`. For CAS, it
uses `If-Match: expected_etag`. Before success, only unreachable immutable
chunks may exist and the old logical state remains authoritative. After
success, every referenced chunk exists and verifies. This one control PUT is
when the streamed value **lands**.

Two writers may upload concurrently. They bind the same target generation
but mint distinct commit IDs. One create/CAS manifest wins; losers receive
`AlreadyExists`/`PreconditionFailed`. Chunk presence never decides a winner.

Inline and chunked values share one sequence:

- create → current incarnation, version 0;
- CAS → same incarnation, checked successor version;
- destroy → tombstone for the observed incarnation/version, then incarnation
  advance and control deletion;
- re-create → new incarnation, version 0.

Partial upload never advances either counter.

### 2.2 Complete lost-response oracle

After a chunked manifest PUT returns an ambiguous transport error, an exact
control/state reread has three—not two—legal adjudications:

| Reread | Outcome |
|---|---|
| Exactly the bound target incarnation/version and same `commit_id` | This writer landed: success. |
| Exactly the bound target incarnation/version and foreign `commit_id` | This writer lost: typed create/CAS conflict. |
| Any generation beyond the bound target, a higher incarnation, `Destroyed`, `Absent`, or logically retired `OffsetExpired` | `Unavailable` with an explicit ambiguous-write operation. The candidate may have landed and then been superseded/deleted, or may never have landed. Never invent success or conflict. |

Malformed current control remains integrity failure. Inline v2 retains its
existing ambiguous-write behavior because it has no commit ID. No outcome is
inferred from candidate chunks.

The third row is load-bearing: PUT applied + response lost + successor CAS or
destroy before reread must not be mislabeled as a lost initial CAS.

### 2.3 `delete_if_match` on v3

Accepted batch 8 composes directly:

- the token guards the control manifest;
- match deletes only that manifest;
- chunks become unreachable garbage until the chunk sweep;
- no tombstone is written and no incarnation is bumped, preserving batch 8's
  raw-delete layering;
- absent retains batch 8's all-`None` `PreconditionFailed` shape.

Every control metadata path must use one v2/v3-aware decoder. In particular:

- `delete_if_match` conflict enrichment must decode v3 incarnation/version
  instead of degrading to `None` at today's
  `atomic_keyspace.rs:820-827` site;
- `compare_exchange` conflict enrichment must do the same; and
- `destroy` must replace its payload-shaped `get_with_version` dependency with
  a control-metadata read that obtains incarnation/version/etag from v2 or v3
  without loading chunks.

A missing/corrupt referenced chunk does not prevent conditional control
deletion when the caller holds the etag; deletion is about the observed
control era. Deliberate witnessed deletion still uses `destroy`.

### 2.4 Stale-era eviction must be conditional

Create's incarnation post-check remains mandatory. If the counter moved after
manifest publication, the writer rereads control **with etag** and confirms
its own exact commit ID/era. It may evict only with
`delete_if_match(observed_etag)`.

Unconditional delete after a read-check is forbidden. Counterexample:

1. stale create publishes candidate A;
2. it reads A and decides A is its stale bytes;
3. a fresh confirmed value B replaces/recreates the key;
4. stale cleanup unconditionally deletes the key.

That silently destroys B. Conditional delete with A's observed etag rejects
at step 4 and B survives. This amendment also requires the existing inline
stale-era eviction at `atomic_keyspace.rs:503-507` to adopt the batch-8
primitive in the implementation batch.

`delete_conditional` fails closed on plain in-memory `object_store`; there is
no read-delete fallback. The loopback's real wire arm is therefore the
contract surface for this race, matching batch 8.

## 3. Read path, algebra, ranges, and caching

### 3.1 Async surface

```text
AtomicKeyspace::open_stream(key)
    -> Result<Option<ValueReader>, KeyspaceError>
AtomicKeyspace::open_stream_range(key, Range<u64>)
    -> Result<Option<ValueReader>, KeyspaceError>
AtomicKeyspace::read_state_stream(key)
    -> Result<StreamKeyState, KeyspaceError>

AtomicKeyspace::begin_stream_create(key)
    -> Result<ValueWriter, KeyspaceError>
AtomicKeyspace::begin_stream_compare_exchange(key, expected_etag)
    -> Result<ValueWriter, KeyspaceError>

ValueWriter: tokio::io::AsyncWrite
ValueWriter::seal(self) -> Result<PendingValue, KeyspaceError>
PendingValue::commit(self) -> Result<CommitReceipt, KeyspaceError>
```

All transitions are async. `seal` uploads/verifies the final chunk and builds
the manifest without publishing it, allowing a caller to finalize an
independent digest before `commit`. The key is known at begin; key-late input
uses a replayable source or bounded ephemeral spool in v1.

Existing `create`, `compare_exchange`, `get`, `get_with_etag`, and
`read_state` remain. Whole writes above the selected threshold use the chunk
writer; whole reads explicitly collect and therefore may allocate to logical
length.

### 3.2 Manifest-first reads

`open_stream` reads the control once with etag:

- v2: validate and yield inline payload, no chunk request;
- v3: validate manifest/root, then fetch ordered chunks with at most four in
  flight; verify a full chunk before yielding its bytes.

The reader is a snapshot of the immutable references in the observed
manifest. A later CAS/destroy does not retarget it. Existing whole methods
buffer until complete success and retain all-or-error behavior; a streaming
consumer may receive a verified prefix before a later error and must commit
side effects only after EOF.

A logical half-open range validates the whole ordered table, fetches only
intersecting complete chunks, verifies each, and slices boundary chunks. v1
does not use backend Range GET inside chunks: partial bytes cannot verify the
manifest's full-chunk SHA-256. Boundary overfetch is less than 32 MiB.

### 3.3 State algebra

```text
StreamKeyState =
    Present { reader: ValueReader, metadata: ValueMetadata }
  | Destroyed { tombstone: Tombstone }
  | OffsetExpired { first_retained: u64 }
  | Absent
```

No fifth logical state represents partial uploads. Physical chunks alone are
invisible. Missing/truncated/mismatched referenced chunks are integrity,
never absence. Destroyed/expired/absent states fetch no chunks.

`ValueMetadata` exposes logical length, opaque control etag, optional v3 root,
and representation kind. It exposes neither versions/incarnations nor
physical paths. `VerifiedChunk` exposes an opaque digest cache identity, not
a storage capability.

### 3.4 Terminal reads and streams structural bound

`StateKernel::read_terminal_record` and `LineageHeadState` do not change;
lineage control records remain whole and terminal reads remain two GETs.

`yeetz-s3-streams` currently has no event/config size bound and base64-expands
payloads. To make the no-small-value-regression claim structural, every
encoded streams `Envelope` written by create, append, or migration must be at
most **16 MiB**, independent of the human's `INLINE_MAX` choice. Enforcement
occurs after canonical JSON/base64 encoding and before the first keyspace
effect. Oversize is typed, for example:

```text
StreamsError::EnvelopeTooLarge {
    encoded_len: u64,
    max_encoded_len: 16 * 1024 * 1024,
}
```

A raw payload around 12 MiB is only an estimate; metadata and base64 padding
make the exact encoded check authoritative. Genesis/config is covered too.
Tail hints and cursors remain structurally small. S11 proves every streams
write remains v2 inline and produces zero chunk-root requests.

### 3.5 Cachey

The kernel does not depend on cachey. Yeetz retains whole-preimage caching
for decompressed Git preimages at or below its 16 MiB admission cap. For a
larger preimage represented by v3, cache entries are verified encoded chunks
keyed by chunk digest; each fits cachey's value codec. Hits rehash against
the manifest entry. Cache misses call the kernel reader. Git OID verification
still runs through decompressed EOF before Git-level success.

A highly compressible large Git preimage whose encoded kernel payload stays
inline bypasses large-preimage cache admission in v1; a safe pre-control-GET
cache key would need representation identity.

## 4. Write ordering and crash states

Ordering:

1. Read/bind absence or expected control etag and successor era.
2. Check the cheap maintenance fence for a streamed begin.
3. Consume input into canonical 16 MiB chunks; hash and put-if-absent.
4. Seal final chunk and manifest; caller may validate an independent digest.
5. Publish the control conditionally.
6. Reconcile response and, for create, recheck incarnation.

| Cut | Logical state | Durable garbage/recovery |
|---|---|---|
| Before chunks | Old state | None. |
| Between chunks | Old state | Candidate chunks; retry may reuse verified matches. |
| All chunks, before manifest | Old state | Complete unreachable candidate. |
| Manifest precondition rejects | Winner/old state | Loser chunks; typed conflict. |
| Manifest applies, response lost | New state, perhaps later superseded | Use the three-row oracle in §2.2. |
| Manifest succeeds | New complete value | All references verified before commit. |
| Referenced chunk later missing/corrupt | Present but damaged | Integrity error; never hide through GC/absence. |
| Conditional delete/destroy | Absent/Destroyed as batch 8/6 define | Chunks await sweep. |

A lost chunk PUT response is reconciled by exact GET + full digest. A wrong
object under the content address is integrity failure. Chunk upload order is
not correctness-bearing; bounded ordinal order gives simpler backpressure and
fault traces.

## 5. GC contract — honest boundary

### 5.1 Quiescence is external

Quiescence is a **deployment-scope operational assertion**: no streamed
writer, no manifest-changing mutation, and no open streamed reader for the
namespace. The kernel cannot infer or prove that all processes drained.
Passing a boolean or acquiring a local guard is not evidence.

A cheap maintenance fence is still recommended:

- namespace state is CAS'd to `fenced` before operators drain;
- every new streamed begin performs one exact GET and refuses while fenced;
- the sweep requires the fence;
- release is another CAS.

This costs one GET per streamed begin and blocks new work. It does **not**
prove that a writer/read handle opened before the fence has drained, nor that
all deployments observed the fence. The operational quiescence assertion
remains load-bearing.

### 5.2 Broken-quiescence blast radius

The exact failure is not merely leaked garbage:

1. writer uploads and verifies candidate chunks;
2. sweep, falsely believing the namespace quiescent, sees no current manifest
   reference and deletes those chunks;
3. writer's conditional manifest PUT succeeds;
4. the committed manifest names absent chunks.

This is the forbidden state P8/A33 exists to exclude. Therefore no physical
delete API may advertise online safety under this design. The proof rig must
include this violated-precondition interleaving as a demonstration cut; it is
expected to produce/detect `ManifestIncomplete`, documenting why the
precondition is non-negotiable.

### 5.3 Delete-free online metering

Online operation gets a read-only inventory API, conceptually:

```text
ChunkInventory {
    listed_chunks,
    referenced_chunks,
    candidate_orphan_chunks,
    unresolved_chunks,
    listed_bytes,
    candidate_orphan_bytes,
}
```

It lists private chunks, derives their logical key/generation, exact-reads
current control, and classifies. It deletes nothing and labels concurrent
candidates as **candidate**, not proven orphan. Unavailable/corrupt control
increments unresolved. This is safe for telemetry and deciding when a
quiesced sweep is worth scheduling.

### 5.4 Quiesced sweep and trim

Under the external assertion and fence:

- list chunk objects, never manifests;
- recover logical key/generation from the bounded private path;
- exact-read control once per key;
- retain exactly current validated manifest references;
- inline/Destroyed/OffsetExpired/Absent retain no chunks;
- unavailable/corrupt control fails closed for that key;
- delete unreferenced chunks in bounded, idempotent, resumable batches.

A stale/frozen chunk LIST hides garbage and causes a leak only; eligibility
comes from exact control. Certified trim remains the logical commit:
`delete_below` may remove a control under a covering certificate, while the
later quiesced sweep reclaims chunks. Destroy behaves analogously. No guessed
TTL or reader lease appears in v1.

## 6. Bounds and ceiling

| Bound | Proposed value | Status/effect |
|---|---:|---|
| `CHUNK_BYTES` | 16 MiB | Recommended; cache/integrity unit. |
| `INLINE_MAX` | 64 MiB | **Recommended human ruling**, alternative 16 MiB. Applies to encoded payload. |
| `MIN_CHUNKS` | 2 | Canonical v3 floor. |
| `MAX_CHUNKS` | 65,536 | Bounded manifest; ordinals `0..=u16::MAX`. |
| Maximum logical encoded value | 1 TiB | 16 MiB × 65,536. |
| Maximum encoded manifest | 4 MiB | Current entry table is about 2.25 MiB at maximum count. |
| Maximum in-flight chunks | 4 | 64 MiB payload window plus codec/manifest overhead. |
| Logical-key path encoding | 2x | 892-byte physical-key worst case. |
| Streams encoded envelope | 16 MiB | Typed structural bound; always inline under either threshold ruling. |

Thus the existing Git 64 MiB source-object policy does not become a kernel
stream ceiling. The recommended inline threshold merely preserves the same
peak payload memory and avoids request regressions in that encoded band.
Whole APIs can still allocate to the 1 TiB format bound because collection is
explicit; untrusted-size code must use streaming or bounded collect.

## 7. Costs, breakage, and sole-consumer migration

### 7.1 Request/latency cost

Small inline create/get remain one request; CAS retains its current control
read + conditional PUT. At the selected threshold, no chunk-root request or
hash exists.

For `N` chunks:

- write: up to `N` conditional chunk PUTs + one manifest PUT;
- identical chunk conflict: full GET/hash before accepting the existing
  object, up to `N` additional reads;
- read: one manifest GET + `N` chunk GETs;
- first byte waits for manifest plus a full first-chunk transfer/hash;
- range overfetch is below two chunks;
- CAS loser may upload the whole successor before final conflict;
- no cross-key storage dedup; garbage persists until quiesced sweep.

The maintenance fence adds one GET to streamed begin. Delete-free metering
adds bounded LIST/control reads but no write-path latency.

### 7.2 API/format breakage

- Existing v2 values and whole signatures remain.
- New inputs over 1 TiB are typed bound failures.
- New errors cover manifest/chunk integrity, bounds, maintenance fencing,
  stale incarnation, ambiguous commit reconciliation, and streams oversize.
- `KeyspaceError` match sites in yeetz must migrate; the crate is 0.x and sole
  consumer.
- In-memory/loopback gain v3 roots/fault cuts. Conditional-delete races run on
  loopback; no unconditional in-memory fallback.
- Physical roots remain private; boundary gates must include
  `keyspace-chunks` and lineage-root construction.

### 7.3 Yeetz migration audit

No stored Git-object rewrite: keyspace v2 and `YGO1` remain readable.
Required downstream work:

1. Incremental `YGO1` zstd encode/decode for large paths; hash decompressed
   canonical loose preimage through EOF.
2. Known-ID writes stream into `ValueWriter`, finalize OID before commit.
   Unknown-ID `gix_object::Write::write_stream` uses bounded ephemeral spool,
   derives OID, then streams once to the key-known writer unless ruling 6
   chooses a prepared-upload protocol.
3. `read_object` gains a stream path; whole gitoxide adapters retain an
   explicit whole-object guard until redesigned.
4. **`GitObjectStore::object_exists()` must stop calling
   `AtomicKeyspace::get`.** Today a boolean existence probe collects and
   decompresses the full value. It must use control/state metadata without
   opening/collecting chunks. Missing this turns a 1 TiB format allowance
   into an OOM grenade on a boolean query.
5. **`gix_object::FindHeader::try_header()` must stop calling
   `read_object`.** It currently collects and verifies the whole preimage to
   return kind/size. Migration must stream with bounded memory (possibly
   consuming through EOF to retain full OID verification) or add a separately
   verified application header format. Merely switching kernel storage does
   not fix it.
6. `gix_object::Find`, current `pack_generate`, and other slice-shaped paths
   still materialize values. Human ruling 5 decides whether the 64 MiB whole-
   adapter guard remains or an end-to-end Git pack/read redesign is in scope.
7. Cache small verified preimages as today; cache large encoded chunks by
   digest; keep Git OID verification independent.
8. Extend Git object/loopback rigs for streaming zstd, known-ID rejection
   before manifest commit, chunk cache equivalence, late integrity failure,
   existence/header boundedness, and no raw adapter access.

LFS/OCI-style consumers can stream end-to-end without waiting for gitoxide's
slice interfaces.

## 8. Alternatives, re-grounded

### A. Native multipart object at the logical key

**Benefits:** one stored data object, one streaming GET, native ranges, no
chunk reachability graph, and provider-managed multipart staging. The pinned
SDK surface is not a blocker: `aws-sdk-s3 = 1.130.0` exposes conditional
completion, and batch 8 already established direct use of the endpoint AWS
client below the object-store abstraction.

**Live result:** run
[32592980637](https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/32592980637)
proved Exoscale accepts current-etag conditional completion, rejects a stale
etag, lists incomplete uploads, aborts them, preserves exact completed bytes,
and cleans up. It also proved `GetObject?partNumber=N` is unsupported.

**Why manifests remain recommended after this partial witness:**

- native multipart does not portably expose the original 16 MiB parts on this
  backend, so cache-aligned part reads fail;
- ordinary Range GET does not provide the manifest's full-part SHA-256
  identity; retaining per-chunk integrity needs extra metadata/sidecars and
  boundary rules;
- a production incomplete-MPU reconciler/lifecycle policy is still a new
  operational subsystem even though list+abort mechanics are witnessed; and
- provider portability of these newer conditional-completion headers remains
  unproven beyond Exoscale.

This is now a real tradeoff, not an API impossibility. A full witness including
portable part-addressed/integrity reads would remove the manifest design's
largest permanent liability—the chunk-GC subsystem. The measured partial
witness supports retaining manifests for v1, subject to human ruling 1.

### B. Mutable numbered chunks discovered through LIST

Rejected: no atomic multi-object replacement. Publishing length before
chunks exposes partial data; after chunks requires generations plus a commit
record—the selected manifest under another name. LIST completeness would
become correctness-bearing and each mutable chunk reopens ABA.

### C. Temporary whole object then conditional copy

Rejected: doubles durable bandwidth, still needs destination-conditional copy
portability and lost-copy reconciliation, and adds staging lifecycle. If
native conditional multipart wins ruling 1 it is strictly better.

### D. Global content-addressed chunks with reference counts

Rejected for v1: per-chunk CAS counters require crash-safe ordering across
increment, manifest publish, decrement, and deletion. Over-count leaks;
under-count deletes live data; zero-count/delete races need another tombstone
protocol. Per-key generation scoping spends storage for boring exact GC.

## 9. Proof plan — assigned ledger starts at A24

Existing A1-A23, I1-I6, W1-W5, R1-R9, G118, and S1-S10 rerun unchanged.
A15/A16 remain the batch-7 create/destroy and non-v2 canaries; A17-A23 remain
batch 8 conditional delete.

| Promise | Witness | Independent oracle / rig attack |
|---|---|---|
| P1. Candidate chunks are invisible; manifest is the only commit. | **A24** manifest-only visibility; **A25** whole/stream byte equivalence across inline↔chunked transitions. | Pure logical map permits only old/new bytes; cut every chunk/control PUT. |
| P2. Create/CAS has one winner and strict successor era. | **A26** concurrent distinct/identical commit-ID matrix. | Model `(incarnation, version, value, commit_id)`; one writer reports target publication. |
| P3. Destroy/recreate and stale cleanup preserve fresh values. | **A27** chunked incarnation race plus stale-era `delete_if_match` interleaving. | Land stale A, replace with B between check/delete, prove stale token cannot delete B; raw unconditional version demonstrates the defect signature. |
| P4. Full/range reads are exact or integrity errors. | **A28** range boundary table; **A29** missing/truncated/swapped chunk and bad-root taxonomy. | Slice generated bytes; mutate every physical position; never convert damage to absence. |
| P5. State and batch-8 deletion compose with v3. | **A30** Present/Destroyed/Expired/Absent parity, v3 `delete_if_match`, v3 conflict enrichment, and v3 destroy metadata. | State-machine oracle; successful conditional delete removes control only, mismatch names v3 era, chunks are garbage not logical state. |
| P6. Small paths stay one-object structurally. | **A31** inline request counts at selected `INLINE_MAX`; **S11** exact 16 MiB encoded-envelope bound and zero chunk-root requests for create/append/migration. | Loopback request log; values in 16–64 MiB encoded band exercise the human threshold ruling. |
| P7. Bounds and physical paths are safe. | **A32** decoder allocation table, `chunk_count` 0/1 rejection, max-count manifest, 2x key encoding and 892-byte maximum. | Allocation/request counters; property-generated 255-byte namespaces/keys; no key >1,024 bytes. |
| P8. Lost responses never fabricate an outcome or publish an incomplete value. | **A33** same-ID success, exact-target foreign-ID conflict, and successor/destroy/absent ambiguous `Unavailable`; crash after every storage request. | Sequential oracle permits old/new/superseded but never conflict inference beyond target and never manifest→missing chunk. |
| P9. GC is safe only under its explicit precondition. | **A34** quiesced idempotent/resumable sweep, unavailable-control fail-closed, delete-free online meter, and broken-quiescence demonstration cut. | Freeze chunk LIST (leak only); race pre-fence writer so sweep deletes candidate then manifest lands, and require the rig to detect the forbidden `ManifestIncomplete` signature. |
| P10. Physical roots cannot collide. | **A35** lineage rejects exact/descendant `keyspace` and `keyspace-chunks`, accepts near-substrings. | Construct both kernel surfaces and prove no accepted lineage can name either root. |
| P11. Backend alternative evidence is measured, not assumed. | Real-S3 run 32592980637; future reruns remain in `real-s3`. | Exact concatenation, correct/stale conditional completion, abort/list visibility, part reads, and empty cleanup each emit separate verdict rows. |

The durable loopback rig gains chunk create/read/delete, manifest create/CAS/
read, v3 conditional delete, corruption/truncation, successor-before-reread,
maintenance fence, orphan meter, frozen LIST, and restart cuts. Its logical
oracle ignores garbage except after successful quiesced sweep, where
`live == retained` is required.

The broken-quiescence leg is a demonstration of an explicitly violated
precondition, not a claim that the kernel can prevent deployment misconduct.
It must remain in the rig so future reviewers see the concrete blast radius.

## 10. Human rulings still required

1. **Manifest vs native conditional multipart.** The Exoscale battery is
   partial: atomic completion and cleanup mechanics pass; part-addressed
   reads fail. Approve manifests for portable chunk integrity/cache alignment
   or choose single-object multipart and accept/design the missing pieces.
2. **Inline threshold.** Approve recommended `INLINE_MAX = 64 MiB` or choose
   16 MiB. `CHUNK_BYTES = 16 MiB` is a separate recommendation.
3. **GC availability.** Approve quiesced-only deletion with an operational
   assertion and cheap fence. If online deletion is required, stop for a
   durable reader/writer lease or epoch protocol; no guessed TTL.
4. **Cross-key storage deduplication.** Approve per-key generations or fund
   crash-safe global reference accounting. Cache dedup is independent.
5. **Large ordinary Git objects.** Keep a 64 MiB guard on collecting
   gitoxide/pack paths, or include their end-to-end streaming redesign before
   removing user-facing LFS guidance.
6. **Key-late writes.** Approve bounded ephemeral spool or require a durable
   prepared-upload protocol with separate lifecycle/GC proof.

No implementation batch starts until these rulings are recorded in an
accepted successor/addendum. This proposed amended record remains immutable.

## Acceptance Record — 2026-08-22

Basis: the human delegated adjudication of this ADR to the synthesis
process ("adopt the best solution derivable from both reviewers;
reserve only physics-class forks for me" — standing directive,
2026-08-22). Both reviewers converged; no fork rose to the physics
bar. The design-review verdict chain: SHIP WITH AMENDMENTS (first
adversarial pass) → this successor → SHIP (re-check, 10/10 amendment
fidelity, evidence verified). The live-Exoscale stance on part-addressed
reads (measured absent on SOS; adopt the portable floor) was
additionally and explicitly human-ratified in conversation.

### Rulings (synthesis-adopted)

1. **Representation: manifest/chunks.** Evidence-settled by the live
   probe (run 32592980637): conditional completion supported, part-
   addressed reads measured absent on Exoscale SOS. Portable-correctness
   floor stands; any backend-specific fast path is a future,
   capability-probed, feature-flagged addition.
2. **Bounds: `CHUNK_BYTES` = 16 MiB, `INLINE_MAX` = 64 MiB.** Both
   reviewers' recommendation: preserves the 16–64 MiB band's one-object
   profile at identical peak memory to the 4-chunk prefetch window.
   With the canonicality floor (`chunk_count ≥ 2`), 2× key encoding,
   892 B max key, 1 TiB logical, 4 MiB manifest, 4 chunks in flight.
3. **Chunk GC: quiesced-only v1**, with the enforceability caveat,
   writer-race blast radius, delete-free orphan metering, and the
   not-drain-proof writer fence as specced in §5.
4. **Chunk scoping: per-key.** Rejection of global refcounted dedup
   stands.
5. **Git whole-read paths: 64 MiB adapter guard retained.** The
   `object_exists()` / `FindHeader::try_header()` anti-OOM migration
   sites are mandatory for the downstream adoption batch.
6. **Key-late writes: bounded ephemeral spool** under the same 64 MiB
   guard; durable prepared-upload remains rejected.

### Acceptance footnotes (from the re-check)

- **N1.** §2.2 row 2's "definite loss" is provable for CAS (an
  If-Match etag is consumable exactly once); for **create**, a raw
  delete/re-create inside the lost-response window can mislabel an
  applied-then-raw-deleted create as never-landed — attribution-only,
  inherited from batch 8's documented raw-delete discipline. A33 should
  add the raw-delete-window leg.
- **N2.** The §5.1 fence object must live at a kernel-reserved key
  (like `trims/`) — caller-writable locations are not fences. The
  implementation batch's contract text states the reserved key.

Implementation is authorized as its own firewalled kernel batch,
carrying these rulings and footnotes as binding contract inputs.
