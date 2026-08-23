# ADR 0005: Unconditional S3 multi-object deletion — typed partial outcomes and bounded chunks

Status: **PROPOSED — design only; no implementation is authorized by this record**

Date: 2026-08-23

## Authorization and acceptance boundary

This proposal is the human-directed design artifact for a first-class batched
deletion primitive in `yeetz-s3-kernel`. The authorization covers this ADR,
its API sketch, and its proof plan. It does not authorize an implementation,
a dependency-pin change, a downstream migration, or publication. If accepted,
implementation remains one separately authorized kernel-extension batch: the
primitive, its contract suite, and its CI witness, with no downstream repair
mixed into that batch.

An independent adversarial review follows this PR. Amendments requested before
merge land as forward commits on the proposal branch. Once accepted and merged,
the decision is append-only; a later change supersedes it rather than editing
its history.

## Verified source ground

This proposal starts from the repository and published boundary, not from the
intended shape of the old API:

- `yeetz-s3-kernel 0.4.0` already publishes
  `AtomicKeyspace::delete_many(&[&str]) -> Result<Vec<DeleteOutcome>,
  KeyspaceError>` and `DeleteOutcome { key: String, deleted: bool }`.
  `delete_many` validates the whole input, then loops over `self.delete(key)`.
  On S3 each `self.delete` supplies a one-item stream to
  `object_store::delete_stream`, so `N` keys produce `N` one-key DeleteObjects
  POSTs. The boolean result exposes neither provider code nor failure class.
- Main at `aaaed58` declares workspace version `0.4.1`; the newest release tag
  is `v0.4.0`. No existing public method, type, variant, or behavior may be
  repurposed to carry this design.
- The pinned `object_store = 0.13.2` S3 adapter can issue native DeleteObjects
  requests in groups of 1,000, but only when one `delete_stream` receives a
  multi-item stream. `ObjectStoreClient::delete` constructs a one-item stream,
  so the current call chain never realizes that batching.
- The same adapter buffers up to 20 chunks and flattens a whole-request error
  into a stream error. Its response parser initially assumes every requested
  path succeeded and then overlays returned `<Error>` entries. That is useful
  adapter behavior, but it is not a sufficient assurance boundary for this
  API: the kernel must never infer success from a missing response member.
- ADR 0001 batch 2 already made the loopback parse multi-key `POST ?delete`
  bodies and emit verbose mixed `<Deleted>`/`<Error>` results
  (`state_kernel.rs:3320-3440`). This proposal extends that one model; it does
  not introduce a second bulk-delete wire truth.
- `yeetz-sdk-s3` already carries the pinned `aws-sdk-s3 = 1.130.0` client for
  explicit multipart operations and `delete_conditional`. That client exposes
  DeleteObjects and its separate `deleted` and `errors` collections, so no new
  dependency or dependency bump is required.
- ADR 0001 batch 8 fixes the layering precedent: raw `delete` and
  `delete_if_match` operate below `destroy`, tombstones, and incarnation
  counters. A raw delete writes no existence witness and advances no deletion
  era. The new primitive has that same layer and side-effect profile.
- ADR 0004's A-series ledger ends at A35. This proposal starts at A36.

The named motivating consumer is wholesale deletion of a sealed repository
epoch in the yeetz forge. An archived epoch is immutable and write-refused
before deletion, so unconditional removal of every supplied object is the
correct operation. The kernel API does not import epoch types or attempt to
prove that policy. It is a general raw keyspace primitive. Certified-trim
physical sweeps and chunk GC are possible later consumers through the same
kernel-closure provider primitive; neither migration is part of this proposal
or its implementation batch.

The governing yeetz epoch-pivot ruling
(`docs/architecture/epoch-pivot-ruling.md`) explicitly says its scope is yeetz
only, kernel 0.4.0 stays untouched, and no kernel work is needed or authorized
for that feature. ADR 0005 has independent human design authorization. A sealed
epoch motivates the safe unconditional shape and request economics; it does
not require this primitive, authorize epoch adoption, or couple the kernel to
the epoch plan.

The epoch review also found that deletion completeness cannot be inferred from
LIST. This primitive does not discover objects and does not make LIST an
authority: it deletes exactly the caller-supplied keys. A consumer still needs
its own witnessed inventory and durable job state.

## Proposal summary

1. Add `AtomicKeyspace::delete_objects`, named after S3 DeleteObjects. It is a
   new API beside the published `delete_many`; neither wraps nor replaces the
   other.
2. Validate the entire input before the first effect. Accepted keys are unique,
   namespace-relative, and outside every kernel-reserved state prefix.
3. Process accepted keys in input order as sequential chunks of at most 1,000
   physical keys. One logical chunk is one verbose DeleteObjects operation.
4. Return one outcome per accepted input key, in input order. `Ok(())` means a
   valid provider response explicitly confirmed that key deleted. Every other
   wire outcome is a typed per-key failure. No store failure becomes a
   wholesale batch error.
5. A valid response may contain both successes and failures. Record both and
   continue. Partial success is ordinary, not rollback-worthy.
6. If a chunk has no complete trustworthy response, mark every key in that
   chunk `Unconfirmed`, mark the untouched tail `NotAttempted`, and stop. Never
   fabricate a success from request ambiguity or malformed response.
7. Every `Err` is unresolved remainder, not an instruction to retry forever.
   Callers checkpoint it, apply a bounded retry policy to transient or
   ambiguous failures, and surface terminal failures such as unsupported
   backends or permanent provider rejection.
8. This is transport batching, never a transaction. There is no cross-key
   atomicity, compensation, or rollback. This boundary is permanent.
9. This is unconditional deletion only. S3 multi-object deletion supplies no
   portable per-object `If-Match` contract used by this kernel. There is no
   batched conditional API or read-check-delete fallback. `delete_if_match`
   remains one key per conditional request, permanently.
10. The operation deletes only logical keyspace control objects. It writes no
    tombstone, does not read or bump an incarnation counter, does not touch trim
    certificates or maintenance fences, and does not reclaim v3 chunks.

## Naming collision and additive coexistence

`delete_many` cannot acquire the new wire or result semantics under its old
name. Request cardinality, failure attribution, output type, and fault-cut
behavior are observable parts of its accepted A6/G117 contract.

The new name is **`delete_objects`**. It deliberately mirrors the S3 operation
whose semantics it exposes and avoids implying that the published
`delete_many` changed underneath callers.

| Surface | Requests on S3 | Per-key result | Role under this proposal |
|---|---|---|---|
| `delete` | One one-key DeleteObjects POST via `object_store::delete_stream` | `Result<(), KeyspaceError>` | Existing single-key unconditional primitive; unchanged. |
| `delete_many` | `N` one-key DeleteObjects POSTs via the same adapter path | `DeleteOutcome { deleted: bool }` | Published compatibility surface; unchanged and not deprecated. |
| `delete_if_match` | One conditional DELETE per key | Existing typed `KeyspaceError` | Only conditional-delete surface; unchanged. |
| `delete_objects` | Sequential direct DeleteObjects POSTs, each at most 1,000 keys | New typed outcome for every key | Opt-in transport-batched primitive. |

At launch, neither public batch method calls the other. A future attempt to
implement `delete_many` as a wrapper would be a separately reviewed semantic
migration and would have to preserve A6/G117, request/fault behavior, and the
boolean information loss exactly. This ADR neither needs nor authorizes that
work. New consumers that require wire batching call `delete_objects`
explicitly.

Coexistence intentionally leaves two DeleteObjects implementations live. The
published paths retain `object_store 0.13.2` response trust: missing non-error
members default to success (`aws/client.rs:579-580`), and an `<Error>` naming
an unrequested key reaches `find_position(...).unwrap()` (`:588`). The new
direct-SDK path rejects both shapes as `InvalidResponse`. This implementation
and assurance split is a mandatory residual; the new method does not harden or
make claims for unchanged `ObjectStoreClient::delete` callers.

The design also does not add variants to `KeyspaceError`. That public enum is
already published without `#[non_exhaustive]`; growing it would make the
claimed additive boundary false for exhaustive downstream matches. New errors
live in new non-exhaustive types.

## Public kernel API sketch

Names and shapes below are binding design, modulo ordinary Rust formatting and
documentation during implementation:

```rust
pub const DELETE_OBJECTS_MAX_KEYS: usize = 1_000;
pub const DELETE_OBJECTS_MAX_INPUT: usize = 100_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteObjectsOutcome {
    pub key: String,
    /// Ok(()) is an explicit confirmation. Err is unresolved remainder,
    /// not necessarily a retryable failure.
    pub result: Result<(), DeleteObjectsFailure>,
}

impl DeleteObjectsOutcome {
    #[must_use]
    pub fn remaining(outcomes: &[Self]) -> Vec<String>;
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DeleteObjectsFailure {
    /// The valid DeleteResult contained an Error entry for this exact key.
    /// It is not a proof that the key remains present.
    #[error("multi-object delete rejected for {code}: {message}")]
    Rejected { code: String, message: String },

    /// The chunk had no trustworthy per-key response. Owned diagnostics
    /// preserve request/service distinction without retaining SDK errors.
    #[error("multi-object delete outcome unconfirmed ({reason:?}, {code:?}): {message}")]
    Unconfirmed {
        reason: DeleteObjectsUnconfirmedReason,
        code: Option<String>,
        message: String,
    },

    /// The client lacks the wire capability or the service definitively
    /// refuses DeleteObjects. This is terminal until backend qualification
    /// changes; no per-key fallback was attempted.
    #[error("multi-object delete unsupported ({code:?}): {message}")]
    Unsupported {
        code: Option<String>,
        message: String,
    },

    /// An earlier chunk stopped processing, so this tail key was not sent.
    #[error("multi-object delete was not attempted")]
    NotAttempted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeleteObjectsUnconfirmedReason {
    RequestFailed,
    InvalidResponse,
}

#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeleteObjectsInputError {
    #[error("delete_objects input has {provided} keys; maximum is {max}")]
    TooManyKeys { provided: usize, max: usize },

    /// Reuses the existing identifier/reserved-state taxonomy without
    /// adding a KeyspaceError variant.
    #[error("delete_objects key {index} ({key:?}) is not admissible: {source}")]
    Key {
        index: usize,
        key: String,
        #[source]
        source: KeyspaceError,
    },

    /// Duplicate physical keys make response-to-input attribution
    /// non-bijective, so they are rejected before any request.
    #[error(
        "delete_objects key {key:?} is duplicated at indexes {first_index} and {duplicate_index}"
    )]
    Duplicate {
        key: String,
        first_index: usize,
        duplicate_index: usize,
    },
}

impl AtomicKeyspace {
    pub async fn delete_objects(
        &self,
        keys: &[&str],
    ) -> Result<Vec<DeleteObjectsOutcome>, DeleteObjectsInputError>;
}
```

The outer `Result` is an **admission result**, not a wire result. It can fail
only while checking the complete input, before any storage request. Once
admitted, the method returns exactly `keys.len()` outcomes; request, service,
and response failures are represented at every affected key. This preserves
the rule that a partial store operation is never collapsed into a wholesale
batch error.

`DeleteObjectsInputError` deliberately omits `PartialEq`/`Eq` because its
`Key` variant carries `KeyspaceError`, which implements neither; the other new
value types remain equality-comparable. `Unconfirmed` copies a sanitized
request code and message into owned strings because `ObjectStoreError` is
neither `Clone` nor equality-comparable; request-level diagnosis is returned,
not tracing-only.

`remaining` retains input order and returns every `Err` key. It is a remainder
for checkpointing and operator disposition, **not a retry set**:
`Unsupported` is terminal, `Rejected` needs provider-code policy, and
`Unconfirmed`/`NotAttempted` are replay candidates only under a bounded retry
policy. After that policy selects owned keys, the caller re-borrows them with
`selected.iter().map(String::as_str).collect::<Vec<_>>()` before a later call.
No failure outcome claims that its key still exists.

## Input admission

Admission completes before allocation of the first provider request:

1. Reject `keys.len() > DELETE_OBJECTS_MAX_INPUT` as
   `TooManyKeys { provided, max }`; do not scan or delete a prefix.
2. Walk members in input order and run `ensure_not_reserved_key` **before**
   identifier/physical-path validation, matching `delete` and `delete_many`.
   A key such as `tombstones/a!b` therefore reports `TombstoneImmutable`, not
   `InvalidIdentifier`. The existing typed sources remain
   `TombstoneImmutable`, `IncarnationCounterImmutable`,
   `TrimCertificateImmutable`, and `MaintenanceFenceImmutable`.
3. Run the existing identifier and physical-path validation for that key.
4. Reject a duplicate namespace-relative key at its second input index. S3
   responses are keyed by object name rather than input ordinal; accepting
   duplicates would make a mixed response impossible to attribute honestly.
5. Only after every key passes, retain provider paths and begin chunking.

The size error precedes every member error. Otherwise the first error by input
index is deterministic, with reserved-state precedence within that member.
Any admission error means zero delete requests and zero effects, including when
valid keys precede the bad member. Empty input returns `Ok(Vec::new())` and
emits zero requests.

## Chunking, outcomes, and resumption

`DELETE_OBJECTS_MAX_KEYS` and `DELETE_OBJECTS_MAX_INPUT` are public sizing
constants. The kernel accepts at most 100,000 keys and walks
`keys.chunks(DELETE_OBJECTS_MAX_KEYS)` sequentially: at most 100 logical
requests per call. It does not run chunks concurrently. One ambiguous chunk
therefore has one deterministic untouched tail, provider staging stays at
1,000 paths, and a consumer with a larger witnessed inventory must page and
checkpoint outside this future.

At the maximum 255-byte logical key, cloned key bytes are bounded near 25.5 MiB
plus vector/allocation and diagnostic overhead. This deliberate `O(N)` bound
buys one outcome per input without admitting a million-key, hours-long future.

For `N > 0`, a clean call makes `ceil(N / 1000)` logical DeleteObjects
operations. The configured AWS SDK may retry an identical logical operation at
the transport layer; retries do not change the key set or the outcome algebra.
The kernel does not automatically resubmit a per-key error or stopped chunk in
the same call.

Verbose response mode is mandatory (`quiet = false`). For each unique requested
physical key, the provider response must contain exactly one of:

- `Deleted` -> `DeleteObjectsOutcome { result: Ok(()) }`; or
- `Error { Code, Message }` ->
  `Err(DeleteObjectsFailure::Rejected { code, message })`.

Response order is irrelevant. The SDK layer reconciles by exact physical key;
the kernel restores original input order and strips only the namespace prefix
it constructed. A `Deleted` response for an already absent key is still
`Ok(())`: that is S3's idempotent unconditional-delete contract. `Ok(())`
means the target control object is confirmed gone, not that `read_state` must
be `Absent` and not that prior bucket versions were physically reclaimed.

A response with a missing requested key, duplicate result for one key,
Deleted/Error conflict, or an unexpected key is not a partial success report
the kernel can safely reinterpret. The current chunk becomes
`Unconfirmed { reason: InvalidResponse, code: None, message }`, the untouched
tail becomes `NotAttempted`, and processing stops. No omitted member defaults
to success.

### Semantics and recovery table

| Cut / provider result | Current-chunk outcomes | Later chunks | Durable possibility | Disposition / bounded resume |
|---|---|---|---|---|
| All requested keys have `Deleted` entries | Every key `Ok(())` | Continue | Every target control is gone; some may already have been absent | Nothing from this chunk remains. |
| One valid `200 DeleteResult` contains both `Deleted` and `Error` entries | Exact `Ok(())` / `Rejected { code, message }` by key | Continue | Confirmed keys are gone. A rejected key is not confirmed and may be present or already gone; the legacy `KeyspaceDelete` AfterEffect cut demonstrates the latter. | Checkpoint every `Err`; retry only provider codes the caller's bounded policy classifies transient. Surface permanent rejection such as `AccessDenied`. |
| Request fails before the service applies it | Every key `Unconfirmed { RequestFailed, code, message }` | `NotAttempted`; stop | Current chunk may remain | Retry current chunk and tail only when the owned diagnostic is retryable under a bounded policy; otherwise stop and surface it. |
| Service applies the request but the response is lost | Same `Unconfirmed { RequestFailed, code, message }` for every key | `NotAttempted`; stop | Some or all current keys may already be gone | A policy-selected replay is idempotent, but not automatic or unbounded. |
| Response is malformed, incomplete, contradictory, or names an unexpected key | Every key `Unconfirmed { InvalidResponse, code: None, message }` | `NotAttempted`; stop | Any subset may be gone; no success is inferred | Treat as provider/protocol qualification failure. Do not blind-retry until the cause is corrected. |
| Client lacks the wire, or service definitively refuses DeleteObjects (`405 MethodNotAllowed`, `501 NotImplemented`, or equivalent service code) | Current and untouched keys `Unsupported { code, message }`; stop | No per-key fallback | Keys not already confirmed by earlier chunks are unchanged or unresolved only where a response was lost | Terminal until backend qualification changes. |
| The future is cancelled or dropped in a live process (`timeout`, `select!`, abort) | No vector reaches the caller | Unknown | Any completed or in-flight request may have applied | Replay the caller's last durable page under its bounded policy; cancellation supplies no hidden checkpoint. |
| Process dies before returning the vector | No vector reaches the caller | Unknown | Any completed or in-flight request may have applied | Same durable-page recovery, without relying on a process-death signal from the API. |

A returned vector is a complete **remainder report** for that invocation, not a
self-executing resume token. The caller owns retry classification, attempt and
time ceilings, and durable inventory/page checkpoints. The kernel does not
create persistent deletion jobs, enumerate keys, or certify namespace
completeness.

## Partial-batch loopback counterpart

ADR 0001 batch 2 already supplies the one authoritative bulk-delete model.
`state_kernel.rs:3320-3440` recognizes path-style `POST ?delete`, parses every
`<Object><Key>` member, mutates per key, and emits one verbose
`<DeleteResult>` containing mixed `<Deleted>` and `<Error>` entries. ADR 0005
extends that branch; building a second DeleteObjects router or response model
is a defect.

The implementation delta is exactly:

1. **Key-vector observation.** Extend `LoopbackRequestObservation` for the
   bucket-level POST to retain the ordered submitted physical-key vector.
   Today `counterpart_key("/{bucket}")` is `None`, so method/path observation
   alone cannot prove A37's count, order, uniqueness, or 1,000-key ceiling.
2. **Dedicated per-entry cut.**
   `StorageFaultCut::KeyspaceMultiDeleteEntry` is matched by physical key
   inside the existing bulk branch without re-labelling the request as method
   `DELETE`. A38 arms its BeforeEffect form on one or more keys; those keys stay
   present and receive configured `<Error Code/Message>` entries while siblings
   receive `<Deleted>`.
3. **Dedicated whole-request cut.**
   `StorageFaultCut::KeyspaceMultiDeleteRequest` matches the bucket-level POST
   even though its single `key` field is `None`. BeforeEffect refuses the
   entire request. AfterEffect applies the request and then drops or corrupts
   the whole response. A40 arms only this cut for request ambiguity.

The deterministic partial case remains one request, for example
`deleted=[k0,k2,k4]`, `errors=[k1,k3]`, with public results restored to input
order. A bounded policy may later replay transient `k1`/`k3`; permanent codes
remain surfaced. The whole-request phases both yield current-chunk
`Unconfirmed` plus tail `NotAttempted`; exact state inspection distinguishes
them only inside the rig.

The legacy `StorageFaultCut::KeyspaceDelete` and G117 remain unchanged, but the
cut is already reachable from a multi-key POST because the existing bulk
handler calls `take_storage_fault(&Method::DELETE, &key, ...)` per entry. Its
AfterEffect form deletes the key and emits synthetic
`InternalError / lost response`; `delete_objects` therefore maps that valid
entry to `Rejected` even though the delete applied. A38/A40 never arm the
legacy cut, and no failure outcome is a presence proof. This compatibility
conflation is retained and declared rather than misdescribed as whole-response
attribution.

## SDK/provider boundary

`yeetz-sdk-s3` gains one one-chunk method and new non-exhaustive result types.
Conceptually:

```rust
pub struct ObjectDeleteOutcome {
    pub path: String,
    pub result: Result<(), ObjectDeleteFailure>,
}

pub struct ObjectDeleteFailure {
    pub code: String,
    pub message: String,
}

#[non_exhaustive]
pub enum DeleteObjectsRequestError {
    Unsupported {
        code: Option<String>,
        message: String,
    },
    Request {
        code: Option<String>,
        message: String,
    },
    InvalidResponse {
        message: String,
    },
}

impl ObjectStoreClient {
    /// Exactly one verbose S3 DeleteObjects operation; 1..=1000 unique paths.
    pub async fn delete_objects(
        &self,
        paths: &[String],
    ) -> Result<Vec<ObjectDeleteOutcome>, DeleteObjectsRequestError>;
}
```

This method uses the already configured pinned AWS client directly. It builds
`ObjectIdentifier` values with only `key`, sets verbose mode, submits one
DeleteObjects operation, and reconciles `deleted` plus `errors` into exactly
one result per requested path. S3 requires a Content-MD5/checksum on
multi-object delete; the selected AWS SDK request construction must supply it.
The loopback does not validate that header, so only the publication-gating
real-S3 leg witnesses checksum-compatible provider traffic.

The method neither chunks nor retries individual entries; the kernel owns
chunk sequencing and policy. It converts request/service metadata into owned,
sanitized `code`/`message` fields. Client capability absence and definitive
operation refusals — HTTP 405/501 or service codes `MethodNotAllowed` /
`NotImplemented` — map to `Unsupported`. Other request errors retain their
diagnosis as `Request`; response-bijection violations carry an
`InvalidResponse` message. `AtomicKeyspace` copies those fields to each
affected public outcome rather than erasing a whole-batch 403 into the same
value as a TCP reset.

Using `object_store::delete_stream` directly was rejected for the new assured
path. Its native batching is useful, but its implementation owns 20-chunk
buffering, flattens a request failure through a stream, treats missing
non-error members as success, and can panic on an unrequested error key. The
new API needs deterministic sequential chunks, provider codes, and an exact
response bijection. The selected AWS client is already an assured pinned
dependency consumed by this closure; no second S3 client is introduced.

In-memory stores have no DeleteObjects wire and return `Unsupported`; there is
no sequential emulation. Loopback additionally exercises a definitive
405/501-style refusal. Wire and race contracts run against the existing
loopback bulk branch. A real-S3 proof leg is required before publication.

## Permanent semantic boundaries

### Batch is transport, not transaction

No request, chunk, or invocation has cross-key atomicity. A valid S3 response
can report a strict subset deleted. Earlier chunks stay deleted if a later
chunk fails. The kernel never saves preimages, restores keys, compensates, or
reports rollback. Multi-key all-or-nothing deletion is out of scope forever.
A design that needs an atomic group must commit through one atomic pointer or
manifest key; it must not build a transaction illusion over this API.

### No batched conditional delete

`delete_objects` accepts no etag, version, incarnation, or predicate. The wire
request carries no `If-Match`, per-key conditional token, or read-before-delete
check. A read-check-unconditional-delete sequence would race and is forbidden.
`AtomicKeyspace::delete_if_match` remains the only conditional delete and stays
per-key, one conditional request at a time. This ADR does not reserve a future
batched conditional surface.

Consequently, a concurrent create or CAS may publish a value that an
unconditional batch request then removes. The kernel does not inspect or
prevent that race. Sealed repository epochs are safe because their write gate
has already made such a mutation impossible. Other callers must establish an
equivalent semantic precondition or choose `delete_if_match`/`destroy`.

### Conditional-boundary supersession trigger

Under the assured S3 contract this ADR's conditional boundary is permanent.
Only a future portable standards-level multi-object operation with an atomic
per-object condition equivalent to `If-Match` would justify reopening it for a
new human ruling. Such a capability would not silently extend
`delete_objects`; it would require a superseding ADR, a distinct API, backend
qualification, and its own contracts. Cross-key transactionality remains out
of scope regardless.

### No inventory authority

The input slice is the complete authority for this call. No LIST, page cursor,
prefix scan, hidden retry queue, or inferred namespace completeness is part of
the primitive. A million-key consumer must page from its witnessed inventory
and persist progress outside the kernel call. `remaining == []` proves only
that every supplied key was confirmed; it never proves the namespace empty.

## Tombstones, incarnations, trims, fences, and chunks

The new method is the unconditional sibling of `delete` and
`delete_if_match`, below lifecycle machinery. Its side effects are fixed:

| State/object | Read by `delete_objects` | Written/deleted by `delete_objects` | Consequence |
|---|---:|---:|---|
| `keyspace/{namespace}/{key}` control | No | Yes, unconditionally | A confirmed outcome means this target is gone. |
| `tombstones/{key}` | No | No | No existence witness is created or erased. A pre-existing tombstone can become visible to `read_state`. |
| `incarnations/{key}` | No | No | No counter bump; raw delete/recreate remains in the same incarnation. |
| `{scope}/trims/{seq}` certificate | No | No | Existing `OffsetExpired` authority remains; direct targeting is refused before effects. |
| `fences/gc` | No | No | Fence-blind by design. Direct targeting is refused, but invoking this method while a chunk-GC sweep holds the fence violates ADR 0004's operational quiescence assertion. |
| `keyspace-chunks/...` | No | No | Deleting v3 controls leaves unreachable chunks; up to 1,000 reachability roots can disappear per request and enlarge ADR 0004's broken-quiescence exposure. |
| Prior provider object versions | No | No explicit version deletion | Bucket versioning/lifecycle policy controls physical reclamation. |

### Quiescence ownership

ADR 0004 makes chunk-GC quiescence an external operational assertion, not a
property the kernel can prove. `delete_objects` deliberately does not read
`fences/gc`; therefore the caller **must not invoke it in a namespace while a
chunk-GC sweep holds that fence**. Such overlap is a violated precondition, not
supported concurrency.

The failure kind is ADR 0004 §5.2's existing broken-quiescence race, but the
batch rate increases its blast radius: one request can remove 1,000 control
roots between the sweep's reachability inventory and physical deletion. Under
the already-invalid writer/sweeper interleaving, the sweep can delete a chunk
that a later manifest names, producing `ChunkMissing` /
`ManifestIncomplete`. A43 must prove fence blindness as this declared property
and retain the broken-quiescence demonstration signature; it must not describe
the absence of a fence read as neutral safety.

`read_state` after a confirmed raw delete follows the state already present:

- without a trim certificate or standing tombstone, it is `Absent`;
- a standing tombstone remains `Destroyed` once no current value masks it;
- a certified retired sequence remains `OffsetExpired`; and
- no new `Destroyed` or higher incarnation can be attributed to this call.

Because the incarnation counter is untouched, a later raw recreation may
reuse the same incarnation and, on a content-etag backend, may reproduce an old
token for byte-identical bytes. That is existing raw-delete behavior, not a
new-era guarantee. Callers that need an existence witness and cross-deletion
era closure use `destroy`. The sealed/write-refused motivating consumer never
recreates in the deleted namespace.

Public admission refuses all reserved state before the first effect. Internal
certified trim and chunk-GC code may later compose the SDK one-chunk primitive
against kernel-owned physical paths, but only in separate implementation
batches that preserve their current reports, proofs, and preconditions. This
ADR does not weaken the reserved-state boundary to make those migrations easy.

## Cost, ordering, and storage consequences

For `N` accepted keys and no request-level stop:

- normal direct-SDK request count is `ceil(N / 1000)`, versus `N` one-key
  DeleteObjects POSTs through published `delete_many`;
- provider request staging is bounded by 1,000 paths;
- `N <= 100,000`; the returned report remains `O(N)`, with maximum logical-key
  bytes near 25.5 MiB plus bounded vector/allocation and diagnostic overhead;
- chunks are sequential, so at most one DeleteObjects operation is active from
  one call;
- verbose responses spend response bytes on every success because explicit
  confirmation is load-bearing; quiet mode is forbidden; and
- a failed entry or ambiguous request can cause an idempotent repeat, so cost
  estimates count attempts rather than assuming exactly-once transport.

On a versioned bucket, unconditional current-object deletion may create delete
markers and prior versions may continue consuming storage. This API makes no
physical-byte-reclamation claim beyond the deployment's bucket lifecycle
policy.

## Versioning, publication, and migration

The implementation is additive and targets the next unpublished **0.4.x**
release, not 0.5. Main currently declares 0.4.1 while crates.io is established
at 0.4.0; therefore 0.4.1 is the expected publication if it remains free when
the implementation lands. If another release consumes it first, use the first
free 0.4.x patch (for example 0.4.2). The design PR itself changes no package
version.

The workspace uses one version and exact internal dependency floors. Publish
the four crates at the selected 0.4.x in dependency order:
`yeetz-sdk-core`, `yeetz-sdk-s3`, `yeetz-s3-kernel`, then
`yeetz-s3-streams`. Only the SDK-S3 and kernel crates contain new code; the
other two receive the unified release version. No assured dependency pin
changes.

Release gates are the real workspace suite plus the new A36-A45 contracts. Gate
claims cite a `ci-dev` run URL. The real-S3 leg must run through
`task=real-s3`; a local run or a green compile is not a witness.

After publication, downstream adoption is separately orchestrated:

1. The yeetz forge may deliberately bump its exact kernel pin and migrate its
   sealed-epoch delete worker to `delete_objects` after its own inventory,
   checkpoint, authorization, and epoch contracts are ready.
2. Certified trim and chunk GC may consume the SDK one-chunk primitive in
   separate kernel batches. Their current methods do not silently change in
   this batch.
3. Existing `delete_many` callers need no migration and receive no changed
   behavior.

Application code still may not call `yeetz-sdk-s3` directly. The SDK method is
a kernel-closure composition seam, not a new storage bypass.

## Alternatives rejected

### Change `delete_many` in place

Rejected. It is published at 0.4.0 with a boolean outcome and one one-key
DeleteObjects POST per key on S3. Replacing its grouping or errors would make
the additive claim false and blur which proof suite applies.

### Implement `delete_many` as a wrapper over the new method

Rejected for this batch. Mapping all typed failures back to `deleted: false`
looks source-compatible but changes request grouping, stop/continue behavior,
fault attribution, and request observations. Coexistence is smaller and
honest.

### Feed all keys to `object_store::delete_stream`

Rejected for the assured surface. It can batch, but it owns cross-chunk
buffering and does not enforce the exact response bijection this contract
requires. Direct use of the already pinned AWS client gives the SDK seam one
request, one key set, and one complete response to adjudicate.

### Return one top-level store error

Rejected. It discards confirmed siblings and makes ordinary S3 partial success
unreportable. Request-level ambiguity is expanded to each current key and an
untouched tail instead.

### Read etags, then issue an unconditional batch

Rejected. The check and delete are different operations; a replacement can
land between them and be erased. This is exactly the race `delete_if_match`
exists to close.

### Add rollback or all-or-nothing compensation

Rejected permanently. DeleteObjects has no transaction and the kernel does not
retain restorable preimages. A compensating write would violate immutable
record and CAS laws and could overwrite a concurrent value.

## Contract-claim ledger — A36 onward

Existing A1-A35, I1-I6, W1-W5, R1-R9, G117/G118, K1-K7, and the streams suite
rerun unchanged. The new implementation batch owns these claims:

| Promise | Witness | Independent oracle / rig attack |
|---|---|---|
| P1. Admission is bounded, complete, and effect-free. | **A36** `a36_delete_objects_input_preflight_is_side_effect_free`: empty input; 100,001-key `TooManyKeys`; invalid identifier; every reserved family; reserved-and-malformed precedence; duplicates at boundary positions; zero delete requests. | Seed valid siblings before each bad member; require reserved-check-first parity with `delete_many`, deterministic index/source, intact values, and an empty loopback delete log. |
| P2. Wire chunks are bounded and ordered. | **A37** `a37_delete_objects_chunks_exactly_at_1000`: sizes 0, 1, 999, 1,000, 1,001, 2,000, 2,001; public constants; each POST has at most 1,000 exact physical keys; output length/order equals input. | Independent partition oracle over the new key-vector request observation; reject quiet mode, duplicate/missing request keys, concurrent/out-of-order chunks, or hidden per-key fallback. |
| P3. A valid partial response is reported per key. | **A38** `a38_delete_objects_partial_batch_is_typed_per_key`: `KeyspaceMultiDeleteEntry` creates multiple `Deleted` and distinct `<Error Code/Message>` entries in shuffled response order; exact `Ok`/`Rejected` vector and remainder. | Exact object map proves confirmed keys gone and BeforeEffect rejects present. A bounded sample policy replays only transient errors; a permanent error remains surfaced. The legacy cut is not armed. |
| P4. Cross-chunk remainder is complete without prescribing infinite retry. | **A39** `a39_delete_objects_remainder_crosses_chunks`: valid per-key errors do not prevent later chunks; `remaining` contains every unresolved key in order; no confirmed key appears. | 2,001-key failures on both sides of boundaries; classify transient, permanent, unsupported, and not-attempted outcomes and prove the selected bounded retry set terminates or surfaces terminal state. |
| P5. Whole-request ambiguity never fabricates success or erase diagnosis. | **A40** `a40_delete_objects_lost_response_marks_chunk_unconfirmed`: `KeyspaceMultiDeleteRequest` BeforeEffect/AfterEffect yield identical current `Unconfirmed { code, message }` plus tail `NotAttempted`; prior chunks stay confirmed. | Pure oracle permits applied or unapplied current keys, never `Ok` without a valid response; assert a service 403 remains distinguishable from a transport reset. |
| P6. Invalid provider responses fail closed. | **A41** `a41_delete_objects_invalid_response_is_never_success`: missing requested member, duplicate member, Deleted/Error conflict, unknown key, malformed XML. | Mutate every response position; current chunk is diagnostic `InvalidResponse`, tail untouched, no omitted key defaults to success, and blind retry is not presented as qualification. |
| P7. Incapable or definitively refusing backends never emulate the wire. | **A42** `a42_delete_objects_fails_closed_without_wire_support`: in-memory capability absence and loopback 405/501 or `NotImplemented` produce diagnostic `Unsupported` outcomes with values intact. | Request counter proves zero sequential deletes and no fallback through `ObjectStoreClient::delete`; other request failures remain diagnostic `Unconfirmed`. |
| P8. Lifecycle machinery is untouched, with quiescence consequence named. | **A43** `a43_delete_objects_stays_below_lifecycle_state`: inline/v3 controls; absent, tombstone, trim, and nonzero-incarnation cases; no tombstone/counter/fence/certificate/chunk requests or mutations. | State oracle checks existing read states, counter equality, and orphan chunks. Request log proves fence blindness; invoking during ADR 0004 GC is marked a violated precondition and retains the broken-quiescence `ChunkMissing`/`ManifestIncomplete` demonstration. |
| P9. The primitive is visibly unconditional and non-atomic. | **A44** `a44_delete_objects_has_no_condition_or_transaction`: mixed success, earlier-chunk success plus later stop, and a parked concurrent replacement the request may delete; no etag/version/If-Match in request. | Demonstration records the allowed race and partial state. Companion per-key `delete_if_match` protects the replacement. |
| P10. Published deletion APIs and their weaker adapter path do not move. | **A45** `a45_delete_many_and_delete_if_match_remain_unchanged`: compile-time signatures/types, A6/G117/G118, `N` one-key DeleteObjects POSTs for `delete_many`, boolean outcomes, and old fault mapping remain beside the new surface. | Dual-call trace distinguishes the legacy `object_store` response-trust path from bounded direct-SDK requests; no wrapper/deprecation/alias exists, and the unchanged missing-member/panic residual is explicit. |

Before publication, the real-S3 rig adds a DeleteObjects leg: 1,001 unique
keys require two logical chunks; present and absent keys confirm idempotently;
output order and verbose response bijection are exact; required checksum
construction is accepted; cleanup exact-reads empty; and provider request
metadata identifies the two DeleteObjects operations. A live partial-error
case is included only if it can be induced without policy changes; loopback A38
remains deterministic.

The decision branch is predeclared: if the assured provider refuses
DeleteObjects, rejects the SDK's checksum construction, or omits/duplicates a
per-key member in verbose mode, publication is **blocked** and that backend is
unqualified for this primitive. The implementation may not weaken the
bijection, infer omitted success, or fall back to per-key deletion to make the
leg green. The retained `ci-dev real-s3` run URL is the witness.

## Explicit non-goals

- Cross-key atomicity, now or later.
- Batched conditional delete, now or later; no `If-Match` approximation.
- Mutation of `delete_many`, `DeleteOutcome`, `delete`, `delete_if_match`,
  `destroy`, `delete_below`, or current chunk-sweep contracts.
- Key discovery, LIST completeness, witnessed inventory construction, or a
  durable deletion-job/checkpoint store.
- Tombstone creation/removal, incarnation movement, trim certification,
  maintenance fencing, or v3 chunk reclamation.
- Physical deletion of prior bucket versions or a guarantee of immediate byte
  reclamation.
- Automatic migration of the yeetz epoch feature or any other downstream
  consumer.
- A public raw-SDK escape hatch outside the kernel closure.

## Residual and decisions still required

No semantic fork is left open inside this proposal: the public name, bounded
input/outcome algebra, remainder-not-retry rule, chunk ordering, lifecycle
layer, and permanent non-goals are selected above.

Load-bearing residuals are explicit:

- **ADR 0004 quiescence remains external.** `delete_objects` is fence-blind.
  Calling it while chunk GC holds `fences/gc` violates the caller's operational
  assertion and can enlarge the documented broken-quiescence
  `ChunkMissing`/`ManifestIncomplete` race to 1,000 removed controls per
  request. This proposal does not prevent or narrow that misuse.
- **Two response-trust implementations coexist.** Unchanged
  `ObjectStoreClient::delete` callers keep `object_store 0.13.2` behavior:
  omitted non-error response members imply success and an error naming an
  unrequested key can panic. The new direct-SDK path fails those shapes closed;
  it does not repair the old path.
- **The legacy rig cut retains its conflation.** An armed
  `KeyspaceDelete::AfterEffect` can delete a key and emit per-entry
  `InternalError`, so the new algebra may report `Rejected` for an applied
  delete. New claims use dedicated cuts; no failure is a presence proof.
- **Retry classification is caller policy.** `remaining()` is complete
  unresolved work, not a promise that replay terminates. The caller must bound
  attempts/time, classify provider codes, and surface permanent or unsupported
  outcomes.

What also remains unassured by this design-only PR:

- human acceptance after adversarial review;
- separate human authorization for the one-batch implementation;
- A36-A45 and publication-gating real-S3 witnesses from that implementation;
- the exact free 0.4.x patch number at publication time; and
- separately orchestrated downstream adoption and witnessed inventory/job
  semantics. The yeetz epoch feature neither requires nor authorizes adoption.

Until those exist, this ADR is a proposal and the published behavior remains
0.4.0's existing `delete_many` contract.
