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
  It therefore submits one store delete operation per key and cannot expose a
  provider error code or distinguish failure classes. That contract is frozen.
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
7. Resume by submitting every failed outcome. Unconditional deletion is
   idempotent at the kernel's current-object boundary, including an already
   absent key.
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
| `delete` | One store delete for one key | `Result<(), KeyspaceError>` | Existing single-key unconditional primitive; unchanged. |
| `delete_many` | One store delete operation per key | `DeleteOutcome { deleted: bool }` | Published compatibility surface; unchanged and not deprecated. |
| `delete_if_match` | One conditional DELETE per key | Existing typed `KeyspaceError` | Only conditional-delete surface; unchanged. |
| `delete_objects` | Sequential DeleteObjects chunks, at most 1,000 keys each | New typed outcome for every key | Opt-in transport-batched primitive. |

At launch, neither public batch method calls the other. A future attempt to
implement `delete_many` as a wrapper would be a separately reviewed semantic
migration and would have to preserve A6/G117, request/fault behavior, and the
boolean information loss exactly. This ADR neither needs nor authorizes that
work. New consumers that require wire batching call `delete_objects`
explicitly.

The design also does not add variants to `KeyspaceError`. That public enum is
already published without `#[non_exhaustive]`; growing it would make the
claimed additive boundary false for exhaustive downstream matches. New errors
live in new non-exhaustive types.

## Public kernel API sketch

Names and shapes below are binding design, modulo ordinary Rust formatting and
documentation during implementation:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteObjectsOutcome {
    pub key: String,
    /// Ok(()) means this key was explicitly confirmed in a valid
    /// DeleteObjects response. Err is the resumable remainder.
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
    #[error("multi-object delete rejected for {code}: {message}")]
    Rejected { code: String, message: String },

    /// The chunk had no trustworthy per-key response. The request may or
    /// may not have applied; unconditional replay is required.
    #[error("multi-object delete outcome unconfirmed: {reason:?}")]
    Unconfirmed { reason: DeleteObjectsUnconfirmedReason },

    /// This client has no S3 DeleteObjects wire capability. No per-key
    /// fallback was attempted.
    #[error("multi-object delete is unsupported by this backend")]
    Unsupported,

    /// An earlier chunk became unconfirmed, so this tail key was not sent.
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
only while validating the complete input, before any storage request. Once
admitted, the method returns exactly `keys.len()` outcomes; request, service,
and response failures are represented at every affected key. This preserves
the rule that a partial store operation is never collapsed into a wholesale
batch error.

`remaining` retains input order and returns every `Err` key, including
`Rejected`, `Unconfirmed`, `Unsupported`, and `NotAttempted`. It does not retry
inside the helper and does not claim that a failed key still exists.

## Input admission

Admission completes before allocation of the first provider request:

1. Run the existing identifier and physical-path validation for every key.
2. Run `ensure_not_reserved_key` for every key. The existing typed sources
   remain `TombstoneImmutable`, `IncarnationCounterImmutable`,
   `TrimCertificateImmutable`, and `MaintenanceFenceImmutable`.
3. Reject a duplicate namespace-relative key at its second input index. S3
   responses are keyed by object name rather than input ordinal; accepting
   duplicates would make a mixed response impossible to attribute honestly.
4. Only after every key passes, construct provider paths and begin chunking.

The first error in input order is deterministic. Any admission error means zero
delete requests and zero effects, including when valid keys precede the bad
member. Empty input returns `Ok(Vec::new())` and emits zero requests.

## Chunking, outcomes, and resumption

`DELETE_OBJECTS_MAX_KEYS` is 1,000. The kernel walks `keys.chunks(1000)`
sequentially. It does not run chunks concurrently: a single ambiguous chunk
then has one deterministic untouched tail, bounds provider staging to 1,000
keys, and avoids turning a deletion job into an unbounded request burst.

For `N > 0`, a clean call makes `ceil(N / 1000)` logical DeleteObjects
operations. The configured AWS SDK may retry an identical logical operation at
the transport layer; retries do not change the key set or the outcome algebra.
The kernel does not automatically resubmit an individual `<Error>` entry in the
same call.

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
`Unconfirmed { InvalidResponse }`, the untouched tail becomes `NotAttempted`,
and processing stops. No omitted member defaults to success.

### Semantics and recovery table

| Cut / provider result | Current-chunk outcomes | Later chunks | Durable possibility | Resume rule |
|---|---|---|---|---|
| All requested keys have `Deleted` entries | Every key `Ok(())` | Continue | Every target control is gone; some may already have been absent | Nothing from this chunk remains. |
| One `200 DeleteResult` contains both `Deleted` and `Error` entries | Exact `Ok(())` / `Rejected { code, message }` by key | Continue | Confirmed keys are gone; rejected keys were not confirmed deleted | Retry only every `Err` key. |
| Request fails before the service applies it | Every key `Unconfirmed { RequestFailed }` | `NotAttempted`; stop | Current chunk may remain | Retry current chunk plus tail. |
| Service applies the request but the response is lost | Same `Unconfirmed { RequestFailed }` for every key | `NotAttempted`; stop | Some or all current keys may already be gone | Same retry; absence makes replay safe. |
| Response is malformed, incomplete, contradictory, or names an unexpected key | Every key `Unconfirmed { InvalidResponse }` | `NotAttempted`; stop | Any subset may be gone; no success is inferred | Retry current chunk plus tail; retain the protocol failure for diagnosis. |
| Backend lacks DeleteObjects | Every accepted key `Unsupported` | No request is attempted | No key changed | Use a capable backend; never degrade to per-key deletes silently. |
| Process dies before returning the vector | No report reaches the caller | Unknown | Any completed request may have applied | Replay the caller's entire submitted slice; the kernel stores no job checkpoint. |

A returned vector is therefore a complete resume token for that invocation.
For crash recovery, the caller's durable inventory/checkpoint is the token;
replaying more keys than strictly necessary is safe. The kernel does not create
persistent deletion jobs, enumerate keys, or certify inventory completeness.

## Partial-batch loopback counterpart

The loopback must model DeleteObjects as one POST carrying an ordered list of
up to 1,000 keys, a verbose `DeleteResult`, and exact request observation. The
request log records the submitted physical key vector, not merely the bucket
path, so request cardinality and conditions are assertable.

Two independent fault cuts are required:

1. **Per-entry partial fault.** Arm one or more physical keys in a single
   request to return `<Error><Key>...` while sibling keys return `<Deleted>`.
   A before-effect entry fault leaves that key present. The contract must be
   able to construct, in one request, an interleaving such as
   `deleted=[k0,k2,k4]`, `errors=[k1,k3]` and require the public output to name
   exactly those confirmations and typed failures in original input order.
   Retrying `[k1,k3]` must converge without touching the confirmed set.
2. **Whole-response ambiguity.** BeforeEffect rejects the request without
   mutation; AfterEffect applies its unconditional deletes and then drops or
   corrupts the response. Both phases must produce the same `Unconfirmed`
   vector for the current chunk. Exact state inspection distinguishes the two
   only inside the rig. The public API never guesses from that hidden fact.

The first cut proves ordinary partial success. The second proves safe
resumption when no per-key response exists. They are distinct; a synthetic
per-key `<Error>` is not used to pretend that losing an entire HTTP response
reveals which individual effects landed.

Existing `StorageFaultCut::KeyspaceDelete`, G117, and the old one-key call
shape stay unchanged. The new cuts may share loopback parsing/storage helpers,
but they have separate observations and claims.

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
    Unsupported,
    Request(ObjectStoreError),
    InvalidResponse,
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
one result per requested path. It neither chunks nor retries individual
entries; the kernel owns chunk sequencing and policy. Request-level SDK errors
remain top-level at this private composition boundary because there is no
truthful per-key response to return; `AtomicKeyspace` expands that error over
every key in the current chunk.

Using `object_store::delete_stream` directly was rejected for the assured path.
Its native batching is useful, but its implementation owns 20-chunk buffering,
flattens a request failure through a stream, and treats missing non-error
members as success. The new API needs deterministic sequential chunks,
provider codes, and a response bijection check. The selected AWS client is
already an assured pinned dependency consumed by this closure; no new S3
client is introduced.

In-memory stores have no DeleteObjects wire and return `Unsupported`; there is
no sequential emulation. Wire and race contracts run against loopback. A real
S3 proof leg is required before publication.

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
| `fences/gc` | No | No | The primitive neither establishes nor proves quiescence; direct targeting is refused. |
| `keyspace-chunks/...` | No | No | Deleting a v3 control leaves unreachable chunks for the existing quiesced GC contract. |
| Prior provider object versions | No | No explicit version deletion | Bucket versioning/lifecycle policy controls physical reclamation. |

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

- normal logical request count is `ceil(N / 1000)`, versus `N` store delete
  operations through published `delete_many`;
- provider request staging is bounded by 1,000 paths;
- the returned report and owned key strings are `O(N)`, required by the
  one-outcome-per-key contract;
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

Rejected. It is published at 0.4.0 with a boolean outcome and one store delete
operation per key. Replacing its transport or errors would make the additive
claim false and blur which proof suite applies.

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
| P1. Admission is complete and effect-free. | **A36** `a36_delete_objects_input_preflight_is_side_effect_free`: empty input; invalid identifier; every reserved family; duplicate at the first, 1,000th, and 1,001st positions; deterministic index/source; zero delete requests. | Seed valid siblings before each bad member; exact-read them afterward and require an empty loopback delete log. |
| P2. Wire chunks are bounded and ordered. | **A37** `a37_delete_objects_chunks_exactly_at_1000`: sizes 0, 1, 999, 1,000, 1,001, 2,000, 2,001; each observed POST has at most 1,000 exact physical keys; clean request count is `ceil(N/1000)`; output length/order equals input. | Independent partition oracle over input indexes; reject quiet mode, duplicate/missing request keys, concurrent/out-of-order chunk dispatch, or hidden per-key fallback. |
| P3. A valid partial response is reported per key. | **A38** `a38_delete_objects_partial_batch_is_typed_per_key`: one request with multiple `Deleted` and multiple distinct `<Error Code/Message>` entries in shuffled response order; exact `Ok`/`Rejected` vector in input order; only failures in `remaining`. | Mandatory loopback per-entry fault cut. Exact object map proves confirmed keys gone and before-effect rejected keys present; retry only the remainder and converge. |
| P4. Resumption crosses chunk boundaries without replaying confirmed keys. | **A39** `a39_delete_objects_resumes_across_chunks`: an early chunk has valid per-key errors, later chunks still execute, and retrying the complete failure subset reaches all-confirmed. | 2,001-key model with failures on both sides of boundaries; request log proves no confirmed key appears in the caller-selected resume set. |
| P5. Whole-request ambiguity never fabricates success. | **A40** `a40_delete_objects_lost_response_marks_chunk_unconfirmed`: BeforeEffect and AfterEffect request cuts yield identical current `Unconfirmed` plus tail `NotAttempted`; prior chunks retain confirmed results; replay converges in both physical states. | Mandatory whole-response fault cut after a completed earlier chunk. Pure oracle permits applied or unapplied current keys, never `Ok` without a valid response. |
| P6. Invalid provider responses fail closed. | **A41** `a41_delete_objects_invalid_response_is_never_success`: missing requested member, duplicate member, Deleted/Error conflict, unknown key, malformed XML. | Generate response mutations at every position; current chunk all `InvalidResponse`, tail untouched, and no omitted key defaults to deleted. |
| P7. Unsupported backends never emulate the wire. | **A42** `a42_delete_objects_fails_closed_without_wire_support`: in-memory backend returns one `Unsupported` outcome per accepted key and leaves all values intact. | Request counter proves zero sequential deletes; a fallback implementation is an explicit test failure. |
| P8. Lifecycle machinery is untouched. | **A43** `a43_delete_objects_stays_below_lifecycle_state`: inline and v3 controls; absent, standing tombstone, certified trim, and nonzero incarnation cases; no tombstone/counter/fence/certificate/chunk requests or mutations. | State-machine oracle checks `Absent`/`Destroyed`/`OffsetExpired` from pre-existing state, counter equality, orphaned v3 chunks, and reserved-key refusal before effects. |
| P9. The primitive is visibly unconditional and non-atomic. | **A44** `a44_delete_objects_has_no_condition_or_transaction`: mixed success in one request, earlier-chunk success plus later stop, and a parked concurrent replacement that the unconditional request may delete; request XML/header log contains no etag/version/If-Match. | Demonstration cut records the allowed race and partial state. A companion `delete_if_match` case proves the conditional alternative remains single-key and protects the replacement. |
| P10. Published deletion APIs do not move. | **A45** `a45_delete_many_and_delete_if_match_remain_unchanged`: compile-time signatures/types, A6/G117/G118 rerun, old one-key request cardinality and boolean outcomes preserved beside the new surface. | Dual-call loopback trace: `delete_many` retains one store operation per key and old fault mapping; `delete_objects` emits bounded multi-key requests. No wrapper/deprecation/source alias exists. |

The real-S3 rig gains a separate DeleteObjects leg before publication: 1,001
unique keys require two logical chunks; present and absent keys are confirmed
idempotently; output order is exact; cleanup exact-reads empty; and provider
request metadata identifies DeleteObjects rather than 1,001 per-key calls. A
live partial-error case is included only if it can be induced without changing
bucket policy or weakening isolation; loopback A38 remains the deterministic
partial-failure oracle. The retained CI run URL is the witness.

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

No semantic fork is left open inside this proposal: the public name, outcome
algebra, stop rule, chunk ordering, lifecycle layer, and permanent non-goals
are selected above.

What remains deliberately unassured by this design-only PR:

- human acceptance after adversarial review;
- separate human authorization for the one-batch implementation;
- A36-A45 and real-S3 CI witnesses from that implementation;
- the exact free 0.4.x patch number at publication time; and
- separately orchestrated downstream adoption and witnessed inventory/job
  semantics.

Until those exist, this ADR is a proposal and the published behavior remains
0.4.0's sequential `delete_many` contract.
