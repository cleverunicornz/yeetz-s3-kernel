# ADR 0001: Kernel extension — AtomicKeyspace, O(1) terminal reads, and the absent/incomplete taxonomy

Status: accepted

Provenance: renumbered from yeetz ADR 0016 (`0016-kernel-atomic-keyspace`)
when the kernel was extracted to this repo; content otherwise carried
verbatim. ADR numbers below 0016 (e.g. 0011) refer to the parent yeetz
decision log, `yeetz/docs/architecture/decisions/`.

## Authorization trail

Human-authorized kernel extension. The streams consensus adjudication
(Fable's design at weight 5, Sol's cross-review at 5.6, Fugu Ultra's
tie-break, human directive) identified the keyed-I/O surface the
streams crate needs and two read-path defects the forge already
suffers. The kernel-extension law landed as PR #60 (the storage-law
revision: the kernel closure is the only S3 client; missing capability
is BLOCKING, never a workaround). This is the firewalled extension
batch that law describes: one batch, its own contract suite, nothing
else. Downstream breakage breaks definitively; repair is separately
orchestrated.

## What was decided

### 1. `AtomicKeyspace` (new module, this crate)

Sol's AtomicKeyspace spec (cross-review §1), verbatim surface:
namespace-scoped validated keys with

- `create` — put-if-absent; a lost race returns typed `AlreadyExists`
- `get` / `get_with_etag`
- `compare_exchange` — If-Match CAS; mismatch returns typed
  `PreconditionFailed` carrying the current etag when observable
- `list_after` — exclusive start-after, strictly ordered, bounded
- `delete` — namespaced; idempotent
- `delete_many` — batch, idempotent, per-key outcome report for
  resumability (future GC)

**No unconditional overwrite exists anywhere in the module.**

Placement: the kernel crate. It needs only primitives the kernel
already consumes (`upload_conditional`, `download_with_etag`,
`list_with_offset`, `delete` on `yeetz-sdk-s3`), so no yeetz-sdk-core
placement is required; keeping it in the kernel crate keeps the
assured-contract boundary (K-suite discipline) in one place.

**Key layout:** every keyspace object lives under the reserved root
`keyspace/` — `keyspace/{namespace}/{key}`. The root is kernel-owned
(the same ownership as `objects/`, `head`, `checkpoints/`), which
structurally prevents a keyspace namespace from colliding with a
lineage name (`issue/demo/hello/1` the lineage and an `issue`
namespace key remain disjoint). Namespace and key validation is
conservative: non-empty slash-joined segments of
`[A-Za-z0-9][A-Za-z0-9._-]*`, total length ≤ 255, no leading/trailing
slash, no empty segments. Listing is prefix-scoped to the namespace
root and therefore cannot observe keys outside the namespace.

### 2. O(1) terminal reads (additive `StateKernel` methods)

`read_terminal_record()` loads the head object and the terminal record
it names — two GETs, no history walk — returning head + payload +
digest + generation. Today the only read path (`fold`) loads the full
chain before anything; the forge's per-read O(n) defect (review
finding) is this shape. Integrity at O(1) scope: the head must parse,
the record must parse, and the record's digest must equal the digest
the head names. Chain linkage below the terminal is NOT verified —
that is the documented trade; a broken chain still surfaces through
`fold`/`fold`-based paths. Equivalence with fold's terminal is
asserted by contract A7.

### 3. `LineageAbsent` vs `StateHistoryIncomplete` (additive taxonomy)

Fugu's amendment: a never-created lineage and a broken-history lineage
are different truths and must not be conflated (law 7's other edge).
Additive: `read_head_state() -> LineageHeadState` where `Absent`
means the head object does not exist (the lineage was never created —
or its head was destroyed, which is itself an integrity statement the
caller cannot repair from) and `Present(HeadRead)` carries the head.
Existing `read_head()` semantics are unchanged (still
`StateHistoryIncomplete` on absent head) — no caller moves; new
callers get the distinction. `StateHistoryIncomplete` remains exactly
its old meaning: the head exists but the chain is broken.

### 4. The A-suite

The extension's contract suite (A1–A8), alongside the K-suite per the
extension protocol: exclusivity, CAS correctness, stable-set
ordering/pagination plus the explicit weak-concurrency boundary,
get_with_etag consistency, delete idempotency + namespace scoping,
delete_many partial-failure resumability, terminal-read
equivalence, taxonomy distinction. The loopback counterpart gains LIST
(ListObjectsV2-subset XML) and DELETE handlers so the contract suite
can run against the fault-injecting rig, reusing its existing
conditional-create lost-response cuts.

## Blast radius (additive-only analysis)

- No existing API signature or semantics changes; K1–K7 meaning does
  not move. The new module and methods cannot break exhaustive
  matches (new types, no enum variants added to existing enums; the
  keyspace carries its own error type rather than growing
  `KernelError`).
- The loopback counterpart additions are test-rig only (ignored
  harness process).
- Expected downstream (accepted per the firewall): none forced — the
  additions are purely new surface. The streams crate (ADR 0002) and
  the six keyed-I/O debt sites consume it in their own phases.

## Consequences

- The streams crate can build on keyed I/O without raw adapter access;
  the law's BLOCKING condition resolves.
- The forge's terminal-read hot path becomes two GETs (after its
  separately-orchestrated migration — not this batch).
- `keyspace/` joins the kernel's reserved key roots;
  `tools/check_storage_boundaries.sh` ownership rules are unaffected
  (the module lives inside the closure).

## Addendum: Batch 2 — loopback rig fidelity and the ABA assumption

Status: accepted (human-authorized batch 2; kernel-crate-firewalled, additive-only).

### Loopback rig additions

- **Keyspace DELETE fault cut** (`StorageFaultCut::KeyspaceDelete`, both
  phases). The rig now models S3's real bulk-delete wire shape
  (`POST /{bucket}?delete` with `<Delete><Object><Key>` bodies and
  `<DeleteResult>` responses — the shape `object_store`'s `delete_stream`
  emits), applying the armed cut per key: BeforeEffect refuses the delete;
  AfterEffect applies it and loses the response (surfacing as a failed
  `<Error>` entry — the G117 lost-response shape).
- **ListObjectsV2 pagination fidelity**: `IsTruncated`, `KeyCount`,
  `NextContinuationToken` (the last returned key — the continuation subset
  this rig's clients use), `continuation-token` honored as the exclusive
  resume point.

### Contracts added

- **G117** (`g117_delete_many_resumes_after_lost_response_cut`): a sweep
  cut mid-batch on one key reports that key in the resumable remainder;
  the resumed sweep is idempotent (the AfterEffect cut already applied the
  delete server-side) and converges to an empty keyspace. Exactly-once
  per key.
- **A9** (`a9_list_multi_page_continuation_fidelity`): multi-page walk —
  exactly-once across pages in byte order; plus a raw counterpart walk
  proving `IsTruncated` flips only when keys remain, `KeyCount` reports
  the page size, and continuation tokens resume exclusively.
- **A10** (`a10_weak_cursor_boundary_on_loopback`): G116 weak-cursor
  semantics on the loopback — inserts at/before the cursor after a page
  never appear in the remaining walk; inserts after it do.
- **A11** (`a11_aba_no_same_etag_recurrence_through_module_surface`):
  see below.

### The ABA assumption, stated precisely

S3 content-ETags recur for byte-identical values: a store may hand back
the same etag for a re-written byte-identical object, so an etag alone is
not a monotonic version. Keyspace `compare_exchange` is ABA-safe **iff**
values are immutable-after-create **or** carry their own monotonic
versions. We rely on the first: keyspace values are create-once through
this module's surface.

The load-bearing fact, proven by A11: **no unconditional write exists on
the module's API surface.** `create` is put-if-absent (If-None-Match `*`;
conflicts with `AlreadyExists`), `compare_exchange` always carries
If-Match, and every write mints a fresh store etag per PUT — so even an
A→B→A' write-back of byte-identical bytes cannot make the era-one etag
match the current head. A same-etag overwrite attempt is unconstructible
through the surface; a stale-etag CAS fails `PreconditionFailed` even
when the current value's bytes are identical to the stale era's.

## Addendum: Batch 3 — structural application boundary

Status: accepted (human-authorized batch 3; ruling this session: "Agree on
fix"; constructor changes are additive and K1-K7 are unmoved).

### Opaque construction

`StateKernel` is deliberately bound to one lineage, while the forge needs one
shared store authority from which it can bind many lineages and keyspaces. The
equivalent constructor surface is therefore `KernelHandle`:

- `KernelHandle::from_s3_config(&S3Config) -> Result<KernelHandle,
  KernelInitError>` constructs the adapter inside `yeetz-s3-kernel`;
- `KernelHandle::with_in_memory_store(name) -> KernelHandle` is available only
  with the `test-support` feature;
- `state_kernel(lineage)` and `atomic_keyspace(namespace)` return opaque kernel
  surfaces, never the adapter.

`S3Config` remains owned by `yeetz-sdk-s3` and is re-exported from
`yeetz-s3-kernel`. It is connection data, not a storage capability; re-exporting
it avoids duplicating configuration parsing while leaving the client type
unreachable to applications. `yeetz-s3-streams` also accepts `KernelHandle`, closing
its former adapter-construction edge.

The test feature adds semantic damage helpers inside the kernel for rebuild and
integrity tests. They delete or inspect kernel-owned test state; they do not
expose keys, an adapter, or an unconditional write.

The live ABA probe engine is kernel-owned for the same structural reason. The
`yeetz-rigs` entry point supplies only `S3Config` and reports the bounded probe's
verdicts; it cannot name the adapter. Probe writes use put-if-absent or
etag-guarded compare-and-exchange, including the A-to-B-to-A cycle, so the
evidence rig does not reopen an unconditional-overwrite path.

### Why lexical enforcement was insufficient

`tools/check_storage_boundaries.sh` remains a useful second layer, but a source
token scan cannot prove capability absence. Three bypass classes compile without
the lexical spellings it expects:

1. a call through `&dyn object_store::ObjectStore`, where the concrete adapter
   name and wrapper method names disappear;
2. an adapter type or operation imported through an aliased re-export, where
   the original crate and type names are absent from the application file;
3. a macro whose invocation is innocuous source text but whose expansion emits
   the storage call after the lexical scan has run.

Removing application manifest access closes all three at the compiler boundary.
Rust does not place transitive dependencies in a crate's extern prelude: without
an application-owned dependency path to the adapter crate, application source,
re-exports, trait objects, and macro expansions cannot resolve it.

### Dependency floor

`tools/check_dependency_floor.sh` evaluates Cargo metadata for `yeetz-forge`,
`yeetz-runner`, `yeetz-protocol`, and `yeetz-rigs`, including each root's dev and
build edges. It rejects and prints any direct or transitive path that reaches
`yeetz-sdk-s3` or `object_store` **before** entering `yeetz-s3-kernel`. Traversal
stops at that floor because the kernel's internal path
`yeetz-s3-kernel -> yeetz-sdk-s3 -> object_store` is the one authorized storage
authority, not an application capability.

The remaining possible bypass is explicit and review-visible: re-add a manifest
edge to an adapter, or add an intermediate crate that depends on one. Either
shape is caught in CI with its full package path. The wiring and debt sections
of `tools/storage-boundary-allowlist` are both empty; there is no graph
allowlist.

The historical bypass shapes are therefore compile failures in every checked
application crate:

```compile_fail
fn dyn_trait(_: &dyn object_store::ObjectStore) {}
```

```compile_fail
use yeetz_s3_kernel::ObjectStoreClient as Store;

fn aliased_reexport(store: Store) {
    let _ = store;
}
```

```compile_fail
macro_rules! generated_call {
    () => {{ yeetz_sdk_s3::ObjectStoreClient::in_memory("bypass") }};
}

fn macro_expansion() {
    let _ = generated_call!();
}
```
---

## Ruled addendum: the real-backend ABA probe — the assumption is
## measured, and it is FALSE on Exoscale SOS

Status: recorded (human ruling #3, teardown finding G155; evidence
first — this section states what the probe measured, not what we
hoped).

### The probe

`rigs/examples/real_s3_aba_probe.rs` (run remotely via the ci-dev
`real-s3` task; the run URL is the durable witness) fires the
battery the loopback cannot model against live Exoscale SOS
(`sos-ch-gva-2.exo.io`, bucket `yeetz-aba-probe`): etag recurrence
(delete + rewrite identical bytes; the A→B→A content cycle), the
If-None-Match create race, If-Match CAS with correct/stale etags,
the ABA case (CAS with the era-1 etag against a recreated era-2
object of identical bytes), CAS against deleted keys,
create-after-delete, and LIST-after-write visibility.

### Measured verdict (run 32423582844)

- **Etags on SOS are content hashes, not incarnation versions.**
  Delete + rewrite of identical 64-byte content returned the
  identical etag (`f789afefff2e7e3c97537c40e730bb3e`), and the
  A→B→A cycle returned A's etag again for A's bytes (B differed:
  `ce7b785b1be7ad4f72773217db8c5d3e`) — a pure content-hash scheme
  for single-PUT objects.
- **THE ABA CASE IS REAL: an If-Match carrying the era-1 etag was
  ACCEPTED against the era-2 object** (same bytes, new incarnation
  after delete + recreate). The store cannot distinguish eras of
  byte-identical content; an SOS etag is not a monotonic version.
- Conditional-PUT mechanics themselves are sound: the 8-way parallel
  If-None-Match create had exactly one winner (7×
  `PreconditionFailed`); If-Match with the correct etag replaced the
  bytes; If-Match with a stale etag of DISTINCT content was rejected.
- LIST-after-write/delete visibility was immediate and consistent
  (sampled; the strong-LIST qualification held in this sample).

### What this does to the Batch-2 argument

The batch-2 addendum's clause "every write mints a fresh store etag
per PUT" is **false on this backend** and must not be load-bearing.
What survives is exactly the surface half of A11: no unconditional
write exists through the module's API (`create` is put-if-absent,
`compare_exchange` always carries If-Match), and keyspace values are
create-once through the surface. On a content-hash etag backend,
`compare_exchange`'s ABA safety reduces to that create-once
discipline alone: CAS discriminates VALUES perfectly (distinct
content ⇒ distinct etag ⇒ stale CAS rejected, measured), but cannot
discriminate ERAS of identical bytes. An A→A' ABA through the
surface therefore requires delete-then-recreate-with-byte-identical
content — impossible for records under the append-only discipline,
and possible only where `delete` is legitimately reachable (damage
sweep, cleanup).

**Open design decision (human, not implemented here):** whether
create-once discipline is sufficient, or keyspace values must carry
their own monotonic versions (e.g., an embedded era/generation
compared inside the CAS loop). The probe rig stands as the permanent
re-measurement harness: it fails loudly (red run) whenever the
hazard is present, so any backend change that fixes or regresses
etag semantics is caught by re-dispatching the `real-s3` task.

---

## Ruled addendum: Batch 4 — versioned keyspace values close CAS-era ABA

Status: accepted (human-authorized kernel batch 4; ruling: "B is
correct" — structural versioning, not caller discipline).

### Authorization and supersession

The real-S3 probe landed in PR #83 and measured Exoscale SOS directly:
single-PUT etags derive from content, A -> B -> A recurs A's etag, and
an era-one If-Match is accepted once identical object bytes recur. The
human selected option B: values in `AtomicKeyspace` carry versions.
This addendum closes the prior open design decision and supersedes the
batch-2 assumption that every PUT mints a fresh etag.

### Mechanism and API boundary

Every `AtomicKeyspace` value is stored in a canonical internal binary
envelope `{ version: u64, payload: bytes }`. `create` writes version 0.
A successful `compare_exchange` reads and validates the current
envelope, checks that the caller's etag names that observation, and
writes the caller's opaque payload at the checked successor version.
Version overflow fails closed. Because A(v0), B(v1), and A(v2) have
different stored bytes, a content-derived backend etag cannot recycle
the token for A(v0) at A(v2).

The existing caller surface remains payload-shaped: `create`, `get`,
`get_with_etag`, and `compare_exchange` retain their signatures and do
not expose or accept versions. `get_with_version` is the sole additive
accessor, for diagnostics and contract probes. Unversioned or malformed
stored values are integrity failures, never absence.

All keyspace values are versioned. The module has no key type or
constructor that can enforce a create-once/mutable split: every valid
key is reachable by `compare_exchange`, so exempting selected callers
would be documented discipline rather than structural enforcement.

### Proof and measured closure

- A11 (`a11_versioned_aba_cycle_rejects_recycled_era_etag`):
  A(v0) -> B(v1) -> A(v2) preserves caller-visible A bytes while the
  stored etag differs; the A(v0) token is rejected at A(v2).
- A12 (`a12_same_version_identical_payload_cas_succeeds`): a current
  token may CAS to an identical payload; it lands as the next version.
- A13 (`a13_version_strictly_monotone_under_concurrent_cas`): concurrent
  contenders produce exactly the versions 1 through N, with no
  duplicate or skipped successful era.
- A14 (`a14_loopback_content_etag_probe_raw_hazard_wrapped_closure`):
  the loopback now models content-derived etags. Its raw A -> B -> A
  cycle recurs A's etag and accepts the stale writer, while the same
  cycle through `AtomicKeyspace` produces A(v0) and A(v2) with distinct
  etags and rejects the stale writer.

The kernel-owned real-S3 probe retains the raw hazard measurement and
adds the wrapped module companion. A raw content-etag hazard is now an
observed backend fact rather than the rig's terminal verdict; the rig
fails unless the wrapped cycle has versions 0, 1, 2, does not recur the
era-zero etag, rejects that stale token, and accepts a current-token
identical-payload transition at version 3.

### Lifetime boundary

The ruled mechanism defines monotonicity over a key's uninterrupted CAS
lifetime. `delete` followed by `create` starts a new version-0 lifetime,
as required by the create contract; an identical version-0 recreation
can therefore reproduce object bytes on a content-etag backend. Closing
tokens across deletion would require an incarnation field or retained
tombstone, neither of which was authorized by the `{version, payload}`
mechanism. Cleanup owners must not carry CAS tokens across deletion; a
stronger cross-deletion guarantee is a separate human ruling.

K1-K7 do not move. The change is confined to the assured kernel closure
and its contract/probe surfaces; application callers do not migrate.

---

## Ruled addendum: Batch 6 — tombstones (existence witnesses)

Status: accepted (human-authorized kernel batch 6, deferred-items
ruling session 2026-08-21; built on batch 5's certified trim).

### The problem

`read_head_state` returned `Absent` for both a never-created lineage
and one whose head was deleted; callers distinguished them through
parent-aggregate witnesses (the registry lineages) — a convention,
not a mechanism. This batch puts the distinction in the kernel: the
witness convention, mechanized.

### The design

1. **Tombstone records.** On intentional deletion —
   `AtomicKeyspace::destroy(key, cause, actor)` (the spec's
   tombstoning `delete`; plain `delete` stays the raw object delete
   used by GC and damage tests) and `StateKernel::destroy(cause,
   actor)` (the lineage head deletion) — an immutable create-once
   witness is written BEFORE the deletion: keyspace
   `tombstones/{key}`, lineage `{lineage}/tombstone`. It carries
   `{deleted_at_gen, cause, actor, ts}` — proof the key existed and
   was deliberately deleted, by whom, why, and at which generation
   (keyspace version / head generation). Destroying an absent key
   fabricates nothing (idempotent no-op).
2. **The existence read.** `AtomicKeyspace::read_state(key)` →
   `KeyState`: `Present { value, etag }` | `Destroyed { tombstone }`
   | `Absent` | `OffsetExpired { first_retained }`. Lineage:
   `LineageHeadState` gains `Destroyed(Tombstone)`. Malformed stored
   tombstones are `TombstoneCorrupt`, never absence (law 7).
3. **Create-after-destroy supersedes.** A re-create succeeds as a
   fresh identity (version 0 / generation 0); the new existence IS
   the truth (`Present`), and the tombstone remains as historical
   record until a certified trim retires it. A lineage reborns the
   same way through a fresh genesis; its records survive destroy
   (repair-by-replay stays possible; record sweeping is deliberately
   unshipped).
4. **Immutability.** The reserved `tombstones/` prefix refuses
   direct `create`, `compare_exchange`, `delete`, and `delete_many`
   (`TombstoneImmutable`). Tombstones are removed only by the
   batch-5 sweeper: `delete_below` now also sweeps mirrored
   `tombstones/{data_prefix}{seq}` below the floor, and `read_state`
   of a seq-shaped key below the namespace's root certificate reads
   `OffsetExpired` — the certificate witnesses the history, never
   object absence, and never a zombie's `Present`. (Lineage
   tombstones are not seq-keyed and are immortal.)
5. **Tombstones are raw objects**, not versioned envelopes: they are
   create-once by construction (put-if-absent, first witness stands
   across re-created lifetimes), so the versioning that protects
   CAS'd values is unnecessary; immutability is enforced by the
   guarded prefix instead.

### Contract suite (W1–W5)

`crates/yeetz-s3-kernel/tests/tombstone_contract.rs`:
`w1_present_destroyed_absent_lifecycle`,
`w2_create_after_destroy_supersedes`,
`w3_tombstones_are_immutable`,
`w4_trim_retires_tombstones_below_the_floor`,
`w5_lineage_tombstone_equivalent`.

### Not shipped (deliberate)

- Record sweeping on lineage destroy (replay repair stays possible).
- Tombstone GC beyond the certified trim floor.
- Stream-log tombstones: streams keep their `OffsetExpired` contract
  (batch 5); per-event tombstones would double storage for no new
  distinction.

---

## Ruled addendum: Batch 7 — incarnation counters (cross-deletion CAS protection)

Status: accepted (human-authorized kernel batch 7, deferred-items
ruling session 2026-08-21; the final kernel batch). Builds on batch
4 (versioned values) and batch 6 (tombstones).

### The gap batch 4 could not close

Versioned values make versions strictly increase within a key's
lifetime — but delete the key and recreate it, and the version
resets to 0: an era-1 etag could match an era-2 value of identical
bytes on a content-derived etag backend (the exact ABA shape the
batch-4 lifetime boundary documented as the caller's problem). Batch
6 proved destruction; batch 7 makes the recreation unable to fool a
CAS.

### The mechanism

1. **Incarnation counter per key**: `incarnations/{key}` — a raw
   big-endian u64, created once (put-if-absent at 1 on the key's
   first destroy), advanced only by monotone CAS (re-read/re-derive
   on contention; gaps possible, decreases impossible), never
   directly writable or deletable by callers
   (`IncarnationCounterImmutable`), removed only by a certified trim
   sweep. Absent counter = incarnation 0 (the first lifetime).
2. **Envelope v2**: every value envelope is now
   `{incarnation, version, payload}` (`yeetz-keyspace-value/v2`).
   Identical payloads in different lifetimes therefore have
   different object bytes — and different content etags on any
   backend. v1 envelopes fail closed (a deliberate format break:
   the kernel is 0.x, pre-release stores only).
3. **CAS**: `compare_exchange` advances the version WITHIN the
   current incarnation (only `destroy` moves the incarnation). On
   mismatch, `PreconditionFailed` now carries the observed
   incarnation and version — a stale-era CAS names the era it lost
   to. The rejection itself is structural: era-1 bytes ≠ era-2
   bytes, so no etag crosses the boundary.
4. **destroy**: writes the tombstone (which now records the closed
   lifetime's incarnation — format v2), then bumps the counter, then
   deletes the value. A re-create starts at version 0 of the NEW
   incarnation (batch 6's supersession, now era-fenced). Lineages
   record incarnation 0 in their tombstones (heads have no
   incarnation model; their rebirth is a fresh genesis).
5. **Trim**: `delete_below` also sweeps mirrored incarnation counters
   below the floor. A trimmed-and-recreated key starts at
   incarnation 0 again — sanctioned: the trim certificate witnesses
   the erased lifetimes, and `OffsetExpired` protects readers of
   that history.

### Contract suite (I1–I6)

`crates/yeetz-s3-kernel/tests/incarnation_contract.rs`:
`i1_stale_era_cas_across_recreate_is_rejected`,
`i2_versions_reset_across_incarnations_not_within`,
`i3_incarnation_never_decreases_across_cycles`,
`i4_tombstone_records_the_destroyed_incarnation`,
`i5_trim_retires_counters_with_the_history`,
`i6_same_incarnation_cas_semantics_preserved`.

With batch 7, the kernel's deferred-items plan is complete: one
storage truth, certified retention, witnessed existence, and
era-fenced CAS across every boundary the object model has.
