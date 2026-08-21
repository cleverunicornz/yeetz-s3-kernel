# ADR 0002: The streams crate — append-only event logs on AtomicKeyspace

Status: accepted

Provenance: renumbered from yeetz ADR 0017 (`0017-streams`) when the
kernel was extracted to this repo; content otherwise carried verbatim.
ADR numbers below 0016 (e.g. 0011) refer to the parent yeetz decision
log, `yeetz/docs/architecture/decisions/`.

## Authorization trail

Human-authorized. The streams consensus (Fable's design at weight 5,
Sol's cross-review at 5.6, Fugu Ultra's tie-break, human rulings)
produced the design below verbatim; kernel batches 1+2 (PRs #62, #66)
landed the surface it consumes. This ADR + the new crate are firewalled
to `crates/yeetz-s3-streams`, its tests, this document, and workspace/Cargo
registration. Forge integration is a following phase; this crate's API
is forge-agnostic by law (opaque stream IDs, opaque payloads, no yeetz
types anywhere).

## Damage matrix (per object class: today vs after)

The forge's current stream (ADR 0011: `events/<o>/<r>`, seq =
generation+1) hangs every repo's events off ONE kernel lineage, so each
append serializes through that lineage's head CAS, replays walk the
lineage fold, and the projection (`rebuild_repo_issues` and friends)
treats the event log as a side artifact of issue state. The streams
crate replaces that mechanism (at integration time — not in this PR)
with per-stream logs of immutable, individually-addressed event
objects.

| Object class | Behavior today (ADR 0011 stream) | Behavior after (this crate) |
|---|---|---|
| Event object | Record inside the repo's event lineage; seq = lineage generation+1; addressing is lineage-relative (fold to reach) | One immutable object per event at `streams/v1/<id>/log/<seq:020>`; the object's conditional create IS the allocation; individually addressable, replay never folds |
| Allocation | Serialized through the repo lineage head CAS (single writer wins per generation; contention retries the whole append) | Per-seq conditional create — concurrent appenders collide only on the same seq; losers advance +1; one winner per seq |
| Retry semantics | Append retry after a lost response re-runs the head CAS against a re-read head — the same event can land twice at different generations | Byte-identical retry with the same stable event id reads back and is recognized: idempotent success, same receipt |
| Read/replay | `fold` over the lineage; gaps impossible (lineage integrity), but cost is O(history) and any integrity failure is lineage-wide | Computed-key GETs, bounded-parallel, clamped; contiguity checked explicitly; damage is per-seq and NAMED (`Corrupt { missing_or_mismatched_seqs }`) |
| Completeness | Implicit in the lineage fold (head is truth) | Explicit: `complete=true` only after a bounded ordered LIST probe past the last fetched key; a LIST that contradicts a fetched witness fails closed (`BackendUnqualified`) |
| Tail discovery | Head read (O(1), authoritative) | Verified tail hint (CAS'd accelerator, allowed to lag) or LIST-derived max; never an unverifiable high hint; self-heals via exponential probe + binary search over computed keys |
| Cursors | `cursors/<consumer>/...` per ADR 0011, forge-typed | CAS pointer objects `{stream_id, seq, event_id, format_version}`; advance validates the target event exists; monotonic-only; missing cursor = replay from start |
| Trim/retention | none possible | none possible in v1 — deliberately: no DELETE, no floor, no epoch, no fencing (Fugu's ruling; trim deferred to a future fenced/retirement ADR; mode-A read-side filtering and mode-B CAS-fencing documented as reserved, neither shipped) |
| Damage loudness | Integrity failure = lineage-wide `StateHistoryIncomplete` | Per-seq `Corrupt` naming the seqs; decode failure is an error, never a skip; deleted mid-log object breaks contiguity loudly; deleted accelerators cost performance only |

## The design (consensus, verbatim)

### Layout

- `streams/v1/<opaque-stream-id>/log/<seq:020>` — 20-digit zero-padded
  decimal seq. Stored under the kernel keyspace root, so the physical
  object key is `keyspace/streams/v1/<id>/log/<seq:020>`.
- seq 0 = immutable genesis/config record (its conditional create
  defines the stream's existence).
- The digest lives in the ENVELOPE, never the key; key↔envelope
  agreement (stream id and seq) is verified on every read.
- Envelope: `{format_version, stream_id, seq, stable_event_id,
  schema_id, payload_len, payload_sha256, payload}` (JSON; payload
  base64). Envelope bytes are the object bytes — one object per event.
- Stream IDs are opaque, immutable, minted at creation; the application
  maps names→IDs at its boundary.

### v1 is APPEND-ONLY

No DELETE, no trim, no floor, no epoch, no fencing. Mode-A (read-side
filtering) and mode-B (CAS-fencing) are reserved designs, documented,
not shipped.

### Append

- The event object's conditional create IS the allocation — no counter.
- Starting guess comes from monotone floors only: a verified tail hint
  (envelope digest at the hinted seq matches the hint's terminal
  digest) or a LIST-derived max. Never an unverifiable high hint.
- On `PreconditionFailed`: read the key back —
  byte/digest-identical with the same stable event id = idempotent
  success (return the receipt); different = advance +1 and retry.
- Retry budget with backoff; exhaustion = typed error.
- The successful create is the linearization point.
- After landing from a verified-hint floor, the tail hint is advanced
  by CAS (monotone; a lost race is fine — the hint is an accelerator).
  A LIST-derived floor does not write the hint (its prefix density is
  unvalidated); the read path rebuilds it instead.

### Tail hint

`{highest_validated_dense_seq, terminal_record_digest}` — a CAS'd
monotone accelerator, written only after validated contiguous truth,
allowed to lag, never declaring EOF, self-healing in the read path
(exponential probe + binary search over computed keys; density makes
existence monotone).

### Reads

Six-state typed API: `NotFound | Empty | Page { events, next_seq,
complete } | Corrupt { missing_or_mismatched_seqs } | Unavailable |
BackendUnqualified`.

- Clamp → bounded-parallel computed-key GETs → contiguity required
  (first seq must be after_seq+1; a hole inside the range stops the
  page and is adjudicated by LIST) → envelope/digest verification →
  decode failure = error, never skip.
- `complete=true` ONLY after a bounded ordered LIST probe past the
  last fetched key. The strong-LIST backend contract is a hard
  qualification: a LIST that errors, or that contradicts a fetched
  witness (stale under-report), fails closed to `BackendUnqualified`.
  Stale-LIST can only under-report — staleness never loss.
- A missing first seq adjudicated by LIST as "no further log keys" is
  the end (Empty/complete); as "a later log key exists" is `Corrupt`
  naming the missing seqs.

### Cursors

CAS pointer objects `{stream_id, seq, event_id, format_version}`.
Advance validates the target event exists, is monotonic-only (target
seq must exceed the current), and moves by CAS. A missing cursor means
replay from start. Pull-only crate: no delivery, no push, no bus.

## What the crate does NOT do (deliberate)

- No trim, no deletion of any log object (S5 asserts the absence).
- No delivery/push/bus — consumers pull.
- No forge types, no name→ID registry (application boundary).
- No new kernel surface: it consumes `AtomicKeyspace`
  (create/get/get_with_etag/compare_exchange/list_after) only. The
  terminal-read/taxonomy surface is for lineages and is not needed
  here — streams are not lineages.

## Consequences

- One object per event: storage cost per event is the envelope
  overhead + payload; listing a stream's log is a prefix walk.
- Concurrent appends to one stream converge without coordination; the
  seq each event lands at is allocator-chosen, not caller-chosen.
- A stale-LIST backend can make appends land below existing events
  (LIST-derived max under-reports); the resulting gap is loud
  (`Corrupt`) on read, never silent loss. Reads never trust LIST alone
  for anything a computed-key GET can witness.
- Completeness is qualified per read; a backend that cannot honor
  strong LIST yields `BackendUnqualified` rather than a false
  `complete=true`.

---

## Addendum: forge integration and the migration contract

Status: accepted (human-authorized quiesced migration).

### Forge integration — emission intents

The forge's write paths embed an
`EmissionIntent { kind, ordinal, actor }` in the source aggregate record
at the commit site that emits. Persisting the acting user is required
for status-only issue successors: the aggregate otherwise retains only
the original poster, which cannot reproduce the request event bytes.
Issue genesis carries a create/pull intent at ordinal 0; issue
successors carry comment intents at `comment_index + 1` and status
flips at ordinal 0, so no two intents from one record share an ordinal.
Release genesis carries one at publish unless draft; package tag
pointers carry one when the digest actually moved. The request path
appends the event immediately. A supervised
reconciler task in `serve` re-derives intents by folding source
lineages and appends missing events at-least-once — idempotent via
stable ids derived identically on both paths:
`sha256(lineage ∥ generation ∥ payload-sha256 ∥ ordinal)`.

### The migration contract (quiesced ADR-0011 → ADR-0017)

1. **Quiesce** — trivially satisfied pre-deployment (no live writers).
2. **Canonical traversal** — fold each `events/<owner>/<repo>` chain at
   an exact head; digest ancestry only (`objects/` is never listed —
   orphaned CAS-loser candidates live there). Any
   `StateHistoryIncomplete` or record failing the strict fold ABORTS
   loudly as damage — never skipped, never repaired.
3. **Mapping** — one opaque stream id per stream, committed in the
   repo aggregate (rename-safe); the packages pseudo-streams map
   through the `forge/stream-map` keyspace (no repo aggregate exists).
4. **Copy** — seqs and payloads preserved; genesis at 0; migrated
   stable ids derive from the OLD event's identity:
   `sha256(old lineage ∥ # ∥ old seq ∥ # ∥ old head digest)`.
5. **Density** — 1..=N verified, count == chain length, digests
   verified (`Streams::migrate_log`'s post-condition).
6. **Seal** — an immutable per-stream object
   `{format_version, source_lineage, source_head_digest,
   event_count, event_root_digest}` (create-once; disagreement is a
   typed error). The seal is also the tail-erasure witness: the
   streams layer cannot detect erasure of the final event without an
   external record (data-model erasure floor).

Old `events/<o>/<r>` lineages stay in place read-only (deletion is a
later cleanup PR). Old cursor lineages were test-only and retired with
the old layout; a deleted ADR-0017 cursor reads as start (at-least-once
re-delivery — the safe direction).

---

## Ruled addendum: bounded-window idempotency and witness-bounded completeness

Status: accepted (human-ruled contract changes, recorded verbatim in
intent; this section amends the contract above without rewriting it).

### Ruling 1 — idempotency is bounded to the pre-scan window

Stable event ids guarantee an idempotent retry ONLY within the
pre-scan window (recent bounded retries). The window is the seqs from
`max(1, tail-hint floor, LIST-derived max − 16)` through the
LIST-derived max, plus the exact-seq readback of the attempted
conditional create.

- **In window, same stable id + identical schema and payload**: the
  original receipt is returned (unchanged behavior, S3).
- **In window, same stable id + changed payload or schema**: a typed
  `IdempotencyConflict` error naming the conflicting seq. This error
  did not exist before this ruling — previously the event silently
  landed at another seq (ambiguous duplication); it is now a typed
  conflict, never a silent second landing.
- **Beyond the window**: a re-append of the same logical event (same
  stable id, identical bytes) SUCCEEDS as a new event. Duplicate
  logical events are possible by contract beyond the window —
  at-least-once philosophy; consumers dedupe by event id.

### Ruling 2 — completeness requires a verified witness (G130)

Teardown finding G130: a frozen (stale, under-reporting) LIST plus an
absent/deleted tail hint let a limit-cut read return `complete=true`
while events existed past the page end — the LIST alone certified
completeness, and with the witness gone nothing contradicted it.

Ruled contract: `complete=true` ONLY when BOTH hold:

1. A VERIFIED tail hint exists — the hint object is present AND its
   named terminal record is confirmed present with a matching digest
   (a witness a stale LIST cannot hide, because hints are read by
   computed-key GET).
2. The ordered probe beyond the last fetched/verified sequence
   returned empty.

No verified hint → `Page{complete: false}` always — completeness is
withheld, never guessed. The read path recovers the witness by
computed-key GET probes (exponential probe + binary search; immune to
a frozen LIST), so the next read can certify; a limit-cut re-read
whose recovered witness sits above the page end fails closed as
`BackendUnqualified` (S9 semantics) rather than false-completing.
`migrate_log` writes the hint after its fully verified density pass —
a migrated stream certifies reads exactly like an append-built one.

The honest claim behind `complete=true`: **no suffix visible under
the qualified backend contract, bounded by a witness.** Standing
erasure floor (explicitly not covered): a suffix deleted together
with ALL witnesses (tail hint, migration seal) is undetectable at
this layer.

### Contract witnesses

- `idempotency_window_conflict_is_typed` (in-memory, ruling 1a)
- `idempotency_beyond_window_reappend_lands_as_new_event`
  (in-memory, ruling 1b)
- `g130_frozen_list_without_witness_withholds_completeness`
  (loopback, ruling 2 regression)
- `s6_accelerator_loss_and_recovery` (loopback, updated to the ruled
  withhold-then-certify contract)

---

## Ruled addendum: real-backend etag semantics affect the CAS
## surfaces (ruling #3, cross-reference)

The streams crate's CAS surfaces (tail-hint advance, cursor advance)
inherit AtomicKeyspace's `compare_exchange` semantics, whose ABA
position lives in ADR 0001. The real-backend probe (ruling #3,
`rigs/examples/real_s3_aba_probe.rs`, ci-dev `real-s3` task)
measured that Exoscale SOS etags are content hashes: identical bytes
recur an identical etag across incarnations, and an If-Match with
the era-1 etag is accepted against a byte-identical recreated
era-2 object. Consequences for this crate: CAS discriminates values
(the hint/cursor payloads embed monotonic state — seq, digest — so
distinct candidates always carry distinct etags and stale CAS is
rejected, measured), but cannot discriminate eras of byte-identical
content; v1's append-only discipline (no log-object deletes) keeps
the log itself outside that ambiguity. The full measured verdict and
the open versioning decision are recorded in ADR 0001's ruled
addendum.
