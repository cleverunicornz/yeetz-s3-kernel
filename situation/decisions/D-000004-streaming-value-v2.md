# D-000004 — Manifest-committed streamed values

## Status

accepted

## Date

2026-08-22

## Supersedes

`situation/decisions/D-000003-streaming-value-v1.md`

## Context

The initial streaming-value proposal needed an accepted representation with
bounded resource use, explicit stale-era handling, and a portable fallback
where a backend could complete multipart uploads but could not provide
part-addressed integrity reads.

## Evidence

- Historical decision: `96a05336c850895143c297fb47ffb55227b0c4fb:situation/record/decision-0004-streaming-value-io-v2.yamlld`
- Current implementation: `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-kernel/src/value_manifest.rs`
- Current streamed-value contracts: `96a05336c850895143c297fb47ffb55227b0c4fb:crates/yeetz-s3-kernel/src/streaming_contract.rs`
- Historical live capability witness: `https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/32592980637`

## Decision

Use a manifest at the logical key and immutable chunks under a kernel-private
root. The conditional manifest operation is the only publication point;
readers validate the manifest before returning logical bytes. Keep the inline
representation for values within the selected inline band. Make stale-era
cleanup conditional, bound chunk upload concurrency, and require an explicit
maintenance fence for destructive chunk collection.

## Why

A manifest preserves one conditional commit point when portable
part-addressed reads are unavailable. Per-key chunks avoid global reference
counting and its crash-recovery authority. Conditional stale cleanup prevents
a reader from deleting a replacement era.

## Rejected alternatives

- Native multipart as the logical representation: the measured backend did not
  provide the needed portable part-addressed reads.
- Mutable LIST-discovered chunks: a multi-object replacement is not atomic and
  reopens per-chunk ABA hazards.
- Global content-addressed deduplication: it requires crash-safe global
  reference accounting outside the chosen boundary.
- Temporary-object copy publication: it adds transfer cost without replacing
  the manifest's portability properties.

## Consequences

P-000004 and O-000004 bound the assured publication and reader behavior.
The kernel cannot prove the operational quiescence required by destructive
chunk collection; that accepted residual is recorded in
`situation/gaps/G-000001-streaming-gc-quiescence.md`.

## Revisit when

A portable, full witness demonstrates part-addressed reads with the needed
integrity properties, or a replacement representation can preserve the current
conditional-publication and stale-era guarantees.
