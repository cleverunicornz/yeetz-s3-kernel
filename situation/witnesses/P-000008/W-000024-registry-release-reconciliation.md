# W-000024 — Completed registry and release reconciliation at M

## Promise

`situation/promises/P-000008-native-crate-publication.md`

## Oracle

`situation/oracles/O-000008-native-crate-publication.md`

## Result

BLOCKED

## Head

`f1f7f2932737424718af0b2016245d6bdbdee002`

## Observed

2026-09-10

## Evidence

- Authoritative public receipt, attached to the release:
  https://github.com/cleverunicornz/yeetz-s3-kernel/releases/download/v0.5.0/publication-receipts.json
  (SHA256 `8380b9fa28472b10960a12989751d2615f7ccad6b22f3af0d457fc7d620c7efc`).
  It carries all four sparse-index and static-archive URLs, the exact
  source SHA, publication times, and run links.
- All four 0.5.0 registry versions are confirmed. Main independently
  downloaded each registry archive and verified it byte-for-byte equal
  to the pre-verified candidates from the native M package run
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34466411701
  — no bypass of any check.
- The four publish runs at M, in dependency order —
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34466876511,
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34467951876,
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34468393355,
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34468827250
  — each FAILED after one successful upload because of the old
  local-tarball assumption (G-000003). Subsequent runs checksum-skipped
  the already-confirmed crates. There is no fifth or redundant success
  run: no publish run at M ever ran to completion.
- Release https://github.com/cleverunicornz/yeetz-s3-kernel/releases/tag/v0.5.0
  published 2026-09-10T11:04:27Z, after the last crate's publication at
  2026-09-10T11:01:25Z and the independent archive checks. The annotated tag object
  `405663773dd7bdcb8070860faecf7bd301ba1baf` resolves to commit
  `f1f7f2932737424718af0b2016245d6bdbdee002`. Six assets — the four
  crates, `SHA256SUMS` (SHA256
  `6c98e4675231a43d1b864ec08c96d64a570b25e23c7d5b1b3ccb29e992c75937`),
  and `publication-receipts.json`. The remote crate-asset digests match
  the registry/native candidates; the two metadata-asset digests match
  the locally uploaded documents.
- Credential lifecycle observed: the temporary repository
  `CARGO_REGISTRY_TOKEN` secret was removed, and a fresh query for that
  secret returned no matching entry.
- Docs.rs rendering succeeded for all four crates:
  https://docs.rs/crate/yeetz-sdk-core/0.5.0/builds/4402960,
  https://docs.rs/crate/yeetz-sdk-s3/0.5.0/builds/4403071,
  https://docs.rs/crate/yeetz-s3-kernel/0.5.0/builds/4403106,
  https://docs.rs/crate/yeetz-s3-streams/0.5.0/builds/4403160.
- Classification: the 0.5.0 release is COMPLETE at this head. This
  witness is NOT a PASS for O-000008 and claims no whole-publisher
  assurance — the oracle's normal in-run post-upload confirmation (P8)
  never completed at M, and other legs remain unexercised or manual.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | Evidenced by the retained package-run log at this head: run 34466411701's preflight held at the pinned SHA and version (publish-run preflights corroborate) |
| P2 | Evidenced at this head: run 34466411701 produced exactly four archives with emitted checksums — the candidates Main later matched against the registry byte-for-byte |
| P3 | Evidenced at this head: run 34466411701's archive checks (LICENSE, version, byte-identity, VCS fields) passed for all four |
| P4 | Evidenced at this head: run 34466411701's normalization checks passed for all four |
| P5 | Executable part evidenced by retained logs of 34466411701 and the publish runs: `--locked`, no bypass flags, verification retained; the manual no-bypass source leg is not credited by this witness |
| P6 | Evidenced by the retained publish-run logs: all four publish runs at M passed the merged-into-`main` ancestry and version checks before any registry contact |
| P7 | Each crate exercised absent-and-uploaded in dependency order; later runs exercised the checksum-match skip branch for already-confirmed crates. Duplicate-V refusal remains unexercised |
| P8 | Not achieved in-run: every publish run at M failed at the post-upload confirmation/retention step (G-000003). Registry confirmation of all four versions exists only through external reconciliation — receipt, index checksums, Main's byte-equality verification — which credits no executable P8 evidence |
| P9 | Exercised and held on every failing run: honest per-crate statuses persisted (`unconfirmed`/`not-attempted`, then checksum-skips), no misreport, no retention/publication conflation |
| P10 | Receipt reconciliation establishes an annotated tag at this head and release creation after all four independent archive checks. The release timestamp 11:04:27Z follows the last crate publication at 11:01:25Z; crate assets match registry/native digests, and the two metadata assets match their uploaded documents |
| P11 | The complete manual source-inspection leg is not credited by this witness. Observed facts: the publish step held the token, the temporary secret was removed, and a fresh query for that secret returned no match |
