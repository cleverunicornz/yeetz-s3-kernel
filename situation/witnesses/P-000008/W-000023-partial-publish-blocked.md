# W-000023 — Partial native publish, blocked after the first upload

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

- Publish run, task `publish` at `ref=f1f7f2932737424718af0b2016245d6bdbdee002`:
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34466876511.
  In-run, all four crates packaged and passed the archive checks
  (LICENSE, normalized manifest, lock, VCS) with Cargo 1.96 `--locked`
  verification retained. The `cargo publish` of `yeetz-sdk-core` 0.5.0
  exited 0 — the upload succeeded. The helper then failed after the
  upload: it assumed `build/package/yeetz-sdk-core-0.5.0.crate` remained
  after `cargo publish`, and it does not. The run's retained
  `release-manifest.json` statuses: `yeetz-sdk-core` `unconfirmed`,
  `yeetz-sdk-s3`, `yeetz-s3-kernel`, `yeetz-s3-streams`
  `not-attempted`. The observation could not run to completion; this is
  a BLOCKED execution, not a PASS and not whole-publisher assurance.
  The invalid assumption is bounded by
  `situation/gaps/G-000003-local-publish-archive-assumption.md`.
- Registry truth for the one uploaded crate, established outside the
  run: `yeetz-sdk-core` 0.5.0 is published with checksum
  `7b77d92b92fbb725f781607cee2952c65e8774feef2a6ae20c7bee952a446b7e`,
  publication time 2026-09-10T10:38:36Z:
  https://index.crates.io/ye/et/yeetz-sdk-core.
- Independent verification reported by Main (public sources): the
  registry artifact
  https://static.crates.io/crates/yeetz-sdk-core/yeetz-sdk-core-0.5.0.crate
  is byte-identical to the pre-verified M candidate from package run
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34466411701,
  and its `.cargo_vcs_info.json` records the exact head SHA, the
  `crates/yeetz-sdk-core` path, and clean status, with the LICENSE
  present. This establishes registry truth for crate 1; it is external
  manual evidence and does not credit the oracle's in-run P8 leg.
- Credential lifecycle observed: the temporary `CARGO_REGISTRY_TOKEN`
  Actions secret was removed after the failed run.
- Head context: `f1f7f2932737424718af0b2016245d6bdbdee002` is the merge
  of PR #48 (https://github.com/cleverunicornz/yeetz-s3-kernel/pull/48)
  after its completed Bedrock closure, under the retained human
  Merge-after-checks authorization
  https://github.com/cleverunicornz/yeetz-s3-kernel/pull/48#issuecomment-5616250101.
- Subsequent publication and release receipts are outside this observation.

## Oracle legs

| Leg | Evidence |
|---|---|
| P1 | Evidenced in run 34466876511: the publish-mode package preflight (workflow guard plus script preflight) executed and held at the pinned head and version |
| P2 | Evidenced in run 34466876511: exactly four archives packaged with checksums; corroborated by package run 34466411701 at the same head |
| P3 | Evidenced in run 34466876511: all four archives passed the LICENSE, version, byte-identity, and VCS-field checks |
| P4 | Evidenced in run 34466876511: all four packaged manifests passed the normalization checks |
| P5 | The retained log of 34466876511 shows `--locked`, no bypass flags, and verification retained; the manual source-absence leg is not credited by this witness |
| P6 | Evidenced in run 34466876511: the merged-into-`main` ancestry and version checks passed before any registry contact (head is the PR #48 merge commit) |
| P7 | Exercised for crate 1 only: `yeetz-sdk-core` adjudicated 0.5.0-absent and uploaded; adjudication of the remaining three crates unexercised in this run |
| P8 | Not achieved in-run: the helper failed at the post-upload confirmation/retention step (G-000003); `yeetz-sdk-core` remains `unconfirmed` in the manifest. Registry truth is established externally (index checksum plus Main's byte-equality verification) but credits no executable leg |
| P9 | Exercised and held on the failure path: the retained `release-manifest.json` reports `yeetz-sdk-core` `unconfirmed` and the other three `not-attempted` — the exact honest state at failure, persisted before exit |
| P10 | Unexercised: tag and release creation were not attempted in this observation |
| P11 | Unexercised by this witness: the manual source-inspection legs await their direct review evidence. Observed operational facts retained above: the publish step alone held the token, and the temporary secret was removed after the failure |
