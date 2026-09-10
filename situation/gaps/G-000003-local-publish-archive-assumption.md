# G-000003 — Release helper's local-publish-archive assumption

## State

open

## Gap

The native release helper's post-upload confirmation assumes `cargo
publish` leaves the packaged archive at the build/package path
(`build/package/<crate>-<version>.crate` under the output's target
directory). It does not: after a successful upload the helper's
confirmation and retention step fails on the missing file, and the
uploaded crate is left `unconfirmed` in the run manifest. The helper
lacks a post-upload confirmation that binds a verified local candidate
to the authoritative registry checksum.

## Relevance

P-000008 clauses 3 and 4 — per-crate index confirmation before the next
crate, and a confirmed status persisted before fallible retention steps
— ride on that confirmation step, as do O-000008's executable P7/P8
legs. Because a published registry version is immutable, the honest
recovery is checksum reconciliation against the sparse index, never
re-upload.

## Evidence

- Blocked publish run, one successful upload then helper failure:
  https://github.com/cleverunicornz/yeetz-s3-kernel/actions/runs/34466876511
  (W-000023)
- `situation/witnesses/P-000008/W-000023-partial-publish-blocked.md`
- Registry truth for the uploaded crate despite the `unconfirmed`
  manifest status: `yeetz-sdk-core` 0.5.0, checksum
  `7b77d92b92fbb725f781607cee2952c65e8774feef2a6ae20c7bee952a446b7e`,
  https://index.crates.io/ye/et/yeetz-sdk-core
- Main's independent byte-equality verification of the registry
  artifact against the pre-verified M candidate (W-000023 evidence)
- The faulty assumption in the M source, pinned as historical bytes:
  `f1f7f2932737424718af0b2016245d6bdbdee002:tools/release_crates.py`
- The four publish runs at M, in dependency order — 34466876511,
  34467951876, 34468393355, 34468827250 — each failed after one
  successful upload on this assumption; later runs checksum-skipped
  already-confirmed crates
- Completed registry/release reconciliation at M:
  `situation/witnesses/P-000008/W-000024-registry-release-reconciliation.md`

## Impact

Recovery at M is complete: all four 0.5.0 registry versions are
confirmed and the release exists, so this gap no longer blocks any 0.5.0
outcome. What remains is the normal uninterrupted
upload-and-confirmation path: at M each new upload run exits failed
after its successful upload, and O-000008's P8 executable leg stays
uncreditable in-run until the fix is landed and exercised.

## Resolution

The fix is implemented on this branch (working tree at the time of this
record): the helper retains the verified candidate hash and confirms
against the authoritative sparse-index checksum instead of the local
tarball. Linux regression coverage is pending; no outcome is claimed
here; closure will be recorded when evidenced.

## References

- `situation/promises/P-000008-native-crate-publication.md`
- `situation/oracles/O-000008-native-crate-publication.md`
