# Bedrock recovery report

## Run

- Run ID: `20260905T015844Z-896d90685fa749a1a735c35e6b3479747d25b93f`
- Opening checkpoint: `179a7e9b5c99de8f817859270ba0b1c3026745a7`
- Original trigger tree: `896d90685fa749a1a735c35e6b3479747d25b93f`
- Recovery workspace baseline: `b44197381c6a857e768ec99449933f0d90d8a7b5`
- Classification: `EVOLUTION` / `DELTA` / `OWNED`

This is a forward recovery record. The published `opening.md`, `closure-report.md`,
and `correction-report.md` remain unchanged: they accurately preserve the earlier
semantic-index blocker and do not claim a successful disposition.

## Recovered semantic discovery

The mounted `semantic_index_status` now succeeded at the recovery baseline with
an index state of `current`, `read_your_writes: true`, 123 indexed files, and no
pending changed paths. A successful default-workspace `semantic_search` for the
completed-adoption, DELTA-classification, nested-opening, recovery, and closing
checkpoint concepts located the operation rule, the run recovery and hard-boundary
rule, this run's published records, and the completed BACKPORT adoption record.

The recovered discovery establishes the condition that the earlier records could
not establish: a completed adoption requires a bounded DELTA review, and an
unclosed opening is resumable through a forward closing checkpoint. It does not
re-ingest donor material or revise historical claims.

## Bounded DELTA recovery

The completed BACKPORT adoption closes at
`896d90685fa749a1a735c35e6b3479747d25b93f`. The scoped comparison from that
commit to this run's opening contains only `opening.md`. The scoped comparison
from this run's opening through its pre-nested-opening correction head
`a12cfc4784b2f4821f0db0b8bb027d3466a71dd2` contains only this run's
`closure-report.md` and `correction-report.md`.

Accordingly, the recovered review surface contains administrative run records
only. No product source, configuration, README, Promise, Oracle, Witness,
Decision, Invariant, Gap, Candidate, or Plan is newly reviewed, re-ingested, or
transitioned. The later opening at `44981d33af4462e21b6f50827665ba0444a462a9`
is outside this recovered run and is handled by that run's forward correction.

## Disposition

Semantic discovery is now successful and the bounded DELTA recovery is complete.
`completion.md` and its matching closing checkpoint record the terminal state for
this older run without rewriting any published record.