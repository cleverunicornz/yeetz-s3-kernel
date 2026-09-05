# Bedrock correction report

## Run

- Run ID: `20260905T015844Z-896d90685fa749a1a735c35e6b3479747d25b93f`
- Branch: `centralized-bedrock-proof`
- Opening checkpoint: `179a7e9b5c99de8f817859270ba0b1c3026745a7`
- Required semantic baseline / opening trigger: `896d90685fa749a1a735c35e6b3479747d25b93f`
- Published closure-record head reviewed by this correction: `34a589cc740d04d50e7bdf95c62b9b18aa674a28`
- Classification: `EVOLUTION` / `DELTA` / `OWNED`

## Reconciled docket disposition

### D1 — blocked: mounted semantic workspace must be re-provisioned or rebound

The mounted semantic-tool schemas were inspected before use. The only exposed
workspace operations are `semantic_index_status` and `semantic_search`; neither
accepts a baseline, workspace, repository-path, re-provision, or rebind
parameter.

`semantic_index_status` was invoked in the default mounted workspace with
`include_changed_paths: true`. It failed before providing coverage with:

> Baseline checkout HEAD does not match the pinned baseline revision

Its returned index context reported `index_state: error` and
`read_your_writes: false`. The failure was reported through
`xd://report_issue`. Rebinding the mount to the required opening trigger and
refreshing its delta through the branch head is not available through the
mounted tool interface, so fresh baseline/delta coverage and read-your-writes
cannot be confirmed.

### D2 — blocked by D1; no semantic-derived correction is claimed

A default-workspace `semantic_search` was nevertheless attempted for the exact
bounded concepts: opening/closure lineage, empty source delta,
ownership/context, root protocol/README orientation, and the
Promise/Oracle/Witness/Decision/Gap/Candidate/Plan lifecycle. It failed with
the same pinned-baseline error, returned no results, and reported
`read_your_writes: false`. That failure was also reported through
`xd://report_issue`.

Because no successful semantic search exists, this record makes no
semantic-derived forward disposition and identifies no contradiction to correct.
The published `closure-report.md` is preserved unchanged. After D1 is repaired,
a corrector must repeat the specified default-workspace discovery against the
then-current branch head and record any verified forward correction.

### D3 — not runnable while D1 and D2 remain blocked

No independent validator can obtain the required successful
`semantic_index_status` and `semantic_search` until the mounted workspace is
rebound or re-provisioned. Accordingly, this correction does not request or
claim `APPROVED`. Once D1 is healthy and D2 has completed successfully, an
independent bounded validator must review this forward correction commit,
verify the pushed branch head and clean worktree, and issue its disposition.

## Non-actions

The opening checkpoint, closure report, protocol-owned root material, README,
behavioral records, and source files were not modified. No formatter, linter,
build, or project-wide test suite was run.
