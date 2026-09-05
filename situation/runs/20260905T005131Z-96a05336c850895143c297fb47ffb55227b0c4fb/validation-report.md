# Bedrock validation report

## Review

- Run ID: `20260905T005131Z-96a05336c850895143c297fb47ffb55227b0c4fb`
- Review interval: `94c39fecb90ca998156078c7532ebab45927d934..a0444fa8b984ad630fabc2c09ec2226e91919039`
- Reviewed head: `a0444fa8b984ad630fabc2c09ec2226e91919039`
- Classification: `EVOLUTION` / `BACKPORT` / `OWNED`
- Verdict: **REVISE**

## Bounded review evidence

Semantic discovery completed before claim review. `semantic_index_status`
reported a current index pinned to trigger revision
`96a05336c850895143c297fb47ffb55227b0c4fb`; successful focused searches covered
BACKPORT provenance and ownership, Promise/Oracle/Witness assurance, and
Gap/Candidate/Decision/Plan lifecycle transitions.

The complete forward interval was reviewed, including all 92 paths in its final
diff and the intermediate correction commits. The opening file has only its
opening commit in history. The installed `situation/*/AGENTS.md` hashes match
`situation/protocol-lock.json`, and the root protocol block is byte-unchanged
through the interval.

The public Actions API for run `32736208926` reports a successful `gates (full
set)` step at `49ba2ced98831d192f6a2371b90aec8e81a081fd` on 2026-08-24. The
historical workflow at that head runs `cargo nextest run --workspace` in the
step, and the source/test/workflow comparison recorded by the assurance work is
empty through the opening checkpoint. The changed boundary checker passes
`bash -n`. No formatter, linter, project-wide build, or project-wide test suite
was run during validation.

## Correction docket

### V1 — Complete the executable-witness corrections for P-000003 through P-000005

P-000001 and P-000002 correctly moved their state evidence to W-000011 and
W-000012. Those witnesses explain that W-000006 and W-000007 cannot be the
executable state evidence because their `Head` and `Observed` fields identify
the later source-identity observation rather than the executable gate's exact
head and date.

The equivalent corrections stop halfway. P-000003, P-000004, and P-000005 still
cite W-000008, W-000009, and W-000010. Each of those records says `PASS` while
naming head `94c39fecb90ca998156078c7532ebab45927d934` and observation date
2026-09-05, although its executable observation is the cited gate run at
`49ba2ced98831d192f6a2371b90aec8e81a081fd` on 2026-08-24. This applies two
incompatible provenance rules to five otherwise equivalent assurance records
and leaves three `assured` dispositions resting on the witness shape that
W-000011/W-000012 explicitly corrected.

Correct this append-only: retain W-000008 through W-000010 unchanged; add one
new historical-gate witness for each of P-000003, P-000004, and P-000005 with
`Head` `49ba2ced98831d192f6a2371b90aec8e81a081fd`, `Observed` 2026-08-24, the
successful public gate evidence, the opening input-identity evidence, and every
leg of the corresponding Oracle; then point each Promise's `State evidence` to
its new witness. Update the closure report's witness range and provenance
summary to describe the completed correction chain.

### V2 — Give PLAN-000001 explicit Promise-state completion targets

`PLAN-000001-streaming-representation-revisit.md` currently completes when any
resulting change to P-000004 has “the target state required by its replacement
records.” That is circular: it names no Promise lifecycle state, and it blurs an
additive optimization with a replacement of an already `assured` Promise. The
Plan contract requires completion to state target Promise states, while assured
behavior may change only through supersession and an atomic Decision/Promise/
Oracle transition.

Rewrite `Completion` to encode this coherent boundary: C-000001 is terminal
(`promoted`, `rejected`, `merged`, or `superseded`); if promoted, the atomic
promotion links its Decision, new Promise, and new Oracle and the new Promise is
`qualified`; P-000004 remains `assured` when the selected behavior is additive,
or becomes `superseded` by that new Promise when the Decision selects a
replacement representation. Update the Plan's links in the same promotion
transaction if promotion occurs.

### V3 — Record ownership directly in the root repository block

`situation/context.md` records `Ownership: OWNED`, but the
`<bedrock-repository>` block in root `AGENTS.md` only says that ownership can be
found in that file. The installed ownership contract requires the
classification to be recorded in both context and root `AGENTS.md`.

Add concise `Ownership: OWNED` orientation (and `Upstream coordinate: none` if
the repository block lists the coordinate) inside `<bedrock-repository>`. Do not
change any byte of `<bedrock-protocol>` and do not add fork procedure for this
non-fork repository.

## Confirmed surfaces

The context classification agrees with the workspace metadata and the public
repository's non-fork identity. The five current Decisions retain resolvable
trigger-tree provenance and the D-000003/D-000004 supersession chain. G-000001
is bounded and accepted by D-000004; C-000001 remains a non-commitment, and no
Candidate promotion is partially applied. The Oracle clauses cover the explicit
Promise clauses inside their stated scopes; V1 concerns the disposition
witnesses, not new behavior outside those scopes. Root README is human
orientation, the changed package README links resolve to current Decisions,
product-functional package documentation remains intact, repository-local
skills are retired, and no competing `AGENTS.md` exists outside protocol-owned
locations.
