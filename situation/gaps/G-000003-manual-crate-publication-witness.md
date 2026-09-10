# G-000003 — Manual crate-publication witness

## State

open

## Gap

No PASS witness presently records a manually dispatched
`.github/workflows/publish.yml` run at its selected source and applies
O-000009 to it.

## Relevance

P-000009 is implemented workflow behavior. Its configured Cargo command does
not become assured publication behavior without a run observation at the
judged source; the workflow itself identifies its run log as the publication
witness.

## Evidence

- `c733386f3577fd6c10322d14dc9c5f07baa6f667:.github/workflows/publish.yml`
  adds the manual workflow that P-000009 describes.
- https://github.com/cleverunicornz/yeetz-s3-kernel/pull/50 — the admitted
  owner-directed scope says no publish run is dispatched for the already
  published 0.5.0 version.
- No `situation/witnesses/P-000009/` record is present in the current tree.
- `situation/decisions/D-000009-manual-crate-publication.md` and
  `situation/oracles/O-000009-manual-crate-publication.md` retain the selected
  route and the judgment rule awaiting an observation.

## Impact

P-000009 cannot advance from `implemented` to `assured`. This record makes no
claim about registry state, successful publication, or the behavior of Cargo
outside a retained run.

## Resolution

None. A later manually dispatched run and its retained Actions URL may provide
a witness; no Candidate is proposed by this absence alone.

## References

- `situation/promises/P-000009-manual-crate-publication.md`
- `situation/oracles/O-000009-manual-crate-publication.md`
- `situation/decisions/D-000009-manual-crate-publication.md`
