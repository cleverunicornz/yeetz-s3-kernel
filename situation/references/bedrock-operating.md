# Installed by bedrock v0.2.1 — DO NOT EDIT. This file is owned by the tool. Base changes: file an issue or PR at https://github.com/cleverunicornz/bedrock. Local refresh: bedrock update.
# Bedrock operating reference — the base protocol

This is the standardized minimum every bedrock repo builds on, plus the
contract of the `update` verb. bedrock ships this text compiled into the
binary and installs it at `situation/references/bedrock-operating.md` on
`init`, `adopt`, and `update`. Every installed base file carries a
provenance stamp naming the generating bedrock version — machine-owned,
never edited locally. Rule C10 (digest-skew) fails a repo whose installed
copy differs from this binary's canonical stamped form (embedded template +
provenance stamp for the current version) and names `bedrock update` as the
fix. Repo law layers on top of this protocol; it never rewrites it.

## THE CHAIN: promise, oracle, witness, residual

A promise states behavior; an oracle judges promise against actual; a
witness is the retained observation of one run and proves existence, never
exclusivity; residual is what was deliberately not assured, declared so
everyone sees the line of demarcation — we assure the positive space only;
the negative space is infinite.

### Field mapping

| chain role | vertex field | meaning |
|---|---|---|
| promise | `intent` (plans), `statement` (invariants, terms, records) | the behavior claimed |
| oracle | `acceptanceCriteria` — inline and required — plus an optional `oracle` ref | the standing comparator that judges the promise against actual |
| witness | `witnesses` — CI-run-URL class, never a local attestation | the retained observation of one run |
| residual | `disposition.residual` | what was deliberately not assured, declared out loud |

Hard rule: `disposition.state: done` with zero `witnesses` is a `bedrock
check` violation (rule C8) — no witness, no done.

## Base ontology

| namespace | named graph | allowed base @types |
|---|---|---|
| definition/ | `https://yeetz.dev/graph/definition` | Invariant, Breadcrumb, Term |
| architecture/ | `https://yeetz.dev/graph/architecture` | Identity, SituationStructure |
| risk/ | `https://yeetz.dev/graph/risk` | Risk |
| plan/ | `https://yeetz.dev/graph/plan` | Plan |
| record/ | `https://yeetz.dev/graph/record` | EpochRecord, DeployRecord, ReflectVerdict |

Base @types are the bedrock vocabulary; consumers never add to it.

## Edge vocabulary

The set is closed. It grows only when a new relationship cannot be
expressed faithfully with what exists — a rename or a collapse is never an
excuse to mint a new edge.

- `requires` — a vertex depends on another vertex being true.
- `references` — a vertex points at a depth document or related vertex.
- `consumes` — a vertex draws on a listed vertex or repo path as input.
- `produces` — a vertex yields a listed repo path as output.
- `member-of` — a vertex belongs to another vertex's set.

## Archetype rule

Every vertex carries AT LEAST ONE base @type; repo-specific archetypes ride
alongside it in the `@type` array, under the repo's own IRI base, in
separate extension schema files:

```yaml
"@type":
  - "https://yeetz.dev/<repo>/ADR"
  - "https://yeetz.dev/bedrock/ontology/Term"
```

Extension schemas live outside `seed/schemas/` (the base schemas are
bedrock-owned). Extensions never redefine or shadow a base term: if a base
term already says it, reference it; do not re-state it under a new name.

## A worked vertex

This real installed floor vertex is the canonical example of profile
conformance (every floor vertex in `situation/definition/` is canonical):

```yaml
# Floor invariant 1 of 16 (spec/floor-v2.md §4). Layer: floor.
"@context": "https://yeetz.dev/bedrock/context/v1"
"@id": "https://yeetz.dev/bedrock/vertex/invariant-01-possibility-space"
"@type": "https://yeetz.dev/bedrock/ontology/Invariant"
label: "Code is a possibility space"
layer: "floor"
statement: >
  Code is a possibility space. A surface does everything it *can* do, not what
  its author meant. Work is collapsing possibilities to defined behavior.
```

Profile rules are enforced by `bedrock check` (C2–C5/C7–C9):

- one `@context` — the embedded repo-local context or the exact bedrock
  context IRI; remote context loading is always disabled;
- `@id` and `@type` are absolute IRIs; no blank nodes anywhere;
- no anchors, aliases, or merge keys — in any file;
- no blank nodes — an object value missing an absolute `@id` produces one;
- comments are welcome; they never survive to the compiled graph.

## Authoring loop

1. Write a vertex at `situation/<ns>/<local-name>.yamlld`.
2. Run `bedrock check` — failures are named and line-cited
   (`RULE path:line message`); fix and rerun.
3. Run `bedrock build` — compiles `situation/graph.trig` and regenerates
   the root `AGENTS.md`.
4. Commit the source vertex AND the generated output together.
5. Open a PR; a human merges.

## Execution-graph primitives

A plan carries exactly these seven fields:

- `intent` — the promise, stated as behavior.
- `acceptanceCriteria` — the oracle, inline; required.
- `consumes` — the vertices and paths the plan draws on.
- `tasks` — the ordered work that makes the promise real.
- `witnesses` — retained CI-run-URL observations; required before `done`.
- `reflectDepth` — how deep the follow-up reflection must go.
- `disposition` + `residual` — the closing state and what was not assured.

Deliberately excluded, permanently: no custody choreography, no per-actor
openings or closures, no round ordinals, no confirmation passes. This is a
thin transaction log with measured ceremony — those four mechanisms are not
coming back.

## Refusals

`bedrock` (init, adopt, update, check, build) never does any of the
following:

- no new top-level directories or namespaces under `situation/`;
- no hand-editing of `AGENTS.md`, `situation/graph.trig`, the base schemas
  in `seed/schemas/`, `seed/context.yamlld`, or this reference;
- no writing anything outside `situation/` (and the installed base files)
  — except the root `AGENTS.md`, which `build` regenerates;
- `update` never touches repo-authored vertices, extension schemas, or the
  workflow template once a consumer copy exists.

Installed base files are machine-owned (their provenance stamp says so).
Routing law when one needs changing:

- never patch an installed base file locally — the next refresh erases your
  edits;
- base defects and friction are issues/PRs at the bedrock repo
  (https://github.com/cleverunicornz/bedrock) — agents are encouraged to
  file them;
- `bedrock update` is the only local refresh.

## Re-situate

`bedrock build` regenerates `AGENTS.md` from `situation/`. After work that
changes reality, re-run it and commit the emitted register — knowledge, not
rules.
