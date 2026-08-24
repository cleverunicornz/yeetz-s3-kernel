# Installed by bedrock v0.6.0 — DO NOT EDIT. This file is owned by the tool. Base changes: file an issue or PR at https://github.com/cleverunicornz/bedrock. Local refresh: bedrock update.
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
| record/ | `https://yeetz.dev/graph/record` | EpochRecord, DeployRecord, ReflectVerdict, Decision |

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
- `supersedes` — a vertex replaces an earlier vertex, which stays untouched
  (append-only — floor invariant 15).

## Resident working set

`situation/` is the complete canonical store. Every YAML-LD source is
parsed, schema-validated, and edge-validated. Root `AGENTS.md` is the
deterministic resident projection — current operational knowledge, never
the execution archive:

| source | residency |
|---|---|
| definition/, architecture/, current risk/ | compact face resident |
| Plan `active` | routing face resident |
| Plan `draft`, `done`, `abandoned` | cold |
| Decision | resident; complete `supersedes` chain walkable |
| EpochRecord, DeployRecord, ReflectVerdict | cold |
| references/, every `body` | cold |

Cold does not mean hidden or discarded. The resident
SituationStructure vertex discloses every namespace path. A task that needs
history walks `situation/plan/`, `record/`, or `references/`; routine work
does not pay to enumerate it.

Projection closure is hard rule C11: a resident vertex may not point by
vertex IRI at a cold source — that would expose an edge with no target in
the injected graph. Use a repo-path pointer for historical evidence, or
promote the target into resident knowledge.

## Vertex anatomy — face and depth

A vertex file is ONE YAML-LD document. For ordinary resident knowledge:

- **face** — identity, routing statement/gate, and relationships; resident;
- **body** — one final `body: |` literal block scalar of unbounded node-local
  depth; never resident.

An active Plan is stricter. Its resident routing allowlist is @type, label,
intent, consumes/requires/references/produces/member-of/oracle/source/path,
synthesized state `active`, and its automatic document edge. Structured
execution payload — acceptanceCriteria, tasks, witnesses, reflectDepth,
residual, statement, body — remains validated but cold in that same file.

`bedrock check`/`build` report exact artifact bytes, resident/cold counts,
and a SOFT line when any resident face exceeds 4096 chars (≈1k tokens).
Target 500–1000 tokens. Soft is advisory, never a violation.

Use `|` (literal) for bodies — it preserves headings, lists, and code.
Use `>` (folded) for compact single-paragraph routing prose. Editing only
cold content leaves AGENTS.md byte-identical; following the resident
`document` edge reads the complete current source on demand. Shared depth
stays in `references/`, linked by a path.

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

Profile and projection rules are enforced by `bedrock check`
(C2–C5/C7–C11):

- one served `@context`; remote loading disabled;
- absolute `@id`/`@type`; no blank nodes;
- no anchors, aliases, or merge keys;
- every source edge resolves;
- every Plan declares draft|active|done|abandoned;
- every resident vertex edge resolves to another resident or a repo path;
- comments, bodies, and cold execution payload never reach the projection.

## Authoring loop

1. Write `situation/<ns>/<local-name>.yamlld`. Keep the routing face lean;
   put node-local depth in `body: |`.
2. Plans declare `disposition.state` from birth. Set `active` only when
   execution should be discoverable in every agent's resident graph.
3. Run `bedrock check`: fix hard `RULE path:line` failures; use the
   projection report to trim SOFT resident faces.
4. Run `bedrock build`: regenerate root AGENTS.md, the resident TriG.
5. Commit source AND generated output; open a scoped PR; a human merges.

## Execution-graph primitives and lifecycle

A Plan source carries the seven execution primitives:

- `intent` — promise and resident read-trigger while active;
- `acceptanceCriteria` — inline oracle, required, cold;
- `consumes` — resident routing relationships while active;
- `tasks` — ordered execution payload, cold;
- `witnesses` — retained CI-run URLs, required before `done`, cold;
- `reflectDepth` — follow-up depth, cold;
- `disposition.state` — required from birth:
  draft|active|done|abandoned; `residual` declares what close did not assure.

Lifecycle: draft is source-only; active projects one compact routing face;
done/abandoned return entirely to cold source. Invocation names the Plan IRI
or source path; the agent sees intent and edges, then follows `document` for
the complete file. Reflection closes state, retains witnesses/findings, and
promotes only durable consequences into definition, architecture, current
risk, or a Decision. Historical Plans remain under situation/plan without
taxing future contexts.

No custody choreography, per-actor openings/closures, round ordinals, or
confirmation passes. This is a thin transaction log with measured ceremony.

## Decisions

A Decision is a named collapse of the possibility space at design level:
the record of a fork that was closed — what was chosen, what was rejected,
and the conditions under which the choice flips. Decisions live in record/.
They are write-once and immutable at birth (floor invariant 15: supersede,
never edit), and every Decision stays resident so the complete
`supersedes` chain remains walkable.

Write one when a fork you close would be plausibly re-opened by a reader
with zero context — at the moment of collapse, never reconstructed later.
Most choices need no vertex; when in doubt, do not write one.

Fields: `statement` carries the choice, the rejected alternatives, and the
revisit conditions in bounded prose — no template, no status field, and the
schema rejects `disposition` and `witnesses`: a decision is not CI-judged.
`timestamp` orders the log. `references` links the invariants and risks the
decision touches — an invariant pointing at a decision carries its reason,
not just its rule.

Reading: a decision with no incoming `supersedes` edge is live. Before
proposing to change a settled choice, walk the chain first — the
alternative you are about to propose may already be recorded, with its
reason and its flip conditions.

## Refusals

`bedrock` (init, adopt, update, check, build) never does any of the
following:

- no new top-level directories or namespaces under `situation/`;
- no hand-editing of AGENTS.md (the resident projection), base schemas,
  seed/context.yamlld, or this reference;
- no writing outside `situation/` and installed base files except root
  AGENTS.md, which build regenerates;
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

`bedrock build` validates the complete situation and regenerates AGENTS.md
from its resident working set. After work changes reality, reflect: close
episodic state, promote durable consequences, rebuild, and commit the
projection — current knowledge, not accumulated history.
