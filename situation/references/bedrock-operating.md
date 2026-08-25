# Installed by bedrock v0.7.0 — DO NOT EDIT. This file is owned by the tool. Base changes: file an issue or PR at https://github.com/cleverunicornz/bedrock. Local refresh: bedrock update.
# Bedrock operating reference — resident protocol and Mount Contract v1

This is the machine-owned minimum installed by init, adopt, and update. C10
compares it with the current binary's canonical stamped copy. Local law layers
on top and never edits it.

## Version and identity

Init/adopt query crates.io before writing. Current or newer proceeds; stale or
unverifiable refuses. `--offline` deliberately bypasses lookup and stamps the
local version. Check/build/update/migrate never use the network.

Canonical coordinates:

- `urn:bedrock:context/v1`;
- `urn:bedrock:ontology/<Term>`;
- `urn:bedrock:vertex/<slug>`;
- `urn:bedrock:path/<repo-relative>`;
- `urn:bedrock:<predicate>`;
- `urn:bedrock:graph/<namespace>`.

New authoring emits URNs only. Reads remain compatible with former
`https://yeetz.dev/bedrock/...` source and `https://yeetz.dev/graph/...` named
graphs. `bedrock update` never rewrites authored vertices;
`bedrock migrate-iris` is the explicit source migration and never enters
registered mounts.

## THE CHAIN

A promise states behavior; an oracle judges it against actual; a witness is a
retained completed CI observation and proves existence, never exclusivity;
residual declares what was deliberately not assured. A Plan or ReflectVerdict
cannot be `done` without at least one HTTPS witness (C8).

## Six base namespaces plus registered opaque mounts

| base namespace | canonical graph | allowed base types |
|---|---|---|
| definition | `urn:bedrock:graph/definition` | Invariant, Breadcrumb, Term |
| architecture | `urn:bedrock:graph/architecture` | Identity, SituationStructure, ExpansionMount |
| risk | `urn:bedrock:graph/risk` | Risk |
| plan | `urn:bedrock:graph/plan` | Plan |
| record | `urn:bedrock:graph/record` | EpochRecord, DeployRecord, ReflectVerdict, Decision |
| references | none | nested cold depth |

The base set has twelve types. Repo/expansion archetypes may ride alongside one
base type and never stand alone. A mount is not a seventh namespace and owns no
Bedrock graph. Unregistered direct children of `situation/` fail C1.

## One artifact and resident working set

`situation/` is the complete validated store. Root AGENTS.md is the one
generated artifact and the resident TriG graph injected into every agent.
There is no Bedrock `situation/graph.trig`.

- definition, architecture, current risks: resident faces;
- ExpansionMount registrations and generated pointer linkage: resident;
- active Plan: compact routing face resident; payload cold;
- draft/done/abandoned Plan: cold;
- Decision: resident, complete `supersedes` chain walkable;
- Epoch/Deploy/Reflect, references, every body: cold.

A vertex is one YAML-LD file. Its face is structured resident/routing data;
optional final `body: |` is unbounded node-local depth, stripped before
expansion and read through the automatic `document` edge. Editing cold-only
content leaves AGENTS.md byte-identical. C11 requires resident vertex edges to
close over resident vertices or paths.

The projection report is advisory: exact AGENTS bytes, source/resident counts,
Plan lifecycle, record residency, and SOFT resident faces over 4096 chars.

## ExpansionMount registration v1

One consumer-authored registration lives at
`situation/architecture/mount-<slug>.yamlld`:

```yaml
"@context": "urn:bedrock:context/v1"
"@id": "urn:bedrock:vertex/mount-example-expansion"
"@type":
  - "urn:example:ontology/ExampleMount"
  - "urn:bedrock:ontology/ExpansionMount"
label: "example-expansion"
mount_contract_version: 1
mount_name: example-expansion
mount_path: "urn:bedrock:path/situation/example-expansion"
checker_identity: "urn:example:checker/v1"
checker_arguments:
  - check
init_path: "urn:bedrock:path/situation/example-expansion/example-init.yaml"
init_sha256: "<64 lowercase hex digits>"
graph_manifest_path: "urn:bedrock:path/situation/example-expansion/graph-manifest.yaml"
graph_manifest_sha256: "<64 lowercase hex digits>"
```

Checker identity/arguments are non-executable data. Roots are unique real
non-symlink direct children of `situation/`. Unsupported versions fail with an
explicit registration-migration instruction; Bedrock never rewrites them.

The mount owns one stable manifest:

```yaml
artifacts:
  - path: situation/example-expansion/runs/run-1/graph.trig
    sha256: "<64 lowercase hex digits>"
```

Entries are strictly sorted normalized repo-relative paths. `artifacts: []` is
the valid empty adoption.

## C12 opaque boundary

Bedrock never compiles or base-schema-validates mount contents. It performs
only:

1. registration/root/version/unique-path checks;
2. duplicate/overlap and nested-AGENTS rejection;
3. structured LD inspection only for Bedrock context/base-type claims;
4. registered RDF parsing only to reject Bedrock-owned subject, predicate,
   object, or graph IRIs, canonical or legacy;
5. containment, regular-file, and SHA-256 verification for init/pin, manifest,
   and every manifest-listed graph.

Interior symlinks are never followed. Other mount content is opaque. Supported
registered RDF syntax: `.trig`, `.nq`, `.ttl`, `.nt`; unknown syntax refuses.
Expansion tooling writes only inside its mount.

## Pointer linkage in AGENTS.md

Bedrock's one artifact carries these resident architecture quads:

```text
registration --references------> manifest path
mount path   --produces---------> manifest path
manifest path --artifact-digest-> exact manifest SHA-256
```

AGENTS.md also has a deterministic comment-preamble section named
`Mounted expansions`, one line per registration with name, path, checker.
Zero expansion graph quads enter AGENTS.md. Mount-owned per-run graph files
remain under the mount and are only validated through C12.

This is Mount Contract v1 adapted to the post-0.4 one-artifact substrate:
“build emits” means linkage lands in AGENTS.md's resident TriG body. No separate
Bedrock graph artifact is reintroduced.

## ReflectVerdict mounted subject

A ReflectVerdict subject may be an existing base vertex or an existing path
canonically contained by a registered mount. The verdict remains episodic and
cold, but its complete source validates. Campaign close carries criteria,
completed two-checker CI URL, findings/residual, and exact relevant digest.
Promotion uses existing source/path/consumes edges and never rewrites mount
history.

## Decisions and Plans remain unchanged

Decision is resident, timestamped, write-once, and append-only through
`supersedes`; it has no disposition or witnesses. Read the chain before
reopening a fork.

Every Plan declares draft|active|done|abandoned. Only active routing fields
project: type, label, intent, consumes/requires/references/produces/member-of/
oracle/source/path, synthesized active state, document edge. Criteria, tasks,
witnesses, reflection depth, residual, statement, and body remain cold.

## Bedrock lock and workflow migration

`seed/substrate-lock.json` is C10-owned and pins exact checker package/ref plus
supported Mount Contract versions. The seed workflow retains the resident
projection dry-run report and AGENTS-only drift gate, but installs Bedrock only
from this lock under runner temp, outside the target checkout.

Update never changes existing workflow bytes. Consumers migrate once:

- unmounted repositories replace always-latest source install with lock
  resolution;
- mounted repositories replace the standalone job with the expansion-owned
  combined witness job;
- fixed order: Bedrock check, expansion check, expansion build, expansion
  graph/manifest no-diff, AGENTS.md no-diff;
- Linux/platform-neutral work uses `org-ci-linux-x64`; fork code is rejected
  before runner assignment.

Expansion checker pins remain independent. Neither derives the other.

## Update and authoring lanes

Update owns only schemas, context, substrate lock, this reference, and a
missing workflow; then it regenerates AGENTS.md. It never mutates authored
vertices, registrations, mounts, extension schemas, or present workflows.

Authoring loop:

1. write base source; keep resident faces lean and depth in body;
2. expansion check/build when mounted;
3. `bedrock check`, then `bedrock build`;
4. verify expansion artifacts/manifest and AGENTS.md unchanged;
5. commit source plus AGENTS.md; open a PR; a human merges.

## Refusals

- no hand editing AGENTS.md;
- no separate Bedrock graph artifact;
- no seventh base namespace or unregistered mount;
- no nested AGENTS.md;
- no automatic IRI or registration migration;
- no execution of registration checker data;
- no expansion-specific concepts or expansion graph quads;
- no workflow rewrite during update.

Base defects route to https://github.com/cleverunicornz/bedrock.
