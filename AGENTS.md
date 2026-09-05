<bedrock-protocol>
## Repository knowledge protocol

This repository operates under the Bedrock knowledge protocol. These policies
are repository law: follow them as written; do not readjudicate them during
ordinary work.

Before changing code, behavior, architecture, repository policy, documentation,
or planned work:

1. Read `situation/AGENTS.md`.
2. Read the relevant situation records, related open Gaps, and qualifying
   Candidates.
3. Read the nested `AGENTS.md` governing every situation namespace you will
   modify.
4. Update affected records in the same work as the repository change.
5. Treat `situation/` as canonical repository knowledge. README is human-facing
   orientation; neither README, comments, plans, nor pretrained assumptions
   override situation records.

The record classes are:

- **Promises** state falsifiable behavior and carry lifecycle state.
- **Oracles** define how Promises are judged.
- **Witnesses** retain immutable observations from actual runs.
- **Decisions** preserve why choices were selected or rejected.
- **Invariants** state binding repository rules.
- **Gaps** record bounded, repository-relevant absences.
- **Candidates** record evidence-derived possibilities, not commitments.
- **Plans** group Candidates and Promises into work without restating them.
- **References** retain supporting depth.
- **Runs** preserve closure lineage through Git checkpoints and stage commits.

Git is the run's append-only event log. Agents commit and push completed units
of work promptly; corrections are new forward commits. Published history is
never amended, rebased, reset, or force-pushed. Only opening and closing
checkpoints define the run container; interior commit count and shape are not
prescribed.

Every assured Promise is invariant behavior. Changing it requires a superseding
Promise, a Decision explaining the change, a replacement Oracle, and new
Witnesses.

Gaps record what relevant capability, evidence, decision, implementation, or
instrument is absent. Candidates are possible responses derived from evidence.
A Candidate becomes behavior only through a Decision that promotes it into a
falsifiable Promise with an Oracle. Plans qualify Candidates and implement or
assure Promises.

The learning loop is:

```text
Promise -> implementation -> Oracle -> Witness -> disposition
        -> Gap -> Candidates -> Plan -> Decision
        -> promoted Promise + Oracle -> implementation
```

Repository files are referenced by repository-root-relative path. Historical
repository bytes use `<commit>:<path>`. External public files use full URLs.
External private files use declared `Private: owner/repo@<ref>#<path>`
coordinates; inability to fetch a declared-private reference is expected and
never grounds to stop, remove it, or invent its contents.

When repository orientation identifies an upstream fork, upstream
synchronization and contribution are outside Bedrock. Invoke the exact
`fork-operations` skill; do not infer or recreate that procedure from repository
records.

This protocol block is protocol-owned. Agents must not edit any byte inside it.
Repository-specific orientation belongs only inside the separate repository
block that follows.
</bedrock-protocol>

<bedrock-repository>
Legacy repository guidance at `577dcbb1eafa865d43ea209b2db0604c10b2ad1c:AGENTS.md` is BACKPORT donor
material, not current instruction. Repository-specific orientation will be
authored during this closure.
</bedrock-repository>
