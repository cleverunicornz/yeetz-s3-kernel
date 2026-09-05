# Runs

A run is a Git event interval bounded by one opening checkpoint and one closing
checkpoint. Git commits are the event log; no parallel result database exists.

## Structure

Each run owns one append-only directory:

```
runs/bedrock-<timestamp>-<head prefix>-<suffix>/
```

Standard contents:

- `opening.md` — opening checkpoint metadata
- `closure-report.md` — closer summary
- `validation-report.md` — validator output
- `correction-report.md` — present when correction ran
- `completion.md` — closing checkpoint metadata

## Opening checkpoint

Subject:

```
bedrock: open closure <run-id>
```

It records run ID and trigger head. The orchestrator pushes and verifies it
before delegating work.

Checkpoint metadata is one contiguous final Git trailer block with no blank
lines between trailer lines, so `git interpret-trailers --parse` can read it.

## Interior events

Everything after opening and before closing is append-only agent work. There may
be one commit or hundreds; records, root AGENTS.md, README, validation, and
corrections may each change repeatedly. No deterministic process counts, orders,
names, or interprets middle commits.

Each agent commits and pushes each completed file immediately. Multiple files
share a commit only when the protocol explicitly requires one atomic state
transition, such as Candidate promotion or Gap closure. Shared topic,
convenience, or completing a phase is not an atomicity reason.
Corrections are new forward commits. Never amend, rebase, reset, or force-push
published work.

## Closing checkpoint

Subject:

```
bedrock: complete closure <run-id>
```

It records the run ID and opening checkpoint SHA. The orchestrator creates it
only after delegated agents have clean working trees and all completed work is
present on the remote PR branch.

Closing metadata uses the same contiguous Git trailer rule as opening metadata.

## Recovery

An opening checkpoint without a closing checkpoint is incomplete and resumable.
The orchestrator inspects Git history and reinvokes the responsible agent for
any dirty or unpushed work. It never publishes another agent's work. Existing
remote commits are retained and work proceeds forward.

## Hard boundary

The only hard run contract is:

- opening checkpoint exists;
- closing checkpoint exists with the same run ID;
- closing descends from opening;
- closing is the remote PR head;
- terminal state identifies the closing head.

Nothing about the interior commit count or shape is part of the hard contract.
