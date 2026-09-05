# Decisions

A decision records why a choice collapsed. It preserves rationale so the
same path is not relitigated without new evidence. Decisions are append-only:
supersede, never edit.

## File naming

```
D-<six digits>-<kebab-case-name>.md
```

## Required headings

- `Status` — `accepted`, `superseded`, or `reversed`
- `Date`
- `Context` — the situation that forced a choice
- `Evidence` — links to witnesses and oracles that informed the choice
- `Decision` — what was chosen, stated directly
- `Why` — the reason the choice collapsed this way
- `Rejected alternatives` — what was not chosen and why
- `Consequences` — what follows from the decision
- `Revisit when` — the condition under which this decision may be reopened

Superseded decisions additionally link `Supersedes` and `Superseded by`.

## Relationship to invariants

A decision states the why. An invariant states the resulting rule. When a
decision produces a binding rule, write the invariant and link this decision
as its Basis. Decisions do not appear in the root `AGENTS.md` directly;
invariants do.

## Relationship to promises

When a promise is refuted or superseded, a decision records why. The promise
links the decision in its state evidence.

During BACKPORT, a donor's selected provider, rejected alternative, or stated
revisit condition is a collapsed choice and requires a Decision even when the
corresponding Promise remains `hypothesis` for lack of feasibility evidence.

A Candidate promotion/rejection Decision links the Candidate and every Promise
and Oracle created by promotion. Promotion is incomplete unless all records and
state changes land atomically.

## Rules

- One decision per file, immutable after acceptance.
- A decision introduced on an unmerged pull request is not yet accepted
  repository history and may be corrected in place before merge.
- Supersession replaces a decision with a new record; both remain.
- A decision without evidence is a preference, not a decision; say which it
  is.

## Reference discipline

Files inside this repository are referenced by repository-root-relative
path. Files in external public repositories are referenced only by full
public URL. Files in external private repositories are referenced by
declared coordinate — `Private: owner/repo@<ref>#<path>` — never by an
unauthenticated URL, never undeclared. Never reference a local clone, a
private checkout, or a machine-local path.

A declared-private reference that cannot be fetched is expected, not an
error: never stop for one, never remove it, never invent its contents.
Content from a private reference never crosses into a public document.
