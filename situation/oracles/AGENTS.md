# Oracles

An oracle defines the judgment rule for a promise. It states what inputs are
examined and what constitutes pass or fail. It does not execute itself; an
oracle may be designed before its executable implementation exists.

## File naming

```
O-<six digits>-<kebab-case-name>.md
```

## Required headings

- `State` — `designed` or `implemented`
- `Judges` — link to the promise
- `Inputs` — what evidence the oracle examines
- `Pass` — the condition that passes
- `Fail` — the condition that fails
- `Implementation` — present only when state is `implemented`
- `Implementation coverage` — one row per Pass and Fail leg, naming the
  executable decision or marking the leg `manual`

## States

`designed` — the judgment rule is stated but no executable check exists.

`implemented` — a test, workflow, or checker command exists and runs. The
Implementation section links it. `implemented` does not mean every leg is
automated; the coverage table states exactly which legs are executable and
which remain manual.

## Implementation coverage

Every independently decidable Pass and Fail leg appears once:

```markdown
| Leg | Decision | Coverage |
|---|---|---|
| P1 | Every locked digest matches | `scripts/protocol-sync.sh` |
| P2 | Release tag resolves to locked commit | manual |
```

An executable may not be credited with a leg it does not decide. A manual
leg is valid, but witnesses must carry direct evidence for it.

## Rules

- Pass and fail conditions must be decidable from the stated inputs.
- The Oracle collectively decides every explicit clause of the Promise inside
  its declared Scope. It must not broaden the Promise, judge behavior outside
  Scope, or attempt to disprove infinite negative space. Outside-Scope behavior
  is neither a failed leg nor a validation finding.
- Scope bounds the positive behavioral claim even when the Promise is phrased
  as a prohibition. Residual names only relevant unassured boundaries; it does
  not enumerate everything the software could theoretically do.
- `implemented` requires at least one executable leg. A wholly manual
  oracle remains `designed`.
- One oracle may judge one promise. When a judgment rule serves multiple
  promises, write one oracle per promise and link them in References.
- The oracle does not record results; witnesses do that.
- Changing a pass or fail condition after witnesses exist requires a new
  oracle superseding the old one.

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
