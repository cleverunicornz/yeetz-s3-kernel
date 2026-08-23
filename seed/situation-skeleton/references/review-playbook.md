# Review playbook

An adversarial, human-invoked deep review of shipped or shipping behavior.
Depth document referenced by a Breadcrumb vertex. It spends real reviewer
budget to attack the target; it is not part of the normal workflow.

## Invocation gate

Run only when a human explicitly invokes it, for one of:

- Expansive review: a delivery-proof audit of a change bank.
- Hard debug: a difficult defect that normal review has not cracked.
- Hard code review: where trusting a self-review would be too cheap, or an
  explicit "prove me wrong" request.

Never auto-trigger it. A passing test, a merge, or a scoped code review does
not start an adversarial review; a normal review suffices for ordinary
change. This review attacks shipped behavior.

## Stages

0. Framing and guarantees. Pin the target (commit, diff, or directory) and
   the human question. Turn them into 3–20 falsifiable guarantees, each
   with an observable counterexample and a completion rule. State the
   inventory whose completeness makes the review meaningful, and the
   exclusions.

1. Recon fan-out. Map the connected surface, then dispatch bounded deep-read
   reviewers over disjoint surfaces or lenses. Each reads whole functions,
   not diff hunks, and follows callers, callees, registrations, and
   persistence. File a candidate only when a specific input, state, or race
   yields a specific forbidden outcome. The fan-out is the discovery engine.

2. Triage: dedupe + rank. Merge candidates only when they assert the same
   mechanism and proof path; preserve lineage. Rank by contract impact ×
   likelihood × proof value — priority, not severity. Never silently drop
   work: mark budget-cut.

3. Independent re-verification. A disjoint reviewer or rig re-checks the
   top tier against source. Tag every finding with an evidence tier:
   VERIFIED (line-cited direct read) / partial (part of the claim
   established) / reviewer-reported (not independently re-checked). Assign
   recheck-first to the reviewer-reported class; never rest anything
   load-bearing on unverified reporting. If a claim is cheap to disprove,
   build the smallest faithful rig (controls plus decisive case), keep it
   replayable, and have an independent actor run it against the untouched
   target. Independent source re-verification is a first-class mode.

4. Root-cause consolidation. Group reproduced symptoms into defect families
   by a single root mechanism and fix boundary; trace blast radius as
   provably-affected vs suspected; hunt variants. A read may clear a
   variant, never confirm it.

5. Report + human gate. Compile guarantee verdicts (falsified, qualified,
   unresolved), reproduced defects with proof lineage, coverage vs
   inventory, and open or budget-cut risk. Never invent evidence, never
   upgrade an unvalidated result, never self-promote — the human decides.

Optional — only when a delivery claim rests on tests: a test-integrity pass
designing the smallest break that should make each test fail, and one
bounded coverage wave; fold both into the report audit and name over-budget
work as residual risk.

## Evidence rules

- Cite path:line for every finding.
- Keep replayable rigs: exact commands, run count, untouched target.
- Every reproduced finding carries an independent-validation lineage.
- Keep reviewer-reported separate from independently verified, in the
  artifact's own legend.
