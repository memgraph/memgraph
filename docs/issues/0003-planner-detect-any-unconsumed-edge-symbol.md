---
title: Generalize planner to detect any unconsumed edge symbol
type: AFK
status: done
---

## Parent

`IMPLEMENTATION_PLAN.md` — Stage 2.

## What to build

The Stage 1 planner rule only fires for *anonymous* edge symbols. Many
real-world queries name their edges but never reference them downstream
(e.g. `MATCH (a)-[e1:E]->()-[e2:E]->(b) RETURN a, b` — `e1`/`e2` are named but
never read). Generalize the rewrite to detect *any* edge symbol whose only
consumer in the entire plan tree is the EUF directly above its producing
Expand.

Reuse the existing symbol-usage machinery (`UsedSymbolsCollector`,
`OutputSymbols`) — walk the plan tree above the EUF and inspect every
operator's used-symbols + expression trees. If no `Identifier` anywhere
references the candidate symbol, it is eligible for fusion.

## Acceptance criteria

- [ ] Planner rewrite handles named-but-unconsumed edge symbols, both
      directions, multi-edge-type Expands, and multiple parallel scratch
      chains in a single MATCH.
- [ ] Detection reuses `UsedSymbolsCollector` / `OutputSymbols` rather than
      reinventing.
- [ ] No regression when the symbol *is* referenced in `RETURN`, `WHERE`,
      `WITH`, subquery, path constructor, `MERGE`, or any other expression
      context — existing e2e tests covering these shapes pass unchanged.
- [ ] Approval-style plan diffs reviewed; fusion only applies where safe.
- [ ] `PROFILE` on a pattern where the edge symbol *is* used (e.g.
      `RETURN e1`) confirms fusion does NOT apply.
- [ ] `PROFILE` from Stage 1 still shows the gains achieved in #0002 — no
      regression on the anonymous-edge benchmark.

## Blocked by

- `0002-expand-unique-pattern-id-narrow-fusion.md`
