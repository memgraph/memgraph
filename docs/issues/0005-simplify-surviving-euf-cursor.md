---
title: Simplify surviving EUF cursor — drop now-dead branches
type: AFK
status: needs-triage
---

## Parent

`IMPLEMENTATION_PLAN.md` — Stage 4 (cleanup).

## What to build

After #0002–#0004, the surviving `EdgeUniquenessFilter` runs only when the
edge symbol is *consumed* downstream (and so cannot be fused into the Expand
below). The set of plan shapes EUF must handle is now strictly smaller than
before. Audit `EdgeUniquenessFilter::EdgeUniquenessFilterCursor::Pull`
(`src/query/plan/operator.cpp:5967`) and surrounding code for branches that
are no longer reachable, e.g.:

- `previous_symbols_` seed-push (line 6013): may apply on fewer plan shapes
  now.
- `candidate_kind_` static-vs-runtime dispatch (lines 6019–6020): the runtime
  fallback may be unreachable.

Pure simplification — no behavioural change.

## Acceptance criteria

- [ ] Audit performed; any unreachable branches removed with comment
      explaining why (or replaced by an assertion).
- [ ] Existing EUF unit + e2e tests pass unchanged.
- [ ] LOC count of `EUF::Cursor::Pull` reduced where dead branches are
      removed; no new tests required.

## Blocked by

- `0004-expand-variable-unique-pattern-id.md`
