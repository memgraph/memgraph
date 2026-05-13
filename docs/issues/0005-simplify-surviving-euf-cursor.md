---
title: Simplify surviving EUF cursor — drop now-dead branches
type: AFK
status: done
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

## Audit result

After Stages 1-3 landed (commits `8bf265fb54`, `dac6f825ac`, `2af3f2f0fd`),
the EUF cursor was audited. **No dead branches were found.** A surviving
EUF still needs:

- The `previous_symbols_` seed-push, reachable when the bottommost EUF of
  a pattern survives (e.g. `MATCH (a)-[e1]->()-[e2]->(b) RETURN e2`
  surfaces an EUF for `e2` with `previous_symbols_=[e1]`).
- Both `SymbolKind::Edge` and `SymbolKind::EdgeList` dispatch, because
  either kind can be consumed downstream and therefore survive fusion.
- The `cand.type() == List` runtime fallback for the back-compat
  constructor used by unit tests with ANY-typed symbols.

The audit conclusion is encoded as a reachability comment at the top of
`EdgeUniquenessFilterCursor::Pull`.

## Acceptance criteria

- [ ] Audit performed; any unreachable branches removed with comment
      explaining why (or replaced by an assertion).
- [ ] Existing EUF unit + e2e tests pass unchanged.
- [ ] LOC count of `EUF::Cursor::Pull` reduced where dead branches are
      removed; no new tests required.

## Blocked by

- `0004-expand-variable-unique-pattern-id.md`
