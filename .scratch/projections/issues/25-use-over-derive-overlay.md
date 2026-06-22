# USE over a derive() overlay projection

Status: done - composes from issues 21 (USE scan) and 24 (element read seam) with no new operator code: a derive() projection is a VirtualGraph, so BindGraphView binds it and the unified read path reads overlay nodes through to their origins. Tests in `tests/unit/interpreter.cpp` and `tests/e2e/write_procedures/virtual_graph.py`.

## Parent

`.scratch/projections/PRD.md` (ADR 0004, ADR 0005)

## What to build

A `USE` scope over a `derive()` overlay projection: `MATCH`/read inside the scope
reads through to the origin per the binding - overlay keys shadow the origin, a
hidden key is invisible. Builds on the element read seam so the operator reads
overlay and real nodes through one uniform path.

## Acceptance criteria

- [x] `MATCH (n) ... RETURN n.prop` inside `USE` over a `derive()` projection returns origin values via read-through
- [x] An overlay-bound key shadows the origin inside the scope
- [x] A hidden key is invisible to reads and predicates inside the scope
- [x] e2e test over a `derive()` overlay projection

## Blocked by

- `21-scan-projection-nodes-in-use`
- `24-element-read-seam`
