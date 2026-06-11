# USE over a derive() overlay projection

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0004, ADR 0005)

## What to build

A `USE` scope over a `derive()` overlay projection: `MATCH`/read inside the scope
reads through to the origin per the binding - overlay keys shadow the origin, a
hidden key is invisible. Builds on the element read seam so the operator reads
overlay and real nodes through one uniform path.

## Acceptance criteria

- [ ] `MATCH (n) ... RETURN n.prop` inside `USE` over a `derive()` projection returns origin values via read-through
- [ ] An overlay-bound key shadows the origin inside the scope
- [ ] A hidden key is invisible to reads and predicates inside the scope
- [ ] e2e test over a `derive()` overlay projection

## Blocked by

- `21-scan-projection-nodes-in-use`
- `24-element-read-seam`
