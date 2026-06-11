# Design USE-scope: AST/plan mechanism + GraphView interface shape

Status: ready-for-human

## Parent

`.scratch/projections/PRD.md` (projections-query arc; ADR 0004, ADR 0005)

## What to build

A **design**, not an implementation. Fix the concrete shapes the rest of the arc
builds on:

- How `CALL { USE <expr> ... }` is represented in the AST and the logical plan -
  e.g. a scope marker on the subquery, or a binding operator that sets the
  ambient graph for the block.
- How execution binds the ambient `GraphView` for the scope, and where the read
  operators (`ScanAll`, `Expand`) and topology functions read the bound view
  from (today they read a concrete `DbAccessor *`).
- How read-only is enforced inside a scope (which clauses error, at what stage) -
  per ADR 0004 v1: no `CREATE/SET/DELETE/MERGE`, single level of nesting.
- That planning inside a scope is full-scan: projections have no label/property
  index or statistics, so the planner must not assume them.
- The concrete `GraphView` interface signature (range-returning scan / expand /
  name-mapping, per ADR 0005), enough to unblock the seam and projection-scan
  slices.

Output is a design note (or an ADR addendum) plus the agreed AST/operator and
interface shapes.

## Acceptance criteria

- [ ] AST/plan representation of `CALL { USE <expr> ... }` decided and written down
- [ ] Mechanism for binding the ambient `GraphView` to the scope, and where operators read it, decided
- [ ] Read-only enforcement approach decided (which clauses error, and at which stage)
- [ ] Full-scan planning inside a scope confirmed (no projection index/stat assumptions)
- [ ] `GraphView` interface signature fixed (range-returning), enough to unblock the seam and scan-surface slices

## Blocked by

None - can start immediately.
