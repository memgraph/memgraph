# Design USE-scope: AST/plan mechanism + GraphView interface shape

Status: done - `.scratch/projections/issue-17-use-scope-design.md`

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

- [x] AST/plan representation of `CALL { USE <expr> ... }` decided and written down
      - `use_graph_` on `CallSubquery`; a `BindGraphView` operator at the head of the subquery plan
- [x] Mechanism for binding the ambient `GraphView` to the scope, and where operators read it, decided
      - `ExecutionContext::graph_view`, rebound by `BindGraphView`, read by `ScanAll`/`Expand`; `db_accessor` kept as interim bridge
- [x] Read-only enforcement approach decided (which clauses error, and at which stage)
      - SymbolGenerator `in_use_scope` scope flag; write clauses and nested `USE` error at symbol generation
- [x] Full-scan planning inside a scope confirmed (no projection index/stat assumptions)
      - `ScanAll` + `Filter` only inside a scope
- [x] `GraphView` interface signature fixed (range-returning), enough to unblock the seam and scan-surface slices
      - range-returning `Vertices()` + name mapping, type-erased `VertexRange` over a common scan element

## Blocked by

None - can start immediately.
