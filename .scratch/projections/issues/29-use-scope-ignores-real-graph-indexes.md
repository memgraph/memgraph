# Scan the bound projection, not a real-graph index, inside a USE scope

Status: done

## Resolution

The leak is already prevented at the rewriter boundary. `HandleSubquery` plans
the USE body into a raw logical plan (`ScanAll` + `Filter`) and wraps it in
`BindGraphView`; index substitution happens later, in a single post-process
rewrite over the whole tree. The index, edge-index, and join rewriters each stop
at `BindGraphView` (their `PreVisit` does not descend), so no `ScanAllByLabel`,
`ScanAllByLabelProperties` (value or range), `ScanAllById`, or edge-type index
scan is ever substituted into the body. The body stays a full scan over the
ambient `GraphView` plus a filter, for every scan flavour.

This was delivered for the label case alongside the rewriter boundary. The work
here is the regression coverage the criteria below ask for: label+property
equality, label+property range, and id, each against a real-graph index, plus
confirming the edge-type path is closed by the same boundary. No production
change was needed; the added tests fail if the boundary is removed.

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

Inside `CALL { USE g ... }` the body must read the bound ambient graph, but a
label, property, or id predicate is currently free to resolve against a
real-graph index instead. The subquery body is planned with the ordinary
rules and the `BindGraphView` boundary is wrapped around the finished plan
afterwards, so the planner never learns it is inside a `USE` scope. When a
matching index exists on the real graph the planner emits an index-backed scan
(`ScanAllByLabel`, `ScanAllByLabelProperty*`, `ScanAllById`) whose cursor reads
the real graph directly and ignores the bound view. The result is silently
wrong: the query returns real-graph vertices rather than the projection's.

A projection exposes no index, so inside a `USE` scope a filtered match is a
full scan of the bound view plus a filter. Make the planner plan the body with
that knowledge: when a `USE` scope is in effect, scans route through the ambient
`GraphView` (full scan) and predicates become filters, for every scan flavour
that would otherwise consult a real-graph index. Behaviour outside a `USE` scope
is unchanged.

This targets the existing (non-plan_v2) planner path where `USE` is supported.
plan_v2 rejects `USE` in a `CALL` subquery and is out of scope here.

## Acceptance criteria

- [x] A real-graph index exists on a label (and on a label+property), a
      projection is built whose members differ from the real graph, and
      `CALL { USE p MATCH (n:Label ...) ... }` returns the projection's members,
      not the real graph's. Covered by `CallUseScopeLabelFilterIgnoresRealIndex`
      and the new `CallUseScopeLabelProperty{Equality,Range}IgnoresRealIndex`.
- [x] Every scan that would otherwise consult a real-graph index inside a `USE`
      scope is covered: label, label+property (value and range), and id. The id
      case is exercised over a `project()` subgraph
      (`CallUseScopeIdFilterScansSubgraphMembers`): a real vertex outside the
      subgraph is not reachable by its id, a member is.
- [x] The edge-typed scan path is closed by the same boundary: the edge-index
      rewriter does not descend into a `BindGraphView` body, so an edge-type
      index scan is never substituted. Type-filtered expansion over a projection
      is covered by `CallUseScopeExpandsTypeFiltered`.
- [x] Existing `USE`-scope scan and expand tests stay green.
- [x] Planning and execution outside a `USE` scope are unchanged: the boundary
      only stops descent into the USE body, so index scans are still selected as
      before everywhere else.

## Blocked by

- None - can start immediately
