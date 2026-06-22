# Scan the bound projection, not a real-graph index, inside a USE scope

Status: not started

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

- [ ] A failing test is written first: a real-graph index exists on a label
      (and on a label+property), a projection is built whose members differ from
      the real graph, and `CALL { USE p MATCH (n:Label ...) ... }` returns the
      projection's members, not the real graph's.
- [ ] The fix covers every scan that would otherwise consult a real-graph index
      inside a `USE` scope: label, label+property (value and range), and id.
- [ ] Equivalent coverage for an edge-typed scan if an expand inside a `USE`
      scope could pick an edge index; otherwise note in the test that no such
      path exists.
- [ ] Existing `USE`-scope scan and expand tests stay green.
- [ ] Planning and execution outside a `USE` scope are unchanged (index scans
      still selected as before).

## Blocked by

- None - can start immediately
