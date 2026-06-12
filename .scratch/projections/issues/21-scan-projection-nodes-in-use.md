# Scan a projection's nodes in a USE scope (first end-to-end)

Status: done - `BindGraphView` operator + planner wiring; tests in `tests/unit/interpreter.cpp` and `tests/e2e/write_procedures/virtual_graph.py`

## Parent

`.scratch/projections/PRD.md` (ADR 0004, ADR 0005)

## What to build

Wire the pieces so `CALL { USE g MATCH (n) RETURN n }` runs over a synthetic
`virtualGraph`: the scope binds the projection `GraphView` as the ambient view,
`ScanAll` reads the projection's nodes through it, and the rows return over Bolt.
Read-only, all-nodes, no label filter, no expand - the thinnest end-to-end path
that proves the seam.

## Acceptance criteria

- [x] `WITH virtualGraph(...) AS g CALL { USE g MATCH (n) RETURN n.x } ...` returns the projection's nodes
- [x] Inside the scope the bound view is the projection; outside it, the real graph
- [x] Real-graph `MATCH` outside the scope is unchanged - asserted alongside the projection scan
- [x] e2e test asserting the returned rows - in-process interpreter tests (run) + Bolt e2e tests

## How it landed

Two commits: (1) widen `VertexRange` to the common scan element
(`VertexAccessor | VirtualNode`) and conform `VirtualGraphView` to `GraphView`,
so `ScanAll` runs over a projection through the seam; (2) the `BindGraphView`
operator, emitted by the rule-based planner at the head of a `USE` subquery's
plan, evaluates the bound graph and sets it as the ambient `GraphView` around the
body's pulls. The body is planned full-scan: the index-lookup, edge-index and
join rewriters treat it as an isolated branch.

Property reads over `VirtualNode` (`n.x`) and the `virtualGraph()` constructor
already existed, so no new evaluator or function work was needed.

## Blocked by

- `18-graphview-interface-identity-view`
- `19-projection-scan-surface`
- `20-parse-use-in-call-subquery`
