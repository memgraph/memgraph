# Topology functions over the ambient projection

Status: done - `degree`/`inDegree`/`outDegree` resolve over the ambient `GraphView` (`graph_view` moved to `EvaluationContext` and threaded into `FunctionContext`): a projection node counts `VirtualGraphView` edges, a subgraph member counts only member edges, a real vertex keeps real degree. Neighbour/relationship *traversal* is `MATCH` expansion, already routed through the view in issue 22 - there is no separate per-node built-in to wire (the `relationships()` built-in is a path function). Tests in `tests/unit/interpreter.cpp` and `tests/e2e/write_procedures/virtual_graph.py`.

## Parent

`.scratch/projections/PRD.md` (ADR 0005; absorbs the topology half of issue 15)

## What to build

Make the topology built-ins (`degree`, `indegree`, `outdegree`, and
neighbour/relationship-yielding functions) resolve over the bound `GraphView`
inside a `USE` scope, so `degree(n)` counts the projection's edges, not the real
graph's. This is the half of issue 15 that falls out of the seam; the
value-function half (`labels`/`properties`/`id` over a standalone virtual value)
stays in issue 15.

## Acceptance criteria

- [x] `degree`/`indegree`/`outdegree` over a projection node inside `USE` count projection edges
- [x] Neighbour/relationship functions resolve over the projection topology (traversal is `MATCH` expansion over the bound view, issue 22; no separate per-node built-in exists)
- [x] The result differs from the real-graph degree when the projection differs (asserted)
- [x] e2e test

## Blocked by

- `22-expand-relationships-in-use`
