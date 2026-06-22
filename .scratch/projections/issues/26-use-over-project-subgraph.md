# USE over a project() subgraph

Status: done - `SubgraphGraphView` (`src/query/subgraph_graph_view.hpp`) implements the same `GraphView` seam: a scan yields the subgraph's member vertices, and expansion drops non-member edges. `BindGraphView` binds a `project()` Graph value, and `Expand` filters real edges to membership when a subgraph is bound - reusing `ScanAll`/`Expand`, no parallel operator path. Tests in `tests/unit/interpreter.cpp` and `tests/e2e/write_procedures/virtual_graph.py`.

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

Bind a `project()` subgraph as a `GraphView` and `MATCH` over it inside a `USE`
scope, returning its real member nodes and respecting membership - through the
**same** seam as projections, not a parallel operator path.

## Acceptance criteria

- [x] `CALL { USE subgraph MATCH (n) ... }` returns the subgraph's member nodes
- [x] Expansion stays within the subgraph (membership respected)
- [x] The subgraph view implements the same `GraphView` seam (no parallel operator path)
- [x] e2e test

## Blocked by

- `21-scan-projection-nodes-in-use`
