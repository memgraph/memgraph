# Expand relationships over a projection in a USE scope

Status: done - `Expand` dispatches on the frame node kind; a `VirtualNode` expands through the bound `VirtualGraphView`'s in/out edge index in `src/query/plan/operator.cpp`. Tests in `tests/unit/interpreter.cpp` and `tests/e2e/write_procedures/virtual_graph.py`.

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

`MATCH (a)-[r]->(b)` inside a `USE` scope resolves a projection node's edges
through the bound `GraphView`'s expand surface, with the endpoints binding back to
projection nodes. Covers directed and undirected patterns, an edge-type filter
`[:TYPE]`, and self-loops.

## Acceptance criteria

- [x] Directed expand returns the projection's edges; both endpoints resolve to projection nodes
- [x] Undirected expand and a type-filtered `[:T]` expand work
- [x] A self-loop appears in both directions
- [x] e2e test over a synthetic projection

## Blocked by

- `21-scan-projection-nodes-in-use`
