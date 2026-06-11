# Projection scan surface (GraphView over a VirtualGraph)

Status: done - `src/query/virtual_graph_view.hpp`, `tests/unit/query_graph_view.cpp`

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

A projection-backed `GraphView` that scans a `VirtualGraph`: iterate all its
nodes, iterate a given node's out-edges and in-edges, and map names to/from ids.
This is the smallest step that lets an operator read a projection's topology,
independent of any query syntax. Today the projection accessor has no scan
surface.

Verified at the unit seam over a synthetic `VirtualGraph` - no query path is
required for this slice.

## Acceptance criteria

- [x] The projection view yields all of a `VirtualGraph`'s nodes - `VirtualGraphView::Nodes()`
- [x] It yields a given node's out-edges and in-edges, direction respected, self-loops in both
      - `OutEdges`/`InEdges` over the `VirtualGraph` in/out indexes
- [x] Name-to-id and id-to-name mapping works through the view - shares the real accessor's namespace
- [x] Unit tests over a synthetic `VirtualGraph` cover the above

## Shape decision

Per the chosen shape (human-in-the-loop): this slice is a **standalone**
`VirtualGraphView`, not yet a `GraphView` subclass, so `VertexRange` and
`ScanAll` are untouched. The name methods already match the `GraphView`
signatures. Issue 21 adopts this as the projection `GraphView`: it widens
`VertexRange` to the common scan element (`VertexAccessor` | `VirtualNode`),
makes `VirtualGraphView::Nodes()` the `Vertices()` override, and wires `ScanAll`.

## Blocked by

- `17-design-use-scope`
