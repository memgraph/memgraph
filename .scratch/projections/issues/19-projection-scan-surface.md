# Projection scan surface (GraphView over a VirtualGraph)

Status: ready-for-agent

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

- [ ] The projection `GraphView` yields all of a `VirtualGraph`'s nodes
- [ ] It yields a given node's out-edges and in-edges, direction respected, self-loops in both
- [ ] Name-to-id and id-to-name mapping works through the view
- [ ] Unit tests over a synthetic `VirtualGraph` cover the above

## Blocked by

- `17-design-use-scope`
