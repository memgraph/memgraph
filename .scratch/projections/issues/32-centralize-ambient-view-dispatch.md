# Centralize ambient-view dispatch instead of per-call-site dynamic_cast

Status: not started

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

The behaviour that depends on the ambient `GraphView`'s concrete kind is
hand-rolled as a `dynamic_cast` cascade at every call site that reads the bound
view: degree resolution (`AmbientInOutDegree`), edge expansion
(`Expand::ExpandCursor::InitEdges` / `InitVirtualEdges` and the virtual-edge
arms), scan visibility (`ScannedVertexVisible`), and the `CALL` ambient-view
routing. Each site probes for `VirtualGraphView` / `SubgraphGraphView` / the
identity view and falls through to a real-graph default.

The fall-through default is the dangerous part: a new `GraphView` subclass, or a
new (element, view) pairing, that is not added to one of these cascades silently
takes the real-graph path and leaks real topology or real visibility into a
projection scope. That is exactly the wrong-but-quiet result these sites were
patched to prevent, and the patches are not kept in sync by anything but
inspection.

Move the per-kind decisions behind the `GraphView` seam (or a single dispatcher)
so each view kind answers degree, expansion membership, and scan visibility for
itself, and adding a view kind is one place, not four. A missing case should be
a compile-time gap, not a silent real-graph fall-through.

## Acceptance criteria

- [ ] The view-kind discrimination for degree, expansion, and scan visibility is
      expressed once per view kind rather than as parallel `dynamic_cast`
      cascades at each call site.
- [ ] Adding a new `GraphView` kind that omits one of these decisions fails to
      build (or is otherwise caught) rather than silently reading the real graph.
- [ ] Existing `USE`-scope scan, expand, and degree behaviour is unchanged;
      the current tests stay green.

## Blocked by

- None
