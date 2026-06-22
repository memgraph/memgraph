# Harden ambient degree against an unexpected (view, element) pairing

Status: done

## Resolution

Both pairings are reachable through valid Cypher, so neither becomes an
assertion (a `DMG_ASSERT` would crash a valid query). The expand-path assertion
holds only for *scanned* nodes; `degree` also accepts nodes from expressions,
which is a wider input space.

- A virtual node with no `VirtualGraphView` bound is reachable as a literal
  `virtualNode()` outside any `USE` scope (`RETURN degree(virtualNode(...))`). A
  node built by `virtualNode()` carries no edges, so `{0, 0}` is correct. Kept,
  with a regression test.
- A real vertex under a bound `VirtualGraphView` is reachable by importing the
  vertex into a projection scope (`MATCH (r) ... CALL (r) { USE g RETURN
  degree(r) }`). It returned the vertex's real-graph degree, leaking real
  topology into a projection scope. A real vertex is not a node of a projection,
  so its ambient degree is now `{0, 0}`, consistent with the subgraph branch
  where a non-member contributes no member edges. This was a wrong-but-quiet
  result and is now fixed.

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

`AmbientInOutDegree` resolves a node's degree over the ambient graph view by
discriminating on the view type. Two branches answer silently when the pairing
of element and bound view is not the one the scan path is expected to produce:

- a projection node (`VirtualNode`) with no `VirtualGraphView` bound returns
  `{0, 0}`;
- a real vertex under a bound `VirtualGraphView` falls through to the real-graph
  degree, ignoring the projection.

Both are wrong-but-quiet if ever reached. The expand path already asserts the
analogous invariant ("a `VirtualNode` is scanned only with a projection bound").
Either prove these pairings unreachable and assert them the same way, so a
violation fails loudly instead of returning a plausible wrong number, or find
the query that reaches one and treat it as a correctness bug.

Confidence that this is reachable is low: the scan path is expected to pair a
`VirtualNode` only with a `VirtualGraphView`, and a real vertex only with a
subgraph or identity view. The deliverable is to settle that, not to assume it.

## Acceptance criteria

- [x] Both branches were shown reachable, so neither is asserted. The
      VirtualNode-without-projection branch is correct (`{0, 0}` for a literal
      virtual node) and kept; the real-vertex-under-projection branch returned a
      wrong real-graph degree and is fixed to `{0, 0}`. Covered by
      `DegreeOverLiteralVirtualNodeIsZero` and
      `DegreeOverRealVertexImportedIntoProjectionScopeIsZero`.
- [x] Existing `USE`-scope degree tests stay green.

## Blocked by

- None - can start immediately
