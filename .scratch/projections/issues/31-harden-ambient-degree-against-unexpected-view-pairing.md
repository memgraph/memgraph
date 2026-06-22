# Harden ambient degree against an unexpected (view, element) pairing

Status: not started

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

- [ ] Each silent branch is shown to be unreachable and converted to an
      assertion matching the expand path, or a query that reaches it is found and
      the wrong-degree result is fixed.
- [ ] If assertions are added, existing `USE`-scope degree tests stay green.

## Blocked by

- None - can start immediately
