# Retire hand-rolled node-kind forks through GraphView

Status: done - the Tier 2-3 collapsible forks were routed through the seam across issues 21-28: `ScanAll`/`Expand` read through the ambient `GraphView`, `PropertyLookup`/subscript read real and projected nodes and edges through one shared `GetProperty` helper (`VirtualEdge` gained the `(View) -> Result` read signature here, completing the node work from issue 24), and `degree`/`inDegree`/`outDegree` resolve over the bound view (issue 27). The value-function arms over standalone virtual values legitimately remain (issue 15 surface). Inventory updated with a "Status after the GraphView/USE arc" section.

## Parent

`.scratch/projections/PRD.md` (ADR 0005; issue 16 cleanup)

## What to build

Now the `GraphView` seam covers the read path, remove the hand-rolled node-kind
forks the inventory flagged as Tier 2-3 - the `IsVertex() || IsVirtualNode()`
special-casing in the read path and the topology functions - by routing them
through the seam. Behaviour is unchanged; the branch-point inventory shrinks.

See `.scratch/projections/issue-16-branch-inventory.md` for the fork list.

## Acceptance criteria

- [x] The identified Tier 2-3 forks are routed through `GraphView` or removed
- [x] The full test suite is green (behaviour unchanged)
- [x] The branch-point inventory is updated to reflect what was removed

## Blocked by

- `26-use-over-project-subgraph`
