# Retire hand-rolled node-kind forks through GraphView

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0005; issue 16 cleanup)

## What to build

Now the `GraphView` seam covers the read path, remove the hand-rolled node-kind
forks the inventory flagged as Tier 2-3 - the `IsVertex() || IsVirtualNode()`
special-casing in the read path and the topology functions - by routing them
through the seam. Behaviour is unchanged; the branch-point inventory shrinks.

See `.scratch/projections/issue-16-branch-inventory.md` for the fork list.

## Acceptance criteria

- [ ] The identified Tier 2-3 forks are routed through `GraphView` or removed
- [ ] The full test suite is green (behaviour unchanged)
- [ ] The branch-point inventory is updated to reflect what was removed

## Blocked by

- `26-use-over-project-subgraph`
