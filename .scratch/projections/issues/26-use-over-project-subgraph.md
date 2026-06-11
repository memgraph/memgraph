# USE over a project() subgraph

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

Bind a `project()` subgraph as a `GraphView` and `MATCH` over it inside a `USE`
scope, returning its real member nodes and respecting membership - through the
**same** seam as projections, not a parallel operator path.

## Acceptance criteria

- [ ] `CALL { USE subgraph MATCH (n) ... }` returns the subgraph's member nodes
- [ ] Expansion stays within the subgraph (membership respected)
- [ ] The subgraph view implements the same `GraphView` seam (no parallel operator path)
- [ ] e2e test

## Blocked by

- `21-scan-projection-nodes-in-use`
