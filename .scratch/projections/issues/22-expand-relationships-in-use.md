# Expand relationships over a projection in a USE scope

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

`MATCH (a)-[r]->(b)` inside a `USE` scope resolves a projection node's edges
through the bound `GraphView`'s expand surface, with the endpoints binding back to
projection nodes. Covers directed and undirected patterns, an edge-type filter
`[:TYPE]`, and self-loops.

## Acceptance criteria

- [ ] Directed expand returns the projection's edges; both endpoints resolve to projection nodes
- [ ] Undirected expand and a type-filtered `[:T]` expand work
- [ ] A self-loop appears in both directions
- [ ] e2e test over a synthetic projection

## Blocked by

- `21-scan-projection-nodes-in-use`
