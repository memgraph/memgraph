# Topology functions over the ambient projection

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0005; absorbs the topology half of issue 15)

## What to build

Make the topology built-ins (`degree`, `indegree`, `outdegree`, and
neighbour/relationship-yielding functions) resolve over the bound `GraphView`
inside a `USE` scope, so `degree(n)` counts the projection's edges, not the real
graph's. This is the half of issue 15 that falls out of the seam; the
value-function half (`labels`/`properties`/`id` over a standalone virtual value)
stays in issue 15.

## Acceptance criteria

- [ ] `degree`/`indegree`/`outdegree` over a projection node inside `USE` count projection edges
- [ ] Neighbour/relationship functions resolve over the projection topology
- [ ] The result differs from the real-graph degree when the projection differs (asserted)
- [ ] e2e test

## Blocked by

- `22-expand-relationships-in-use`
