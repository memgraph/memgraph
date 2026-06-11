# Projection from lists of virtual elements + graph validation

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md`

## What to build

Assemble a projection from a list of virtual nodes and a list of virtual edges
in one query, so external table data can be imported as a graph. Edge
endpoints are validated against the node list; a dangling edge (endpoint absent
from the node list) is either an error or silently dropped, per a config option.

## Acceptance criteria

- [ ] A projection can be constructed from `[virtual nodes]` and `[virtual edges]` in a single query
- [ ] The resulting projection is consumable by an algorithm procedure
- [ ] Default validation: a dangling edge raises a clear construction-time error
- [ ] `drop` mode: a dangling edge is silently omitted, construction succeeds
- [ ] e2e test covering import, the `error` default, and the `drop` mode

## Blocked by

- `03-virtual-node-constructor`
- `04-virtual-edge-constructor`
