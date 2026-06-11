# virtualEdge() constructor

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md`

## What to build

A `virtualEdge(type, from, to)` query function that constructs a synthetic edge,
accepting either gids or virtual nodes as endpoints. Returns the existing
`VirtualEdge` TypedValue. Settle the semantics of referencing endpoints by gid
versus by virtual node.

## Acceptance criteria

- [ ] `RETURN virtualEdge("TYPE", 1, 2)` constructs an edge between gids 1 and 2
- [ ] `RETURN virtualEdge("TYPE", node_1, node_2)` constructs an edge between two virtual nodes
- [ ] The edge carries its type and a synthetic GID
- [ ] The edge serializes over Bolt and renders in Lab
- [ ] e2e test asserting both endpoint forms

## Blocked by

- `03-virtual-node-constructor`
