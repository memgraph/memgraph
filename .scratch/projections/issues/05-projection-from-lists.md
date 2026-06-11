# Projection from lists of virtual elements + graph validation

Status: done

## Parent

`.scratch/projections/PRD.md`

## What to build

Assemble a projection from a list of virtual nodes and a list of virtual edges
in one query, so external table data can be imported as a graph. Edge
endpoints are validated against the node list; a dangling edge (endpoint absent
from the node list) is either an error or silently dropped, per a config option.

## Acceptance criteria

- [x] A projection can be constructed from `[virtual nodes]` and `[virtual edges]` in a single query
- [x] The resulting projection is consumable by an algorithm procedure
- [x] Default validation: a dangling edge raises a clear construction-time error
- [x] `drop` mode: a dangling edge is silently omitted, construction succeeds
- [x] e2e test covering import, the `error` default, and the `drop` mode

## Blocked by

- `03-virtual-node-constructor`
- `04-virtual-edge-constructor`

## Comments

### Delivered

A grilling session settled the surface (ADR 0003): list-import is a **scalar
constructor**, not an aggregation. The existing `project(nodes, edges)` is an
aggregation over real elements producing a real `Graph`; an aggregation fixes
its result type before input is seen and the AST carries only two expression
slots, so it cannot host a virtual-list target plus a validation config. A
scalar function sidesteps both and is the natural sibling of `virtualNode`/
`virtualEdge`.

As built:

- `virtualGraph(nodes, edges, config?)` returns a `VirtualGraph`, consumable by a
  procedure (and exposing `.nodes` / `.edges` in Cypher) exactly as `derive()`.
- `AssembleVirtualGraph` (in `virtual_graph.cpp`, unit-tested) holds the binding
  logic: insert nodes, index by import handle, bind each edge endpoint by handle
  or by synthetic-gid membership, dedup and insert.
- A **dangling edge** (endpoint matching no listed node) errors by default or is
  dropped under `{onDanglingEdge: 'drop'}`. A **duplicate import handle** among
  the nodes is always an error. Nulls in either list are skipped; a real vertex
  or edge is rejected.

Out of scope here: edges with their own properties survive assembly (the rebind
keeps the edge's property store), but there is no list-import-specific validation
of edge properties. Cross-row merging is not offered - the upstream `collect` is
where rows are combined.
