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

## Comments

Endpoint-by-gid semantics were settled in `03`: the `gid`/`from_gid`/`to_gid`
values are logical **handles**, not node identities (a synthetic node's identity
is its own synthetic gid). `virtualNode()` already accepts a handle but does not
store it. This slice should:

- Persist the handle on `VirtualNode` (a field, defaulting to none so
  `derive()`-built nodes are unaffected), set by `virtualNode()`.
- Resolve `virtualEdge("T", from, to)` endpoints given either a handle or a
  virtual node; standalone, an edge may hold unresolved handle endpoints that a
  projection assembly (`05`) binds to nodes by handle.
